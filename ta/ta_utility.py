import numpy as np
import client_config
import pandas as pd
from . import ta_masks
import utility
import logging
from helper.aws import debug_to_s3

logger = logging.getLogger()


def drop_rows(df, system_config):
    """
    Drops rows based on the 'drop_rows' configuration.
    Supports "Blank" (NaN/NaT/Empty), single strings, or lists of strings.
    """
    # 1. Safety check: does the config even have drop rules?
    drop_rules = system_config.get("drop_rows", {})
    if not drop_rules:
        return df

    initial_row_count = len(df)

    # 2. Build a boolean mask (Start with all False = 'Keep All')
    # We use a mask so we only slice the DataFrame once at the end.
    combined_mask = pd.Series([False] * len(df), index=df.index)

    for col, value in drop_rules.items():
        if col not in df.columns:
            print(f"⚠️ Warning: Column '{col}' defined in drop_rows not found in data.")
            continue

        # Case A: Handle "Blank" (NaN, NaT, or whitespace strings)
        if value == "Blank":
            # .isna() handles both numeric NaN and datetime NaT
            combined_mask |= df[col].isna() | (df[col].astype(str).str.strip() == "")

        # Case B: Handle a list of values (e.g., ["Sick Pay", "Vacation"])
        elif isinstance(value, list):
            combined_mask |= df[col].isin(value)

        # Case C: Handle a single exact match (e.g., "Sick Pay")
        else:
            combined_mask |= df[col] == value

    # 3. Apply the mask (Keep rows where the mask is NOT True)
    df_cleaned = df[~combined_mask].copy()

    # 4. Logging for your backend visibility
    dropped_count = initial_row_count - len(df_cleaned)
    if dropped_count > 0:
        print(
            f"✂️ System Config: Dropped {dropped_count} rows based on {list(drop_rules.keys())}"
        )

    return df_cleaned


def normalize_client_data(df, system_config):
    """
    Normalizes client data based on system-specific config:
    1. Force column types (optional)
    2. Apply mappings (simple renames or transformations)
    3. Drop system-specific unwanted columns
    """

    # --- 1. Apply mappings (if provided) ---
    mappings = system_config.get("mappings", {})

    for target_col, rule in mappings.items():
        if isinstance(rule, str):
            # Simple rename
            if rule in df.columns:
                df = df.rename(columns={rule: target_col})

        elif isinstance(rule, dict):
            transform_type = rule.get("transform")

            if transform_type == "concat":
                source_cols = rule.get("source_columns", [])
                delimiter = rule.get("delimiter", "")

                for col in source_cols:
                    if col not in df.columns:
                        df[col] = ""

                df[target_col] = df[source_cols].fillna("").agg(delimiter.join, axis=1)

            # Additional transform types can be added here later:
            # elif transform_type == "pad_left": ...
            # elif transform_type == "upper": ...
            # etc.

    # --- 2. Drop columns specified in config ---
    drop_cols = system_config.get("drop_columns", [])
    if drop_cols:
        df = df.drop(columns=drop_cols, errors="ignore")
        logger.info(
            f"Dropped {len(drop_cols)} columns based on system config: {drop_cols}"
        )
    return df


def add_time_helper_cols(df):
    # Columns to shift within ID
    shift_config = {
        "Prev In Punch": ("In Punch", 1),
        "Prev Out Punch": ("Out Punch", 1),
        "Next In Punch": ("In Punch", -1),
        "Next Out Punch": ("Out Punch", -1),
        "Prev Date": ("Date", 1),
        "Next Date": ("Date", -1),
        "Prev Punch Length (hrs)": ("Punch Length (hrs) Raw", 1),
        "Next Punch Length (hrs)": ("Punch Length (hrs) Raw", -1),
    }

    # Apply all shifts in one loop
    for new_col, (source_col, shift_periods) in shift_config.items():
        df[new_col] = df.groupby("ID")[source_col].shift(shift_periods)

    df["Prev ID"] = df["ID"].shift(1)
    df["Next ID"] = df["ID"].shift(-1)

    return df


def add_break_time(df):
    df["Break Time (min)"] = (
        df["In Punch"] - df["Prev Out Punch"]
    ).dt.total_seconds() / 60
    return df


def add_next_break_time(df):
    df["Next Break Time (min)"] = (
        df["Next In Punch"] - df["Out Punch"]
    ).dt.total_seconds() / 60
    return df


# TODO
def add_hours_worked_shift_and_shift_id(df):
    # New shift starts if gap from prev punch >= 60 minutes or first punch for employee (boolean)
    df["New Shift?"] = (df["Break Time (min)"] >= 60) | df["Break Time (min)"].isna()

    # Create shift id per employee (1, 2, 3, ...)
    df["Shift Number"] = df.groupby("ID")["New Shift?"].cumsum()

    # Compute shift length (sum of hours per shift)
    df["Hours Worked Shift"] = (
        df.groupby(["ID", "Shift Number"])["Punch Length (hrs) Raw"].transform("sum")
    ).round(4)
    return df


def add_twelve_hour_check(df):
    ##################
    # The credit will be due when in a unique shift:
    # Condition 1: 1) Hours Worked Shift is equal or longer than 12 hours
    #              2) There are less than two punches with Break Time (min) > 0
    # OR
    # Condition 2: 1) Hours Worked Shift is equal or longer than 12 hours
    #              2) There are two punches or more with Break Time (min) > 0,
    #                 however the second break started after 10 hours
    ##################

    # Shift start time
    df["Shift Start"] = df.groupby(["ID", "Shift Number"])["In Punch"].transform("min")

    # Identify first punch of shift
    df["First Punch of Shift?"] = df.groupby(["ID", "Shift Number"]).cumcount().eq(0)

    # Identify break-causing punches
    df["Is Break?"] = (df["Break Time (min)"] > 0) & (~df["First Punch of Shift?"])

    # Count breaks per shift
    df["Break Count"] = df.groupby(["ID", "Shift Number"])["Is Break?"].transform("sum")

    # Rank ONLY break-causing punches by Out Punch
    df["Break Order"] = (
        df.loc[df["Is Break?"]]
        .groupby(["ID", "Shift Number"])["Out Punch"]
        .rank(method="first")
    )

    # Filter to get only the rows where conditions are met
    second_break = df[(df["Break Count"] == 2) & (df["Break Order"] == 1)][
        ["ID", "Shift Number", "Out Punch"]
    ].rename(columns={"Out Punch": "2nd Break Start"})

    # Merge back to the original dataframe to create the column
    df = df.merge(second_break, on=["ID", "Shift Number"], how="left")

    # Hours from shift start to 2nd break
    df["Hours to 2nd Break"] = (
        (df["2nd Break Start"] - df["Shift Start"]).dt.total_seconds().div(3600)
    )

    # Condition 1: ≥ 12 hours and fewer than 2 breaks
    cond1 = (df["Hours Worked Shift"] >= 12) & (df["Break Count"] < 2)

    # Condition 2: ≥ 12 hours, ≥ 2 breaks, second break after 10 hours
    cond2 = (
        (df["Hours Worked Shift"] >= 12)
        & (df["Break Count"] >= 2)
        & (df["Hours to 2nd Break"] > 10)
    )

    df["12hr Credit Due"] = cond1 | cond2

    return df


def add_split_shift(df, processed_wfn_df, min_wage):
    # TODO Prev Punch Length is coming from Raw. Verify implications.
    # Create df with Straight Rate ($) Lookup. Note that Regular Rate on the WFN is a misnomer,
    # it's actually Straight Rate ($)
    df["Regular Rate Paid"] = df["ID"].map(
        processed_wfn_df.set_index("IDX")["Regular Rate Paid"]
    )
    # Auxiliary: Current plus previous totaled hours x straight rate paid
    df["Split Paid ($)"] = df["Regular Rate Paid"] * (
        df["Punch Length (hrs) Raw"] + df["Prev Punch Length (hrs)"]
    )
    # Auxiliary: Current and previous totaled hours x min wage
    df["Split at Min Wage ($)"] = min_wage * (
        1 + df["Punch Length (hrs) Raw"] + df["Prev Punch Length (hrs)"]
    )
    # Split Shift Due ($) (applicable if Master boolean above)
    df["Split Shift Due ($)"] = df["Split at Min Wage ($)"] - df["Split Paid ($)"]
    return df


# def add_col_from_another_df(
#     home_df, lookup_df, home_ref, lookup_ref, lookup_tgt, home_new_col
# ):
#     home_df[home_new_col] = home_df[home_ref].map(
#         lookup_df.set_index(lookup_ref)[lookup_tgt]
#     )
#     return home_df

import logging

logger = logging.getLogger(__name__)


def add_col_from_another_df(
    home_df, lookup_df, home_ref, lookup_ref, lookup_tgt, home_new_col
):
    # Create the mapping series
    mapper = lookup_df.set_index(lookup_ref)[lookup_tgt]

    # Check for duplicates in the index we just created
    if not mapper.index.is_unique:
        duplicated_ids = mapper.index[mapper.index.duplicated()].unique().tolist()

        logger.error(
            f"Non-unique index found in lookup_df! "
            f"The following {lookup_ref} values are duplicated: {duplicated_ids}"
        )

        # Optional: If you want the code to keep running despite the duplicates,
        # you can drop them here:
        # mapper = mapper[~mapper.index.duplicated(keep='first')]

    home_df[home_new_col] = home_df[home_ref].map(mapper)

    return home_df


def add_waiver_check(df, processed_waiver_df):
    add_col_from_another_df(
        home_new_col="Waiver on File?",
        home_df=df,
        home_ref="ID",
        lookup_df=processed_waiver_df,
        lookup_ref="ID",
        lookup_tgt="Has_Waiver_Bool",
    )
    # Fill the NaN values with False
    df["Waiver on File?"] = df["Waiver on File?"].fillna(False)
    return df


def add_ot_and_dt_cols(
    df, locations_config, ot_day_max, ot_week_max, dt_day_max, first_date, last_date
):
    # Add "Workday Hours" column. It totals for each Date/ID pair.
    df["Workday Hours"] = df.groupby(["Date", "ID"])[
        "Punch Length (hrs) Raw"
    ].transform("sum")

    # Add "Add Week Hours" column. Creates a helper label 0, 1, 2. It should only be 0 and 1, but if punches from a prior week is carried over it can be 2 or more. We will use this to exclude carryover hours from OT calculation later on this function.
    df["Work Week"] = ((df["Date"] - first_date).dt.days // 7) + 1
    # print("First date", first_date)
    df["Week Hours"] = df.groupby(["ID", "Work Week"])[
        "Punch Length (hrs) Raw"
    ].transform("sum")

    # Add "Total hours on the work period" columns
    df["Total Hours Pay Period"] = df.groupby(["ID"])[
        "Punch Length (hrs) Raw"
    ].transform("sum")

    ## OVERRIDE COLUMNS ## process time 0.02 seconds

    # Is there a location based day overtime trigger? Else take global "ot_day_max"
    df["OT Day Max"] = utility.apply_override_else_global(
        df, "Location", "ot_day_max", ot_day_max, locations_config
    )

    # Is there a location based week overtime trigger? Else take global "ot_week_max"
    df["OT Week Max"] = utility.apply_override_else_global(
        df, "Location", "ot_week_max", ot_week_max, locations_config
    )
    # Is there a location based day doubletime trigger? Else take global "dt_day_max"
    df["DT Day Max"] = utility.apply_override_else_global(
        df, "Location", "dt_day_max", dt_day_max, locations_config
    )

    #######################

    # Overtime per workday (exclude DT hours)
    df["Workday OT Hours"] = np.maximum(
        np.minimum(df["Workday Hours"], df["DT Day Max"]) - df["OT Day Max"],
        0,
    )

    # Add individual Workday OT hours per week
    df["Sum of Workday OT Hours"] = df.set_index(["Work Week", "ID"]).index.map(
        df.drop_duplicates(subset=["Work Week", "ID", "Date"])
        .groupby(["Work Week", "ID"])["Workday OT Hours"]
        .sum()
    )

    # Double time per workday
    df["Workday DT Hours"] = np.maximum(df["Workday Hours"] - df["DT Day Max"], 0)

    # Add individual Workday DT hours per week
    df["Sum of Workday DT Hours"] = df.set_index(["Work Week", "ID"]).index.map(
        df.drop_duplicates(subset=["Work Week", "ID", "Date"])
        .groupby(["Work Week", "ID"])["Workday DT Hours"]
        .sum()
    )

    # Gross and Net Week Overtime (double dipping check)
    df["Week OT Hours Gross"] = np.maximum(0, df["Week Hours"] - df["OT Week Max"])
    df["Week OT Hours Net"] = np.maximum(
        0,
        (
            df["Week OT Hours Gross"]
            - df["Sum of Workday OT Hours"]
            - df["Sum of Workday DT Hours"]
        ),
    ).round(6)

    # Calculate Total OT and Total DT Hours for week
    df["Total OT Hours Week"] = df["Sum of Workday OT Hours"] + df["Week OT Hours Net"]
    df["Total DT Hours Week"] = df["Sum of Workday DT Hours"]

    # Adjust "Total OT Hours Week" to zero for any row where "Shift Start" does not fall within the pay period. This is to prevent counting OT hours from shifts that started before the pay period, even if they ended within it.
    start_bound = pd.to_datetime(first_date).normalize()
    end_bound = pd.to_datetime(last_date).normalize()
    is_in_pay_period = df["Shift Start"].dt.normalize().between(start_bound, end_bound)
    df["Adjusted Total OT Hours Week"] = np.where(
        is_in_pay_period, df["Total OT Hours Week"], 0.0
    )
    df["Adjusted Total DT Hours Week"] = np.where(
        is_in_pay_period, df["Total DT Hours Week"], 0.0
    )
    # Calculate Total OT and Total DT Hours for the pay period

    # Step 1: Get unique totals per (ID, Work Week)
    unique_totalsOT = df.drop_duplicates(subset=["ID", "Work Week"])[
        ["ID", "Adjusted Total OT Hours Week"]
    ]
    unique_totalsDT = df.drop_duplicates(subset=["ID", "Work Week"])[
        ["ID", "Adjusted Total DT Hours Week"]
    ]

    # Step 2: Sum per ID
    totalsOT = unique_totalsOT.groupby("ID")["Adjusted Total OT Hours Week"].sum()
    totalsDT = unique_totalsDT.groupby("ID")["Adjusted Total DT Hours Week"].sum()

    # Broadcast to every row belonging to that "ID"
    df["Total OT Hours Pay Period"] = df["ID"].map(totalsOT).round(4)
    df["Total DT Hours Pay Period"] = df["ID"].map(totalsDT).round(4)

    ###DEBUG 1#####
    debug_id = "2JV0005917"
    debug_cols = [
        "ID",
        "Employee",
        "Date",
        "In Punch",
        "Out Punch",
        "Punch Length (hrs) Raw",
        "Punch Length (hrs)",
        "Punch Number in Shift",
        "Shift Number",
        "Hours Worked Shift",
        "Workday Hours",
        "Work Week",
        "Week Hours",
        "Total Hours Pay Period",
        "OT Day Max",
        "OT Week Max",
        "Workday OT Hours",
        "Sum of Workday OT Hours",
        "Week OT Hours Gross",
        "Week OT Hours Net",
        "Total OT Hours Week",
        "Total DT Hours Week",
        "Adjusted Total OT Hours Week",
        "Adjusted Total DT Hours Week",
        "Total OT Hours Pay Period",
        "Total DT Hours Pay Period",
    ]
    debug_to_s3(df, debug_id, debug_cols, "pp-debug-bucket")
    #############################################

    return df


def create_anomalies_new(df):
    # Step 1: Define anomaly columns as either 1 or 0
    df["Short Break"] = (ta_masks.break_less_than_30(df)).astype(int)
    df["Did Not Break"] = (ta_masks.did_not_break_new_all(df)).astype(int)
    df["Over Twelve"] = (ta_masks.over_twelve(df)).astype(int)

    # # --- DEBUGGING BLOCK ---
    # target_emp = "GUH0008109"

    # # Adjust 'EmployeeID' to match your actual column name
    # debug_df = df[(df["ID"] == target_emp) & (df["Did Not Break"] == 1)]

    # if not debug_df.empty:
    #     # .to_string() ensures Pandas formats the output as a clean text table
    #     # rather than truncating it arbitrarily.
    #     logger.info(
    #         f"DEBUG: 'Did Not Break' triggered for {target_emp}. Constituent records:\n{debug_df.to_string()}"
    #     )
    # else:
    #     logger.info(
    #         f"DEBUG: No 'Did Not Break' records found for {target_emp} in this batch."
    #     )
    # # -----------------------

    # Step 2: Aggregate anomalies by Employee and ID
    anomalies_df = df.groupby(["ID"], as_index=False).agg(
        {
            "Employee": "first",  # keeps this column
            "Paid Break Credit (hrs)": "first",  # keeps this column
            "Short Break": "sum",  # consolidate
            "Did Not Break": "sum",  # consolidate
            "Over Twelve": "sum",  # consolidate
        }
    )

    # Step 3: Keep only rows with anomalies or paid break credits
    anomalies_df = anomalies_df[
        (anomalies_df["Short Break"] != 0)
        | (anomalies_df["Did Not Break"] != 0)
        | (anomalies_df["Over Twelve"] != 0)
        | (anomalies_df["Paid Break Credit (hrs)"] != 0)
    ]

    # Step 4 Create sum and variance dfs
    anomalies_df["Due Break Credit (hrs)"] = (
        anomalies_df["Short Break"]
        + anomalies_df["Did Not Break"]
        + anomalies_df["Over Twelve"]
    )
    anomalies_df["Variance"] = (
        anomalies_df["Due Break Credit (hrs)"] - anomalies_df["Paid Break Credit (hrs)"]
    )

    return anomalies_df


def add_punch_length(df):
    # This function calculates a "Punch Length (hrs)" column that accounts for stapled punches. It uses the "Punch Length (hrs) Raw" column, which is the unstapled punch length, and then aggregates it based on whether punches are considered part of the same continuous working block (i.e., stapled together) or not.
    # A true new punch starts if there is at lease some break time (Break Time (min) > 0) or if it's the first punch of the shift.
    df["Is New Punch?"] = (df.groupby(["ID", "Shift Number"]).cumcount() == 0) | (
        df["Break Time (min)"] > 0
    )
    # By taking a cumulative sum of the boolean values "Is New Punch?" (True = 1, False = 0), this line assigns the exact same "Punch Number" to consecutive rows that belong to the same continuous working block.
    df["Punch Number in Shift"] = df.groupby(["ID", "Shift Number"])[
        "Is New Punch?"
    ].cumsum()
    # Aggregate into the Punch Length DataFrame
    df["Punch Length (hrs)"] = (
        df.groupby(["ID", "Shift Number", "Punch Number in Shift"])[
            "Punch Length (hrs) Raw"
        ].transform("sum")
    ).round(4)

    return df


def merge_source_into_target_auto(source_df, target_df, key_col="ID"):
    """
    Merge source_df into target_df:
    - Updates existing rows based on key_col.
    - Adds new rows from source_df if key_col not in target_df.
    - Brings over all new columns from source_df.
    - Fills all numeric columns with 0 for missing values.
    - Fills missing values from source for overlapping columns (like Employee).
    """
    # Identify columns in source not in target
    new_cols = [col for col in source_df.columns if col not in target_df.columns]

    # Outer merge
    merged = pd.merge(
        target_df, source_df[[key_col] + new_cols], on=key_col, how="outer"
    )

    # Fill missing values for overlapping columns (like Employee)
    common_cols = [
        col for col in source_df.columns if col in target_df.columns and col != key_col
    ]
    for col in common_cols:
        id_map = source_df.set_index(key_col)[col]
        merged[col] = merged.apply(
            lambda row: (
                id_map[row[key_col]]
                if pd.isna(row[col]) and row[key_col] in id_map
                else row[col]
            ),
            axis=1,
        )

    # Detect all numeric columns and fill NaN with 0
    numeric_cols = merged.select_dtypes(include="number").columns
    merged[numeric_cols] = merged[numeric_cols].fillna(0)

    # Reorder columns: target columns first, then new columns
    final_cols = target_df.columns.tolist() + [
        col for col in new_cols if col not in target_df.columns
    ]
    merged = merged[final_cols]

    return merged


def add_seventh_day_hours(df, locations_config, number_of_consec_days_before_ot):
    # Is there a location based day overtime trigger? Else take global "ot_day_max"
    df["Number of Consec Days Before OT"] = utility.apply_override_else_global(
        df,
        "Location",
        "number_of_consec_days_before_ot",
        number_of_consec_days_before_ot,
        locations_config,
    )

    # Ensure data is sorted by ID and Date
    df = df.sort_values(["ID", "Date"]).copy()

    def compute_group(g):
        # Each ID group has a consistent threshold
        consec_days = int(g["Number of Consec Days Before OT"].iloc[0]) + 1
        # Difference in days
        diff = g["Date"].diff().dt.days.fillna(0)
        # Identify consecutive sequences
        consec_group = (diff != 1).cumsum()
        # Compute streak length within each sequence
        streak = g.groupby(consec_group).cumcount() + 1
        # Rolling sum based on that employee's rule
        rolling_sum = (
            g["Punch Length (hrs) Raw"]
            .rolling(consec_days, min_periods=consec_days)
            .sum()
        )
        # Keep only sums where streak >= thresold
        return rolling_sum.where(streak >= consec_days).fillna(0)

    # Apply per ID group
    df["Hours in Consecutive Days"] = (
        df.groupby("ID", group_keys=False).apply(compute_group)
    ).round(4)
    df["First day of Streak"] = df["Date"] - pd.Timedelta(days=6)

    return df
