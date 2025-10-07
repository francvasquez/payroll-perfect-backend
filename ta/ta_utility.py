import numpy as np
import config
import pandas as pd
from . import ta_masks
import utility


def add_time_helper_cols(df):
    # Columns to shift within ID
    shift_config = {
        "Prev In Punch": ("In Punch", 1),
        "Prev Out Punch": ("Out Punch", 1),
        "Next In Punch": ("In Punch", -1),
        "Next Out Punch": ("Out Punch", -1),
        "Prev Date": ("Date", 1),
        "Next Date": ("Date", -1),
        "Prev Punch Length (hrs)": ("Totaled Amount", 1),
        "Next Punch Length (hrs)": ("Totaled Amount", -1),
    }

    # Apply all shifts in one loop
    for new_col, (source_col, shift_periods) in shift_config.items():
        df[new_col] = df.groupby("ID")[source_col].shift(shift_periods)

    df["Prev ID"] = df["ID"].shift(1)
    df["Next ID"] = df["ID"].shift(-1)

    return df


def sort_and_staple(df):
    # Sort by ID and In Punch
    df = df.sort_values(["ID", "In Punch"]).reset_index(drop=True)
    df = df.reset_index()  # column 'index' is original row index

    # Initialize Flag column if it doesn't exist
    if "Flag" not in df.columns:
        df["Flag"] = pd.NA

    # Merge df with itself to find Out Punch -> In Punch matches for same ID
    merged = df.merge(
        df,
        left_on=["ID", "Out Punch"],
        right_on=["ID", "In Punch"],
        suffixes=("", "_next"),
    )

    if not merged.empty:
        keep_idx = merged["index"].values
        drop_idx = merged["index_next"].values

        # Extend Out Punch
        df.loc[keep_idx, "Out Punch"] = merged["Out Punch_next"].values

        # Only sum the "Totaled Amount"
        if "Totaled Amount" in df.columns:
            df.loc[keep_idx, "Totaled Amount"] = (
                df.loc[keep_idx, "Totaled Amount"].fillna(0).values
                + merged["Totaled Amount_next"].fillna(0).values
            )

        # Mark surviving stapled rows
        df.loc[keep_idx, "Flag"] = "Stapled"

        # Drop the merged-away rows
        df = df.drop(index=drop_idx)

    # Restore original structure
    df = df.drop(columns="index").reset_index(drop=True)

    # Final sort for readability
    df = df.sort_values(["Employee", "In Punch"]).reset_index(drop=True)

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


def add_shift_length(df):
    # Positive calculation of shift length (e.g. only when have all the data, else na)
    # eleven_pm = datetime.time(23, 0)

    df["Shift Length (hrs)"] = np.where(
        df["Next Break Time (min)"] < 60,  # Case a - next punch is part of shift
        df["Totaled Amount"] + df["Next Punch Length (hrs)"],
        np.where(
            df["Next Break Time (min)"]
            >= 60,  # Case b - nexr punch is not part of shift
            df["Totaled Amount"],
            np.where(
                df["Next Break Time (min)"].isna()
                # & (df["Out Punch"].dt.time < eleven_pm),  # Case c - no info on next shift - by removing this we assume this shift has no subsequent punches
                ,
                df["Totaled Amount"],
                np.nan,  # default: NA
            ),
        ),
    )

    return df


def add_split_shift(df, processed_wfn_df, min_wage):
    # Create df with Straight Rate ($) Lookup. Note that Regular Rate on the WFN is a misnomer,
    # it's actually Straight Rate ($)
    df["Regular Rate Paid"] = df["ID"].map(
        processed_wfn_df.set_index("IDX")["Regular Rate Paid"]
    )
    # Auxiliary: Current plus previous totaled hours x straight rate paid
    df["Split Paid ($)"] = df["Regular Rate Paid"] * (
        df["Totaled Amount"] + df["Prev Punch Length (hrs)"]
    )
    # Auxiliary: Current and previous totaled hours x min wage
    df["Split at Min Wage ($)"] = min_wage * (
        1 + df["Totaled Amount"] + df["Prev Punch Length (hrs)"]
    )
    # Split Shift Due ($) (applicable if Master boolean above)
    df["Split Shift Due ($)"] = df["Split at Min Wage ($)"] - df["Split Paid ($)"]
    return df


def add_col_from_another_df(
    home_df, lookup_df, home_ref, lookup_ref, lookup_tgt, home_new_col
):
    home_df[home_new_col] = home_df[home_ref].map(
        lookup_df.set_index(lookup_ref)[lookup_tgt]
    )
    return home_df


def add_waiver_check(df, processed_waiver_df):
    df["Location"] = df["ID"].str[:3]
    df["Waiver Lookup"] = df["Location"] + " " + df["Employee"]

    add_col_from_another_df(
        home_new_col="Waiver on File?",
        home_df=df,
        home_ref="Waiver Lookup",
        lookup_df=processed_waiver_df,
        lookup_ref="Name",
        lookup_tgt="Check_Pure",
    )

    return df


def create_bypunch(df, locations_config, ot_day_max, ot_week_max, dt_day_max):
    # Calculate total sum for each date and ID combination
    bypunch_df = df[
        [
            "Employee",
            "ID",
            "Location",
            "Date",
            "Totaled Amount",
            "In Punch",
            "Out Punch",
        ]
    ].copy()

    # Add "Workday Hours" column. It totals for each Date/ID pair.
    # Repeats for all rows that share the same "Date" and "ID" combination
    bypunch_df["Workday Hours"] = bypunch_df.groupby(["Date", "ID"])[
        "Totaled Amount"
    ].transform("sum")

    # Add "Add Week Hours" column. Creates a helper label 1 or 2.
    start_date = bypunch_df["Date"].min()  # This is a Pandas timestamp
    days_diff = (bypunch_df["Date"] - start_date).dt.days
    bypunch_df["Work Week"] = (days_diff // 7) + 1
    bypunch_df["Week Hours"] = bypunch_df.groupby(["ID", "Work Week"])[
        "Totaled Amount"
    ].transform("sum")

    # Add "Total hours on the work period" columns
    bypunch_df["Total Hours Pay Period"] = bypunch_df.groupby(["ID"])[
        "Totaled Amount"
    ].transform("sum")

    ## OVERRIDE COLUMNS ## process time 0.02 seconds

    # Is there a location based day overtime trigger? Else take global "ot_day_max"
    bypunch_df["OT Day Max"] = utility.apply_override_else_global(
        bypunch_df, "Location", "ot_day_max", ot_day_max, locations_config
    )
    # Is there a location based week overtime trigger? Else take global "ot_week_max"
    bypunch_df["OT Week Max"] = utility.apply_override_else_global(
        bypunch_df, "Location", "ot_week_max", ot_week_max, locations_config
    )
    # Is there a location based day doubletime trigger? Else take global "dt_day_max"
    bypunch_df["DT Day Max"] = utility.apply_override_else_global(
        bypunch_df, "Location", "dt_day_max", dt_day_max, locations_config
    )

    #######################

    # Overtime per workday (exclude DT hours)
    bypunch_df["Workday OT Hours"] = np.maximum(
        np.minimum(bypunch_df["Workday Hours"], bypunch_df["DT Day Max"])
        - bypunch_df["OT Day Max"],
        0,
    )

    # Add individual Workday OT hours per week
    bypunch_df["Sum of Workday OT Hours"] = bypunch_df.set_index(
        ["Work Week", "ID"]
    ).index.map(
        bypunch_df.drop_duplicates(subset=["Work Week", "ID", "Date"])
        .groupby(["Work Week", "ID"])["Workday OT Hours"]
        .sum()
    )

    # Double time per workday
    bypunch_df["Workday DT Hours"] = np.maximum(
        bypunch_df["Workday Hours"] - bypunch_df["DT Day Max"], 0
    )

    # Add individual Workday DT hours per week
    bypunch_df["Sum of Workday DT Hours"] = bypunch_df.set_index(
        ["Work Week", "ID"]
    ).index.map(
        bypunch_df.drop_duplicates(subset=["Work Week", "ID", "Date"])
        .groupby(["Work Week", "ID"])["Workday DT Hours"]
        .sum()
    )

    # Gross and Net Week Overtime (double dipping check)
    bypunch_df["Week OT Hours Gross"] = np.maximum(
        0, bypunch_df["Week Hours"] - bypunch_df["OT Week Max"]
    )
    bypunch_df["Week OT Hours Net"] = np.maximum(
        0,
        (
            bypunch_df["Week OT Hours Gross"]
            - bypunch_df["Sum of Workday OT Hours"]
            - bypunch_df["Sum of Workday DT Hours"]
        ),
    ).round(6)

    # Calculate Total OT and Total DT Hours for week
    bypunch_df["Total OT Hours Week"] = (
        bypunch_df["Sum of Workday OT Hours"] + bypunch_df["Week OT Hours Net"]
    )
    bypunch_df["Total DT Hours Week"] = bypunch_df["Sum of Workday DT Hours"]

    # Calculate Total OT and Total DT Hours for the pay period

    # Step 1: Get unique totals per (ID, Work Week)
    unique_totalsOT = bypunch_df.drop_duplicates(subset=["ID", "Work Week"])[
        ["ID", "Total OT Hours Week"]
    ]
    unique_totalsDT = bypunch_df.drop_duplicates(subset=["ID", "Work Week"])[
        ["ID", "Total DT Hours Week"]
    ]

    # Step 2: Sum per ID
    totalsOT = unique_totalsOT.groupby("ID")["Total OT Hours Week"].sum()
    totalsDT = unique_totalsDT.groupby("ID")["Total DT Hours Week"].sum()

    # Put the above total on every row belonging to that "ID"
    bypunch_df["Total OT Hours Pay Period"] = bypunch_df["ID"].map(totalsOT)
    bypunch_df["Total DT Hours Pay Period"] = bypunch_df["ID"].map(totalsDT)

    return bypunch_df


def create_anomalies(df, stapled_df):
    # Step 1: Define anomaly columns as either 1 or 0
    stapled_df["Short Break"] = (ta_masks.short_break(stapled_df)).astype(int)
    stapled_df["Did Not Break"] = (ta_masks.did_not_break(stapled_df)).astype(int)
    df["Over Twelve"] = (ta_masks.over_twelve(df)).astype(int)

    # Step 2: Aggregate anomalies by Employee and ID
    anomalies_stapled_df = stapled_df.groupby(["ID"], as_index=False).agg(
        {
            "Employee": "first",  # keeps this column
            "Paid Break Credit (hrs)": "first",  # keeps this column
            "Short Break": "sum",  # consolidate
            "Did Not Break": "sum",  # consolidate
        }
    )
    anomalies_df = df.groupby(["ID"], as_index=False).agg(
        {
            "Employee": "first",  # keeps this column
            "Paid Break Credit (hrs)": "first",  # keeps this column
            "Over Twelve": "sum",  # consolidate
        }
    )
    # Discard rows that have both zero calculated and paid brakes
    anomalies_stapled_df = anomalies_stapled_df[
        (anomalies_stapled_df["Short Break"] != 0)
        | (anomalies_stapled_df["Did Not Break"] != 0)
        | (anomalies_stapled_df["Paid Break Credit (hrs)"] != 0)
    ]
    anomalies_df = anomalies_df[(anomalies_df["Over Twelve"] != 0)]

    output_df = merge_source_into_target_auto(
        anomalies_df, anomalies_stapled_df, key_col="ID"
    )

    # Create sum and variance dfs
    output_df["Due Break Credit (hrs)"] = (
        output_df["Short Break"] + output_df["Did Not Break"] + output_df["Over Twelve"]
    )
    output_df["Variance"] = (
        output_df["Due Break Credit (hrs)"] - output_df["Paid Break Credit (hrs)"]
    )

    # Sort for columns correct display in UI
    output_df = output_df[config.COLS_PRINT10]

    return output_df


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


def add_seventh_day_hours(df):
    # Ensure data is sorted by ID and Date
    df = df.sort_values(["ID", "Date"]).copy()

    def compute_group(g):
        # Difference in days
        diff = g["Date"].diff().dt.days.fillna(0)
        # Identify consecutive sequences
        consec_group = (diff != 1).cumsum()
        # Compute streak length within each sequence
        streak = g.groupby(consec_group).cumcount() + 1
        # Rolling sum of 7 days
        rolling_sum = g["Totaled Amount"].rolling(7, min_periods=7).sum()
        # Keep only sums where streak >= 7
        return rolling_sum.where(streak >= 7).fillna(0)

    # Apply per ID group
    df["Hours in Seven Consecutive Days"] = df.groupby("ID", group_keys=False).apply(
        compute_group
    )
    df["First day of Seven"] = df["Date"] - pd.Timedelta(days=6)

    return df


def add_total_hours_workday(df):
    df["Total Worked Hours Workday"] = df.groupby(["ID", "Date"])[
        "Totaled Amount"
    ].transform("sum")

    return df
