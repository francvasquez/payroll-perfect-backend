import numpy as np
import pandas as pd
from . import ta_masks
import utility
import logging
from helper.db_utils import get_carryover_streaks


logger = logging.getLogger()


def get_effective_param(client_params, location, param_name):
    """Fetches a parameter from client_params prioritizing location overrides over global defaults."""
    # 1. Check if the location exists in the config and has the specific parameter
    if location in client_params.get("locations", {}):
        if param_name in client_params["locations"][location]:
            return client_params["locations"][location][param_name]

    # 2. Fallback to the global parameter
    return client_params.get("global", {}).get(param_name)


def add_consec_day_reporting(daily_df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds explicit reporting columns for consecutive day violations,
    including the start date of the streak and the exact hours penalized.
    Columns added are:
    "First_Day_of_Streak", "Consec_OT_Hours", "Consec_DT_Hours"
    """
    df = daily_df.copy()

    # 1. Fetch dynamic OT limits in case locations have different rules for the 8-hour threshold
    # Removed - fixed Standard CA Consecutive Day math: First 8 hours = OT, everything over 8 = DT

    # 2. Find the First Day of the Streak
    # We group by the specific Streak_ID so if there was a gap, the start date resets!
    if "Streak_ID" in df.columns:
        first_days_of_week = df.groupby(["Employee", "ID", "Workweek_ID", "Streak_ID"])[
            "Attributed_Workday"
        ].transform("min")
    else:
        # Fallback just in case
        first_days_of_week = df.groupby(["Employee", "ID", "Workweek_ID"])[
            "Attributed_Workday"
        ].transform("min")

    # Initialize the new columns with empty defaults
    df["First_Day_of_Streak"] = pd.NaT
    df["Consec_OT_Hours"] = 0.0
    df["Consec_DT_Hours"] = 0.0

    # 3. Apply the logic ONLY to rows that triggered the violation
    mask_consec = df["Is_Consecutive_Day_Rule"] == True

    if mask_consec.any():
        # Tag the start date of the streak
        df.loc[mask_consec, "First_Day_of_Streak"] = first_days_of_week[mask_consec]

        # Calculate the split: First 8 hours to OT, remainder to DT
        df.loc[mask_consec, "Consec_OT_Hours"] = np.minimum(
            df.loc[mask_consec, "Hours_Worked"], 8.0
        )
        df.loc[mask_consec, "Consec_DT_Hours"] = np.maximum(
            0, df.loc[mask_consec, "Hours_Worked"] - 8.0
        )

    # 4. Clean up the date formatting for clean JSON serialization to React
    df["First_Day_of_Streak"] = pd.to_datetime(df["First_Day_of_Streak"]).dt.date

    # Optional: Round the float columns
    df["Consec_OT_Hours"] = df["Consec_OT_Hours"].round(4)
    df["Consec_DT_Hours"] = df["Consec_DT_Hours"].round(4)

    # Clean up the Streak_ID so it doesn't clutter the final export
    if "Streak_ID" in df.columns:
        df = df.drop(columns=["Streak_ID"])

    return df


def validate_intake_pay_date(
    raw_df: pd.DataFrame,
    target_pay_date: str,
    client_params: dict,
    pay_date_anchor: str,
) -> tuple[bool, str]:
    """
    Validates user-inputted pay date against the actual punch timestamps in the raw file.
    Returns a tuple: (is_valid: bool, status_message: str)
    """

    # 1. Catch completely invalid date strings FIRST
    try:
        target_date = pd.to_datetime(target_pay_date).normalize()
    except Exception:
        return False, f"Intake Error: '{target_pay_date}' is not a valid date format."

    # 2. Extract configuration logic
    pp_length = client_params["global"]["pay_period_length"]
    days_to_pay = client_params["global"]["days_bet_payroll_end_and_pay_date"]

    # 3. --- Checks against anchor pay date ---
    master_anchor = pd.to_datetime(pay_date_anchor).normalize()

    # 4. Check if the inputted date is an exact multiple of 14 days from the master anchor
    days_diff = (target_date - master_anchor).days
    if days_diff % pp_length != 0:
        error_msg = (
            f"Invalid Pay Date! {target_date.date()} does not align with the company's {pp_length}-day payroll cycle.\n"
            f"Please double-check the date. Valid pay dates fall exactly every {pp_length} days."
        )
        return False, error_msg
    # ------------------------------------------

    # 5. Calculate the expected work window for the inputted pay date
    expected_end = target_date - pd.to_timedelta(days_to_pay, unit="D")
    expected_start = expected_end - pd.to_timedelta(pp_length - 1, unit="D")

    # 6. Extract the actual physical boundaries of the raw data
    actual_min = raw_df["In Punch"].min().normalize()
    actual_max = raw_df["In Punch"].max().normalize()

    # 7. Sanity Check: Is there an overlap?
    if expected_end < actual_min or expected_start > actual_max:
        error_msg = (
            f"Date Mismatch! You entered Pay Date: {target_date.date()}.\n"
            f"That corresponds to a work period of {expected_start.date()} to {expected_end.date()}.\n"
            f"However, the uploaded file only contains data from {actual_min.date()} to {actual_max.date()}."
        )
        return False, error_msg

    # 8. Optional: Strict bounds check (Warning only)
    if actual_min > expected_start or actual_max < expected_end:
        warning_msg = (
            f"Warning: The file was accepted, but the data range ({actual_min.date()} to {actual_max.date()}) "
            f"does not fully cover the expected period ({expected_start.date()} to {expected_end.date()}). "
            f"Calculations may be incomplete."
        )
        return True, warning_msg

    return True, "Validation Passed."


def filter_target_pay_period(df: pd.DataFrame, target_pay_date: str) -> pd.DataFrame:
    """
    Filters the dataframe to isolate only the pay period(s) the user explicitly wants to audit,
    dropping incomplete tails from previous or future periods.
    """
    # 1. Convert the passed string to a Pandas date object to match the dataframe column
    target_date = pd.to_datetime(target_pay_date).date()

    # 2. Apply the boolean mask to keep only the matching rows
    filtered_df = df[df["Fiscal_Pay_Date"] == target_date].copy()

    # 3. Reset the index for a clean output
    return filtered_df.reset_index(drop=True)


def apply_ot_and_dt_paid_from_wfn(daily_df, processed_wfn_df):
    # TODO: Need to incorporate pay date otherwise some of the variances won't make sense
    # Perform check of time cards vs payroll OT
    "Columns created in this step are: OT_Hours_Paid, DT_Hours_Paid, OT_Variance_(hrs), DT_Variance_(hrs)"  # For reference
    daily_df = add_col_from_another_df(
        home_df=daily_df,
        lookup_df=processed_wfn_df,
        home_ref="ID",
        lookup_ref="IDX",
        lookup_tgt="OT",
        home_new_col="OT_Hours_Paid",
    )

    # Perform check of time cards vs payroll DT
    daily_df = add_col_from_another_df(
        home_df=daily_df,
        lookup_df=processed_wfn_df,
        home_ref="ID",
        lookup_ref="IDX",
        lookup_tgt="DBLTIME HRS",
        home_new_col="DT_Hours_Paid",
    )

    # Add TA vs WFN variances cols.
    daily_df["OT_Variance_(hrs)"] = (
        (daily_df["OT_Hours_Pay_Period"] - daily_df["OT_Hours_Paid"])
    ).round(4)
    daily_df["DT_Variance_(hrs)"] = (
        (daily_df["DT_Hours_Pay_Period"] - daily_df["DT_Hours_Paid"])
    ).round(4)

    return daily_df


def apply_pay_period_totals(
    daily_df: pd.DataFrame, client_params: dict, pay_date_anchor: str
) -> pd.DataFrame:
    """
    Calculates the Fiscal Pay Date dynamically using an anchor date,
    and broadcasts the total OT and DT for that period to every row.
    Columns created in this step are: Fiscal_Pay_Date, OT_Hours_Pay_Period, DT_Hours_Pay_Period
    """
    df = daily_df.copy()

    # 1. Extract Parameters
    pp_length = client_params["global"]["pay_period_length"]
    days_to_pay = client_params["global"]["days_bet_payroll_end_and_pay_date"]

    # 2. Date Math Setup
    df["Attributed_Workday"] = pd.to_datetime(df["Attributed_Workday"])
    anchor_date = pd.to_datetime(pay_date_anchor)

    # Calculate the boundaries of the specific pay period tied to the anchor
    anchor_end_date = anchor_date - pd.to_timedelta(days_to_pay, unit="D")
    anchor_start_date = anchor_end_date - pd.to_timedelta(pp_length - 1, unit="D")

    # 3. Dynamically Assign Fiscal_Pay_Date
    # Calculate how many days each workday is from the anchor's start date
    delta_days = (df["Attributed_Workday"] - anchor_start_date).dt.days

    # Use floor division to find how many pay periods away this row is (handles past & future dates)
    period_offset = np.floor(delta_days / pp_length)

    # Calculate the specific pay date for this row
    row_period_end = anchor_end_date + pd.to_timedelta(
        period_offset * pp_length, unit="D"
    )
    df["Fiscal_Pay_Date"] = row_period_end + pd.to_timedelta(days_to_pay, unit="D")

    # Convert to clean date format without timestamps
    df["Fiscal_Pay_Date"] = df["Fiscal_Pay_Date"].dt.date

    # --- 4. Broadcast Pay Period Totals ---
    # Group by the specific employee and their newly calculated pay date
    group_cols = ["Employee", "ID", "Fiscal_Pay_Date"]

    # .transform('sum') calculates the group total and broadcasts it to every row in the group
    df["OT_Hours_Pay_Period"] = df.groupby(group_cols)["OT_Hrs"].transform("sum")
    df["DT_Hours_Pay_Period"] = df.groupby(group_cols)["DT_Hrs"].transform("sum")

    # Rounding for clean float math
    df["OT_Hours_Pay_Period"] = df["OT_Hours_Pay_Period"].round(4)
    df["DT_Hours_Pay_Period"] = df["DT_Hours_Pay_Period"].round(4)

    return df


def apply_weekly_rules(
    daily_df: pd.DataFrame, client_params: dict, clientId: str, pay_date: str
) -> pd.DataFrame:
    """
    Applies weekly overtime (>40 hours) and consecutive day premium rules dynamically based on Location overrides. If an employee worked the last day of the preceding pay period, and has a true boolean for cba_consec_anyweek, the streak count will carry over across pay periods instead of resetting at the start of each workweek.
    Columns created in this step are: Workweek_ID, Days_Worked_In_Week, Is_Consecutive_Day_Rule, Cum_Reg_Hrs, Weekly_OT_Spillover
    """
    df = daily_df.copy()

    # 1. --- Extract Config ---
    workweek_start = client_params["global"]["workweek_start"]
    locs = client_params.get("locations", {})
    g_ot_week = client_params["global"]["ot_week_max"]
    g_consec = client_params["global"]["number_of_consec_days_before_ot"]

    # NEW 1.1 Check if ANY location requires the rolling CBA logic (cba_consec_anyweek)
    needs_carryover = client_params.get("global", {}).get("cba_consec_anyweek", False)
    for loc_data in client_params.get("locations", {}).values():
        if loc_data.get("cba_consec_anyweek", False):
            needs_carryover = True
            break

    # NEW 1.2 If needs_carryover, get carryover dictionary from db
    carryover_dict = {}
    if needs_carryover:
        carryover_dict = get_carryover_streaks(clientId, pay_date, client_params)

    # 2. --- Map Limits to Dataframe ---
    ot_week_map = {
        loc: config.get("ot_week_max", g_ot_week) for loc, config in locs.items()
    }
    consec_map = {
        loc: config.get("number_of_consec_days_before_ot", g_consec)
        for loc, config in locs.items()
    }
    # NEW: Map the CBA boolean!
    g_cba = client_params.get("global", {}).get("cba_consec_anyweek", False)
    cba_map = {
        loc: config.get("cba_consec_anyweek", g_cba) for loc, config in locs.items()
    }

    df["limit_ot_week"] = df["Location"].map(ot_week_map).fillna(g_ot_week)
    df["limit_consec"] = df["Location"].map(consec_map).fillna(g_consec)
    df["is_cba_rolling"] = df["Location"].map(cba_map).fillna(g_cba)

    # 3. --- Generate Workweek_ID ---
    day_map = {
        "Monday": 0,
        "Tuesday": 1,
        "Wednesday": 2,
        "Thursday": 3,
        "Friday": 4,
        "Saturday": 5,
        "Sunday": 6,
    }
    start_day_int = day_map.get(workweek_start, 6)

    df["Attributed_Workday"] = pd.to_datetime(df["Attributed_Workday"])
    days_to_subtract = (df["Attributed_Workday"].dt.dayofweek - start_day_int) % 7
    df["Workweek_ID"] = df["Attributed_Workday"] - pd.to_timedelta(
        days_to_subtract, unit="D"
    )

    df = df.sort_values(
        by=["Employee", "ID", "Workweek_ID", "Attributed_Workday"]
    ).reset_index(drop=True)

    # --- 4. Dynamic Consecutive Day Logic ---
    # 4A. Calculate Standard Workweek-Bounded Math (Your existing logic)
    df["Prev_Workday"] = df.groupby(["Employee", "ID", "Workweek_ID"])[
        "Attributed_Workday"
    ].shift(1)
    df["Days_Diff"] = (df["Attributed_Workday"] - df["Prev_Workday"]).dt.days
    df["New_Streak"] = df["Days_Diff"] != 1
    df["Streak_ID"] = df.groupby(["Employee", "ID", "Workweek_ID"])[
        "New_Streak"
    ].cumsum()
    # Save this into a temporary column
    df["Days_Worked_Standard"] = (
        df.groupby(["Employee", "ID", "Workweek_ID", "Streak_ID"]).cumcount() + 1
    )

    # 4B. Define the Traffic Cop Function

    # 4B1. NEW: Calculate the exact date of "yesterday" (relative to the new pay period)
    pay_date_obj = pd.to_datetime(pay_date)
    days_bet_payroll_end_and_pay_date = client_params.get("global", {}).get(
        "days_bet_payroll_end_and_pay_date", 6
    )
    pay_period_length = client_params.get("global", {}).get("pay_period_length", 14)
    prior_period_date = (
        pay_date_obj
        - pd.Timedelta(days=days_bet_payroll_end_and_pay_date)
        - pd.Timedelta(days=pay_period_length)
    )

    def apply_streaks(group):
        is_cba = group["is_cba_rolling"].iloc[0]
        emp_id = group["ID"].iloc[0]

        if not is_cba:
            group["Days_Worked_In_Week"] = group["Days_Worked_Standard"]
            return group

        # --- THE FIX ---
        current_streak = carryover_dict.get(emp_id, 0)
        streaks = []

        # 4B2. Seed the loop with the actual calendar date of the previous period's final day
        last_date = prior_period_date

        for current_date in group["Attributed_Workday"]:

            # If they didn't work on the final day of the prior period, they have no streak to continue.
            if current_streak == 0:
                current_streak = 1

            # If the gap between this shift and 'last_date' is exactly 1 day, the streak continues!
            # (If this is their first shift of the period, it perfectly checks against 'prior_period_date')
            elif (current_date - last_date).days == 1:
                current_streak += 1

            # A gap of 2+ days occurred. The streak breaks.
            else:
                current_streak = 1

            streaks.append(current_streak)
            last_date = current_date

        group["Days_Worked_In_Week"] = streaks
        return group

    # 4C. Execute the Traffic Cop across all employees
    # (Since we sort chronologically in Step 3, the rolling math works perfectly)
    df = df.groupby("ID", group_keys=False).apply(apply_streaks)

    # 4D. Trigger the Penalty based on the dynamic limit mapped in Step 2!
    df["Is_Consecutive_Day_Rule"] = df["Days_Worked_In_Week"] > df["limit_consec"]

    mask_consec = df["Is_Consecutive_Day_Rule"]

    # Standard CA Consecutive Day math: First 8 hours = OT, everything over 8 = DT
    df.loc[mask_consec, "Regular_Hrs"] = 0.0
    df.loc[mask_consec, "OT_Hrs"] = np.minimum(df.loc[mask_consec, "Hours_Worked"], 8.0)
    df.loc[mask_consec, "DT_Hrs"] = np.maximum(
        0, df.loc[mask_consec, "Hours_Worked"] - 8.0
    )

    # Clean up intermediate helpers (Leave Streak_ID for your reporting function!)
    df = df.drop(
        columns=[
            "Prev_Workday",
            "Days_Diff",
            "New_Streak",
            "Days_Worked_Standard",
            "is_cba_rolling",
        ]
    )

    # --- 5. Dynamic Weekly Overtime Logic ---
    df["Cum_Reg_Hrs"] = df.groupby(["Employee", "ID", "Workweek_ID"])[
        "Regular_Hrs"
    ].cumsum()
    df["Prior_Cum_Reg"] = (
        df.groupby(["Employee", "ID", "Workweek_ID"])["Cum_Reg_Hrs"].shift(1).fillna(0)
    )

    df["Weekly_OT_Spillover"] = 0.0

    # Scenario A: Already crossed custom limit
    mask_already_over = df["Prior_Cum_Reg"] >= df["limit_ot_week"]
    df.loc[mask_already_over, "Weekly_OT_Spillover"] = df.loc[
        mask_already_over, "Regular_Hrs"
    ]
    df.loc[mask_already_over, "Regular_Hrs"] = 0.0

    # Scenario B: Crossing custom limit today
    mask_crossing_today = (df["Prior_Cum_Reg"] < df["limit_ot_week"]) & (
        df["Cum_Reg_Hrs"] > df["limit_ot_week"]
    )
    spillover_hours = (
        df.loc[mask_crossing_today, "Cum_Reg_Hrs"]
        - df.loc[mask_crossing_today, "limit_ot_week"]
    )

    df.loc[mask_crossing_today, "Weekly_OT_Spillover"] = spillover_hours
    df.loc[mask_crossing_today, "Regular_Hrs"] = (
        df.loc[mask_crossing_today, "Regular_Hrs"] - spillover_hours
    )

    df["OT_Hrs"] = df["OT_Hrs"] + df["Weekly_OT_Spillover"]

    # 6. Final Cleanup
    df = df.drop(columns=["Prior_Cum_Reg", "limit_ot_week", "limit_consec"])
    float_cols = [
        "Hours_Worked",
        "Regular_Hrs",
        "OT_Hrs",
        "DT_Hrs",
        "Cum_Reg_Hrs",
        "Weekly_OT_Spillover",
    ]
    df[float_cols] = df[float_cols].round(4)

    return df


def create_daily_df(df: pd.DataFrame, client_params: dict) -> pd.DataFrame:
    """
    Transforms the punch dataframe into a daily aggregated dataframe with  OT and DT calculations, applying dynamic thresholds based on the employee's Location.
    Columns created in this step are: Employee, ID, Location, Attributed_Workday, Hours_Worked, Regular_Hrs, OT_Hrs, DT_Hrs
    """
    # 1. Filter to required columns
    required_cols = [
        "Employee",
        "ID",
        "Location",
        "In Punch",
        "Out Punch",
        "Punch Length (hrs) Raw",
    ]
    df = df[required_cols].copy()

    # 2. Extract globals and create mapping dictionaries for locations
    locs = client_params.get("locations", {})
    g_ot_day = client_params["global"]["ot_day_max"]
    g_dt_day = client_params["global"]["dt_day_max"]

    ot_day_map = {
        loc: config.get("ot_day_max", g_ot_day) for loc, config in locs.items()
    }
    dt_day_map = {
        loc: config.get("dt_day_max", g_dt_day) for loc, config in locs.items()
    }

    # 3. Map the custom limits to the dataframe (filling any unmapped locations with the global default)
    df["limit_ot_day"] = df["Location"].map(ot_day_map).fillna(g_ot_day)
    df["limit_dt_day"] = df["Location"].map(dt_day_map).fillna(g_dt_day)

    # 4. Create a boolean mask to identify shifts that cross midnight
    df["In_Date"] = df["In Punch"].dt.date
    df["Out_Date"] = df["Out Punch"].dt.date
    mask_same_day = df["In_Date"] == df["Out_Date"]

    # --- Group A: Same-Day Shifts ---
    df_same = df[mask_same_day].copy()
    df_same["Attributed_Workday"] = df_same["In_Date"]
    df_same["Hours_Worked"] = df_same["Punch Length (hrs) Raw"]

    # --- Group B: Cross-Midnight Shifts ---
    df_cross = df[~mask_same_day].copy()

    if not df_cross.empty:
        midnight_series = df_cross["Out Punch"].dt.normalize()

        df_cross_1 = df_cross.copy()
        df_cross_1["Attributed_Workday"] = df_cross_1["In_Date"]
        df_cross_1["Hours_Worked"] = (
            midnight_series - df_cross_1["In Punch"]
        ).dt.total_seconds() / 3600.0

        df_cross_2 = df_cross.copy()
        df_cross_2["Attributed_Workday"] = df_cross_2["Out_Date"]
        df_cross_2["Hours_Worked"] = (
            df_cross_2["Out Punch"] - midnight_series
        ).dt.total_seconds() / 3600.0

        df_splits = pd.concat([df_cross_1, df_cross_2], ignore_index=True)
    else:
        df_splits = pd.DataFrame(columns=df_same.columns)

    # 5. Concatenate and Aggregate
    cols_to_keep = [
        "Employee",
        "ID",
        "Location",
        "limit_ot_day",
        "limit_dt_day",
        "Attributed_Workday",
        "Hours_Worked",
    ]
    df_combined = pd.concat(
        [df_same[cols_to_keep], df_splits[cols_to_keep]], ignore_index=True
    )

    daily_df = (
        df_combined.groupby(["Employee", "ID", "Location", "Attributed_Workday"])
        .agg(
            {
                "Hours_Worked": "sum",
                "limit_ot_day": "first",  # The limit is the same for the day, grab the first one
                "limit_dt_day": "first",
            }
        )
        .reset_index()
    )

    # 6. Calculate California Overtime and Double Time Dynamically
    # Regular: Cap at the row's specific OT limit
    daily_df["Regular_Hrs"] = np.minimum(
        daily_df["Hours_Worked"], daily_df["limit_ot_day"]
    )

    # OT: (Hours - OT Limit), capped at (DT Limit - OT Limit). E.g., max 4 OT hours globally, or 4.5 OT hours for 2JT
    ot_potential = daily_df["Hours_Worked"] - daily_df["limit_ot_day"]
    max_ot_allowed = daily_df["limit_dt_day"] - daily_df["limit_ot_day"]
    daily_df["OT_Hrs"] = np.maximum(0, np.minimum(ot_potential, max_ot_allowed))

    # DT: Everything over the DT Limit
    daily_df["DT_Hrs"] = np.maximum(
        0, daily_df["Hours_Worked"] - daily_df["limit_dt_day"]
    )

    # Clean up helper columns
    daily_df = daily_df.drop(columns=["limit_ot_day", "limit_dt_day"])
    daily_df = daily_df.sort_values(by=["ID", "Attributed_Workday"]).reset_index(
        drop=True
    )

    return daily_df


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
    ## TODO: This is an older system to determine overrides versus what we have in daily_df. We should consider unifying the approach for cleanup. This does work well however.

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
