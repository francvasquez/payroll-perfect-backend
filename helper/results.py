import pandas as pd
import numpy as np
from ta import ta_masks
from wfn import wfn_masks
import config
from helper.aux import convert_datetime_columns_to_iso


def filter_and_sort_df_to_dict(
    df,
    base_filter=None,
    cols=None,
    rename_map=None,
    sort_col=None,
    ascending=True,
    max_rows=None,
):
    """
    Filter, select, rename, sort, and cap a DataFrame, then convert
    datetimes to ISO strings and NaNs to None for JSON serialization.
    Returns a list of dicts.
    """
    # Default to all rows if no filter
    if base_filter is None:
        mask = pd.Series(True, index=df.index)
    else:
        mask = base_filter

    # Default to all columns if none provided
    if cols is None:
        cols = df.columns

    # Apply filter and column selection
    df_check = df.loc[mask, cols].copy()

    # Rename columns if rename_map is provided
    if rename_map:
        df_check = df_check.rename(columns=rename_map)

    # Sort if requested
    if sort_col is not None and sort_col in df_check.columns:
        df_check = df_check.sort_values(by=sort_col, ascending=ascending)

    # Cap rows only if max_rows is provided
    if max_rows is not None:
        df_check = df_check.head(max_rows)

    # Convert Pandas datetime object to ISO
    safe_df_check = convert_datetime_columns_to_iso(df_check)

    # Convert NaN → None for JSON safety
    safe_df_check = safe_df_check.replace({np.nan: None})

    return safe_df_check.to_dict("records")


def generate_results(
    processed_ta_df,
    anomalies_df,
    bypunch_df,
    stapled_df,
    processed_wfn_df,
    processed_waiver_df,
    ta_process_time,
    wfn_process_time,
    waiver_process_time,
):

    result = {
        "success": True,
        "summary": {
            "rows": {
                "ta_rows": len(processed_ta_df),
                "anomalies_rows": len(anomalies_df),
                "bypunch_rows": len(bypunch_df),
                "stapled_rows": len(stapled_df),
                "wfn_rows": len(processed_wfn_df),
                "waiver_rows": len(processed_waiver_df),
            },
            "timing": {
                "ta_process_time_ms": ta_process_time,
                "wfn_process_time_ms": wfn_process_time,
                "waiver_process_time_ms": waiver_process_time,
            },
        },
        "wfn": {
            ##Overtime Checks Variances
            "overtime_checks_variances": filter_and_sort_df_to_dict(
                df=processed_wfn_df,
                sort_col="Payroll Name",
                ascending=True,
                base_filter=wfn_masks.var_below(processed_wfn_df, "Variance"),
                max_rows=200,
                cols=config.COLUMNS_TO_SHOW,
                rename_map={
                    "Variance": "Variance ($)",
                    "1.5 OT Earnings Due": "1.5 OT Earnings Due ($)",
                    "Actual Pay Check": "Actual Pay Check ($)",
                },
            ),
            ##Double Time Checks Variances
            "doubletime_checks_variances": filter_and_sort_df_to_dict(
                df=processed_wfn_df,
                sort_col="Payroll Name",
                ascending=True,
                base_filter=wfn_masks.var_below(processed_wfn_df, "Variance Dble"),
                max_rows=200,
                cols=config.COLUMNS_TO_SHOW_DBLE,
                rename_map={
                    "Double Time Due": "Double Time Due ($)",
                    "Actual Pay Check Double": "Actual Pay Check Double ($)",
                    "Variance Dble": "Variance Dble ($)",
                },
            ),
            ##Break Credit Variances
            "break_credit_variances": filter_and_sort_df_to_dict(
                df=processed_wfn_df,
                sort_col="Payroll Name",
                ascending=True,
                base_filter=wfn_masks.var_below(processed_wfn_df, "Variance BrkCrd"),
                max_rows=200,
                cols=config.COLUMNS_TO_SHOW_BRKCRD,
                rename_map={
                    "Actual Pay BrkCrd": "Actual Paid Break Credit",
                    "Variance BrkCrd": "Variance Break Credit",
                },
            ),
            ##Rest Credit Variances
            "rest_credit_variances": filter_and_sort_df_to_dict(
                df=processed_wfn_df,
                sort_col="Payroll Name",
                ascending=True,
                base_filter=wfn_masks.var_below(processed_wfn_df, "Variance RestCrd"),
                max_rows=200,
                cols=config.COLUMNS_TO_SHOW_REST,
                rename_map={
                    "Actual Pay RestCrd": "Actual Paid Rest Credit ($)",
                    "Variance RestCrd": "Variance Rest Credit ($)",
                },
            ),
            ##Sick Credit Variances
            "sick_credit_variances": filter_and_sort_df_to_dict(
                df=processed_wfn_df,
                sort_col="Payroll Name",
                ascending=True,
                base_filter=wfn_masks.var_below(processed_wfn_df, "Variance Sick"),
                max_rows=200,
                cols=config.COLUMNS_TO_SHOW_SICK,
                rename_map={
                    "Sick Credit Due": "Sick Credit Due ($)",
                    "Sick Paid": "Actual Paid Sick Credit ($)",
                    "Variance Sick": "Variance Sick Credit ($)",
                    "Regular Rate Paid": "Regular Rate Paid ($)",
                },
            ),
            ##FLSA Check
            "flsa_check": filter_and_sort_df_to_dict(
                df=processed_wfn_df,
                sort_col="Payroll Name",
                ascending=True,
                base_filter=wfn_masks.flsa(processed_wfn_df),
                max_rows=200,
                cols=config.COLUMNS_TO_SHOW_FLSA,
            ),
            ##Minimum Wage Check
            "min_wage_check": filter_and_sort_df_to_dict(
                df=processed_wfn_df,
                sort_col="Payroll Name",
                ascending=True,
                base_filter=wfn_masks.min_wage_check(processed_wfn_df),
                max_rows=200,
                cols=config.COLUMNS_TO_SHOW_MINWAGE,
            ),
            ##Non-Active Check
            "non_active_check": filter_and_sort_df_to_dict(
                df=processed_wfn_df,
                sort_col="Payroll Name",
                ascending=True,
                base_filter=wfn_masks.non_active_check(processed_wfn_df),
                max_rows=200,
                cols=config.COLUMNS_TO_SHOW_NONACTIVE,
                rename_map={
                    "V_Vacation_Hours": "Vacation Hours",
                    "Job Title Description": "Job Description",
                    "HIREDATE": "Hire Date",
                    "REG": "Straight Hours Worked",
                    "Variance Sick": "Variance Sick Credit ($)",
                    "Regular Rate Paid": "Regular Rate Paid ($)",
                },
            ),
        },
        "ta": {
            ## "1. Break Credit Summary"
            "break_credit_summary": filter_and_sort_df_to_dict(
                df=anomalies_df,
                sort_col="Paid Break Credit (hrs)",
                base_filter=ta_masks.non_zero_var(anomalies_df),
                ascending=False,
                max_rows=200,
            ),
            ##1a. Short Break: Earned credits
            "short_break_earned_credits": filter_and_sort_df_to_dict(
                df=processed_ta_df,
                sort_col="Employee",
                ascending=True,
                base_filter=ta_masks.short_break(processed_ta_df),
                max_rows=200,
                cols=config.COLS_PRINT3,
                rename_map={
                    "Regular Rate Paid": "Straight Rate ($)",
                    "Totaled Amount": "Punch Length (hrs)",
                },
            ),
            ##1b. Short Break: Cases to investigate further
            "short_break_to_investigate": filter_and_sort_df_to_dict(
                df=stapled_df,
                sort_col="Employee",
                ascending=True,
                base_filter=ta_masks.short_break_possible(stapled_df),
                max_rows=200,
                cols=config.COLS_PRINT3,
                rename_map={
                    "Regular Rate Paid": "Straight Rate ($)",
                    "Totaled Amount": "Punch Length (hrs)",
                },
            ),
            ##1c. Did not take break: Meal Waiver Check
            "did_not_break_meal_waiver_check": filter_and_sort_df_to_dict(
                df=stapled_df,
                sort_col="Employee",
                ascending=True,
                base_filter=ta_masks.did_not_break_bet_five_and_six_not_waived(
                    stapled_df
                ),
                max_rows=200,
                cols=config.COLS_PRINT2_A,
                rename_map={"Totaled Amount": "Punch Length (hrs)"},
            ),
            ##1d. Did not take break: Cases to investigate further
            "did_not_break_to_investigate": filter_and_sort_df_to_dict(
                df=stapled_df,
                sort_col="Employee",
                ascending=True,
                base_filter=ta_masks.did_not_break_possible(stapled_df),
                max_rows=200,
                cols=config.COLS_PRINT2_A,
                rename_map={"Totaled Amount": "Punch Length (hrs)"},
            ),
            ##1e. Over 12 hours Check
            "over_12_hours": filter_and_sort_df_to_dict(
                df=processed_ta_df,
                sort_col="Employee",
                ascending=True,
                base_filter=ta_masks.over_twelve(processed_ta_df),
                max_rows=200,
                cols=config.COLS_PRINT7,
            ),
            ## NEW ##
            ##2. Employees with Seven Consecutive Days
            "seven_consecutive": filter_and_sort_df_to_dict(
                df=bypunch_df,
                sort_col="Employee",
                ascending=True,
                base_filter=ta_masks.check_seven_consec(bypunch_df),
                max_rows=200,
                cols=config.COLS_PRINT8,
                rename_map={
                    "Totaled Amount": "Hours Worked on Trigger Date",
                    "In Punch": "Trigger Date",
                },
            ),
            ##3. Check Overtime (OT) hours versus WFN
            "ot_vs_wfn": filter_and_sort_df_to_dict(
                df=bypunch_df,
                sort_col="Employee",
                ascending=True,
                base_filter=(
                    ta_masks.unique_ids(bypunch_df)
                    & ~ta_masks.zero_rows_bypunch(bypunch_df)
                    & ta_masks.OT_var_mask(bypunch_df)
                ),
                max_rows=200,
                cols=config.COLS_PRINT9,
                rename_map={"Total OT Hours Pay Period": "OT Hours on Time Card"},
            ),
            ##3a. Check Doubletime (DT) hours versus WFN
            "dt_vs_wfn": filter_and_sort_df_to_dict(
                df=bypunch_df,
                sort_col="Employee",
                ascending=True,
                base_filter=(
                    ta_masks.unique_ids(bypunch_df)
                    & ~ta_masks.zero_rows_bypunch(bypunch_df)
                    & ta_masks.DT_var_mask(bypunch_df)
                ),
                max_rows=200,
                cols=config.COLS_PRINT9a,
                rename_map={"Total DT Hours Pay Period": "DT Hours on Time Card"},
            ),
            ##4. Split Shift Check
            "split_shift": filter_and_sort_df_to_dict(
                df=stapled_df,
                sort_col="Employee",
                ascending=True,
                base_filter=ta_masks.split_shift(stapled_df),
                max_rows=200,
                cols=config.COLS_PRINT5,
                rename_map={"Regular Rate Paid": "Straight Rate ($)"},
            ),
        },
    }
    print("result.ta.split_shift: ", result["ta"]["split_shift"])
    return result
