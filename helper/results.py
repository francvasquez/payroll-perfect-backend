import pandas as pd
import numpy as np
from ta import ta_masks
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
        "ta": {
            ## "1. Break Credit Summary"
            "break_credit_summary": filter_and_sort_df_to_dict(
                df=anomalies_df,
                sort_col="Paid Break Credit (hrs)",
                ascending=False,
                max_rows=200,
            ),
            ##1a. Short Break: Earned credits
            "short_break_earned_credits": filter_and_sort_df_to_dict(
                df=stapled_df,
                sort_col="Employee",
                ascending=True,
                base_filter=ta_masks.short_break(stapled_df),
                max_rows=200,
                cols=config.COLS_PRINT3,
                rename_map={
                    "Regular Rate Paid": "Straight Rate ($)",
                    "Totaled Amount": "Hours Worked",
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
                    "Totaled Amount": "Hours Worked",
                },
            ),
            ##1c. Did not take break: Earned credits
            "did_not_break_earned_credits": filter_and_sort_df_to_dict(
                df=stapled_df,
                sort_col="Employee",
                ascending=True,
                base_filter=ta_masks.did_not_break(stapled_df),
                max_rows=200,
                cols=config.COLS_PRINT2_A,
                rename_map={"Totaled Amount": "Hours Worked"},
            ),
            ##1d. Did not take break: Cases to investigate further
            "did_not_break_to_investigate": filter_and_sort_df_to_dict(
                df=stapled_df,
                sort_col="Employee",
                ascending=True,
                base_filter=ta_masks.did_not_break_possible(stapled_df),
                max_rows=200,
                cols=config.COLS_PRINT2_A,
                rename_map={"Totaled Amount": "Hours Worked"},
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
        },
    }
    return result
