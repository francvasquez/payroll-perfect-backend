import pandas as pd
from ta import ta_masks
import config


def filter_and_sort_df_to_dict(
    df,
    base_filter=None,
    cols=None,
    rename_map=None,
    sort_col=None,
    ascending=True,
    max_rows=None,
):
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

    # 🔑 Normalize columns for JSON export
    # Convert datetime to ISO8601 strings
    for col in df_check.select_dtypes(include=["datetime"]).columns:
        df_check[col] = df_check[col].dt.isoformat()

    # Convert timedelta to strings
    for col in df_check.select_dtypes(include=["timedelta"]).columns:
        df_check[col] = df_check[col].astype(str)

    # Convert numpy numbers to native Python
    for col in df_check.select_dtypes(include=["int64", "float64"]).columns:
        df_check[col] = df_check[col].astype(object)

    return df_check.to_dict("records")


def generate_results(
    df,
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
                "ta_rows": len(df),
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
        },
    }
    return result
