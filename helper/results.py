import pandas as pd


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
        ## "1. Break Credit Summary"
        "break_credit_summary": filter_and_sort_df_to_dict(
            df=anomalies_df,
            sort_col="Paid Break Credit (hrs)",
            ascending=False,
            max_rows=200,
        ),
        # "anomalies_df": (
        #     anomalies_df.head(200).to_dict("records")  # Cap at 200 rows
        #     if len(anomalies_df) > 0
        #     else []
        # ),
    }
    return result
