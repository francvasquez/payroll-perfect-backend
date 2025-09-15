import utility
from . import ta_utility
import time


def process_data_ta(
    df, min_wage, ot_day_max, processed_waiver_df=None, processed_wfn_df=None
):
    # Import Excel file to df with only specific files
    # df = utility.import_excel(ta_file, config.TA_KEY_COLS, config.TA_COLS)

    # Updated df: Assure timestamps are in Panda's datetime format

    df = utility.to_pandas_datetime(df, "In Punch", "Out Punch", "Date/Time")

    # Normalize Date in case it came with hours - converts the time to midnight (00:00:00). Rename.
    df["Date/Time"] = df["Date/Time"].dt.normalize()
    df = df.rename(columns={"Date/Time": "Date"})

    # Updated df: Adds "Total Worked Hours Workday" col.
    df = ta_utility.add_total_hours_workday(df)

    # New df: Sort and staple system generated midnight punches
    stapled_df = ta_utility.sort_and_staple(df)

    # Updated df: Add time helper columns
    df = ta_utility.add_time_helper_cols(df)
    stapled_df = ta_utility.add_time_helper_cols(stapled_df)

    # Updated df: Add Break Credit from WFN File.
    df = ta_utility.add_col_from_another_df(
        home_df=df,
        lookup_df=processed_wfn_df,
        home_ref="ID",
        lookup_ref="IDX",
        lookup_tgt="J_Break Credits_Additional Hours",
        home_new_col="Paid Break Credit (hrs)",
    )
    stapled_df = ta_utility.add_col_from_another_df(
        home_df=stapled_df,
        lookup_df=processed_wfn_df,
        home_ref="ID",
        lookup_ref="IDX",
        lookup_tgt="J_Break Credits_Additional Hours",
        home_new_col="Paid Break Credit (hrs)",
    )

    # Updated df: Adds Short ID, Waiver Lookup, Waiver on File? cols
    df = ta_utility.add_waiver_check(df, processed_waiver_df)
    stapled_df = ta_utility.add_waiver_check(stapled_df, processed_waiver_df)

    # Updated df: Adds breaks check columns
    stapled_df = ta_utility.add_break_time(stapled_df)
    stapled_df = ta_utility.add_next_break_time(stapled_df)
    stapled_df = ta_utility.add_shift_length(stapled_df)

    # Updated df: Add Regular Rate Paid (a.k.a "Straight Rate ($)") from wfn, Split Paid ($),
    # Split at Min Wage ($), Split Shift Due ($) cols.
    # df = ta_utility.add_split_shift(df, processed_wfn_df, min_wage)
    stapled_df = ta_utility.add_split_shift(stapled_df, processed_wfn_df, min_wage)

    # BY PUNCH DF ######################################

    # New df: A reduced col df with daily and add DT and OT calc cols
    bypunch_df = ta_utility.create_bypunch(df, ot_day_max)

    t7 = time.time()
    # Updated df: Adds col "Hours in Seven Consecutive Days" and "First day of Seven".
    bypunch_df = ta_utility.add_seventh_day_hours(bypunch_df)  # SLOWWW
    print(f"7th day hours: {time.time()-t7:.2f}s")

    # Updated df: Add OT and DT columns from WFN

    ta_utility.add_col_from_another_df(
        home_df=bypunch_df,
        lookup_df=processed_wfn_df,
        home_ref="ID",
        lookup_ref="IDX",
        lookup_tgt="OT",
        home_new_col="OT Hours Paid",
    )
    ta_utility.add_col_from_another_df(
        home_df=bypunch_df,
        lookup_df=processed_wfn_df,
        home_ref="ID",
        lookup_ref="IDX",
        lookup_tgt="DBLTIME HRS",
        home_new_col="DT Hours Paid",
    )

    # Updated df: Add OT vs WFN variances cols.
    bypunch_df["OT Variance (hrs)"] = (
        bypunch_df["Total OT Hours Pay Period"] - bypunch_df["OT Hours Paid"]
    )
    bypunch_df["DT Variance (hrs)"] = (
        bypunch_df["Total DT Hours Pay Period"] - bypunch_df["DT Hours Paid"]
    )

    # Create anomalies DF
    anomalies_df = ta_utility.create_anomalies(df, stapled_df)

    return (df, bypunch_df, stapled_df, anomalies_df)
