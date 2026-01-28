from helper.db_utils import save_to_database_fast, get_db_connection
import utility
from . import ta_utility
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def process_data_ta(
    df,
    locations_config,
    number_of_consec_days_before_ot,
    min_wage,
    ot_day_max,
    ot_week_max,
    dt_day_max,
    first_date,
    clientId,
    processed_waiver_df=None,
    processed_wfn_df=None,
):

    # Drops rows that are not punches (i.e. NA In/Out Punch)
    df = df.dropna(subset=["In Punch", "Out Punch"]).copy()

    # Updated df: Assure timestamps are in Panda's datetime format
    df = utility.to_pandas_datetime(df, "In Punch", "Out Punch", "Date/Time")

    # Normalize and create new Date column
    df["Date"] = df["In Punch"].dt.normalize()

    # Updated df: Adds "Total Worked Hours Workday" col.
    df = ta_utility.add_total_hours_workday(df)

    # Updated df: Add time helper columns
    df = ta_utility.add_time_helper_cols(df)

    # Updated df: Add Break Credit from WFN File.
    df = ta_utility.add_col_from_another_df(
        home_df=df,
        lookup_df=processed_wfn_df,
        home_ref="ID",
        lookup_ref="IDX",
        lookup_tgt="J_Break Credits_Additional Hours",
        home_new_col="Paid Break Credit (hrs)",
    )

    # Updated df: Adds Short ID, Waiver Lookup, Waiver on File? cols
    df = ta_utility.add_waiver_check(df, processed_waiver_df)

    # Updated df: Adds breaks check columns
    df = ta_utility.add_break_time(df)

    # Updated df: Add Hours Worked Shift and Shift ID, 12 hour check
    df = ta_utility.add_hours_worked_shift_and_shift_id(df)
    df = ta_utility.add_twelve_hour_check(df)

    # Updated df: Add Regular Rate Paid (a.k.a "Straight Rate ($)") from wfn, Split Paid ($),
    # Split at Min Wage ($), Split Shift Due ($) cols.
    df = ta_utility.add_split_shift(df, processed_wfn_df, min_wage)

    # BY PUNCH DF ######################################

    # New df: A reduced col df with daily and add DT and OT calc cols
    bypunch_df = ta_utility.create_bypunch(
        df, locations_config, ot_day_max, ot_week_max, dt_day_max, first_date
    )

    #  = time.time()
    # Updated df: Adds col "Hours in Consecutive Days" and "First day of Streak".
    bypunch_df = ta_utility.add_seventh_day_hours(
        bypunch_df, locations_config, number_of_consec_days_before_ot
    )  # SLOW
    # print(f"7th day hours: {time.time()-t7:.2f}s")

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

    # Updated df: Add Punch Length df which adds staples midnight punches
    df = ta_utility.add_punch_length(df)

    # Create new anomalies DF
    anomalies_df_new = ta_utility.create_anomalies_new(df)

    # Attempt connection to dababase and save
    conn = get_db_connection()
    if conn:
        try:
            save_to_database_fast(df, "ta", clientId, conn)
        except Exception as e:
            logger.error(f"Failed to save to database: {e}")
        finally:
            conn.close()
    else:
        # We just log it and move on, we don't 'return' here
        logger.warning(
            "DB is paused. Skipping the save step, but continuing with the response."
        )

    # Always execute this
    return (df, bypunch_df, anomalies_df_new)
