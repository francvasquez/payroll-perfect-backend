from helper.db_utils import save_ta_to_db, get_db_connection
import utility
from . import ta_utility
import logging
import client_config
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def process_data_ta(
    df,
    locations_config,
    system_config,
    number_of_consec_days_before_ot,
    min_wage,
    ot_day_max,
    ot_week_max,
    dt_day_max,
    first_date,
    pay_date,
    clientId,
    processed_waiver_df=None,
    processed_wfn_df=None,
):

    ######### DF CLEANUP AND PREP #################

    # 1. Normalization: Columns Rename, Transform & Drop. Doesn't crash if cols missing.
    df = ta_utility.normalize_client_data(df, system_config)

    # 2. Validation: Check if all neccesary columns are present, if not stop processing.
    missing = [
        col for col in client_config.PP_REQUIRED_COLUMNS["ta"] if col not in df.columns
    ]
    if missing:
        logger.info(f"Columns actually received: {list(df.columns)}")
        error_msg = f"CRITICAL: Missing required columns: {missing}"
        logger.error(error_msg)  # CloudWatch Logs trigger alerts if set up
        raise ValueError(error_msg)  # Raise stops execution in Lambda

    # 3. Re-order 'Core' columns are always first (makes the DB readable)
    other_cols = [
        col for col in df.columns if col not in client_config.PP_REQUIRED_COLUMNS["ta"]
    ]
    df = df[client_config.PP_REQUIRED_COLUMNS["ta"] + other_cols]

    # 4. Drops rows that are not punches base on CLIENT_CONFIGS
    # df = df.dropna(subset=["In Punch", "Out Punch"]).copy()
    df = ta_utility.drop_rows(df, system_config)

    ######### DF PROCESSING #################

    # Assure timestamps are in Panda's datetime format
    df = utility.to_pandas_datetime(df, "In Punch", "Out Punch", "Status Date")

    # Add Location. TODO Base on Client Settings for scalability
    df["Location"] = df["ID"].str[:3]

    # Normalize and create new Date column
    df["Date"] = df["In Punch"].dt.normalize()

    # Create unstapled Punch Length
    df["Punch Length (hrs) Raw"] = (df["Out Punch"] - df["In Punch"]) / pd.Timedelta(
        hours=1
    )

    # Add time helper columns
    df = ta_utility.add_time_helper_cols(df)

    # Add Break Credit from WFN File.
    df = ta_utility.add_col_from_another_df(
        home_df=df,
        lookup_df=processed_wfn_df,
        home_ref="ID",
        lookup_ref="IDX",
        lookup_tgt="J_Break Credits_Additional Hours",
        home_new_col="Paid Break Credit (hrs)",
    )

    # Add Hire Date from WFN File.
    df = ta_utility.add_col_from_another_df(
        home_df=df,
        lookup_df=processed_wfn_df,
        home_ref="ID",
        lookup_ref="IDX",
        lookup_tgt="HIREDATE",
        home_new_col="Hire Date",
    )

    # Adds Short ID, Waiver Lookup, Waiver on File? cols
    df = ta_utility.add_waiver_check(df, processed_waiver_df)

    # Updated df: Adds breaks check columns
    df = ta_utility.add_break_time(df)

    # Updated df: Add Hours Worked Shift and Shift ID, 12 hour check
    df = ta_utility.add_hours_worked_shift_and_shift_id(df)
    df = ta_utility.add_twelve_hour_check(df)

    # Updated df: Add Punch Length df which adds stapled midnight punches
    # Needs  Break Time (min), "Shift Number", "Punch Number in Shift", Punch Length (hrs) Raw
    df = ta_utility.add_punch_length(df)

    # Updated df: Add Regular Rate Paid (a.k.a "Straight Rate ($)") from wfn, Split Paid ($),
    # Split at Min Wage ($), Split Shift Due ($) cols.
    df = ta_utility.add_split_shift(df, processed_wfn_df, min_wage)

    df = ta_utility.add_ot_and_dt_cols(
        df, locations_config, ot_day_max, ot_week_max, dt_day_max, first_date
    )
    df = ta_utility.add_seventh_day_hours(
        df, locations_config, number_of_consec_days_before_ot
    )  # SLOW
    df = ta_utility.add_col_from_another_df(
        home_df=df,
        lookup_df=processed_wfn_df,
        home_ref="ID",
        lookup_ref="IDX",
        lookup_tgt="OT",
        home_new_col="OT Hours Paid",
    )
    df = ta_utility.add_col_from_another_df(
        home_df=df,
        lookup_df=processed_wfn_df,
        home_ref="ID",
        lookup_ref="IDX",
        lookup_tgt="DBLTIME HRS",
        home_new_col="DT Hours Paid",
    )

    # Updated df: Add OT vs WFN variances cols.
    df["OT Variance (hrs)"] = (
        (df["Total OT Hours Pay Period"] - df["OT Hours Paid"])
    ).round(4)
    df["DT Variance (hrs)"] = (
        (df["Total DT Hours Pay Period"] - df["DT Hours Paid"])
    ).round(4)
    # BY PUNCH DF ######################################

    # New df: A reduced col df with daily and add DT and OT calc cols
    # bypunch_df = ta_utility.create_bypunch(
    #     df, locations_config, ot_day_max, ot_week_max, dt_day_max, first_date
    # )

    # Updated df: Adds col "Hours in Consecutive Days" and "First day of Streak".
    # bypunch_df = ta_utility.add_seventh_day_hours(
    #     bypunch_df, locations_config, number_of_consec_days_before_ot
    # )  # SLOW
    # print(f"7th day hours: {time.time()-t7:.2f}s")

    # Updated df: Add OT and DT columns from WFN

    # bypunch_df = ta_utility.add_col_from_another_df(
    #     home_df=bypunch_df,
    #     lookup_df=processed_wfn_df,
    #     home_ref="ID",
    #     lookup_ref="IDX",
    #     lookup_tgt="OT",
    #     home_new_col="OT Hours Paid",
    # )
    # bypunch_df = ta_utility.add_col_from_another_df(
    #     home_df=bypunch_df,
    #     lookup_df=processed_wfn_df,
    #     home_ref="ID",
    #     lookup_ref="IDX",
    #     lookup_tgt="DBLTIME HRS",
    #     home_new_col="DT Hours Paid",
    # )

    # # Updated df: Add OT vs WFN variances cols.
    # bypunch_df["OT Variance (hrs)"] = (
    #     (bypunch_df["Total OT Hours Pay Period"] - bypunch_df["OT Hours Paid"])
    # ).round(4)
    # bypunch_df["DT Variance (hrs)"] = (
    #     (bypunch_df["Total DT Hours Pay Period"] - bypunch_df["DT Hours Paid"])
    # ).round(4)

    # Create new anomalies DF
    anomalies_df_new = ta_utility.create_anomalies_new(df)

    # Write to DB TODO Improve error handling
    conn = get_db_connection()
    if conn:
        try:
            save_ta_to_db(df, clientId, pay_date, conn)
        except Exception as e:
            logger.error(f"Failed to save to database: {e}")
        finally:
            conn.close()
    else:
        # We just log it and move on, we don't 'return' here
        logger.warning(
            "DB is paused. Skipping the save step, but continuing with the response."
        )

    return (
        df,
        # bypunch_df,
        anomalies_df_new,
    )
