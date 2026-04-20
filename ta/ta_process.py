from helper.db_utils import (
    get_db_connection,
    worker_save_daily,
    worker_save_ta,
)
from client_config import PP_REQUIRED_COLUMNS, CLIENT_CONFIGS
import utility
from . import ta_utility
import logging
import json
import pandas as pd

# from helper.aws import debug_to_s3
import concurrent.futures

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def process_data_ta(
    df,
    client_params,
    system_config,
    min_wage,
    pay_date,
    clientId,
    processed_waiver_df=None,
    processed_wfn_df=None,
):

    ######### DF CLEANUP AND PREP #################

    # 1. Normalization: Columns Rename, Transform & Drop. Doesn't crash if cols missing.
    df = ta_utility.normalize_client_data(df, system_config)

    # 2. Validation: Check if all neccesary columns are present, if not stop processing.
    missing = [col for col in PP_REQUIRED_COLUMNS["ta"] if col not in df.columns]
    if missing:
        logger.info(f"Columns actually received: {list(df.columns)}")
        error_msg = f"CRITICAL: Missing required columns: {missing}"
        logger.error(error_msg)  # CloudWatch Logs trigger alerts if set up
        raise ValueError(error_msg)  # Raise stops execution in Lambda

    # 3. Re-order 'Core' columns are always first (makes the DB readable)
    other_cols = [col for col in df.columns if col not in PP_REQUIRED_COLUMNS["ta"]]
    df = df[PP_REQUIRED_COLUMNS["ta"] + other_cols]

    # 4. Drops rows that are not punches base on client configuration
    df = ta_utility.drop_rows(df, system_config)

    # 5. Assure timestamps are in Panda's datetime format
    df = utility.to_pandas_datetime(df, "In Punch", "Out Punch", "Status Date")

    # 6. Ensure inputed Pay Date matches the contents of the file
    is_valid, msg = ta_utility.validate_intake_pay_date(
        df, pay_date, client_params, CLIENT_CONFIGS[clientId]["anchor_pay_date"]
    )
    if not is_valid:
        # Return a 400 Bad Request to tell React the user messed up
        return {
            "statusCode": 400,
            "headers": {
                "Access-Control-Allow-Origin": "*",  # Critical so React can read the error!
                "Content-Type": "application/json",
            },
            "body": json.dumps({"error": msg}),  # Pass your detailed string here
        }

    ######### DF PROCESSING #################

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

    # Adds breaks check columns
    df = ta_utility.add_break_time(df)

    # Add Hours Worked Shift and Shift ID, 12 hour check
    df = ta_utility.add_hours_worked_shift_and_shift_id(df)
    df = ta_utility.add_twelve_hour_check(df)

    # Add Punch Length df by adding Punch Lenght (Raw) that have no break in between.
    # Needs  Break Time (min), "Shift Number", "Punch Number in Shift", Punch Length (hrs) Raw
    df = ta_utility.add_punch_length(df)

    # Add Regular Rate Paid (a.k.a "Straight Rate ($)") from wfn, Split Paid ($),
    # Split at Min Wage ($), Split Shift Due ($) cols.
    df = ta_utility.add_split_shift(df, processed_wfn_df, min_wage)

    # Create the Daily dataframe with OT and DT calculations (exclusing 40 hours and consecutive days OT)
    daily_df = ta_utility.create_daily_df(df, client_params)

    # Add to daily_df 40 hours and consecutive days calcs
    daily_df = ta_utility.apply_weekly_rules(daily_df, client_params)

    # Add pay period totals
    daily_df = ta_utility.apply_pay_period_totals(
        daily_df, client_params, CLIENT_CONFIGS[clientId]["anchor_pay_date"]
    )
    # Add OT and DT actually paid from WFN for variance analysis
    daily_df = ta_utility.apply_ot_and_dt_paid_from_wfn(daily_df, processed_wfn_df)

    # Drop workdays that don't belong to the pay period
    daily_df = ta_utility.filter_target_pay_period(daily_df, pay_date)

    # Add reporting columns for consecutive day calcs
    daily_df = ta_utility.add_consec_day_reporting(daily_df, client_params)

    # Create anomalies DF - i.e. Break Credit Summary table
    anomalies_df_new = ta_utility.create_anomalies_new(df)

    # Write to DB TODO Improve error handling
    # 1. Quick ping to see if the DB is awake before spinning up threads
    ping_conn = get_db_connection()

    if ping_conn:
        # Close the ping connection immediately! The threads will get their own.
        ping_conn.close()

        try:
            # 2. Spin up the ThreadPool
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:

                # Submit both jobs to run simultaneously
                future_ta = executor.submit(worker_save_ta, df, clientId, pay_date)
                future_daily = executor.submit(
                    worker_save_daily, daily_df, clientId, pay_date
                )

                # 3. Wait for them to finish.
                # .result() will raise any exceptions that happened inside the threads.
                future_ta.result()
                future_daily.result()

        except Exception as e:
            logger.error(f"Failed to save to database concurrently: {e}")
    else:
        # DB is paused fallback
        logger.warning(
            "DB is paused. Skipping the save step, but continuing with the response."
        )

    return (
        df,
        daily_df,
        anomalies_df_new,
    )
