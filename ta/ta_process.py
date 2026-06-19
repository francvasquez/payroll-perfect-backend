from helper.db_utils import (
    get_db_connection,
    get_last_db_connection_error,
    worker_save_daily,
    worker_save_ta,
)
from client_config import TA_TARGET_SCHEMA, CLIENT_CONFIGS
import utility
from . import ta_utility
import logging
import pandas as pd
import concurrent.futures
from . import ta_weekly_rules
from exceptions import AppError

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _save_to_database(df, daily_df, clientId, pay_date):
    """
    Attempts to persist punch and daily totals to PostgreSQL.
    Returns a status dict suitable for the frontend — never raises.
    """
    ta_rows = len(df)
    daily_rows = len(daily_df)
    pay_date_ts = pd.Timestamp(pay_date)

    ping_conn = get_db_connection()
    if not ping_conn:
        reason = (
            get_last_db_connection_error()
            or "Database is paused or unavailable."
        )
        return {
            "status": "skipped",
            "message": (
                f"{reason} "
                f"Processed {ta_rows} punches in memory, but nothing was saved to the database. "
                "Re-run intake once the database is available."
            ),
            "ta_rows_attempted": ta_rows,
            "daily_rows_attempted": daily_rows,
        }

    ping_conn.close()

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            future_ta = executor.submit(worker_save_ta, df, clientId, pay_date_ts)
            future_daily = executor.submit(
                worker_save_daily, daily_df, clientId, pay_date_ts
            )
            future_ta.result()
            future_daily.result()

        if daily_rows == 0:
            message = (
                f"Saved {ta_rows} punches to the database. "
                "No daily totals were saved because the daily dataframe was empty."
            )
        else:
            message = (
                f"Saved {ta_rows} punches and {daily_rows} daily totals to the database."
            )

        return {
            "status": "completed",
            "message": message,
            "ta_rows_written": ta_rows,
            "daily_rows_written": daily_rows,
        }

    except Exception as e:
        logger.error(f"Failed to save to database concurrently: {e}")
        return {
            "status": "failed",
            "message": (
                f"Failed to save punches to the database: {e}. "
                "Audit results were generated, but the database may be missing or "
                "partially updated for this pay period. Re-run intake after resolving the issue."
            ),
            "ta_rows_attempted": ta_rows,
            "daily_rows_attempted": daily_rows,
        }


def process_data_ta(
    df,
    client_params,
    ta_system_config,
    min_wage,
    pay_date,
    clientId,
    processed_waiver_df=None,
    processed_wfn_df=None,
    ignore_warnings=False,
):

    ######### DF CLEANUP AND PREP #################

    # 1. Normalization: Columns Rename, Transform & Drop. Doesn't crash if cols missing.
    df = utility.normalize_client_data(df, ta_system_config)

    # 2. Validation: Check if all neccesary columns post-mapping are present, if not stop processing.
    missing = [col for col in TA_TARGET_SCHEMA if col not in df.columns]
    if missing:
        logger.info(f"Columns in ta dataframe post normalization: {list(df.columns)}")
        error_msg = f"CRITICAL: Missing required columns: {missing}"
        logger.error(error_msg)  # CloudWatch Logs trigger alerts if set up
        raise ValueError(error_msg)  # Raise stops execution in Lambda

    # 3. Drops rows that are not punches base on client configuration
    df = utility.drop_rows(df, ta_system_config)

    # 4. Re-order 'Core' columns are always first (makes the DB readable);
    #    drop any intake columns outside the target schema
    df = utility.keep_target_schema_columns(df, TA_TARGET_SCHEMA)

    # 5. Assure timestamps are in Panda's datetime format
    df = utility.to_pandas_datetime(df, "In Punch", "Out Punch", "Status Date")

    # 6. Ensure inputed Pay Date matches the contents of the file
    is_valid, msg, error_type = ta_utility.validate_intake_pay_date(
        df,
        pay_date,
        client_params,
        CLIENT_CONFIGS[clientId]["anchor_pay_date"],
        ignore_warnings,
    )
    if not is_valid:
        if error_type == "STRAGGLER_WARNING":
            # Throw a 409 Conflict! This will open a dialog in React prompting the user if they really want to continue"
            raise AppError(msg, status_code=409)
        else:
            # Standard hard error
            raise AppError(msg, status_code=400)

    ######### DF PROCESSING #################

    # Add Location
    # df["Location"] = df["ID"].str[:3] Moved to normalization step. See client_config.py for details.

    # Normalize and create new Date column
    df["Date"] = df["In Punch"].dt.normalize()

    # Create unstapled Punch Length
    df["Punch Length (hrs) Raw"] = (df["Out Punch"] - df["In Punch"]) / pd.Timedelta(
        hours=1
    )

    # Add time helper columns
    df = ta_utility.add_time_helper_cols(df)

    # Add Break Credit from WFN File.
    df = ta_utility.add_col_from_another_df_if_present(
        home_df=df,
        lookup_df=processed_wfn_df,
        home_ref="ID",
        lookup_ref="IDX",
        lookup_tgt="Break Credit Hours",
        home_new_col="Paid Break Credit (hrs)",
    )

    # Add Hire Date from WFN File.
    df = ta_utility.add_col_from_another_df_if_present(
        home_df=df,
        lookup_df=processed_wfn_df,
        home_ref="ID",
        lookup_ref="IDX",
        lookup_tgt="Hire Date",
        home_new_col="Hire Date",
    )

    # Adds Short ID, Waiver Lookup, Waiver on File? cols
    df = ta_utility.add_waiver_check(df, processed_waiver_df)

    # Adds breaks check columns
    df = ta_utility.add_break_time(df)

    # Add Hours Worked Shift and Shift ID, 12 hour check
    df = ta_utility.add_hours_worked_shift_and_shift_id(df, client_params)
    df = ta_utility.add_twelve_hour_check(df)

    # Add Punch Length df by adding Punch Lenght (Raw) that have no break in between.
    # Needs  Break Time (min), "Shift Number", "Punch Number in Shift", Punch Length (hrs) Raw
    df = ta_utility.add_punch_length(df)

    # Add Regular Rate Paid (a.k.a "Straight Rate ($)") from wfn, Split Paid ($),
    # Split at Min Wage ($), Split Shift Due ($) cols.
    df = ta_utility.add_split_shift(df, processed_wfn_df, min_wage)

    # Add Report Time Warning tag to df (short shift)
    df = ta_utility.add_report_time_warning(df)

    # Create the Daily dataframe with OT and DT calculations (exclusing 40 hours and consecutive days OT)
    daily_df = ta_utility.create_daily_df(df, client_params)

    # Add to daily_df 40 hours and consecutive days calcs. This will make a db call to check for previous periods punches if the employee worked the last day of the previous period and has the cba_consec_anyweek boolean set to true.
    daily_df = ta_weekly_rules.apply_weekly_rules(
        daily_df, client_params, clientId, pay_date
    )

    # Add pay period totals
    daily_df = ta_utility.apply_pay_period_totals(
        daily_df, client_params, CLIENT_CONFIGS[clientId]["anchor_pay_date"]
    )
    # Add OT and DT actually paid from WFN for variance analysis
    daily_df = ta_utility.apply_ot_and_dt_paid_from_wfn(daily_df, processed_wfn_df)

    # Drop workdays that don't belong to the pay period
    daily_df = ta_utility.filter_target_pay_period(daily_df, pay_date)

    # Add reporting columns for consecutive day calcs
    daily_df = ta_utility.add_consec_day_reporting(daily_df)

    # Create anomalies DF - i.e. Break Credit Summary table
    anomalies_df_new = ta_utility.create_anomalies_new(df)

    # Write to DB and capture status for the frontend
    db_write = _save_to_database(df, daily_df, clientId, pay_date)

    return (
        df,
        daily_df,
        anomalies_df_new,
        db_write,
    )
