import pandas as pd
from app_config import (
    DEFAULT_PAY_PERIOD_LENGTH,
    DEFAULT_DAYS_BET_PAYROLL_END_AND_PAY_DATE,
    DEFAULT_MIN_WAGE,
    DEFAULT_STATE_MIN_WAGE,
    DEFAULT_DT_DAY_MAX,
    DEFAULT_OT_DAY_MAX,
    DEFAULT_PAY_PERIODS_PER_YEAR,
    DEFAULT_OT_WEEK_MAX,
    DEFAULT_CONSEC_DAYS_BEFORE_OT,
    CORS_HEADERS,
)
from helper.aux import verify_files
from helper.aws import (
    read_excel_from_s3,
    read_ta_excel_from_s3,
    save_csv_to_s3,
    save_waiver_json_s3,
    put_result_to_s3,
    delete_annotations,
)
from helper.results import generate_results
from ta.ta_process import process_data_ta
from waiver.waiver_process import process_waiver
from wfn.wfn_process import process_data_wfn
import json, traceback, time


def handle_file_upload(event, params):
    """
    Processes all three files in sequence: Waiver → WFN → TA
    Frontend ensures all three files are provided
    """
    try:
        ### 1. Verify all three files are provided (they should be from frontend)
        error_response = verify_files(params)
        if error_response:
            return error_response

        ### 2. Extract client_params from params
        client_id = params["clientId"]
        client_params = params["client_config"]
        global_config = client_params.get("global", {})
        locations_config = client_params.get("locations", {})  ## overrides

        ### 3. Extract global parameters with default fallback (TODO consolidate)
        pay_period_length = global_config.get(
            "pay_period_length", DEFAULT_PAY_PERIOD_LENGTH
        )
        days_bet_payroll_end_and_pay_date = global_config.get(
            "days_bet_payroll_end_and_pay_date",
            DEFAULT_DAYS_BET_PAYROLL_END_AND_PAY_DATE,
        )
        min_wage = global_config.get("min_wage", DEFAULT_MIN_WAGE)
        state_min_wage = global_config.get("state_min_wage", DEFAULT_STATE_MIN_WAGE)
        pay_periods_per_year = global_config.get(
            "pay_periods_per_year", DEFAULT_PAY_PERIODS_PER_YEAR
        )
        ot_day_max = global_config.get("ot_day_max", DEFAULT_OT_DAY_MAX)
        ot_week_max = global_config.get("ot_week_max", DEFAULT_OT_WEEK_MAX)
        dt_day_max = global_config.get("dt_day_max", DEFAULT_DT_DAY_MAX)
        number_of_consec_days_before_ot = global_config.get(
            "number_of_consec_days_before_ot", DEFAULT_CONSEC_DAYS_BEFORE_OT
        )

        ### 4. Extract pay_date and calculate first_date
        pay_date = pd.to_datetime(params["payDate"])
        first_date = (
            pay_date
            - pd.Timedelta(days=days_bet_payroll_end_and_pay_date)
            - pd.Timedelta(days=pay_period_length)
            + pd.Timedelta(days=1)
        )
        last_date = pay_date - pd.Timedelta(days=days_bet_payroll_end_and_pay_date)
        print(
            f"file_processor.py - Processing: client_params={client_params}, pay_date={pay_date}, first date ={first_date}"
        )

        ### 5. Delete existing annotations before reprocessing
        if client_id and pay_date:
            print(f"Deleting annotations for {client_id}/{pay_date} b4 reprocessing.")
            delete_annotations(client_id, pay_date)

        ### 6. Process WAIVER
        waiver_df = read_excel_from_s3(params["waiver_key"])
        waiver_start = time.time()
        processed_waiver_df = process_waiver(waiver_df)
        waiver_process_time = round((time.time() - waiver_start) * 1000, 2)
        print(f"Waiver processed: {len(processed_waiver_df)} rows")

        ### 7. Process WFN
        wfn_df = read_excel_from_s3(params["wfn_key"], header=5)
        wfn_start = time.time()
        processed_wfn_df = process_data_wfn(
            wfn_df, locations_config, min_wage, state_min_wage, pay_periods_per_year
        )
        wfn_process_time = round((time.time() - wfn_start) * 1000, 2)
        print(f"WFN processed: {len(processed_wfn_df)} rows")

        ### 8. Process TA (using results from first two)
        ta_df, system_name, system_config = read_ta_excel_from_s3(
            params["ta_key"], client_id
        )
        print(
            f"Will normalize for system: {system_name}, using {system_config} for client: {client_id}"
        )
        ta_start = time.time()
        processed_ta_df, daily_df, anomalies_df_new = process_data_ta(
            ta_df,
            client_params,
            locations_config,
            system_config,
            number_of_consec_days_before_ot,
            min_wage,
            ot_day_max,
            ot_week_max,
            dt_day_max,
            first_date,
            last_date,
            pay_date,
            client_id,
            processed_waiver_df,  # From step 1
            processed_wfn_df,  # From step 2
        )
        ta_process_time = round((time.time() - ta_start) * 1000, 2)
        print("TA processed")

        ### 9. Store raw files to csv for future reference
        if ta_df is not None:
            save_csv_to_s3(ta_df, "ta", event)
        if wfn_df is not None:
            save_csv_to_s3(wfn_df, "wfn", event)
        if waiver_df is not None:
            save_csv_to_s3(waiver_df, "waiver", event)
        # Store json files for ready-to-serve front consumption
        save_waiver_json_s3(waiver_df, "waiver", event)

        ### 10. Generate result for React front-end
        result = generate_results(
            processed_ta_df,
            daily_df,
            anomalies_df_new,
            processed_wfn_df,
            processed_waiver_df,
            ta_process_time,
            wfn_process_time,
            waiver_process_time,
            first_date,
            last_date,
            pay_date,
            client_id,
        )
        put_result_to_s3(
            result, event
        )  # save JSON for ready-to-serve front consumption

        # Return to front end upon uploading files
        return {
            "statusCode": 200,
            "headers": CORS_HEADERS,
            "body": json.dumps(result),
        }
    except ValueError as ve:
        print(f"Value error during file processing: {str(ve)}")
        return {
            "statusCode": 400,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": str(ve)}),
        }

    except Exception as e:
        print(f"Error processing files: {str(e)}")
        traceback.print_exc()
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": "Internal server error"}),
        }
