import pandas as pd
from config import (
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


def handle_file_processing(event, params):
    """
    Processes all three files in sequence: Waiver → WFN → TA
    Frontend ensures all three files are provided
    """
    try:
        ### 1. Extract client_config from request body
        client_config = params["client_config"]
        global_config = client_config.get("global", {})
        locations_config = client_config.get("locations", {})  ## overrides

        ### 2. Extract global parameters with default fallback (TODO consolidate)
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

        ### 3. Extract pay_date and calculate first_date
        pay_date = pd.to_datetime(params["payDate"])
        first_date = (
            pay_date
            - pd.Timedelta(days=days_bet_payroll_end_and_pay_date)
            - pd.Timedelta(days=pay_period_length)
            + pd.Timedelta(days=1)
        )
        print(
            f"file_processor.py - Processing: client_config={client_config}, pay_date={pay_date}, first date ={first_date}"
        )

        ### 4. Verify all three files are provided (they should be from frontend)
        error_response = verify_files(params)
        if error_response:
            return error_response

        # Delete existing annotations before reprocessing
        if clientId and pay_Date:
            print(
                f"Deleting annotations for {clientId}/{payDate} before reprocessing..."
            )
            delete_result = delete_annotations(clientId, payDate)

        # Process Waiver FIRST
        waiver_df = read_excel_from_s3(body["waiver_key"])
        waiver_start = time.time()
        processed_waiver_df = process_waiver(waiver_df)
        waiver_process_time = round((time.time() - waiver_start) * 1000, 2)
        print(f"Waiver processed: {len(processed_waiver_df)} rows")

        # Process WFN SECOND
        wfn_df = read_excel_from_s3(body["wfn_key"], header=5)
        wfn_start = time.time()
        processed_wfn_df = process_data_wfn(
            wfn_df, locations_config, min_wage, state_min_wage, pay_periods_per_year
        )
        wfn_process_time = round((time.time() - wfn_start) * 1000, 2)
        print(f"WFN processed: {len(processed_wfn_df)} rows")

        # Process TA THIRD (using results from first two)
        ta_df, system_name, system_config = read_excel_from_s3(body["ta_key"], clientId)
        print(
            f"Will normalize for system: {system_name}, using {system_config} for client: {clientId}"
        )
        ta_start = time.time()
        processed_ta_df, bypunch_df, anomalies_df_new = process_data_ta(
            ta_df,
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
            processed_waiver_df,  # From step 1
            processed_wfn_df,  # From step 2
        )
        ta_process_time = round((time.time() - ta_start) * 1000, 2)
        print("TA processed")

        # Store raw files to csv for future reference
        if ta_df is not None:
            save_csv_to_s3(ta_df, "ta", event)
        if wfn_df is not None:
            save_csv_to_s3(wfn_df, "wfn", event)
        if waiver_df is not None:
            save_csv_to_s3(waiver_df, "waiver", event)

        # Store json files for ready-to-serve front consumption
        save_waiver_json_s3(waiver_df, "waiver", event)

        # Generate result
        result = generate_results(
            processed_ta_df,
            anomalies_df_new,
            bypunch_df,
            processed_wfn_df,
            processed_waiver_df,
            ta_process_time,
            wfn_process_time,
            waiver_process_time,
        )

        # Save result as JSON to S3 for ready-to-serve consumption
        put_result_to_s3(result, event)

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
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": str(e)}),
        }
