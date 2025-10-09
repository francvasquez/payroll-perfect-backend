import json
import time
from waiver.waiver_process import process_waiver
from wfn.wfn_process import process_data_wfn
from ta.ta_process import process_data_ta
from config import *
from helper.aws import (
    handle_get_client_config,
    handle_save_client_config,
    read_excel_from_s3,
    handle_presigned_url_request,
    save_csv_to_s3,
    save_waiver_json_s3,
    put_result_to_s3,
    load_processed_results,
    list_pay_periods,
)
from helper.results import generate_results
import pandas as pd


def lambda_handler(event, context):

    # Handle CORS preflight
    if (
        event.get("httpMethod") == "OPTIONS"
        or event.get("requestContext", {}).get("http", {}).get("method") == "OPTIONS"
    ):
        print("Preflight: Returning OPTIONS response for CORS")
        return {"statusCode": 200, "headers": CORS_HEADERS, "body": ""}

    # Handle actual request (either presigned URL or file processing)
    try:
        # Parse the body to check for action
        body = json.loads(event.get("body", "{}"))
        action = body.get("action")
        clientId = body.get("clientId")
        payDate = body.get("payDate")

        print("ACTION: ", action, "RAW EVENT: ", json.dumps(event))

        # Routing
        if action == "get-client-config":
            return handle_get_client_config(body)
        if action == "save-client-config":
            return handle_save_client_config(body)
        if action == "list-pay-periods":
            return list_pay_periods(clientId)
        if action == "load-processed-results":
            return load_processed_results(clientId, payDate)
        if action == "get-upload-url":
            return handle_presigned_url_request(event)
        else:
            return handle_file_processing(event)

    except Exception as e:
        print(f"Error in lambda_handler: {str(e)}")
        import traceback

        print(traceback.format_exc())
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": str(e)}),
        }


def handle_file_processing(event):
    """
    Processes all three files in sequence: Waiver → WFN → TA
    Frontend ensures all three files are provided
    """
    try:
        # Parse request body
        body = json.loads(event.get("body", "{}"))

        # Extract client_config from request body
        client_config = body.get("client_config", {})
        global_config = client_config.get("global", {})
        locations_config = client_config.get("locations", {})  ## overrides

        # Extract global parameters with default fallback
        min_wage = global_config.get("min_wage", DEFAULT_MIN_WAGE)
        min_wage_40 = global_config.get("min_wage_40", DEFAULT_MIN_WAGE_40)
        ot_day_max = global_config.get("ot_day_max", DEFAULT_OT_DAY_MAX)
        ot_week_max = global_config.get("ot_week_max", DEFAULT_OT_WEEK_MAX)
        dt_day_max = global_config.get("dt_day_max", DEFAULT_DT_DAY_MAX)

        # Variables for future use
        # workweek_start = global_config.get("workweek_start", DEFAULT_WORKWEEK_START)
        # exempt_min_annual_wage = global_config.get(
        #     "exempt_min_annual_wage", DEFAULT_EXEMPT_MIN_ANNUAL_WAGE
        # )
        # number_of_consec_days_before_ot = global_config.get(
        #     "number_of_consec_days_before_ot", DEFAULT_CONSEC_DAYS
        # )
        # consec_days_workweek = global_config.get(
        #     "consec_days_workweek", DEFAULT_CONSEC_DAYS_WORKWEEK
        # )

        # Extract First date of pay period and convert to pandas
        first_date = pd.to_datetime(body.get("pay_date"))

        print(
            f"Processing with parameters: client_config={client_config}, min_wage={min_wage}, first date ={first_date}"
        )

        # Verify all three files are provided (they should be from frontend)
        if not all(k in body for k in ["waiver_key", "wfn_key", "ta_key"]):
            return {
                "statusCode": 400,
                "headers": CORS_HEADERS,
                "body": json.dumps(
                    {"error": "All three files (waiver, wfn, ta) are required"}
                ),
            }

        # Process Waiver FIRST
        waiver_df = read_excel_from_s3(body["waiver_key"])
        waiver_start = time.time()
        processed_waiver_df = process_waiver(waiver_df)
        waiver_process_time = round((time.time() - waiver_start) * 1000, 2)
        print(f"Waiver processed: {len(processed_waiver_df)} rows")

        # Process WFN SECOND
        print(f"Reading WFN from S3: {body['wfn_key']}")
        wfn_df = read_excel_from_s3(body["wfn_key"], header=5)
        print(f"WFN read from Excel: {len(wfn_df)} rows")
        wfn_start = time.time()
        processed_wfn_df = process_data_wfn(
            wfn_df, locations_config, min_wage, min_wage_40
        )
        wfn_process_time = round((time.time() - wfn_start) * 1000, 2)
        print(f"WFN processed: {len(processed_wfn_df)} rows")

        # Process TA THIRD (using results from first two)
        print(f"Reading TA from S3: {body['ta_key']}")
        ta_df = read_excel_from_s3(body["ta_key"], header=7)
        print(f"TA read from Excel: {len(ta_df)} rows")
        ta_start = time.time()
        processed_ta_df, bypunch_df, stapled_df, anomalies_df = process_data_ta(
            ta_df,
            locations_config,
            min_wage,
            ot_day_max,
            ot_week_max,
            dt_day_max,
            first_date,
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
            anomalies_df,
            bypunch_df,
            stapled_df,
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

    except Exception as e:
        print(f"Error processing files: {str(e)}")
        import traceback

        print(traceback.format_exc())
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": str(e)}),
        }
