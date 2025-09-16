import json
import pandas as pd
from waiver.waiver_process import process_waiver
from wfn.wfn_process import process_data_wfn
from ta.ta_process import process_data_ta
from config import *
from helper.aws import read_excel_from_s3, handle_presigned_url_request


def lambda_handler(event, context):
    # Main entry point - routes request to approapriate function

    # DEBUG: Log everything about the request
    print(f"Full event: {json.dumps(event)}")

    # Handle CORS preflight
    if (
        event.get("httpMethod") == "OPTIONS"
        or event.get("requestContext", {}).get("http", {}).get("method") == "OPTIONS"
    ):
        print("Returning OPTIONS response for CORS")
        return {"statusCode": 200, "headers": CORS_HEADERS, "body": ""}

    try:
        # Parse the body to check for action
        body = json.loads(event.get("body", "{}"))
        action = body.get("action")

        # Routing
        if action == "get-upload-url":
            print("Routing to presigned URL handler")
            return handle_presigned_url_request(event, context)
        else:
            print("Routing to file processing handler")
            return handle_file_processing(event, context)

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

        # Get processing parameters
        min_wage = body.get("min_wage", DEFAULT_MIN_WAGE)
        min_wage_40 = body.get("min_wage_40", DEFAULT_MIN_WAGE_40)
        ot_day_max = body.get("ot_day_max", DEFAULT_OT_DAY_MAX)

        print(
            f"Processing with parameters: min_wage={min_wage}, min_wage_40={min_wage_40}, ot_day_max={ot_day_max}"
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
        print(f"Reading waiver from S3: {body['waiver_key']}")
        waiver_df = read_excel_from_s3(body["waiver_key"])
        processed_waiver_df = process_waiver(waiver_df)
        print(f"Waiver processed: {len(processed_waiver_df)} rows")

        # Process WFN SECOND
        print(f"Reading WFN from S3: {body['wfn_key']}")
        wfn_df = read_excel_from_s3(body["wfn_key"], header=5)
        processed_wfn_df = process_data_wfn(wfn_df, min_wage, min_wage_40)
        print(f"WFN processed: {len(processed_wfn_df)} rows")

        # Process TA THIRD (using results from first two)
        print(f"Reading TA from S3: {body['ta_key']}")
        ta_df = read_excel_from_s3(body["ta_key"], header=7)
        print(f"TA loaded: {len(ta_df)} rows")

        # Run TA processing with dependencies
        df, bypunch_df, stapled_df, anomalies_df = process_data_ta(
            ta_df,
            min_wage,
            ot_day_max,
            processed_waiver_df,  # From step 1
            processed_wfn_df,  # From step 2
        )

        # Return complete results
        result = {
            "success": True,
            "summary": {
                "ta_rows": len(df),
                "anomalies": len(anomalies_df),
                "bypunch_rows": len(bypunch_df),
            },
            "anomalies_sample": (
                anomalies_df.head(50).to_dict("records")
                if len(anomalies_df) > 0
                else []
            ),
        }

        return {"statusCode": 200, "headers": CORS_HEADERS, "body": json.dumps(result)}

    except Exception as e:
        print(f"Error processing files: {str(e)}")
        import traceback

        print(traceback.format_exc())
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": str(e)}),
        }
