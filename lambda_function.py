import json
import pandas as pd
from waiver.waiver_process import process_waiver
from wfn.wfn_process import process_data_wfn
from ta.ta_process import process_data_ta
from config import *
from helper.aws import read_excel_from_s3, generate_presigned_url


def lambda_handler(event, context):
    # Presigned URL route (returns URL for direct S3 upload)
    if event.get("path") == "/get-upload-url":
        return generate_presigned_url(event, context)

    # File processing route (returns processed dataframes)
    if event.get("httpMethod") == "OPTIONS":
        return {"statusCode": 200, "headers": CORS_HEADERS, "body": ""}
    try:
        # Check if this is a real request with files
        if event.get("body"):
            body = json.loads(event["body"])

            # Get parameters from request
            min_wage = body.get("min_wage", DEFAULT_MIN_WAGE)
            min_wage_40 = body.get("min_wage_40", DEFAULT_MIN_WAGE_40)
            ot_day_max = body.get("ot_day_max", DEFAULT_OT_DAY_MAX)

            # Process files from S3
            if "waiver_key" in body:
                waiver_key = body["waiver_key"]
                waiver_df = read_excel_from_s3(waiver_key)
                processed_waiver_df = process_waiver(waiver_df)

            if "wfn_key" in body:
                wfn_key = body["wfn_key"]
                wfn_df = read_excel_from_s3(wfn_key, header=5)
                processed_wfn_df = process_data_wfn(wfn_df, min_wage, min_wage_40)

            if "ta_key" in body:
                ta_key = body["ta_key"]
                ta_df = read_excel_from_s3(ta_key, header=7, engine="openpyxl")
                df, bypunch_df, stapled_df, anomalies_df = process_data_ta(
                    ta_df, min_wage, ot_day_max, processed_waiver_df, processed_wfn_df
                )  # SLOWWW - check add_seventh_day_hours function @ 2 seconds

                result = {
                    "success": True,
                    "summary": {
                        "ta_rows": len(df),
                        "anomalies": len(anomalies_df),
                        "bypunch_rows": len(bypunch_df),
                    },
                    "anomalies_sample": (
                        anomalies_df.head(100).to_dict("records")
                        if len(anomalies_df) > 0
                        else []
                    ),
                }
                response_body = json.dumps(result)

                return {
                    "statusCode": 200,
                    "headers": CORS_HEADERS,
                    "body": response_body,
                }

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": str(e)}),
        }
