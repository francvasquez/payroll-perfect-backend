import json, base64, io
import pandas as pd
from waiver.waiver_process import process_waiver
from wfn.wfn_process import process_data_wfn
from ta.ta_process import process_data_ta
from config import *


def lambda_handler(event, context):
    if event.get("httpMethod") == "OPTIONS":
        return {"statusCode": 200, "headers": CORS_HEADERS, "body": ""}
    try:
        # Check if this is a real request with files
        if event.get("body"):
            body = json.loads(event["body"])

            # Get parameters from request
            min_wage = body.get("min_wage", 15.00)
            min_wage_40 = body.get("min_wage_40", 22.50)
            ot_day_max = body.get("ot_day_max", 8)

            # Process uploaded files (Base64 encoded)
            if "waiver_file" in body:
                waiver_content = base64.b64decode(body["waiver_file"])
                # Waiver has headers on row 1 (default)
                waiver_df = pd.read_excel(io.BytesIO(waiver_content))
                processed_waiver_df = process_waiver(waiver_df)

            if "wfn_file" in body:
                wfn_content = base64.b64decode(body["wfn_file"])
                # WFN has headers on row 6 (0-indexed = row 5)
                wfn_df = pd.read_excel(io.BytesIO(wfn_content), header=5)
                processed_wfn_df = process_data_wfn(wfn_df, min_wage, min_wage_40)

            if "ta_file" in body:
                ta_content = base64.b64decode(body["ta_file"])
                # TA has headers on row 8 (0-indexed = row 7)
                ta_df = pd.read_excel(io.BytesIO(ta_content), header=7)
                # Process TA with both waiver and wfn data
                df, bypunch_df, stapled_df, anomalies_df = process_data_ta(
                    ta_df, min_wage, ot_day_max, processed_waiver_df, processed_wfn_df
                )

                # Return processed data
                return {
                    "statusCode": 200,
                    "headers": CORS_HEADERS,
                    "body": json.dumps(
                        {
                            "success": True,
                            "summary": {
                                "ta_rows": len(df),
                                "anomalies": len(anomalies_df),
                                "bypunch_rows": len(bypunch_df),
                                "stapled_rows": len(stapled_df),
                            },
                            # Include sample data for UI
                            "anomalies_sample": (
                                anomalies_df.head(10).to_dict("records")
                                if len(anomalies_df) > 0
                                else []
                            ),
                        }
                    ),
                }

        # Otherwise run test data (your existing code)
        # ... existing test code ...

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": str(e)}),
        }
