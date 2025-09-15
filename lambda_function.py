import json, base64, io
import pandas as pd
from waiver.waiver_process import process_waiver
from wfn.wfn_process import process_data_wfn
from ta.ta_process import process_data_ta
from config import *
import time


def lambda_handler(event, context):
    start_time = time.time()
    print(f"Starting backeend processing...")  # DEBUG
    if event.get("httpMethod") == "OPTIONS":
        return {"statusCode": 200, "headers": CORS_HEADERS, "body": ""}
    try:
        # Check if this is a real request with files
        if event.get("body"):
            body_size = len(event["body"])
            print(
                f"Request body size: {body_size} bytes ({body_size/1024/1024:.2f} MB)"
            )  # DEBUG
            body = json.loads(event["body"])

            # Get parameters from request
            min_wage = body.get("min_wage", 15.00)
            min_wage_40 = body.get("min_wage_40", 22.50)
            ot_day_max = body.get("ot_day_max", 8)

            # Process uploaded files (Base64 encoded)
            if "waiver_file" in body:
                print(f"Processing waiver file...")
                waiver_content = base64.b64decode(body["waiver_file"])
                print(f"Waiver decoded size: {len(waiver_content)} bytes")
                # Waiver has headers on row 1 (default)
                waiver_df = pd.read_excel(io.BytesIO(waiver_content))
                print(f"Waiver df rows: {len(waiver_df)}")
                processed_waiver_df = process_waiver(waiver_df)
                print(f"Waiver processed in {time.time()-start_time:.2f}s")

            if "wfn_file" in body:
                print(f"Processing wfn file...")
                wfn_content = base64.b64decode(body["wfn_file"])
                print(f"WFN decoded size: {len(wfn_content)} bytes")
                # WFN has headers on row 6 (0-indexed = row 5)
                wfn_df = pd.read_excel(io.BytesIO(wfn_content), header=5)
                print(f"WFN df rows: {len(wfn_df)}")
                processed_wfn_df = process_data_wfn(wfn_df, min_wage, min_wage_40)
                print(f"WFN processed in {time.time()-start_time:.2f}s")

            if "ta_file" in body:
                print(f"Processing ta file...")
                ta_content = base64.b64decode(body["ta_file"])
                print(f"TA decoded size: {len(ta_content)} bytes")
                # TA has headers on row 8 (0-indexed = row 7)
                ta_df = pd.read_excel(io.BytesIO(ta_content), header=7)
                print(f"TA df rows: {len(ta_df)}")
                # Process TA with both waiver and wfn data
                df, bypunch_df, stapled_df, anomalies_df = process_data_ta(
                    ta_df, min_wage, ot_day_max, processed_waiver_df, processed_wfn_df
                )
                print(f"TA processed in {time.time()-start_time:.2f}s")

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
