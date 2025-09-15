import json, base64, io
import pandas as pd
from waiver.waiver_process import process_waiver
from wfn.wfn_process import process_data_wfn
from ta.ta_process import process_data_ta
from config import *
import time, os


def lambda_handler(event, context):
    total_start = time.time()
    print(f"Starting lambda_handler")  # DEBUG

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
                waiver_df = pd.read_excel(io.BytesIO(waiver_content))
                processed_waiver_df = process_waiver(waiver_df)

            if "wfn_file" in body:
                wfn_content = base64.b64decode(body["wfn_file"])
                wfn_df = pd.read_excel(io.BytesIO(wfn_content), header=5)
                processed_wfn_df = process_data_wfn(wfn_df, min_wage, min_wage_40)

            if "ta_file" in body:
                print(f"Starting process for TA file")

                t1 = time.time()
                ta_content = base64.b64decode(body["ta_file"])
                print(f"Base64 decode: {time.time()-t1:.2f}s")
                print(f"TA decoded size: {len(ta_content)} bytes")

                t2 = time.time()
                ta_df = pd.read_excel(
                    io.BytesIO(ta_content), engine="openpyxl", header=7
                )  # SLOWWW
                print(f"TA df rows: {len(ta_df)}")
                print(f"Excel read: {time.time()-t2:.2f}s")

                t3 = time.time()
                df, bypunch_df, stapled_df, anomalies_df = process_data_ta(
                    ta_df, min_wage, ot_day_max, processed_waiver_df, processed_wfn_df
                )
                print(f"process_data_ta time: {time.time()-t3:.2f}s")

                t4 = time.time()
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
                print(f"Result creation: {time.time()-t4:.2f}s")

                t5 = time.time()
                response_body = json.dumps(result)
                print(f"JSON dumps: {time.time()-t5:.2f}s")

                print(f"TOTAL TIME: {time.time()-total_start:.2f}s")

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
