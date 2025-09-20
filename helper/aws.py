# lambda_function.py - Add this before lambda_handler
import json, boto3, io
from datetime import datetime
import pandas as pd
from config import S3_BUCKET, CORS_HEADERS

s3_client = boto3.client("s3")


def read_excel_from_s3(key, header=0, engine=None):
    """Reads Excel file from S3 into pandas DataFrame"""
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
    file_bytes = io.BytesIO(obj["Body"].read())
    return pd.read_excel(file_bytes, header=header, engine=engine)


def handle_presigned_url_request(event):
    """
    Generates presigned URL for direct S3 upload
    This is called when React wants to upload a file
    """
    try:
        # Parse request body
        body = json.loads(event.get("body", "{}"))
        file_name = body.get("fileName")
        # file_type = body.get("fileType")  # 'waiver', 'wfn', or 'ta'
        s3_path = body.get("s3Path")  # full S3 path if provided
        # client_id = body.get("clientId")
        # pay_date = body.get("payDate")    # string, e.g. "2025-09-19"

        # Create S3 key
        s3_key = f"{s3_path}/{file_name}"
        # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # s3_key = f"uploads/{file_type}/{timestamp}_{file_name}"

        # Generate presigned URL for PUT
        presigned_url = s3_client.generate_presigned_url(
            "put_object",
            Params={
                "Bucket": S3_BUCKET,
                "Key": s3_key,
                "ContentType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            },
            ExpiresIn=300,  # 5 minutes
        )

        return {
            "statusCode": 200,
            "headers": CORS_HEADERS,
            "body": json.dumps({"uploadUrl": presigned_url, "s3Key": s3_key}),
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps(
                {"error": str(e), "s3Key": s3_key if "s3_key" in locals() else None}
            ),
        }
