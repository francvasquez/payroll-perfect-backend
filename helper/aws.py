# lambda_function.py - Add this before lambda_handler
import json, boto3, io
import pandas as pd
from config import S3_BUCKET, CORS_HEADERS
from io import StringIO

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
        s3_path = body.get("s3Path")  # full S3 path if provided

        # Create S3 key
        s3_key = f"{s3_path}/{file_name}"

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


def save_csv_to_s3(df, file_type, event, s3_client=None):

    ## Save DataFrame as CSV file to S3 following the folder structure.

    if s3_client is None:
        s3_client = boto3.client("s3")

    body = json.loads(event.get("body", "{}"))
    payDate = body.get("pay_date")
    clientID = body.get("client_id")

    # Determine S3 path based on file type
    if file_type == "waiver":
        s3_key = f"clients/{clientID}/waiver/waiver.csv"
    else:
        # Changed from 'parquet' to 'csv' folder
        s3_key = f"clients/{clientID}/csv/{payDate}/{file_type}.csv"

    # Convert DataFrame to CSV string
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    # Upload to S3
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=csv_buffer.getvalue(),
        ContentType="text/csv",
    )

    print(f"Saved {file_type} as CSV to: s3://{S3_BUCKET}/{s3_key}")
    return s3_key


def save_waiver_json_s3(df, file_type, event, s3_client=None):

    if s3_client is None:
        s3_client = boto3.client("s3")

    body = json.loads(event.get("body", "{}"))
    clientID = body.get("client_id")

    # Determine S3 path based on file type
    if file_type == "waiver":
        s3_key = f"clients/{clientID}/waiver/waiver.json"
    else:
        return

    # Convert dataframe to JSON (orient="records" makes an array of dicts)
    json_body = df.to_json(orient="records", date_format="iso")

    # Upload to S3
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json_body,
        ContentType="application/json",
    )

    print(f"Saved {file_type} as JSON to: s3://{S3_BUCKET}/{s3_key}")
    return s3_key


def put_result_to_s3(
    result: dict,
    event,
    s3_client=None,
):
    if s3_client is None:
        s3_client = boto3.client("s3")

    body = json.loads(event.get("body", "{}"))
    payDate = body.get("pay_date")
    clientID = body.get("client_id")
    s3_key = f"clients/{clientID}/processed/{payDate}/result.json"

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(result),
        ContentType="application/json",
    )
    print(f"Saved 'result' as JSON to: s3://{S3_BUCKET}/{s3_key}")
    return s3_key


def save_table_json_s3(
    df,
    name,
    event,
    ##mask, //To include masks later for other tables.
    s3_client=None,
):

    if s3_client is None:
        s3_client = boto3.client("s3")

    body = json.loads(event.get("body", "{}"))
    payDate = body.get("pay_date")
    clientID = body.get("client_id")
    s3_key = f"clients/{clientID}/processed/{payDate}/{name}.json"

    # Convert dataframe to JSON (orient="records" makes an array of dicts)
    json_body = df.to_json(orient="records", date_format="iso")

    # Upload to S3
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json_body,
        ContentType="application/json",
    )

    print(f"Saved {name} as JSON to: s3://{S3_BUCKET}/{s3_key}")
    return s3_key
