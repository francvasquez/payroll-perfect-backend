# lambda_function.py - Add this before lambda_handler
import json, boto3, io
import pandas as pd
from config import S3_BUCKET, CORS_HEADERS
from io import StringIO
from botocore.exceptions import ClientError

s3_client = boto3.client("s3")


def list_pay_periods(client_id):
    """List available pay periods in processed folder"""
    prefix = f"clients/{client_id}/processed/"

    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix, Delimiter="/")

    # Extract dates from folder names
    periods = []
    for obj in response.get("CommonPrefixes", []):
        # Extract date from path like 'clients/demo_client/processed/2025-01-16/'
        date = obj["Prefix"].split("/")[-2]
        periods.append(date)
        print("AVAILABLE PERIODS: ", periods)

    return {"periods": periods}


def load_processed_results(client_id, pay_date):
    """Load the processed JSON from S3"""
    key = f"clients/{client_id}/processed/{pay_date}/results.json"

    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        results = json.loads(response["Body"].read())
        print(f"Loaded results for {pay_date}: ", results)
        return {
            "statusCode": 200,
            "headers": CORS_HEADERS,
            "body": json.dumps({"results": results}),
        }
    except s3_client.exceptions.NoSuchKey:
        print(f"[DEBUG] NoSuchKey: The object does not exist at key: {key}")

        # Optional: list objects under the prefix to see what exists
        prefix = f"clients/{client_id}/processed/{pay_date}/"
        list_response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
        existing_keys = [obj["Key"] for obj in list_response.get("Contents", [])]
        print(f"[DEBUG] Existing objects under prefix {prefix}: {existing_keys}")

        return {
            "statusCode": 404,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": "No processed data for this period"}),
        }

    except Exception as e:
        print(f"[ERROR] Unexpected error loading S3 object {key}: {str(e)}")
        import traceback

        print(traceback.format_exc())
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": str(e)}),
        }


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
    s3_key = f"clients/{clientID}/processed/{payDate}/results.json"

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


def handle_get_client_config(body):
    """Fetch client configuration from S3"""
    client_id = body.get("clientId")

    if not client_id:
        return {
            "statusCode": 400,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
            },
            "body": json.dumps({"error": "clientId is required"}),
        }

    try:
        # Build S3 key path
        config_key = f"clients/{client_id}/config.json"

        print(f"Fetching config from S3: s3://{S3_BUCKET}/{config_key}")

        # Fetch from S3
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=config_key)
        config_data = json.loads(response["Body"].read().decode("utf-8"))

        print(f"Successfully loaded config for client: {client_id}")

        return {
            "statusCode": 200,
            "headers": CORS_HEADERS,
            "body": json.dumps({"config": config_data}),
        }

    except ClientError as e:
        error_code = e.response["Error"]["Code"]

        if error_code == "NoSuchKey":
            print(f"Config file not found for client: {client_id} at {config_key}")
            return {
                "statusCode": 404,
                "headers": CORS_HEADERS,
                "body": json.dumps(
                    {"error": f"Configuration not found for client: {client_id}"}
                ),
            }
        elif error_code == "NoSuchBucket":
            print(f"Bucket not found: {S3_BUCKET}")
            return {
                "statusCode": 500,
                "headers": CORS_HEADERS,
                "body": json.dumps({"error": "Storage bucket not configured"}),
            }
        else:
            print(f"S3 error: {str(e)}")
            return {
                "statusCode": 500,
                "headers": CORS_HEADERS,
                "body": json.dumps({"error": f"Failed to fetch config: {str(e)}"}),
            }

    except json.JSONDecodeError as e:
        print(f"Invalid JSON in config file: {str(e)}")
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
            },
            "body": json.dumps({"error": "Invalid configuration file format"}),
        }

    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
            },
            "body": json.dumps({"error": f"Internal server error: {str(e)}"}),
        }


def handle_save_client_config(body):
    """Save client configuration to S3"""
    client_id = body.get("clientId")
    config = body.get("config")

    if not client_id:
        return {
            "statusCode": 400,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": "clientId is required"}),
        }

    if not config:
        return {
            "statusCode": 400,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": "config is required"}),
        }

    try:
        # Build S3 key path
        config_key = f"clients/{client_id}/config.json"

        print(f"Saving config to S3: s3://{S3_BUCKET}/{config_key}")

        # Save to S3
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=config_key,
            Body=json.dumps(config, indent=2),  # Pretty print with indent
            ContentType="application/json",
        )

        print(f"Successfully saved config for client: {client_id}")

        return {
            "statusCode": 200,
            "headers": CORS_HEADERS,
            "body": json.dumps({"message": "Configuration saved successfully"}),
        }

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        print(f"S3 error: {str(e)}")
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": f"Failed to save config: {str(e)}"}),
        }

    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": f"Internal server error: {str(e)}"}),
        }
