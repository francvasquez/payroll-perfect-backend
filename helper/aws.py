import json, boto3, io, json
from datetime import datetime, timezone
import pandas as pd
from app_config import S3_BUCKET, CORS_HEADERS
from client_config import CLIENT_CONFIGS
from io import StringIO
from botocore.exceptions import ClientError
from helper.db_utils import delete_ta_from_db, get_db_connection
from exceptions import AppError
from botocore.exceptions import ClientError

s3_client = boto3.client("s3")
ses = boto3.client("ses", region_name="us-west-1")


def debug_to_s3(df, debug_id, debug_cols, bucket_name):
    """Utility function to save any DataFrame to S3 for debugging purposes"""

    # 0. Don't crash if debug_cols not in df, just save the ones that exist
    safe_cols = [col for col in debug_cols if col in df.columns]

    # 1. Filter the DataFrame
    debug_df = df[df["ID"] == debug_id][safe_cols]

    if debug_df.empty:
        print(f"Debug Drop Skipped: No records found for {debug_id}")
        return

    # 2. Convert DataFrame to a CSV string in memory
    csv_buffer = StringIO()
    debug_df.to_csv(csv_buffer, index=False)

    # 3. Create a unique filename with a timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_key = f"debug_outputs/debug_{debug_id}_{timestamp}.csv"

    # 4. Upload directly to S3
    try:
        s3_client.put_object(
            Bucket=bucket_name, Key=file_key, Body=csv_buffer.getvalue()
        )
        print(f"Successfully dropped debug file to s3://{bucket_name}/{file_key}")
    except Exception as e:
        print(f"Failed to write debug file to S3: {e}")


def delete_pay_period(client_id, pay_date):
    """
    Delete all data for a specific pay period
    Removes: processed/, raw/, and csv/ folders for the given pay_date
    """

    # Define all the prefixes (folders) to delete
    prefixes_to_delete = [
        f"clients/{client_id}/processed/{pay_date}/",
        f"clients/{client_id}/raw/{pay_date}/",
        f"clients/{client_id}/csv/{pay_date}/",
    ]

    deleted_files = []
    errors = []

    try:
        for prefix in prefixes_to_delete:
            print(f"Deleting all objects under: s3://{S3_BUCKET}/{prefix}")

            # List all objects under this prefix
            paginator = s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix)

            objects_to_delete = []
            for page in pages:
                if "Contents" in page:
                    for obj in page["Contents"]:
                        objects_to_delete.append({"Key": obj["Key"]})

            # Delete objects in batches (S3 delete_objects supports up to 1000 at a time)
            if objects_to_delete:
                # Delete in chunks of 1000
                for i in range(0, len(objects_to_delete), 1000):
                    chunk = objects_to_delete[i : i + 1000]
                    delete_response = s3_client.delete_objects(
                        Bucket=S3_BUCKET, Delete={"Objects": chunk}
                    )

                    # Track successful deletions
                    if "Deleted" in delete_response:
                        for deleted in delete_response["Deleted"]:
                            deleted_files.append(deleted["Key"])
                            print(f"Deleted: {deleted['Key']}")

                    # Track errors
                    if "Errors" in delete_response:
                        for error in delete_response["Errors"]:
                            errors.append(f"{error['Key']}: {error['Message']}")
                            print(f"Error deleting {error['Key']}: {error['Message']}")

                print(f"Deleted {len(deleted_files)} files from {prefix}")
            else:
                print(f"No files found under {prefix}")

        # Prepare response
        if errors:
            return {
                "statusCode": 207,  # Multi-Status (partial success)
                "headers": CORS_HEADERS,
                "body": json.dumps(
                    {
                        "message": "Pay period partially deleted with some errors",
                        "deleted_count": len(deleted_files),
                        "deleted_files": deleted_files,
                        "errors": errors,
                    }
                ),
            }
        else:
            # Now deleting from database
            conn = get_db_connection()
            db_rows = 0
            if conn:
                try:
                    # Call your new delete function
                    db_rows = delete_ta_from_db(conn, client_id, pay_date)
                except Exception as e:
                    print(
                        f"Failed to delete from database. S3 succeeded but not db: {e}"
                    )
                finally:
                    conn.close()
            else:
                print("DB is paused or unavailable. Skipping DB row deletion.")
            return {
                "statusCode": 200,
                "headers": CORS_HEADERS,
                "body": json.dumps(
                    {
                        "message": f"Pay period {pay_date} deletion processed",
                        "s3_deleted_count": len(deleted_files),
                        "db_rows_affected": db_rows,
                        "db_status": "skipped" if not conn else "completed",
                    }
                ),
            }

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        print(f"S3 error deleting pay period: {str(e)}")
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": f"Failed to delete pay period: {str(e)}"}),
        }

    except Exception as e:
        print(f"Unexpected error deleting pay period: {str(e)}")
        import traceback

        print(traceback.format_exc())
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": f"Internal server error: {str(e)}"}),
        }


def save_annotations(client_id, pay_date, annotations_data):
    """
    Save annotations to S3 with full payload structure
    Path: clients/{client_id}/processed/{pay_date}/annotations.json

    [NEW] Frontend sends just Record<string, TableAnnotations>
    Lambda constructs the full AnnotationsPayload before saving
    """
    s3_key = f"clients/{client_id}/processed/{pay_date}/annotations.json"

    try:
        # [NEW] Construct the full AnnotationsPayload with metadata
        payload = {
            "clientId": client_id,
            "payDate": pay_date,
            "annotations": annotations_data,
            "savedAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }

        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json.dumps(payload, indent=2),
            ContentType="application/json",
        )

        print(f"Saved annotations to: s3://{S3_BUCKET}/{s3_key}")

        return {
            "statusCode": 200,
            "headers": CORS_HEADERS,
            "body": json.dumps({"message": "Annotations saved successfully"}),
        }

    except ClientError as e:
        print(f"S3 error saving annotations: {str(e)}")
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": f"Failed to save annotations: {str(e)}"}),
        }
    except Exception as e:
        print(f"Unexpected error saving annotations: {str(e)}")
        import traceback

        print(traceback.format_exc())
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": f"Internal server error: {str(e)}"}),
        }


def load_annotations(client_id, pay_date):
    """
    Load annotations from S3
    Path: clients/{client_id}/processed/{pay_date}/annotations.json

    [NEW] Returns full AnnotationsPayload (with metadata)
    """
    s3_key = f"clients/{client_id}/processed/{pay_date}/annotations.json"

    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        # [NEW] Load the full AnnotationsPayload
        annotations_payload = json.loads(response["Body"].read().decode("utf-8"))

        print(f"Loaded annotations from: s3://{S3_BUCKET}/{s3_key}")

        # [NEW] Return the entire payload, not wrapped in another "annotations" key
        return {
            "statusCode": 200,
            "headers": CORS_HEADERS,
            "body": json.dumps(annotations_payload),
        }

    except ClientError as e:
        error_code = e.response["Error"]["Code"]

        if error_code == "NoSuchKey":
            print(f"No annotations found for {client_id}/{pay_date}")
            return {
                "statusCode": 200,
                "headers": CORS_HEADERS,
                "body": json.dumps(None),
            }
        else:
            print(f"S3 error loading annotations: {str(e)}")
            return {
                "statusCode": 500,
                "headers": CORS_HEADERS,
                "body": json.dumps({"error": f"Failed to load annotations: {str(e)}"}),
            }

    except json.JSONDecodeError as e:
        print(f"Invalid JSON in annotations file: {str(e)}")
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": "Invalid annotations file format"}),
        }

    except Exception as e:
        print(f"Unexpected error loading annotations: {str(e)}")
        import traceback

        print(traceback.format_exc())
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": f"Internal server error: {str(e)}"}),
        }


def delete_annotations(client_id, pay_date):
    """
    Delete annotations from S3
    Path: clients/{client_id}/processed/{pay_date}/annotations.json
    Called before reprocessing files
    """
    s3_key = f"clients/{client_id}/processed/{pay_date}/annotations.json"

    try:
        s3_client.delete_object(Bucket=S3_BUCKET, Key=s3_key)
        print(f"Deleted annotations from: s3://{S3_BUCKET}/{s3_key}")
        # For React use
        return "Annotations cleared"
    except ClientError as e:
        # If we hit this, it's a real problem (e.g., AccessDenied, KMS issue)
        print(f"S3 error deleting annotations: {e}")
        raise AppError("Failed to delete annotations from storage", status_code=503)


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
        # print("AVAILABLE PERIODS: ", periods)

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
    """Reads WFN or Waiver excel from S3"""
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
    file_bytes = io.BytesIO(obj["Body"].read())
    return pd.read_excel(file_bytes, header=header, engine=engine)


def read_ta_excel_from_s3(key, clientId, engine=None):
    """
    Reads Excel file from S3, auto-detects system, and returns (system_name, df, config)

    Steps:
    1. Download the file from S3 into memory.
    2. Loop through each system defined for the client.
        a. For each system, get its detection header row and required columns.
        b. Read only the header row (or nrows=0) to peek at columns.
        c. Normalize column names (strip whitespace) for robust matching.
        d. Check if all required columns are present.
        e. If matched, read the full Excel file using this system's header.
    3. If no system matches, raise an error.
    """

    # Step 1: Download file into memory
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
    file_bytes = io.BytesIO(obj["Body"].read())

    systems = CLIENT_CONFIGS[clientId]["systems"]

    # Step 2: Loop through systems for detection
    for system_name, config in systems.items():

        # --- a. Detection inputs ---
        header_row = config["detection"]["header"]
        required_cols = config["detection"]["columns"]
        # ----------------------------

        # --- b. Peek at header row only ---
        try:
            file_bytes.seek(0)  # reset buffer before each read
            df_header = pd.read_excel(
                file_bytes, nrows=0, header=header_row, engine=engine
            )
        except Exception:
            continue  # skip this system if read fails

        # --- c. Normalize column names for robust matching ---
        df_header.columns = df_header.columns.str.strip()

        # --- d. Check required columns presence ---
        if all(col in df_header.columns for col in required_cols):

            # --- e. Read full DataFrame once the system is matched ---
            file_bytes.seek(0)
            force_type = config.get("force_type", {})  # from CLIENT_CONFIGS
            df = pd.read_excel(
                file_bytes,
                header=header_row,
                engine=engine,
                dtype=force_type or None,
            )
            df.columns = df.columns.str.strip()  # normalize full DF too
            return (
                df,
                system_name,
                config,
            )  # Return the matched system's config for downstream processing

    # Step 3: No system matched
    raise ValueError(
        f"Could not determine system type for client '{clientId}' and file '{key}'."
    )


def handle_presigned_url_request(event):
    """
    Generates presigned URL for direct S3 upload
    Returns pure Python data.
    """
    # 2. Parse the body exactly like your old code did to guarantee we get the data
    body = json.loads(event.get("body", "{}"))
    file_name = body.get("fileName")
    s3_path = body.get("s3Path")

    # The safety net!
    if not file_name or not s3_path:
        raise AppError("Missing fileName or s3Path in request.", status_code=400)

    s3_key = f"{s3_path}/{file_name}"

    try:
        presigned_url = s3_client.generate_presigned_url(
            "put_object",
            Params={
                "Bucket": S3_BUCKET,
                "Key": s3_key,
                "ContentType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            },
            ExpiresIn=300,
        )

        # 3. STILL RETURN PURE DATA!
        return {"uploadUrl": presigned_url, "s3Key": s3_key}

    except ClientError as e:
        print(f"AWS ClientError generating presigned URL: {e}")
        raise AppError("Failed to generate secure upload link.", status_code=500)


def save_csv_to_s3(df, file_type, event, s3_client=None):
    """Save DataFrame as CSV file to S3 following the folder structure."""
    if s3_client is None:
        s3_client = boto3.client("s3")

    body = json.loads(event.get("body", "{}"))
    payDate = body.get("pay_date")
    clientID = body.get("client_id")

    # Determine S3 path based on file type
    if file_type == "waiver":
        s3_key = f"clients/{clientID}/waiver/waiver.csv"
    else:
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
        Body=json.dumps(
            result, default=str
        ),  # default=str to handle non-serializable objects, e.g. Pandas datetime
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


def handle_get_client_config(client_id):
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


def handle_save_client_config(client_id, config):
    """Save client configuration to S3"""

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
