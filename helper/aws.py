import json, boto3, io, json, traceback
import re
from datetime import datetime, timezone
import pandas as pd
from app_config import S3_BUCKET
from client_config import CLIENT_CONFIGS
from io import StringIO
from botocore.exceptions import ClientError
from helper.db_utils import (
    delete_ta_from_db,
    delete_daily_df_from_db,
    get_db_connection,
)
from exceptions import (
    AppError,
    TA_SYSTEM_UNRECOGNIZED,
    TA_SYSTEM_UNRECOGNIZED_MESSAGE,
    WFN_SYSTEM_UNRECOGNIZED,
    WFN_SYSTEM_UNRECOGNIZED_MESSAGE,
)
from botocore.exceptions import ClientError

s3_client = boto3.client("s3")
ses = boto3.client("ses", region_name="us-west-1")

PARCELED_RAW_KEY_PATTERN = re.compile(
    r"clients/[^/]+/raw/\d{4}-\d{2}-\d{2}/(?:wfn|ta)\.csv$"
)


def is_parceled_raw_key(key: str) -> bool:
    """True for per-pay-period CSV slices saved during discover-pay-periods."""
    if not key or "/raw/intake/" in key:
        return False
    return bool(PARCELED_RAW_KEY_PATTERN.search(key))


def save_parcel_csv_to_s3(df, client_id: str, pay_date: str, file_type: str) -> str:
    """Save a single pay-period slice for later processing without re-reading bulk intake."""
    s3_key = f"clients/{client_id}/raw/{pay_date}/{file_type}.csv"
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=csv_buffer.getvalue(),
        ContentType="text/csv",
    )
    print(f"Saved pay-period parcel to s3://{S3_BUCKET}/{s3_key} ({len(df)} rows)")
    return s3_key


def _read_parceled_csv_from_s3(key, client_id, system_kind: str):
    """Read a prepared CSV slice and return default system config for the client."""
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))

    if system_kind == "wfn":
        systems = CLIENT_CONFIGS.get(client_id, {}).get("wfn_systems", {})
    else:
        systems = CLIENT_CONFIGS.get(client_id, {}).get("ta_systems", {})

    if not systems:
        raise ValueError(f"No '{system_kind}_systems' configured for client '{client_id}'.")

    system_name = next(iter(systems))
    return df, system_name, systems[system_name]


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
                        # Safely grab the key
                        key = obj.get("Key")
                        if key:
                            objects_to_delete.append({"Key": key})

            # Delete in chunks of 1000
            if objects_to_delete:
                for i in range(0, len(objects_to_delete), 1000):
                    chunk = objects_to_delete[i : i + 1000]
                    delete_response = s3_client.delete_objects(
                        Bucket=S3_BUCKET, Delete={"Objects": chunk}
                    )

                    # Track successful deletions
                    if "Deleted" in delete_response:
                        for deleted in delete_response["Deleted"]:
                            # Safely grab the deleted key
                            del_key = deleted.get("Key")
                            if del_key:
                                deleted_files.append(del_key)
                                print(f"Deleted: {del_key}")

                    # Track errors
                    if "Errors" in delete_response:
                        for error in delete_response["Errors"]:
                            # Provide fallback strings if Key or Message are missing
                            err_key = error.get("Key", "UnknownKey")
                            err_msg = error.get("Message", "Unknown Error")

                            errors.append(f"{err_key}: {err_msg}")
                            print(f"Error deleting {err_key}: {err_msg}")

                print(f"Deleted {len(deleted_files)} files from {prefix}")
            else:
                print(f"No files found under {prefix}")

        # --- Database Deletion ---
        conn = get_db_connection()

        # 1. Hard fail if not connected to DB
        if not conn:
            # We raise an AppError here! Let the user know the DB is paused.
            raise AppError(
                "Database is paused or unavailable. S3 files were deleted, but database records remain.",
                status_code=503,
            )
        db_rows = 0
        db_status = "skipped"

        # 2. Attempt to delete from both tables.
        try:
            # 1. Delete from the raw punches table
            ta_deleted = delete_ta_from_db(conn, client_id, pay_date)

            # 2. Delete from the daily totals table
            daily_deleted = delete_daily_df_from_db(conn, client_id, pay_date)

            db_rows = ta_deleted + daily_deleted
            db_status = "completed"
            print(
                f"Total DB rows deleted: {db_rows} ({ta_deleted} TA, {daily_deleted} Daily)"
            )
        except Exception as e:
            # 2. HARD FAIL IF DELETION CRASHES
            print(f"Failed to delete from database. S3 succeeded but not db: {e}")
            # Do NOT swallow the error. Raise it so the frontend shows the red warning.
            raise AppError(
                f"S3 files deleted, but database deletion failed: {str(e)}",
                status_code=500,
            )
        finally:
            # This safely runs even if an AppError is raised inside the try block!
            conn.close()

        # --- RETURN PURE DATA ---
        # If we made it here, S3 and DB both succeeded!
        return {
            "message": (
                "Pay period partially deleted with some errors"
                if errors
                else f"Pay period {pay_date} deletion processed"
            ),
            "s3_deleted_count": len(deleted_files),
            "deleted_files": deleted_files,
            "errors": errors,
            "db_rows_affected": db_rows,
            "ta_rows_deleted": ta_deleted,  # <-- NEW
            "daily_rows_deleted": daily_deleted,  # <-- NEW
            "db_status": db_status,
        }

    except ClientError as e:
        print(f"S3 error deleting pay period: {str(e)}")
        # Raise AppError to be caught by the Traffic Cop
        raise AppError("Failed to delete pay period from storage", status_code=500)


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

        return {"message": "Annotations saved successfully"}

    except ClientError as e:
        print(f"S3 error saving annotations: {str(e)}")
        # RAISE THE ERROR
        raise AppError(f"Failed to save annotations: {str(e)}", status_code=500)

    except Exception as e:
        print(f"Unexpected error saving annotations: {str(e)}")
        traceback.print_exc()
        # RAISE THE ERROR
        raise AppError(f"Internal server error: {str(e)}", status_code=500)


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
        return annotations_payload

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "UnknownCode")

        if error_code == "NoSuchKey":
            print(f"No annotations found for {client_id}/{pay_date}")
            # 2. PURE DATA RETURN (Valid empty state)
            return None
        else:
            print(f"S3 error loading annotations: {str(e)}")
            # 3. RAISE THE ERROR
            raise AppError(f"Failed to load annotations: {str(e)}", status_code=500)

    except json.JSONDecodeError as e:
        print(f"Invalid JSON in annotations file: {str(e)}")
        # 4. RAISE THE ERROR
        raise AppError("Invalid annotations file format", status_code=500)

    except Exception as e:
        print(f"Unexpected error loading annotations: {str(e)}")
        traceback.print_exc()
        # 5. RAISE THE ERROR
        raise AppError(f"Internal server error: {str(e)}", status_code=500)


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
        # Safely grab the Prefix
        folder_path = obj.get("Prefix")

        # Prove to Pylance that the path exists before running string methods
        if folder_path:
            # Extract date from path like 'clients/demo_client/processed/2025-01-16/'
            date = folder_path.split("/")[-2]
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

        # 1. Return the data as pure Python data, let lambda_handler wrap it
        return {"results": results}
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")

        # 2. HANDLE EXPECTED 404
        if error_code == "NoSuchKey":
            print(f"[DEBUG] NoSuchKey: No processed data at {key}")

            # Raise AppError! lambda_handler will format this as an HTTP 404.
            raise AppError("No processed data for this period", status_code=404)

        # 3. HANDLE UNEXPECTED AWS ERRORS
        else:
            print(f"[ERROR] AWS Error loading S3 object {key}: {str(e)}")
            raise AppError(
                "Failed to load processed results from storage", status_code=500
            )


def read_wfn_excel_from_s3(key, clientId, engine=None):
    """
    Reads WFN Excel file from S3, auto-detects system configuration,
    and returns (df, system_name, config)
    """
    if is_parceled_raw_key(key):
        return _read_parceled_csv_from_s3(key, clientId, "wfn")

    # Step 1: Download file into memory
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
    file_bytes = io.BytesIO(obj["Body"].read())

    # Get WFN configurations for this client
    wfn_systems = CLIENT_CONFIGS.get(clientId, {}).get("wfn_systems", {})

    if not wfn_systems:
        raise ValueError(
            f"No 'wfn_systems' configured in CLIENT_CONFIGS for client '{clientId}'."
        )

    # Step 2: Loop through WFN systems for detection
    for wfn_system_name, wfn_config in wfn_systems.items():

        # --- a. Detection inputs ---
        header_row = wfn_config["detection"]["header"]
        required_cols = wfn_config["detection"]["columns"]

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
            force_type = wfn_config.get("force_type", {})  # from CLIENT_CONFIGS
            df = pd.read_excel(
                file_bytes,
                header=header_row,
                engine=engine,
                dtype=force_type or None,
            )
            df.columns = df.columns.str.strip()  # normalize full DF too

            # Return the dataframe and matched config details
            return (
                df,
                wfn_system_name,
                wfn_config,
            )

    # Step 3: No system matched
    print(
        f"WFN system detection failed for client '{clientId}' and file '{key}'."
    )
    raise AppError(
        WFN_SYSTEM_UNRECOGNIZED_MESSAGE,
        status_code=400,
        error_code=WFN_SYSTEM_UNRECOGNIZED,
    )


def read_waiver_excel_from_s3(key, header=0, engine=None):
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

    if is_parceled_raw_key(key):
        return _read_parceled_csv_from_s3(key, clientId, "ta")

    # Step 1: Download file into memory
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
    file_bytes = io.BytesIO(obj["Body"].read())

    systems = CLIENT_CONFIGS[clientId]["ta_systems"]

    # Step 2: Loop through systems for detection
    for ta_system_name, ta_config in systems.items():

        # --- a. Detection inputs ---
        header_row = ta_config["detection"]["header"]
        required_cols = ta_config["detection"]["columns"]
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
            force_type = ta_config.get("force_type", {})  # from CLIENT_CONFIGS
            df = pd.read_excel(
                file_bytes,
                header=header_row,
                engine=engine,
                dtype=force_type or None,
            )
            df.columns = df.columns.str.strip()  # normalize full DF too
            return (
                df,
                ta_system_name,
                ta_config,
            )  # Return the matched system's config for downstream processing

    # Step 3: No system matched
    print(
        f"TA system detection failed for client '{clientId}' and file '{key}'."
    )
    raise AppError(
        TA_SYSTEM_UNRECOGNIZED_MESSAGE,
        status_code=400,
        error_code=TA_SYSTEM_UNRECOGNIZED,
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
    # 1. RAISE validation errors (Don't return HTTP dicts)
    if not client_id:
        raise AppError("clientId is required", status_code=400)

    try:
        config_key = f"clients/{client_id}/config.json"
        print(f"Fetching config from S3: s3://{S3_BUCKET}/{config_key}")

        response = s3_client.get_object(Bucket=S3_BUCKET, Key=config_key)
        config_data = json.loads(response["Body"].read().decode("utf-8"))

        print(f"Successfully loaded config for client: {client_id}")

        # 2. RETURN PURE DATA! (No statusCode, no headers, no json.dumps)
        # Your lambda_handler will wrap this automatically.
        return {"config": config_data}

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")

        # 3. TRANSLATE specific AWS errors into AppErrors
        if error_code == "NoSuchKey":
            print(f"Config file not found: {config_key}")
            raise AppError(
                f"Configuration not found for client: {client_id}", status_code=404
            )

        elif error_code == "NoSuchBucket":
            print(f"Bucket not found: {S3_BUCKET}")
            raise AppError("Storage bucket not configured", status_code=500)

        else:
            print(f"S3 error: {str(e)}")
            raise AppError(
                "Failed to fetch configuration from storage", status_code=500
            )

    except json.JSONDecodeError as e:
        print(f"Invalid JSON in config file: {str(e)}")
        raise AppError("Invalid configuration file format", status_code=500)

    # NOTE: The generic `except Exception as e:` block is GONE!
    # If anything unexpected happens, it instantly bubbles up to lambda_handler.


def handle_save_client_config(client_id, config):
    """Save client configuration to S3"""

    # 1. RAISE EARLY ERRORS
    if not client_id:
        raise AppError("clientId is required", status_code=400)

    if not config:
        raise AppError("config is required", status_code=400)

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

        # 2. PURE DATA RETURN (Success)
        return {"message": "Configuration saved successfully"}

    except ClientError as e:
        print(f"S3 error: {str(e)}")
        # 3. RAISE S3 ERRORS
        raise AppError(f"Failed to save config: {str(e)}", status_code=500)

    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        traceback.print_exc()
        # 4. RAISE GENERAL ERRORS
        raise AppError(f"Internal server error: {str(e)}", status_code=500)
