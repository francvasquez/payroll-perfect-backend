import time
import pandas as pd
import json
from helper.aws import (
    handle_get_client_config,
    handle_save_client_config,
    handle_presigned_url_request,
    load_processed_results,
    list_pay_periods,
    save_annotations,
    load_annotations,
    delete_annotations,
    delete_pay_period,
)
from helper.db_utils import handle_query_ta_records, handle_get_ta_columns
from file_processor import handle_file_processing
from config import CORS_HEADERS


def parse_event_params(event):
    body = json.loads(event.get("body", "{}"))
    params = {
        "action": body.get("action"),
        "clientId": body.get("clientId") or body.get("client_id"),
        "payDate": body.get("payDate"),
        "employeeId": body.get("employeeId"),
        "startDate": body.get("startDate"),
        "endDate": body.get("endDate"),
        "selectedCols": body.get("selectedCols", []),
        "config": body.get("config"),
        "annotations": body.get("annotations"),
        "client_config": body.get("client_config", {}),
        "waiver_key": body.get("waiver_key"),
        "wfn_key": body.get("wfn_key"),
        "ta_key": body.get("ta_key"),
    }
    return params


def route_action(action, params, event):
    clientId = params.get("clientId")
    employeeId = params.get("employeeId")
    startDate = params.get("startDate")
    endDate = params.get("endDate")
    selectedCols = params.get("selectedCols", [])
    payDate = params.get("payDate")
    annotations = params.get("annotations")
    config = params.get("config")

    if action == "query-ta-records":
        return handle_query_ta_records(
            clientId, employeeId, startDate, endDate, selectedCols
        )
    if action == "get-ta-columns":
        return handle_get_ta_columns(clientId)
    if action == "get-client-config":
        return handle_get_client_config(clientId)
    if action == "save-client-config":
        return handle_save_client_config(clientId, config)
    if action == "list-pay-periods":
        return list_pay_periods(clientId)
    if action == "load-processed-results":
        return load_processed_results(clientId, payDate)
    if action == "get-upload-url":
        return handle_presigned_url_request(event)
    if action == "save-annotations":
        return save_annotations(clientId, payDate, annotations)
    if action == "load-annotations":
        return load_annotations(clientId, payDate)
    if action == "delete-annotations":
        return delete_annotations(clientId, payDate)
    if action == "delete-pay-period":
        return delete_pay_period(clientId, payDate)
    else:
        return handle_file_processing(event, params)


def verify_files(params):
    # Verify all three files are provided (they should be from frontend)
    waiver_key = params.get("waiver_key")
    wfn_key = params.get("wfn_key")
    ta_key = params.get("ta_key")

    if not all([waiver_key, wfn_key, ta_key]):
        return {
            "statusCode": 400,
            "headers": CORS_HEADERS,
            "body": json.dumps(
                {"error": "All three files (waiver, wfn, ta) are required"}
            ),
        }
    # Return None to indicate success
    return None


def convert_datetime_columns_to_iso(df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a copy of the DataFrame where all datetime columns are converted
    to ISO8601 strings in a fully vectorized way. Other columns are untouched.
    """
    df_copy = df.copy()

    # Select all datetime columns
    datetime_cols = df_copy.select_dtypes(include=["datetime"]).columns

    # Vectorized ISO conversion
    df_copy[datetime_cols] = df_copy[datetime_cols].apply(
        lambda col: col.dt.strftime("%Y-%m-%dT%H:%M:%S")
    )

    # Replace NaT strings with None for JSON safety
    df_copy[datetime_cols] = df_copy[datetime_cols].where(
        df_copy[datetime_cols] != "NaT", None
    )

    return df_copy


def time_and_run_function(func, logs, *args, **kwargs):
    """
    Runs func, measures execution time, appends log, and returns func's result.

    :param func: function to run
    :param logs: list to append log messages
    :return: result of func
    """
    start = time.time()
    result = func(*args, **kwargs)
    end = time.time()
    elapsed_ms = round((end - start) * 1000, 2)
    logs.append(f"{func.__name__} took {elapsed_ms} ms")
    return result
