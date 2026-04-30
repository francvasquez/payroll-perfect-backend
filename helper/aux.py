import time
import pandas as pd
import json
from app_config import (
    CORS_HEADERS,
    DEFAULT_PAY_PERIOD_LENGTH,
    DEFAULT_DAYS_BET_PAYROLL_END_AND_PAY_DATE,
    DEFAULT_MIN_WAGE,
    DEFAULT_STATE_MIN_WAGE,
    DEFAULT_PAY_PERIODS_PER_YEAR,
    CORS_HEADERS,
)


def extract_global_config(params: dict) -> tuple:
    global_config = params["client_config"]["global"]
    pay_period_length = global_config.get(
        "pay_period_length", DEFAULT_PAY_PERIOD_LENGTH
    )
    days_bet_payroll_end_and_pay_date = global_config.get(
        "days_bet_payroll_end_and_pay_date", DEFAULT_DAYS_BET_PAYROLL_END_AND_PAY_DATE
    )
    min_wage = global_config.get("min_wage", DEFAULT_MIN_WAGE)
    state_min_wage = global_config.get("state_min_wage", DEFAULT_STATE_MIN_WAGE)
    pay_periods_per_year = global_config.get(
        "pay_periods_per_year", DEFAULT_PAY_PERIODS_PER_YEAR
    )
    pay_date = pd.to_datetime(params["payDate"])
    first_date = (
        pay_date
        - pd.Timedelta(days=days_bet_payroll_end_and_pay_date)
        - pd.Timedelta(days=pay_period_length)
        + pd.Timedelta(days=1)
    )
    last_date = pay_date - pd.Timedelta(days=days_bet_payroll_end_and_pay_date)

    return (
        min_wage,
        state_min_wage,
        pay_periods_per_year,
        pay_date,
        first_date,
        last_date,
    )


def parse_event_params(event):
    body = json.loads(event.get("body", "{}"))
    print("Body:", body)
    params = {  # Use get as these params may or may not come
        "action": body.get("action"),
        "clientId": body.get("clientId") or body.get("client_id"),
        "payDate": body.get("payDate") or body.get("pay_date"),
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
