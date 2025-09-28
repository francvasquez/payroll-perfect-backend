import time
import pandas as pd

# import numpy as np


# def json_default(o):
#     ## Handles pandas/numpy datetime objects
#     if isinstance(o, pd.Timestamp):
#         return o.isoformat()
#     if isinstance(o, pd.Timedelta):
#         return str(o)
#     if isinstance(o, (np.integer,)):
#         return int(o)
#     if isinstance(o, (np.floating,)):
#         return float(o)
#     return str(o)  # fallback for any other object


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
