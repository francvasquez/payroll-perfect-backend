import time
import pandas as pd
import numpy as np


def json_default(o):
    ## Handles pandas/numpy datetime objects
    if isinstance(o, pd.Timestamp):
        return o.isoformat()
    if isinstance(o, pd.Timedelta):
        return str(o)
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.floating,)):
        return float(o)
    return str(o)  # fallback for any other object


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
