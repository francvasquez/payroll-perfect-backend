import pandas as pd
import utility, io
from functools import wraps


def debug_to_csv(df, IDs, cols, filename):
    print("Columns:", df.columns)
    debug_subset = df.loc[df["ID"].isin(IDs), cols]
    debug_subset.to_csv(filename, index=False)


def format_datetime_columns(df, global_date_format, per_column_format=None):
    """Convert all datetime64[ns] columns to strings using either a global
    format or column-specific overrides.
    """
    df = df.copy()
    for col in df.select_dtypes(include="datetime64[ns]").columns:
        fmt = global_date_format
        if per_column_format and col in per_column_format:
            fmt = per_column_format[col]
        df[col] = df[col].dt.strftime(fmt).fillna("N/A")
    return df


def print_except(df, rows, *exclude_columns):
    print(df.loc[:rows].drop(columns=list(exclude_columns)))


def print_with_mask(df, mask, *exclude_columns):
    print(df.loc[mask, :].drop(columns=list(exclude_columns)))


def to_pandas_datetime(df, *columns):
    """
    Convert multiple DataFrame columns to pandas datetime.
    *columns (str): Column names to convert.
    *means the function accepts any number of column names without having to pass them as a list.
    """
    for col in columns:
        df[col] = pd.to_datetime(df[col])
    return df


def import_excel(file, key_cols, keep_cols=None):
    excel_io = io.BytesIO(file)
    header_row = utility.find_header_row(excel_io, key_cols)
    df = pd.read_excel(excel_io, header=header_row, usecols=keep_cols)
    return df


def find_header_row(file, key_columns=None):
    if key_columns is None:
        key_columns = ["Employee", "ID"]

    # Load a preview of the file without headers
    raw = pd.read_excel(file, header=None)

    # Loop through rows to find likely header
    for i in range(len(raw)):
        row = raw.iloc[i].astype(str).str.lower()
        if any(key.lower() in row.tolist() for key in key_columns):
            return i  # Found header row

    raise ValueError("Header row not found.")
