import pandas as pd
import utility, io


def apply_override_else_global(
    df, location_col, param_name, global_value, locations_config
):
    return df[location_col].map(
        lambda x: locations_config.get(x, {}).get(param_name, global_value)
    )


def debug_to_csv(df, IDs, cols, filename):
    print("Columns:", df.columns)
    debug_subset = df.loc[df["ID"].isin(IDs), cols]
    debug_subset.to_csv(filename, index=False)


def to_pandas_datetime(df, *columns):
    """
    Convert multiple DataFrame columns to pandas datetime.
    *columns (str): Column names to convert.
    *means the function accepts any number of column names without having to pass them as a list.
    """
    for col in columns:
        df[col] = pd.to_datetime(df[col])
    return df
