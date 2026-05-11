import pandas as pd
import logging

logger = logging.getLogger()


def normalize_client_data(df, system_config):
    """
    Normalizes client data based on system-specific config:
    1. Force column types (optional)
    2. Apply mappings (simple renames or transformations)
    3. Drop system-specific unwanted columns
    """

    # --- 1. Apply mappings (if provided) ---
    mappings = system_config.get("mappings", {})

    for target_col, rule in mappings.items():
        if isinstance(rule, str):
            # Simple rename
            if rule in df.columns:
                df = df.rename(columns={rule: target_col})

        elif isinstance(rule, dict):
            transform_type = rule.get("transform")

            # Transform type 1: Concatenation of multiple columns with a delimiter
            if transform_type == "concat":
                source_cols = rule.get("source_columns", [])
                delimiter = rule.get("delimiter", "")

                for col in source_cols:
                    if col not in df.columns:
                        df[col] = ""

                df[target_col] = df[source_cols].fillna("").agg(delimiter.join, axis=1)

            # Transform type 2: Substring extraction from a source column
            elif transform_type == "substring":
                source_col = rule.get("source_column")

                if source_col in df.columns:
                    # Use .get() to allow standard Python slicing defaults (None)
                    start = rule.get("start", None)
                    end = rule.get("end", None)

                    # Convert to string first to ensure .str accessor works safely
                    df[target_col] = df[source_col].astype(str).str[start:end]

            # Additional transform types can be added here later:
            # elif transform_type == "pad_left": ...
            # elif transform_type == "upper": ...
            # etc.

    # --- 2. Drop columns specified in config ---
    drop_cols = system_config.get("drop_columns", [])
    if drop_cols:
        df = df.drop(columns=drop_cols, errors="ignore")
        logger.info(
            f"Dropped {len(drop_cols)} columns based on system config: {drop_cols}"
        )
    return df


def apply_override_else_global(
    df, location_col, param_name, global_value, locations_config
):
    return df[location_col].map(
        lambda x: locations_config.get(x, {}).get(param_name, global_value)
    )


def to_pandas_datetime(df, *columns):
    """
    Convert multiple DataFrame columns to pandas datetime.
    *columns (str): Column names to convert.
    *means the function accepts any number of column names without having to pass them as a list.
    """
    for col in columns:
        df[col] = pd.to_datetime(df[col])
    return df
