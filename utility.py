import pandas as pd
import logging

logger = logging.getLogger()


def normalize_client_data(df, system_config):
    """
    Normalizes client data based on system-specific config:
    1. Apply mappings (simple renames or transformations)

    Column pruning to target schema happens later in process_data_* (after drop_rows).
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
                preprocess = rule.get("preprocess", {})

                series_list = []
                for col in source_cols:
                    if col not in df.columns:
                        df[col] = ""
                    series = df[col].fillna("")
                    fmt = preprocess.get(col, {})
                    if fmt.get("astype") == "int":
                        series = series.astype(int)
                    if "zfill" in fmt:
                        series = series.astype(str).str.zfill(fmt["zfill"])
                    else:
                        series = series.astype(str)
                    series_list.append(series)

                df[target_col] = pd.concat(series_list, axis=1).agg(
                    delimiter.join, axis=1
                )

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

    return df


def keep_target_schema_columns(df, target_schema):
    """
    Keeps only columns in target_schema, ordered as defined in the schema
    (core columns first — makes the DB readable). Drops all other intake columns.
    Run after drop_rows so non-schema columns used for row filters are still available.
    """
    extra_cols = [col for col in df.columns if col not in target_schema]
    if extra_cols:
        logger.info(
            f"Dropped {len(extra_cols)} columns outside target schema: {extra_cols}"
        )
    return df[target_schema].copy()


def drop_rows(df, system_config):
    """
    Drops rows based on the 'drop_rows' configuration.
    Supports "Blank" (NaN/NaT/Empty), single strings, or lists of strings.
    """
    drop_rules = system_config.get("drop_rows", {})
    if not drop_rules:
        return df

    initial_row_count = len(df)
    combined_mask = pd.Series([False] * len(df), index=df.index)

    for col, value in drop_rules.items():
        if col not in df.columns:
            logger.warning(
                f"Column '{col}' defined in drop_rows not found in data."
            )
            continue

        if value == "Blank":
            combined_mask |= df[col].isna() | (df[col].astype(str).str.strip() == "")
        elif isinstance(value, list):
            combined_mask |= df[col].isin(value)
        else:
            combined_mask |= df[col] == value

    df_cleaned = df[~combined_mask].copy()

    dropped_count = initial_row_count - len(df_cleaned)
    if dropped_count > 0:
        logger.info(
            f"Dropped {dropped_count} rows based on drop_rows keys: {list(drop_rules.keys())}"
        )

    return df_cleaned


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


def validate_wfn_pay_date(df, target_pay_date) -> tuple[bool, str]:
    """
    Validates user-selected pay date against the WFN file's PAY DATE column.
    Returns (is_valid, message).
    """
    try:
        target = pd.to_datetime(target_pay_date).normalize()
    except Exception:
        return (
            False,
            f"Intake Error: '{target_pay_date}' is not a valid date format.",
        )

    if "PAY DATE" not in df.columns:
        return False, "Validation Error: 'PAY DATE' column missing from the payroll file."

    if "IDX" not in df.columns:
        return False, "Validation Error: 'IDX' column missing from the payroll file."

    file_dates = pd.to_datetime(df["PAY DATE"], errors="coerce").dt.normalize()
    mismatch_mask = file_dates.isna() | (file_dates != target)
    mismatch_df = df.loc[mismatch_mask]

    if mismatch_df.empty:
        return True, "Validation Passed."

    unique_idx = mismatch_df["IDX"].drop_duplicates()
    total_mismatch = len(unique_idx)
    sample_idx = unique_idx.head(5)

    msg = (
        f"Pay Date Mismatch!\n"
        f"You selected {target.date()}, but this payroll file contains "
        f"{total_mismatch} employee(s) with a different PAY DATE.\n\n"
        f"Sample mismatches:\n"
    )

    for idx in sample_idx:
        row = mismatch_df.loc[mismatch_df["IDX"] == idx].iloc[0]
        file_date = pd.to_datetime(row["PAY DATE"], errors="coerce")
        date_str = file_date.date() if pd.notna(file_date) else "missing/invalid"
        msg += f"• {idx}: PAY DATE {date_str}\n"

    if total_mismatch > 5:
        msg += f"\n...and {total_mismatch - 5} more."

    return False, msg
