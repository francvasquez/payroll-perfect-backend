import pandas as pd


def _normalize_id_series(series: pd.Series) -> pd.Series:
    """Strip and drop blank / NaN ID values."""
    cleaned = series.astype("string").str.strip()
    return cleaned[cleaned.notna() & ~cleaned.isin(["", "nan", "<NA>", "None", "<na>"])]


def process_waiver(df):
    """
    Build a waiver lookup keyed by employee ID.

    Presence on the waiver file means the employee has a waiver on file.
    Optional Prior_ID_1 values are included so hotel moves still match.
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["ID", "Has_Waiver_Bool"])

    if "ID" not in df.columns:
        raise ValueError("Waiver file must contain an 'ID' column.")

    id_parts = [_normalize_id_series(df["ID"])]

    # Prior IDs (e.g. previous hotel CO.) also count as waived for the same person
    if "Prior_ID_1" in df.columns:
        id_parts.append(_normalize_id_series(df["Prior_ID_1"]))

    all_ids = pd.concat(id_parts, ignore_index=True).drop_duplicates(keep="first")

    processed_waiver_df = pd.DataFrame({"ID": all_ids})
    processed_waiver_df["Has_Waiver_Bool"] = True
    return processed_waiver_df.reset_index(drop=True)
