"""
Discover pay periods from multi-period TA/WFN intake files using anchor pay date math.
"""

import numpy as np
import pandas as pd

from app_config import MIN_PAY_PERIOD_WORKDAYS


def compute_pay_period_window(pay_date: str, client_params: dict) -> tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp]:
    """Return (pay_date, work_start, work_end) for a fiscal pay date."""
    global_config = client_params["global"]
    pp_length = global_config["pay_period_length"]
    days_to_pay = global_config["days_bet_payroll_end_and_pay_date"]

    target = pd.to_datetime(pay_date).normalize()
    work_end = target - pd.Timedelta(days=days_to_pay, unit="D")
    work_start = work_end - pd.Timedelta(days=pp_length - 1, unit="D")
    return target, work_start, work_end


def assign_fiscal_pay_dates(
    punch_dates: pd.Series, client_params: dict, anchor_pay_date: str
) -> pd.Series:
    """Map each punch date to its fiscal pay date using anchor calendar math."""
    global_config = client_params["global"]
    pp_length = global_config["pay_period_length"]
    days_to_pay = global_config["days_bet_payroll_end_and_pay_date"]

    normalized = pd.to_datetime(punch_dates, errors="coerce").dt.normalize()
    anchor_date = pd.to_datetime(anchor_pay_date).normalize()
    anchor_end = anchor_date - pd.Timedelta(days=days_to_pay, unit="D")
    anchor_start = anchor_end - pd.Timedelta(days=pp_length - 1, unit="D")

    delta_days = (normalized - anchor_start).dt.days
    period_offset = np.floor(delta_days / pp_length)
    row_period_end = anchor_end + pd.to_timedelta(period_offset * pp_length, unit="D")
    fiscal_pay_dates = row_period_end + pd.to_timedelta(days_to_pay, unit="D")
    return fiscal_pay_dates.dt.date


def filter_ta_to_pay_period_window(
    ta_df: pd.DataFrame, pay_date: str, client_params: dict
) -> pd.DataFrame:
    """Keep TA rows whose In Punch falls within the target pay period work window."""
    if ta_df is None or ta_df.empty or "In Punch" not in ta_df.columns:
        return ta_df

    _, work_start, work_end = compute_pay_period_window(pay_date, client_params)
    punch_dates = pd.to_datetime(ta_df["In Punch"], errors="coerce").dt.normalize()
    mask = (punch_dates >= work_start) & (punch_dates <= work_end)
    return ta_df.loc[mask].copy()


def filter_wfn_to_pay_date(wfn_df: pd.DataFrame, pay_date: str) -> pd.DataFrame:
    """Keep WFN rows matching the target pay date."""
    if wfn_df is None or wfn_df.empty or "Pay Date" not in wfn_df.columns:
        return wfn_df

    target = pd.to_datetime(pay_date).normalize()
    file_dates = pd.to_datetime(wfn_df["Pay Date"], errors="coerce").dt.normalize()
    return wfn_df.loc[file_dates == target].copy()


def _count_ta_workdays_by_period(
    ta_df: pd.DataFrame, client_params: dict, anchor_pay_date: str
) -> dict:
    if ta_df is None or ta_df.empty or "In Punch" not in ta_df.columns:
        return {}

    punch_dates = pd.to_datetime(ta_df["In Punch"], errors="coerce").dt.normalize()
    fiscal_dates = assign_fiscal_pay_dates(punch_dates, client_params, anchor_pay_date)
    valid_mask = punch_dates.notna() & fiscal_dates.notna()

    workday_df = pd.DataFrame(
        {
            "fiscal_pay_date": fiscal_dates[valid_mask],
            "workday": punch_dates[valid_mask].dt.date,
        }
    )
    if workday_df.empty:
        return {}

    counts = (
        workday_df.groupby("fiscal_pay_date")["workday"]
        .nunique()
        .to_dict()
    )
    return {str(k): int(v) for k, v in counts.items()}


def _count_wfn_rows_by_period(wfn_df: pd.DataFrame) -> dict:
    if wfn_df is None or wfn_df.empty or "Pay Date" not in wfn_df.columns:
        return {}

    pay_dates = pd.to_datetime(wfn_df["Pay Date"], errors="coerce").dt.date
    valid = pay_dates.notna()
    if not valid.any():
        return {}

    counts = wfn_df.loc[valid].groupby(pay_dates[valid]).size().to_dict()
    return {str(k): int(v) for k, v in counts.items()}


def discover_pay_periods(
    ta_df: pd.DataFrame,
    wfn_df: pd.DataFrame,
    client_params: dict,
    anchor_pay_date: str,
    min_workdays: int = MIN_PAY_PERIOD_WORKDAYS,
) -> list[dict]:
    """
    Discover fiscal pay periods present in intake files.
    Returns sorted list of period metadata dicts.
    """
    ta_counts = _count_ta_workdays_by_period(ta_df, client_params, anchor_pay_date)
    wfn_counts = _count_wfn_rows_by_period(wfn_df)

    all_dates = sorted(set(ta_counts.keys()) | set(wfn_counts.keys()))
    periods = []

    for pay_date in all_dates:
        ta_workday_count = ta_counts.get(pay_date, 0)
        wfn_row_count = wfn_counts.get(pay_date, 0)

        _, work_start, work_end = compute_pay_period_window(pay_date, client_params)

        processable = True
        skip_reason = None

        if ta_workday_count < min_workdays:
            processable = False
            skip_reason = (
                f"Only {ta_workday_count} workday(s) in TA data "
                f"(minimum {min_workdays} required)."
            )
        elif wfn_row_count == 0:
            processable = False
            skip_reason = "No matching WFN rows for this pay date."

        periods.append(
            {
                "pay_date": pay_date,
                "first_date": work_start.date().isoformat(),
                "last_date": work_end.date().isoformat(),
                "ta_workday_count": ta_workday_count,
                "wfn_row_count": wfn_row_count,
                "processable": processable,
                "skip_reason": skip_reason,
            }
        )

    return periods


def merge_period_config(client_config: dict, pay_date: str) -> dict:
    """Merge default config with pay-period-specific overrides for processing."""
    merged = {
        "global": dict(client_config.get("global", {})),
        "locations": {
            loc: dict(overrides)
            for loc, overrides in client_config.get("locations", {}).items()
        },
    }

    period_overrides = client_config.get("pay_periods", {}).get(pay_date, {})
    if period_overrides.get("global"):
        merged["global"].update(period_overrides["global"])

    for loc, overrides in period_overrides.get("locations", {}).items():
        merged["locations"][loc] = {**merged["locations"].get(loc, {}), **overrides}

    return merged
