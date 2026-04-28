"""
apply_weekly_rules.py
─────────────────────
Senior Data Engineering: Weekly Overtime & Consecutive Day Premium Logic
Handles both Standard workweek rules and CBA Rolling rules with carryover streaks.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from typing import Any
from helper.db_utils import get_carryover_streaks

# ── Type alias ────────────────────────────────────────────────────────────────
ClientParams = dict[str, Any]

# ── Weekday name → integer (Monday = 0 … Sunday = 6) ─────────────────────────
_WEEKDAY_MAP: dict[str, int] = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}


# ─────────────────────────────────────────────────────────────────────────────
# Helper: resolve a config key with location-level override
# ─────────────────────────────────────────────────────────────────────────────
def _resolve(
    client_params: ClientParams,
    location: str,
    key: str,
) -> Any:
    """
    Return the value for `key` using the following priority:
        1. client_params["locations"][location][key]   (if present)
        2. client_params["global"][key]                (fallback)

    Leading/trailing whitespace is stripped from the location string before
    the lookup so that DataFrame string inconsistencies don't cause misses.
    """
    loc_cfg: dict = client_params.get("locations", {}).get(location.strip(), {})
    if key in loc_cfg:
        return loc_cfg[key]
    return client_params["global"][key]


# ─────────────────────────────────────────────────────────────────────────────
# Helper: assign Workweek_ID (Sunday-anchored by default, or custom)
# ─────────────────────────────────────────────────────────────────────────────
def _assign_workweek_id(
    dates: pd.Series,
    workweek_start_name: str,
) -> pd.Series:
    """
    Return the start-of-workweek date for each date in `dates`.

    Pandas week anchor:  Monday = 0 … Sunday = 6
    We convert the config name to the correct offset so that the workweek
    boundary is exactly right regardless of locale.
    """
    anchor: int = _WEEKDAY_MAP[workweek_start_name.strip().lower()]
    # Normalize to midnight to eliminate any residual time-of-day / tz offset
    norm: pd.Series = dates.dt.normalize()
    # Day-of-week in Pandas: Monday=0, Sunday=6
    dow: pd.Series = norm.dt.dayofweek
    # Days to subtract to land on the workweek start
    days_back: pd.Series = (dow - anchor) % 7
    return norm - pd.to_timedelta(days_back, unit="D")


# ─────────────────────────────────────────────────────────────────────────────
# Helper: compute consecutive-day streaks for one employee group
# ─────────────────────────────────────────────────────────────────────────────
def _compute_streaks_for_employee(
    group: pd.DataFrame,
    cba_rolling: bool,
    carryover_streak: int,
    workweek_start_dow: int,
    prior_period_date: pd.Timestamp,
) -> pd.Series:
    """
    Given a single-employee DataFrame (already sorted by Attributed_Workday),
    compute the Days_Worked_In_Week streak for every row.

    Parameters
    ----------
    group              : DataFrame slice for one employee, sorted by date.
    cba_rolling        : True  → CBA Rolling rule (ignore workweek boundary).
                         False → Standard rule (reset at workweek boundary).
    carryover_streak   : Streak the employee carried out of the prior period
                         (only meaningful when cba_rolling=True).
    workweek_start_dow : Integer 0–6 (Mon=0…Sun=6) of the workweek start day.

    Returns
    -------
    pd.Series of int, same index as `group`, representing the streak value
    for each row.
    """
    # Normalise dates once to avoid timezone / hour-offset bugs
    dates: np.ndarray = (
        group["Attributed_Workday"].dt.normalize().values
    )  # numpy datetime64

    n = len(dates)
    streaks = np.empty(n, dtype=np.int64)

    if n == 0:
        return pd.Series(streaks, index=group.index, dtype="int64")

    # ── First row ────────────────────────────────────────────────────────────
    if cba_rolling:
        first_date = pd.Timestamp(dates[0])
        # Only continue the streak if the first shift is exactly 1 day after the prior period ended!
        if carryover_streak > 0 and (first_date - prior_period_date).days == 1:
            streaks[0] = carryover_streak + 1
        else:
            streaks[0] = 1
    else:
        streaks[0] = 1

    # ── Subsequent rows ───────────────────────────────────────────────────────
    for i in range(1, n):
        prev_date = pd.Timestamp(dates[i - 1])
        curr_date = pd.Timestamp(dates[i])

        gap_days: int = (curr_date - prev_date).days  # always ≥ 1 (sorted unique dates)

        if gap_days > 1:
            # Gap in worked days → streak breaks
            streaks[i] = 1
        elif cba_rolling:
            # CBA Rolling: only gap matters, no workweek boundary
            streaks[i] = streaks[i - 1] + 1
        else:
            # Standard rule: also reset if we cross the workweek start boundary
            crossed_boundary = (prev_date.dayofweek != workweek_start_dow) and (
                curr_date.dayofweek == workweek_start_dow
            )
            if crossed_boundary:
                streaks[i] = 1
            else:
                streaks[i] = streaks[i - 1] + 1

    return pd.Series(streaks, index=group.index, dtype="int64")


# ─────────────────────────────────────────────────────────────────────────────
# Helper: apply consecutive-day premium to one employee-workweek group
# ─────────────────────────────────────────────────────────────────────────────
def _apply_consec_premium(group: pd.DataFrame, consec_threshold: int) -> pd.DataFrame:
    """
    For rows where Days_Worked_In_Week > consec_threshold:
        Regular_Hrs = 0
        OT_Hrs      = min(Hours_Worked, 8)
        DT_Hrs      = max(Hours_Worked - 8, 0)

    Operates in-place on the group slice and returns it.
    """
    mask: pd.Series = group["Days_Worked_In_Week"] > consec_threshold

    if mask.any():
        hw = group.loc[mask, "Hours_Worked"]
        group.loc[mask, "Regular_Hrs"] = 0.0
        group.loc[mask, "OT_Hrs"] = hw.clip(upper=8.0)
        group.loc[mask, "DT_Hrs"] = (hw - 8.0).clip(lower=0.0)
        group.loc[mask, "Is_Consecutive_Day_Rule"] = True

    return group


# ─────────────────────────────────────────────────────────────────────────────
# Helper: apply weekly OT spillover to one employee-workweek group
# ─────────────────────────────────────────────────────────────────────────────
def _apply_weekly_ot(group: pd.DataFrame, ot_week_max: float) -> pd.DataFrame:
    """
    Rolling cumulative sum of Regular_Hrs within the workweek.
    Once the employee's Regular_Hrs exceed ot_week_max, the overflow is:
        • Removed from Regular_Hrs
        • Added to OT_Hrs
        • Recorded in Weekly_OT_Spillover

    The group must be sorted chronologically before calling this helper.
    """
    reg = group["Regular_Hrs"].values.copy()
    spillover = np.zeros(len(reg), dtype=np.float64)
    cum_reg = np.zeros(len(reg), dtype=np.float64)

    running_total: float = 0.0

    for i, r in enumerate(reg):
        running_total += r
        cum_reg[i] = running_total

        if running_total > ot_week_max:
            overflow = running_total - ot_week_max
            # Cap how much we can spill from this single row
            actual_spill = min(overflow, r)
            spillover[i] = actual_spill
            reg[i] -= actual_spill
            running_total = ot_week_max  # cap the counter

    group = group.copy()
    group["Regular_Hrs"] = reg
    group["OT_Hrs"] = group["OT_Hrs"] + spillover
    group["Weekly_OT_Spillover"] = spillover
    group["Cum_Reg_Hrs"] = cum_reg
    return group


# ─────────────────────────────────────────────────────────────────────────────
# Main public function
# ─────────────────────────────────────────────────────────────────────────────
def apply_weekly_rules(
    daily_df: pd.DataFrame,
    client_params: ClientParams,
    clientId: str,
    pay_date: str,
) -> pd.DataFrame:
    """
    Apply dynamic Weekly Overtime and Consecutive Day Premium rules to a
    timecard DataFrame.

    Parameters
    ----------
    daily_df      : Timecard DataFrame with columns:
                      Employee, ID, Attributed_Workday, Hours_Worked,
                      Regular_Hrs, OT_Hrs, DT_Hrs, Location
    client_params : Config dict with a 'global' block and a 'locations' block.
    clientId      : Client identifier string (e.g. "demo_client").
    pay_date      : Pay date string (e.g. "2026-04-10").

    Returns
    -------
    Modified DataFrame with original columns plus:
        Workweek_ID           (datetime64[ns])
        Days_Worked_In_Week   (int64)
        Is_Consecutive_Day_Rule (bool)
        Cum_Reg_Hrs           (float64)
        Weekly_OT_Spillover   (float64)
    """

    # ── 0. Defensive copy & type normalisation ────────────────────────────────
    df = daily_df.copy()

    # Strip whitespace from string columns to prevent lookup mismatches
    for col in ("Employee", "ID", "Location"):
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # Ensure Attributed_Workday is datetime
    df["Attributed_Workday"] = pd.to_datetime(df["Attributed_Workday"])

    # Initialise new columns
    df["Workweek_ID"] = pd.NaT
    df["Days_Worked_In_Week"] = 0
    df["Is_Consecutive_Day_Rule"] = False
    df["Cum_Reg_Hrs"] = 0.0
    df["Weekly_OT_Spillover"] = 0.0

    # ── 1. Global config values ───────────────────────────────────────────────
    g_cfg: dict = client_params["global"]
    workweek_start_name: str = g_cfg["workweek_start"].strip()
    workweek_start_dow: int = _WEEKDAY_MAP[workweek_start_name.lower()]

    # ── 2. Determine if ANY location uses CBA rolling rule ────────────────────
    any_cba_rolling: bool = any(
        loc_cfg.get("cba_consec_anyweek", g_cfg.get("cba_consec_anyweek", False))
        for loc_cfg in client_params.get("locations", {}).values()
    )
    # Also check the global fallback
    any_cba_rolling = any_cba_rolling or g_cfg.get("cba_consec_anyweek", False)

    # ── 3. Fetch carryover streaks if needed ──────────────────────────────────
    # Calculate exact prior period date for gap checking
    pay_date_obj = pd.to_datetime(pay_date).normalize()
    days_gap = g_cfg.get("days_bet_payroll_end_and_pay_date", 6)
    pay_length = g_cfg.get("pay_period_length", 14)
    prior_period_date = pay_date_obj - pd.Timedelta(days=days_gap + pay_length)

    carryover_dict: dict[str, int] = {}
    if any_cba_rolling:
        raw_carryover = get_carryover_streaks(clientId, pay_date, client_params)
        carryover_dict = {str(k).strip(): int(v) for k, v in raw_carryover.items()}

    # ── 4. Assign Workweek_ID ─────────────────────────────────────────────────
    df["Workweek_ID"] = _assign_workweek_id(
        df["Attributed_Workday"], workweek_start_name
    )

    # ── 5. Sort for correct streak & cumulative calculations ──────────────────
    df.sort_values(["ID", "Attributed_Workday"], inplace=True)
    df.reset_index(drop=True, inplace=True)

    # ── 6. Compute per-employee consecutive-day streaks ───────────────────────
    #
    # We iterate employee groups (not row-by-row) so Pandas alignment is safe.
    # Each group is a contiguous slice after sort, so index is monotonic.

    streak_parts: list[pd.Series] = []

    for emp_id, emp_group in df.groupby("ID", sort=False):
        # Resolve per-employee location (take first occurrence — location is
        # assumed constant per employee within a pay period)
        emp_location: str = emp_group["Location"].iloc[0]
        cba_rolling: bool = bool(
            _resolve(client_params, emp_location, "cba_consec_anyweek")
        )

        # Carryover streak: only meaningful for CBA rolling employees
        carryover: int = (
            carryover_dict.get(str(emp_id).strip(), 0) if cba_rolling else 0
        )

        streaks = _compute_streaks_for_employee(
            emp_group,
            cba_rolling=cba_rolling,
            carryover_streak=carryover,
            workweek_start_dow=workweek_start_dow,
            prior_period_date=prior_period_date,
        )
        streak_parts.append(streaks)

    df["Days_Worked_In_Week"] = pd.concat(streak_parts).astype("int64")

    # ── 7. Apply Consecutive Day Premium (per employee) ───────────────────────
    #
    # We group by ID only (not workweek) because CBA rolling streaks can span
    # workweek boundaries. The threshold check is per-row, so grouping by ID
    # is sufficient and correct.

    processed_parts: list[pd.DataFrame] = []

    for emp_id, emp_group in df.groupby("ID", sort=False):
        emp_location = emp_group["Location"].iloc[0]
        consec_threshold: int = int(
            _resolve(client_params, emp_location, "number_of_consec_days_before_ot")
        )
        emp_group = emp_group.copy()
        emp_group = _apply_consec_premium(emp_group, consec_threshold)
        processed_parts.append(emp_group)

    df = pd.concat(processed_parts).sort_values(["ID", "Attributed_Workday"])
    df.reset_index(drop=True, inplace=True)

    # ── 8. Apply Weekly OT Spillover (per employee × workweek) ───────────────
    #
    # Standard OT is always workweek-bounded, even for CBA employees.

    ot_parts: list[pd.DataFrame] = []

    for (_, _), ww_group in df.groupby(["ID", "Workweek_ID"], sort=False):
        emp_location = ww_group["Location"].iloc[0]
        ot_week_max: float = float(_resolve(client_params, emp_location, "ot_week_max"))
        ww_group = _apply_weekly_ot(ww_group, ot_week_max)
        ot_parts.append(ww_group)

    df = pd.concat(ot_parts).sort_values(["ID", "Attributed_Workday"])
    df.reset_index(drop=True, inplace=True)

    # ── 9. Final type enforcement ─────────────────────────────────────────────
    df["Days_Worked_In_Week"] = df["Days_Worked_In_Week"].astype("int64")
    df["Is_Consecutive_Day_Rule"] = df["Is_Consecutive_Day_Rule"].astype(bool)
    df["Cum_Reg_Hrs"] = df["Cum_Reg_Hrs"].astype("float64")
    df["Weekly_OT_Spillover"] = df["Weekly_OT_Spillover"].astype("float64")
    df["Workweek_ID"] = pd.to_datetime(df["Workweek_ID"])
    # --- THE FIX: Round all float columns to 4 decimal places ---
    float_cols = [
        "Hours_Worked",
        "Regular_Hrs",
        "OT_Hrs",
        "DT_Hrs",
        "Cum_Reg_Hrs",
        "Weekly_OT_Spillover",
    ]
    df[float_cols] = df[float_cols].round(4)
    # ------------------------------------------------------------

    return df
