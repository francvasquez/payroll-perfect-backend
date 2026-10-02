"""
PAGA Audit report generator.

Rebuilds one row per employee × pay period from S3 raw CSVs (WFN master,
FLSA Code = N), recomputing RROP / TA metrics so results.json caps do not
truncate the population.
"""

from __future__ import annotations

import io
import logging
from datetime import datetime, timezone

import boto3
import openpyxl
import pandas as pd
from botocore.exceptions import ClientError
from openpyxl.styles import Font

from app_config import S3_BUCKET
from client_config import CLIENT_CONFIGS
from exceptions import AppError
from helper.aux import extract_global_config
from ta.ta_process import process_data_ta
from waiver.waiver_process import process_waiver
from wfn.wfn_process import process_data_wfn

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")

# Guide row-5 headers (output order). Keys are internal; values are Excel headers.
# Duplicate display names are intentional.
OUTPUT_SPEC = [
    ("co", "CO."),
    ("pay_date", "PAY DATE"),
    ("file_num", "FILE#"),
    ("employee_id", "Employee ID"),
    ("payroll_name", "Payroll Name"),
    ("home_dept_code", "Home Department Code"),
    ("home_dept_desc", "Home Department Description"),
    ("job_title", "Job Title Description"),
    ("flsa_code", "FLSA Code"),
    ("position_status", "Position Status"),
    ("hire_date", "HIREDATE"),
    ("rehire_date", "Rehire Date"),
    ("termination_date", "Termination Date"),
    ("regular_rate_paid", "Regular Rate Paid"),
    ("reg", "REG"),
    ("ot", "OT"),
    ("dbltime_hrs", "DBLTIME HRS"),
    ("hw_holiday_worked_hours", "HW_Holiday Worked_Hours"),
    ("h_holiday_hours", "H_Holiday_Hours"),
    ("s_sick_pay_hours", "S_Sick Pay_Hours"),
    ("s19_sick_c19_hours", "S19_Sick C19_Hours"),
    ("v_vacation_hours", "V_Vacation_Hours"),
    ("regular_earnings_total", "Regular Earnings Total"),
    ("overtime_earnings_total", "Overtime Earnings Total"),
    ("d_double_time_earnings", "D_Double Time_Additional Earnings"),
    ("hw_holiday_worked_earnings", "HW_Holiday Worked_Earnings"),
    ("h_holiday_earnings", "H_Holiday_Earnings"),
    ("s_sick_pay_earnings", "S_Sick Pay_Earnings"),
    ("s19_sick_c19_earnings", "S19_Sick C19_Earnings"),
    ("v_vacation_earnings", "V_Vacation_Earnings"),
    ("a_misc_adjust", "A_MISC ADJUST_flsa earnings"),
    ("c_commission", "C_Ee Commission_Additional Earnings"),
    ("e_auto_gratuities", "E_Auto Gratuities_Additional Earnings"),
    ("x_restr_svc", "X_RESTR SVC CHG_Additional Earnings"),
    ("y_bellman", "Y_BELLMANSVCCHG_Additional Earnings"),
    ("b_bonus", "B_Bonus_Additional Earnings"),
    ("sb_sales_bonus", "SB_Sales Bonus_Earnings"),
    ("bd_bonus_disc", "BD_BonusDiscretion_Earnings"),
    ("t_tips", "T - Tips"),
    ("ret_retro", "RET_$ Retro Pay_Earnings"),
    ("j_break_earnings", "J_Break Credits_Additional Earnings"),
    ("j_break_hours", "J_Break Credits_Additional Hours"),
    ("rc_rest_earnings", "RC_Rest Credit_Earnings"),
    ("rc_rest_hours", "RC - Rest Credit Hours"),
    ("ot_rrop", "OT RROP"),
    ("ot_hours", "OT Hours"),
    ("ot_earnings_due", "OT Earnings Due"),
    ("ot_actual", "Actual Pay Check"),
    ("ot_variance", "Variance"),
    ("ot_overpay", "Overpay"),
    ("ot_underpay", "Underpay"),
    ("blank_1", "Blank Separator"),
    ("dt_rrop", "DT RROP"),
    ("dt_hours", "Double Time Hours"),
    ("dt_due", "Double Time Due"),
    ("dt_actual", "Actual Pay Check"),
    ("dt_variance", "Variance"),
    ("dt_overpay", "Overpay"),
    ("dt_underpay", "Underpay"),
    ("blank_2", "Blank Separator"),
    ("brk_hours", "Break Credit Hours"),
    ("brk_rrop", "Break Credit RROP"),
    ("brk_due", "Break Credit Due"),
    ("brk_actual", "Actual Pay"),
    ("brk_variance", "Variance"),
    ("brk_overpay", "Overpay"),
    ("brk_underpay", "Underpay"),
    ("blank_3", "Blank separator"),
    ("rest_hours", "Rest Credit Hours"),
    ("rest_rrop", "Rest Credit RROP"),
    ("rest_due", "Rest Credit Due"),
    ("rest_actual", "Actual Pay"),
    ("rest_variance", "Variance"),
    ("rest_overpay", "Overpay"),
    ("rest_underpay", "Underpay"),
    ("blank_4", "Blank separator"),
    ("sick_hours", "Sick Hours"),
    ("sick_rrop", "Sick Credit RROP"),
    ("sick_due", "Sick Due"),
    ("sick_paid", "Sick Paid"),
    ("sick_variance", "Variance"),
    ("sick_overpay", "Overpay"),
    ("sick_underpay", "Underpay"),
    ("blank_5", "Blank separator"),
    ("rrop_comments", "RROP Comments"),
    ("blank_6", "Blank separator"),
    ("short_breaks", "Short Breaks"),
    ("meal_waiver_check", 'End shift >5, <=6 (in PP, "Meal Waiver Check")'),
    ("late_missed_1st", "Late/missed 1st Break"),
    ("late_missed_2nd", "Late/missed 2nd Break"),
    ("meal_waiver_on_file", "Meal Waiver on File"),
    ("total_brk_due", "Total Break Credits Due"),
    ("total_brk_paid", "Total Break Credits Paid"),
    ("brk_summary_variance", "Variance"),
    ("blank_7", ""),  # intentional empty spacer (guide col 96)
    ("reporting_time_pay", "Reporting Time Pay"),
    ("comments", "Comments"),
]

RAW_WFN_COL_MAP = {
    "co": "CO.",
    "pay_date": "PAY DATE",
    "file_num": "FILE#",
    "payroll_name": "Payroll Name",
    "home_dept_code": "Home Department Code",
    "home_dept_desc": "Home Department Description",
    "job_title": "Job Title Description",
    "flsa_code": "FLSA Code",
    "position_status": "Position Status",
    "hire_date": "HIREDATE",
    "rehire_date": "Rehire Date",
    "termination_date": "Termination Date",
    "regular_rate_paid": "Regular Rate Paid",
    "reg": "REG",
    "ot": "OT",
    "dbltime_hrs": "DBLTIME HRS",
    "hw_holiday_worked_hours": "HW_Holiday Worked_Hours",
    "h_holiday_hours": "H_Holiday_Hours",
    "s_sick_pay_hours": "S_Sick Pay_Hours",
    "s19_sick_c19_hours": "S19_Sick C19_Hours",
    "v_vacation_hours": "V_Vacation_Hours",
    "regular_earnings_total": "Regular Earnings Total",
    "overtime_earnings_total": "Overtime Earnings Total",
    "d_double_time_earnings": "D_Double Time_Additional Earnings",
    "hw_holiday_worked_earnings": "HW_Holiday Worked_Earnings",
    "h_holiday_earnings": "H_Holiday_Earnings",
    "s_sick_pay_earnings": "S_Sick Pay_Earnings",
    "s19_sick_c19_earnings": "S19_Sick C19_Earnings",
    "v_vacation_earnings": "V_Vacation_Earnings",
    "a_misc_adjust": "A_MISC ADJUST_flsa earnings",
    "c_commission": "C_Ee Commission_Additional Earnings",
    "e_auto_gratuities": "E_Auto Gratuities_Additional Earnings",
    "x_restr_svc": "X_RESTR SVC CHG_Additional Earnings",
    "y_bellman": "Y_BELLMANSVCCHG_Additional Earnings",
    "b_bonus": "B_Bonus_Additional Earnings",
    "sb_sales_bonus": "SB_Sales Bonus_Earnings",
    "bd_bonus_disc": "BD_BonusDiscretion_Earnings",
    "t_tips": "T - Tips",
    "ret_retro": "RET_$ Retro Pay_Earnings",
    "j_break_earnings": "J_Break Credits_Additional Earnings",
    "j_break_hours": "J_Break Credits_Additional Hours",
    "rc_rest_earnings": "RC_Rest Credit_Earnings",
    "rc_rest_hours": "RC - Rest Credit Hours",
}


def _blank_if_na(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if pd.isna(value):
        return None
    return value


def _split_over_under(variance):
    """Positive → Overpay; negative → Underpay (signed). Else both blank."""
    if variance is None or (isinstance(variance, float) and pd.isna(variance)):
        return None, None
    try:
        v = float(variance)
    except (TypeError, ValueError):
        return None, None
    if v > 0:
        return v, None
    if v < 0:
        return None, v
    return None, None


def _build_employee_id(co_series, file_series) -> pd.Series:
    co = co_series.fillna("").astype(str).str.strip()
    file_num = pd.to_numeric(file_series, errors="coerce")
    file_str = file_num.map(
        lambda x: "" if pd.isna(x) else f"{int(x):06d}"
    )
    return co + "0" + file_str


def _detect_wfn_system(df: pd.DataFrame, client_id: str):
    wfn_systems = CLIENT_CONFIGS.get(client_id, {}).get("wfn_systems", {})
    cols = set(df.columns.astype(str).str.strip())
    for name, config in wfn_systems.items():
        required = config.get("detection", {}).get("columns", [])
        if required and all(c in cols for c in required):
            return name, config
    if wfn_systems:
        # CSV already normalized to ADP headers — fall back to first system
        name = next(iter(wfn_systems))
        return name, wfn_systems[name]
    raise AppError(
        f"No WFN systems configured for client '{client_id}'.", status_code=400
    )


def _detect_ta_system(df: pd.DataFrame, client_id: str):
    ta_systems = CLIENT_CONFIGS.get(client_id, {}).get("ta_systems", {})
    cols = set(df.columns.astype(str).str.strip())
    for name, config in ta_systems.items():
        required = config.get("detection", {}).get("columns", [])
        if required and all(c in cols for c in required):
            return name, config
    if ta_systems:
        name = next(iter(ta_systems))
        return name, ta_systems[name]
    raise AppError(
        f"No TA systems configured for client '{client_id}'.", status_code=400
    )


def _load_csv_from_s3(key: str) -> pd.DataFrame:
    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("NoSuchKey", "404"):
            raise AppError(f"Missing S3 object: {key}", status_code=404)
        raise AppError(f"Failed to load {key} from storage", status_code=500) from e
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))
    df.columns = df.columns.astype(str).str.strip()
    return df


def _load_waiver_processed(client_id: str) -> pd.DataFrame | None:
    key = f"clients/{client_id}/waiver/waiver.csv"
    try:
        raw = _load_csv_from_s3(key)
    except AppError as e:
        if e.status_code == 404:
            logger.info("No waiver.csv for %s; continuing without waiver.", client_id)
            return None
        raise
    return process_waiver(raw)


def _extract_raw_master(raw_wfn: pd.DataFrame) -> pd.DataFrame:
    """Pull ADP-named identity/earnings columns; build Employee ID; filter FLSA=N."""
    out = pd.DataFrame(index=raw_wfn.index)
    for key, col in RAW_WFN_COL_MAP.items():
        if col in raw_wfn.columns:
            out[key] = raw_wfn[col]
        else:
            out[key] = pd.NA

    if "CO." not in raw_wfn.columns or "FILE#" not in raw_wfn.columns:
        raise AppError(
            "WFN CSV is missing CO. or FILE# required to build Employee ID.",
            status_code=400,
        )

    out["employee_id"] = _build_employee_id(raw_wfn["CO."], raw_wfn["FILE#"])
    out["co"] = out["co"].astype("string").str.strip()

    flsa = out["flsa_code"].astype("string").str.strip().str.upper()
    out = out.loc[flsa == "N"].copy()
    return out.reset_index(drop=True)


def _normalize_co_set(co_codes) -> set[str] | None:
    """None = no filter (include all). Empty set is invalid at call site."""
    if co_codes is None:
        return None
    return {str(c).strip() for c in co_codes if str(c).strip()}


def _filter_master_by_co(
    master: pd.DataFrame, include_co: set[str] | None
) -> pd.DataFrame:
    if include_co is None:
        return master
    if master.empty or "co" not in master.columns:
        return master
    co = master["co"].astype("string").str.strip()
    return master.loc[co.isin(include_co)].copy().reset_index(drop=True)


def list_paga_co_codes(client_id: str, pay_dates: list):
    """
    Distinct CO. values among FLSA=N employees in the selected processed periods.
    Used by the PAGA Audit UI for include/exclude checkboxes.
    """
    if not client_id:
        raise AppError("clientId is required.", status_code=400)
    if not pay_dates:
        raise AppError("Select at least one pay period.", status_code=400)

    ordered = sorted({str(d) for d in pay_dates})
    # co -> {employee_ids}, co -> {pay_dates present}
    employees_by_co: dict[str, set[str]] = {}
    periods_by_co: dict[str, set[str]] = {}
    errors: list[str] = []

    for pay_date in ordered:
        results_key = f"clients/{client_id}/processed/{pay_date}/results.json"
        try:
            s3_client.head_object(Bucket=S3_BUCKET, Key=results_key)
        except ClientError:
            errors.append(f"{pay_date}: not processed")
            continue

        wfn_key = f"clients/{client_id}/csv/{pay_date}/wfn.csv"
        try:
            raw_wfn = _load_csv_from_s3(wfn_key)
            master = _extract_raw_master(raw_wfn)
        except AppError as e:
            errors.append(f"{pay_date}: {e.message}")
            continue
        except Exception as e:
            errors.append(f"{pay_date}: {e}")
            continue

        if master.empty:
            continue

        slim = master[["co", "employee_id"]].dropna(subset=["co"]).copy()
        slim["co"] = slim["co"].astype("string").str.strip()
        slim = slim[slim["co"] != ""]
        for co, group in slim.groupby("co", sort=False):
            co_key = str(co)
            employees_by_co.setdefault(co_key, set()).update(
                group["employee_id"].dropna().astype(str).str.strip().tolist()
            )
            periods_by_co.setdefault(co_key, set()).add(pay_date)

    co_codes = [
        {
            "co": co,
            "uniqueEmployeeIds": len(employees_by_co[co]),
            "periodsPresent": len(periods_by_co[co]),
        }
        for co in sorted(employees_by_co.keys())
    ]

    return {
        "coCodes": co_codes,
        "periodCount": len(ordered),
        "warnings": errors,
    }


def _over_under_cols(series: pd.Series):
    over, under = [], []
    for v in series:
        o, u = _split_over_under(v)
        over.append(o)
        under.append(u)
    return over, under


def _attach_wfn_metrics(master: pd.DataFrame, processed: pd.DataFrame) -> pd.DataFrame:
    if processed is None or processed.empty or "IDX" not in processed.columns:
        return master

    cols = {
        "IDX": "employee_id",
        "1.5x OT Rate": "ot_rrop",
        "1.5 OT Earnings Due": "ot_earnings_due",
        "Actual Pay Check": "ot_actual",
        "Variance": "ot_variance",
        "Double Time Rate": "dt_rrop",
        "Double Time Due": "dt_due",
        "Actual Pay Check Dble": "dt_actual",
        "Variance Dble": "dt_variance",
        "RROP": "brk_rrop",
        "Break Credit Due": "brk_due",
        "Actual Pay BrkCrd": "brk_actual",
        "Variance BrkCrd": "brk_variance",
        "Rest Credit Due": "rest_due",
        "Actual Pay RestCrd": "rest_actual",
        "Variance RestCrd": "rest_variance",
        "Sick Credit Hours": "sick_hours",
        "RROP Sick": "sick_rrop",
        "Sick Credit Due": "sick_due",
        "Sick Paid": "sick_paid",
        "Variance Sick": "sick_variance",
    }
    present = [c for c in cols if c in processed.columns]
    slim = processed[present].copy()
    slim = slim.rename(columns={c: cols[c] for c in present})
    # Rest / break share RROP
    if "brk_rrop" in slim.columns:
        slim["rest_rrop"] = slim["brk_rrop"]

    merged = master.merge(slim, on="employee_id", how="left", suffixes=("", "_proc"))
    return merged


def _attach_ta_metrics(
    master: pd.DataFrame,
    anomalies: pd.DataFrame | None,
    processed_ta: pd.DataFrame | None,
) -> pd.DataFrame:
    out = master

    if anomalies is not None and not anomalies.empty and "ID" in anomalies.columns:
        a = anomalies.rename(
            columns={
                "ID": "employee_id",
                "Short Break": "short_breaks",
                "Did Not Break": "meal_waiver_check",
                "First Meal Break": "late_missed_1st",
                "Over Twelve": "late_missed_2nd",
                "Due Break Credit (hrs)": "total_brk_due",
                "Paid Break Credit (hrs)": "total_brk_paid",
                "Variance": "brk_summary_variance",
            }
        )
        keep = [
            c
            for c in [
                "employee_id",
                "short_breaks",
                "meal_waiver_check",
                "late_missed_1st",
                "late_missed_2nd",
                "total_brk_due",
                "total_brk_paid",
                "brk_summary_variance",
            ]
            if c in a.columns
        ]
        out = out.merge(a[keep], on="employee_id", how="left")

    if processed_ta is not None and not processed_ta.empty and "ID" in processed_ta.columns:
        ta = processed_ta.copy()
        if "Waiver on File?" in ta.columns:
            waiver_any = (
                ta.groupby("ID", as_index=False)["Waiver on File?"]
                .any()
                .rename(columns={"ID": "employee_id", "Waiver on File?": "meal_waiver_on_file"})
            )
            out = out.merge(waiver_any, on="employee_id", how="left")

        if "RTP_Warning" in ta.columns:
            rtp = (
                ta.loc[ta["RTP_Warning"] == True]  # noqa: E712
                .groupby("ID")
                .size()
                .reset_index(name="reporting_time_pay")
                .rename(columns={"ID": "employee_id"})
            )
            out = out.merge(rtp, on="employee_id", how="left")

    return out


def _finalize_period_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
    """Derive aliases, over/under, drop duplicate IDX (keep first); return notes."""
    notes = []
    work = df.copy()

    # Aliases from this report's own columns
    work["ot_hours"] = work.get("ot", pd.Series(index=work.index, dtype=object))
    work["dt_hours"] = work.get("dbltime_hrs", pd.Series(index=work.index, dtype=object))
    work["brk_hours"] = work.get("j_break_hours", pd.Series(index=work.index, dtype=object))
    work["rest_hours"] = work.get("rc_rest_hours", pd.Series(index=work.index, dtype=object))

    for var_key, over_key, under_key in [
        ("ot_variance", "ot_overpay", "ot_underpay"),
        ("dt_variance", "dt_overpay", "dt_underpay"),
        ("brk_variance", "brk_overpay", "brk_underpay"),
        ("rest_variance", "rest_overpay", "rest_underpay"),
        ("sick_variance", "sick_overpay", "sick_underpay"),
    ]:
        if var_key in work.columns:
            o, u = _over_under_cols(work[var_key])
            work[over_key] = o
            work[under_key] = u

    work["rrop_comments"] = None
    work["comments"] = None
    for blank in ("blank_1", "blank_2", "blank_3", "blank_4", "blank_5", "blank_6", "blank_7"):
        work[blank] = None

    # Duplicate employee IDs within the period
    dup_mask = work.duplicated(subset=["employee_id"], keep=False)
    if dup_mask.any():
        for _, row in work.loc[dup_mask].iterrows():
            notes.append(
                {
                    "pay_date": row.get("pay_date"),
                    "employee_id": row.get("employee_id"),
                    "payroll_name": row.get("payroll_name"),
                    "issue": "Duplicate Employee ID in WFN for this pay period; kept first row.",
                }
            )
        work = work.drop_duplicates(subset=["employee_id"], keep="first")

    return work, notes


def _process_one_period(
    client_id: str,
    pay_date: str,
    client_params: dict,
    processed_waiver_df: pd.DataFrame | None,
    include_co: set[str] | None = None,
) -> tuple[pd.DataFrame, list[dict], dict]:
    notes: list[dict] = []
    empty_check = {
        "payDate": pay_date,
        "wfnUniqueEmployeeIds": 0,
        "auditUniqueEmployeeIds": 0,
        "match": True,
    }
    wfn_key = f"clients/{client_id}/csv/{pay_date}/wfn.csv"
    ta_key = f"clients/{client_id}/csv/{pay_date}/ta.csv"

    raw_wfn = _load_csv_from_s3(wfn_key)
    raw_ta = _load_csv_from_s3(ta_key)

    master = _extract_raw_master(raw_wfn)
    master = _filter_master_by_co(master, include_co)
    wfn_unique = (
        int(master["employee_id"].nunique())
        if not master.empty and "employee_id" in master.columns
        else 0
    )
    empty_check["wfnUniqueEmployeeIds"] = wfn_unique

    if master.empty:
        notes.append(
            {
                "pay_date": pay_date,
                "employee_id": "",
                "payroll_name": "",
                "issue": (
                    "No FLSA Code = N employees in WFN for this period"
                    + (" after CO. filter." if include_co is not None else ".")
                ),
            }
        )
        return master, notes, empty_check

    # Force pay date column for the report (from folder / processing context)
    master["pay_date"] = pay_date

    _, wfn_config = _detect_wfn_system(raw_wfn, client_id)
    _, ta_config = _detect_ta_system(raw_ta, client_id)

    params = {
        "clientId": client_id,
        "payDate": pay_date,
        "client_config": client_params,
    }
    (
        min_wage,
        state_min_wage,
        pay_periods_per_year,
        pay_date_ts,
        _first,
        _last,
    ) = extract_global_config(params)

    processed_wfn, _exceptions = process_data_wfn(
        raw_wfn.copy(),
        client_params,
        wfn_config,
        min_wage,
        state_min_wage,
        pay_periods_per_year,
        pay_date_ts,
        disregard_pay_date_mismatches=True,
    )

    master = _attach_wfn_metrics(master, processed_wfn)

    processed_ta, _daily, anomalies, _db = process_data_ta(
        raw_ta.copy(),
        client_params,
        ta_config,
        min_wage,
        pay_date_ts,
        client_id,
        processed_waiver_df=processed_waiver_df,
        processed_wfn_df=processed_wfn,
        ignore_warnings=True,
        persist_to_db=False,
        compute_daily=False,
    )

    master = _attach_ta_metrics(master, anomalies, processed_ta)
    master, dup_notes = _finalize_period_frame(master)
    notes.extend(dup_notes)

    # Non-violator convention: blank Reporting Time Pay when count is 0 / missing
    if "reporting_time_pay" in master.columns:
        master["reporting_time_pay"] = master["reporting_time_pay"].replace(0, pd.NA)

    audit_unique = (
        int(master["employee_id"].nunique())
        if not master.empty and "employee_id" in master.columns
        else 0
    )
    id_check = {
        "payDate": pay_date,
        "wfnUniqueEmployeeIds": wfn_unique,
        "auditUniqueEmployeeIds": audit_unique,
        "match": wfn_unique == audit_unique,
    }
    if not id_check["match"]:
        notes.append(
            {
                "pay_date": pay_date,
                "employee_id": "",
                "payroll_name": "",
                "issue": (
                    f"Employee ID count mismatch vs WFN (FLSA=N): "
                    f"WFN={wfn_unique}, audit={audit_unique}."
                ),
            }
        )

    return master, notes, id_check


def _sheet_title_for_pay_date(pay_date: str, used: set[str]) -> str:
    """Excel sheet names max 31 chars; cannot contain \\ / ? * [ ]."""
    base = str(pay_date).strip() or "Period"
    for ch in ("\\", "/", "?", "*", "[", "]"):
        base = base.replace(ch, "-")
    base = base[:31] or "Period"
    title = base
    n = 2
    while title in used:
        suffix = f"_{n}"
        title = f"{base[: 31 - len(suffix)]}{suffix}"
        n += 1
    used.add(title)
    return title


def _write_data_sheet(ws, rows: list[dict]) -> None:
    headers = [h for _, h in OUTPUT_SPEC]
    keys = [k for k, _ in OUTPUT_SPEC]
    ws.append(headers)
    for cell in ws[1]:
        cell.font = Font(bold=True)
    for row in rows:
        ws.append([_blank_if_na(row.get(k)) for k in keys])


def _write_notes_sheet(wb, notes: list[dict]) -> None:
    notes_ws = wb.create_sheet("Notes")
    notes_ws.append(["Pay Date", "Employee ID", "Payroll Name", "Issue"])
    for cell in notes_ws[1]:
        cell.font = Font(bold=True)
    if notes:
        for n in notes:
            notes_ws.append(
                [
                    n.get("pay_date"),
                    n.get("employee_id"),
                    n.get("payroll_name"),
                    n.get("issue"),
                ]
            )
    else:
        notes_ws.append(["", "", "", "No notes. Kept first row on any duplicates."])


def _rows_to_workbook(
    rows: list[dict],
    notes: list[dict],
    sheet_layout: str = "stacked",
    ordered_pay_dates: list[str] | None = None,
) -> bytes:
    wb = openpyxl.Workbook()
    # Remove default sheet; rebuild based on layout
    default = wb.active
    wb.remove(default)

    layout = (sheet_layout or "stacked").strip().lower()
    if layout not in ("stacked", "split"):
        layout = "stacked"

    if layout == "split":
        by_date: dict[str, list[dict]] = {}
        for row in rows:
            pd_key = str(row.get("pay_date") or "").strip() or "Unknown"
            by_date.setdefault(pd_key, []).append(row)

        order = ordered_pay_dates or sorted(by_date.keys())
        used_titles: set[str] = set()
        for pay_date in order:
            period_rows = by_date.get(pay_date)
            if not period_rows:
                continue
            ws = wb.create_sheet(_sheet_title_for_pay_date(pay_date, used_titles))
            _write_data_sheet(ws, period_rows)

        # If every period failed (no data sheets), still produce an empty shell
        if not wb.worksheets:
            ws = wb.create_sheet("PAGA Audit")
            _write_data_sheet(ws, [])
    else:
        ws = wb.create_sheet("PAGA Audit")
        _write_data_sheet(ws, rows)

    _write_notes_sheet(wb, notes)

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def generate_paga_audit(
    client_id: str,
    pay_dates: list,
    client_params: dict | None,
    sheet_layout: str = "stacked",
    include_co_codes=None,
):
    if not client_id:
        raise AppError("clientId is required.", status_code=400)
    if not pay_dates:
        raise AppError("Select at least one pay period.", status_code=400)
    if not client_params:
        raise AppError("client_config is required.", status_code=400)

    layout = (sheet_layout or "stacked").strip().lower()
    if layout not in ("stacked", "split"):
        raise AppError(
            "sheetLayout must be 'stacked' or 'split'.",
            status_code=400,
        )

    include_co = _normalize_co_set(include_co_codes)
    if include_co_codes is not None and not include_co:
        raise AppError("Select at least one CO. code to include.", status_code=400)

    # Dedupe + sort ascending for stacked readability
    ordered = sorted({str(d) for d in pay_dates})

    processed_waiver = _load_waiver_processed(client_id)

    all_rows: list[dict] = []
    all_notes: list[dict] = []
    errors: list[str] = []
    employee_id_checks: list[dict] = []

    for pay_date in ordered:
        # Confirm period was processed
        results_key = f"clients/{client_id}/processed/{pay_date}/results.json"
        try:
            s3_client.head_object(Bucket=S3_BUCKET, Key=results_key)
        except ClientError:
            errors.append(f"{pay_date}: not a processed pay period (missing results.json).")
            all_notes.append(
                {
                    "pay_date": pay_date,
                    "employee_id": "",
                    "payroll_name": "",
                    "issue": "Skipped — period has not been processed.",
                }
            )
            continue

        try:
            frame, notes, id_check = _process_one_period(
                client_id,
                pay_date,
                client_params,
                processed_waiver,
                include_co=include_co,
            )
            all_notes.extend(notes)
            employee_id_checks.append(id_check)
            if not frame.empty:
                all_rows.extend(frame.to_dict(orient="records"))
        except AppError as e:
            errors.append(f"{pay_date}: {e.message}")
            all_notes.append(
                {
                    "pay_date": pay_date,
                    "employee_id": "",
                    "payroll_name": "",
                    "issue": e.message,
                }
            )
        except Exception as e:
            logger.exception("PAGA audit failed for %s/%s", client_id, pay_date)
            errors.append(f"{pay_date}: {e}")
            all_notes.append(
                {
                    "pay_date": pay_date,
                    "employee_id": "",
                    "payroll_name": "",
                    "issue": f"Unexpected error: {e}",
                }
            )

    if not all_rows and errors:
        raise AppError(
            "PAGA audit could not build any rows. " + " | ".join(errors),
            status_code=400,
        )

    if not all_rows and include_co is not None:
        raise AppError(
            "No employees matched the selected CO. codes for the chosen periods.",
            status_code=400,
        )

    xlsx_bytes = _rows_to_workbook(
        all_rows, all_notes, sheet_layout=layout, ordered_pay_dates=ordered
    )

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filename = f"paga_audit_{client_id}_{stamp}.xlsx"
    s3_key = f"clients/{client_id}/exports/{filename}"

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=xlsx_bytes,
        ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    download_url = s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": S3_BUCKET, "Key": s3_key},
        ExpiresIn=600,
    )

    employee_id_match = bool(employee_id_checks) and all(
        c.get("match") for c in employee_id_checks
    )

    return {
        "downloadUrl": download_url,
        "s3Key": s3_key,
        "filename": filename,
        "rowCount": len(all_rows),
        "periodCount": len(ordered),
        "notesCount": len(all_notes),
        "sheetLayout": layout,
        "includedCoCodes": sorted(include_co) if include_co is not None else None,
        "employeeIdChecks": employee_id_checks,
        "employeeIdMatch": employee_id_match,
        "warnings": errors,
    }
