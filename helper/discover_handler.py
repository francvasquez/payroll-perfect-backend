import utility
from client_config import CLIENT_CONFIGS
from helper.aws import (
    read_wfn_excel_from_s3,
    read_ta_excel_from_s3,
    save_parcel_csv_to_s3,
)
from helper.pay_period_discovery import (
    discover_pay_periods,
    filter_ta_to_pay_period_window,
    filter_wfn_to_pay_date,
    merge_period_config,
)
from exceptions import ValidationError


def _prepare_wfn_for_discovery(wfn_df, wfn_system_config):
    df = utility.normalize_client_data(wfn_df, wfn_system_config)
    df = utility.drop_rows(df, wfn_system_config)
    if "Pay Date" in df.columns:
        df = utility.to_pandas_datetime(df, "Pay Date")
    return df


def _prepare_ta_for_discovery(ta_df, ta_system_config):
    df = utility.normalize_client_data(ta_df, ta_system_config)
    df = utility.drop_rows(df, ta_system_config)
    if "In Punch" in df.columns:
        df = utility.to_pandas_datetime(df, "In Punch")
    return df


def _parcel_processable_periods(periods, prepared_wfn, prepared_ta, client_config, client_id):
    """
    Write one WFN/TA CSV per processable pay period so each process-files
    invocation reads a single-period slice instead of the full bulk intake.
    """
    for period in periods:
        if not period.get("processable"):
            continue

        pay_date = period["pay_date"]
        period_config = merge_period_config(client_config, pay_date)
        sliced_wfn = filter_wfn_to_pay_date(prepared_wfn, pay_date)
        sliced_ta = filter_ta_to_pay_period_window(prepared_ta, pay_date, period_config)

        if sliced_wfn is None or sliced_wfn.empty:
            period["processable"] = False
            period["skip_reason"] = "No matching WFN rows for this pay date."
            continue
        if sliced_ta is None or sliced_ta.empty:
            period["processable"] = False
            period["skip_reason"] = "No TA punches found for this pay date."
            continue

        period["wfn_key"] = save_parcel_csv_to_s3(sliced_wfn, client_id, pay_date, "wfn")
        period["ta_key"] = save_parcel_csv_to_s3(sliced_ta, client_id, pay_date, "ta")


def handle_discover_pay_periods(params):
    """
    Discover fiscal pay periods from multi-period intake files already uploaded to S3.
    Also parcels each processable period to raw/{payDate}/ for fast per-period processing.
    """
    client_id = params.get("clientId")
    wfn_key = params.get("wfn_key")
    ta_key = params.get("ta_key")
    client_config = params.get("client_config") or {}

    if not client_id:
        raise ValidationError("clientId is required.")
    if not all([wfn_key, ta_key]):
        raise ValidationError("Both WFN and TA S3 keys are required.")
    if not client_config.get("global"):
        raise ValidationError("client_config.global is required.")

    client_cfg = CLIENT_CONFIGS.get(client_id, {})
    anchor_pay_date = client_cfg.get("anchor_pay_date")
    if not anchor_pay_date:
        raise ValidationError(
            f"No anchor_pay_date configured for client '{client_id}'."
        )

    wfn_df, _, wfn_system_config = read_wfn_excel_from_s3(wfn_key, client_id)
    ta_df, _, ta_system_config = read_ta_excel_from_s3(ta_key, client_id)

    prepared_wfn = _prepare_wfn_for_discovery(wfn_df, wfn_system_config)
    prepared_ta = _prepare_ta_for_discovery(ta_df, ta_system_config)

    if "Pay Date" not in prepared_wfn.columns:
        raise ValidationError("Could not locate Pay Date column in WFN file.")
    if "In Punch" not in prepared_ta.columns:
        raise ValidationError("Could not locate In Punch column in TA file.")

    periods = discover_pay_periods(
        prepared_ta,
        prepared_wfn,
        client_config,
        anchor_pay_date,
    )

    _parcel_processable_periods(
        periods, prepared_wfn, prepared_ta, client_config, client_id
    )

    processable_count = sum(1 for p in periods if p["processable"])

    return {
        "anchor_pay_date": anchor_pay_date,
        "pay_periods": periods,
        "processable_count": processable_count,
        "total_count": len(periods),
    }
