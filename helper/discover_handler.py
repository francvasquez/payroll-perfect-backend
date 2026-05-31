import utility
from client_config import CLIENT_CONFIGS
from helper.aws import (
    read_wfn_excel_from_s3,
    read_ta_excel_from_s3,
)
from helper.pay_period_discovery import discover_pay_periods
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


def handle_discover_pay_periods(params):
    """
    Discover fiscal pay periods from multi-period intake files already uploaded to S3.
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

    processable_count = sum(1 for p in periods if p["processable"])

    return {
        "anchor_pay_date": anchor_pay_date,
        "pay_periods": periods,
        "processable_count": processable_count,
        "total_count": len(periods),
    }
