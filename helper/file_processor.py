from helper.aux import verify_files, extract_global_config
from helper.aws import (
    read_wfn_excel_from_s3,
    read_ta_excel_from_s3,
    read_waiver_excel_from_s3,
    save_csv_to_s3,
    save_waiver_json_s3,
    put_result_to_s3,
    delete_annotations,
    is_parceled_raw_key,
)
from helper.discover_handler import (
    _prepare_ta_for_discovery,
    _prepare_wfn_for_discovery,
)
from helper.pay_period_discovery import (
    filter_ta_to_pay_period_window,
    filter_wfn_to_pay_date,
    merge_period_config,
)
from helper.results import generate_results
from ta.ta_process import process_data_ta
from waiver.waiver_process import process_waiver
from wfn.wfn_process import process_data_wfn
import json, time, copy


def _get_ignore_warnings(event, params):
    try:
        raw_body = json.loads(event.get("body", "{}"))
        return raw_body.get("ignore_warnings", False)
    except Exception:
        return params.get("ignore_warnings", False)


def _patch_event_pay_date(event, pay_date_str):
    """Return a shallow copy of event with pay_date updated in the body."""
    patched = copy.deepcopy(event)
    body = json.loads(patched.get("body", "{}"))
    body["pay_date"] = pay_date_str
    patched["body"] = json.dumps(body)
    return patched


def handle_file_upload(event, params):
    """
    Processes Waiver → WFN → TA for a single pay period.

    Supports three intake modes:
    - Legacy single-period: WFN/TA in raw/{payDate}/ (Excel, full normalize)
    - Multi-period parceled: CSV slices written at discover time (fast path)
    - Multi-period bulk fallback: raw/intake/ bulk files filtered per request (slow)
    """
    waiver_key, wfn_key, ta_key = verify_files(params)

    client_id = params["clientId"]
    base_client_config = params["client_config"]
    pay_date = params["payDate"]
    intake_id = params.get("intake_id")
    is_multi_period = bool(intake_id)
    is_bulk_intake = is_multi_period and wfn_key and "/raw/intake/" in wfn_key
    is_parceled = is_parceled_raw_key(wfn_key) and is_parceled_raw_key(ta_key)

    period_config = (
        merge_period_config(base_client_config, pay_date)
        if is_multi_period
        else base_client_config
    )
    params_for_config = {**params, "client_config": period_config}

    ignore_warnings = _get_ignore_warnings(event, params)

    (
        min_wage,
        state_min_wage,
        pay_periods_per_year,
        pay_date_ts,
        first_date,
        last_date,
    ) = extract_global_config(params_for_config)

    pay_date_str = pd_to_date_str(pay_date_ts)

    print(
        f"file_processor.py - Processing pay_date={pay_date_str}, "
        f"multi_period={is_multi_period}, parceled={is_parceled}, "
        f"bulk_intake={is_bulk_intake}, intake_id={intake_id}"
    )

    del_annot_msg = None
    if client_id and pay_date_str:
        print(f"Deleting annotations for {client_id}/{pay_date_str} b4 reprocessing.")
        del_annot_msg = delete_annotations(client_id, pay_date_str)

    if waiver_key:
        waiver_start = time.time()
        waiver_df = read_waiver_excel_from_s3(waiver_key)
        processed_waiver_df = process_waiver(waiver_df)
        waiver_process_time = round((time.time() - waiver_start) * 1000, 2)
        print(f"Waiver processed: {len(processed_waiver_df)} rows")
    else:
        waiver_df = None
        processed_waiver_df = None
        waiver_process_time = 0
        print("No waiver file provided, skipping waiver processing.")

    wfn_df, wfn_system_name, wfn_system_config = read_wfn_excel_from_s3(
        wfn_key, client_id
    )
    ta_df, ta_system_name, ta_system_config = read_ta_excel_from_s3(ta_key, client_id)

    if is_bulk_intake:
        prepared_wfn = _prepare_wfn_for_discovery(wfn_df, wfn_system_config)
        prepared_ta = _prepare_ta_for_discovery(ta_df, ta_system_config)
        wfn_df = filter_wfn_to_pay_date(prepared_wfn, pay_date_str)
        ta_df = filter_ta_to_pay_period_window(prepared_ta, pay_date_str, period_config)

        if wfn_df is None or wfn_df.empty:
            from exceptions import AppError

            raise AppError(
                f"No WFN rows found for pay date {pay_date_str}.",
                status_code=400,
            )
        if ta_df is None or ta_df.empty:
            from exceptions import AppError

            raise AppError(
                f"No TA punches found for pay date {pay_date_str}.",
                status_code=400,
            )

    skip_intake_prep = is_bulk_intake or is_parceled

    print(
        f"Will normalize WFN system: {wfn_system_name}; TA system: {ta_system_name}"
    )

    wfn_start = time.time()
    processed_wfn_df, wfn_exceptions = process_data_wfn(
        wfn_df,
        period_config,
        wfn_system_config,
        min_wage,
        state_min_wage,
        pay_periods_per_year,
        pay_date_ts,
        skip_intake_prep=skip_intake_prep,
    )
    wfn_process_time = round((time.time() - wfn_start) * 1000, 2)
    print(f"WFN processed: {len(processed_wfn_df)} rows")

    ta_start = time.time()
    processed_ta_df, daily_df, anomalies_df_new = process_data_ta(
        ta_df,
        period_config,
        ta_system_config,
        min_wage,
        pay_date_ts,
        client_id,
        processed_waiver_df,
        processed_wfn_df,
        ignore_warnings,
        skip_intake_prep=skip_intake_prep,
    )
    ta_process_time = round((time.time() - ta_start) * 1000, 2)
    print("TA processed")

    event_with_pay_date = _patch_event_pay_date(event, pay_date_str)

    if ta_df is not None:
        save_csv_to_s3(ta_df, "ta", event_with_pay_date)
    if wfn_df is not None:
        save_csv_to_s3(wfn_df, "wfn", event_with_pay_date)
    if waiver_df is not None:
        save_csv_to_s3(waiver_df, "waiver", event_with_pay_date)
        save_waiver_json_s3(waiver_df, "waiver", event_with_pay_date)

    result = generate_results(
        processed_ta_df,
        daily_df,
        anomalies_df_new,
        processed_wfn_df,
        processed_waiver_df,
        ta_process_time,
        wfn_process_time,
        waiver_process_time,
        first_date,
        last_date,
        pay_date_ts,
        client_id,
        wfn_exceptions=wfn_exceptions,
    )
    put_result_to_s3(result, event_with_pay_date)

    result["details"] = {"del_annot_msg": del_annot_msg}
    if is_multi_period:
        result["details"]["intake_id"] = intake_id

    return result


def pd_to_date_str(pay_date):
    """Normalize pay date to YYYY-MM-DD string."""
    import pandas as pd

    return pd.to_datetime(pay_date).strftime("%Y-%m-%d")
