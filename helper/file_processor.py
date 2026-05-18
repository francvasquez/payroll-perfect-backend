from helper.aux import verify_files, extract_global_config
from helper.aws import (
    read_wfn_excel_from_s3,
    read_ta_excel_from_s3,
    read_waiver_excel_from_s3,
    save_csv_to_s3,
    save_waiver_json_s3,
    put_result_to_s3,
    delete_annotations,
)
from helper.results import generate_results
from ta.ta_process import process_data_ta
from waiver.waiver_process import process_waiver
from wfn.wfn_process import process_data_wfn
import json, time


def handle_file_upload(event, params):
    """
    Processes all three files in sequence: Waiver → WFN → TA
    Frontend ensures all three files are provided
    """

    ### 1. Verify TA and WFN are provided (if no Waiver, user has already provided consent in frontend)
    waiver_key, wfn_key, ta_key = verify_files(params)

    ### 2. Extract client_id and client_params
    client_id = params["clientId"]
    client_params = params["client_config"]

    ### 3. Extract user bypass
    try:
        raw_body = json.loads(event.get("body", "{}"))
        ignore_warnings = raw_body.get("ignore_warnings", False)
    except Exception:
        # Fallback just in case
        ignore_warnings = params.get("ignore_warnings", False)

    ### 3 & 4. Extract global parameters with default fallback
    (
        min_wage,
        state_min_wage,
        pay_periods_per_year,
        pay_date,
        first_date,
        last_date,
    ) = extract_global_config(params)

    print(
        f"file_processor.py - Processing: client_params={client_params}, pay_date={pay_date}, first date ={first_date}"
    )

    ### 5. Delete existing annotations before reprocessing
    if client_id and pay_date:
        print(f"Deleting annotations for {client_id}/{pay_date} b4 reprocessing.")
        del_annot_msg = delete_annotations(client_id, pay_date)

    ### 6. Process WAIVER
    if waiver_key:
        waiver_start = time.time()
        waiver_df = read_waiver_excel_from_s3(waiver_key)
        processed_waiver_df = process_waiver(waiver_df)
        waiver_process_time = round((time.time() - waiver_start) * 1000, 2)
        print(f"Waiver processed: {len(processed_waiver_df)} rows")
    else:
        # No waiver file provided
        waiver_df = None
        processed_waiver_df = None
        waiver_process_time = 0
        print("No waiver file provided, skipping waiver processing.")

    ### 7. Process WFN
    wfn_df, wfn_system_name, wfn_system_config = read_wfn_excel_from_s3(
        wfn_key, client_id
    )
    print(
        f"Will normalize for WFN system: {wfn_system_name}, using {wfn_system_config} for client: {client_id}"
    )
    wfn_start = time.time()
    processed_wfn_df = process_data_wfn(
        wfn_df,
        client_params,
        wfn_system_config,
        min_wage,
        state_min_wage,
        pay_periods_per_year,
        pay_date,
    )
    wfn_process_time = round((time.time() - wfn_start) * 1000, 2)
    print(f"WFN processed: {len(processed_wfn_df)} rows")

    ### 8. Process TA (using results from first two)
    ta_df, ta_system_name, ta_system_config = read_ta_excel_from_s3(ta_key, client_id)
    print(
        f"Will normalize for TA system: {ta_system_name}, using {ta_system_config} for client: {client_id}"
    )
    ta_start = time.time()
    processed_ta_df, daily_df, anomalies_df_new = process_data_ta(
        ta_df,
        client_params,
        ta_system_config,
        min_wage,
        pay_date,
        client_id,
        processed_waiver_df,
        processed_wfn_df,
        ignore_warnings,
    )
    ta_process_time = round((time.time() - ta_start) * 1000, 2)
    print("TA processed")

    ### 9. Store raw files to csv for future reference
    if ta_df is not None:
        save_csv_to_s3(ta_df, "ta", event)
    if wfn_df is not None:
        save_csv_to_s3(wfn_df, "wfn", event)
    if waiver_df is not None:
        save_csv_to_s3(waiver_df, "waiver", event)
        save_waiver_json_s3(waiver_df, "waiver", event)

    ### 10. Generate result for React front-end
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
        pay_date,
        client_id,
    )
    put_result_to_s3(result, event)  # save JSON for ready-to-serve front consumption

    ### 11. Add any success details to the result dictionary so front-end can display it after processing
    result["details"] = {
        "del_annot_msg": del_annot_msg
        # You can easily add more here later!
        # "s3_backup": "Success",
        # "email_sent": True
    }

    # Return the flat dictionary so React finds exactly what it expects
    return result
