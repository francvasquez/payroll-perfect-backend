import pandas as pd
from app_config import (
    CORS_HEADERS,
)
from helper.aux import verify_files, extract_global_config
from helper.aws import (
    read_excel_from_s3,
    read_ta_excel_from_s3,
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

    ### 1. Verify all three files are provided (we already do in frontend, consider removing)
    error_response = verify_files(params)
    if error_response:
        return error_response

    ### 2. Extract client_id and client_params
    client_id = params["clientId"]
    client_params = params["client_config"]

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
        annotation_result = delete_annotations(client_id, pay_date)

    ### 6. Process WAIVER
    waiver_df = read_excel_from_s3(params["waiver_key"])
    waiver_start = time.time()
    processed_waiver_df = process_waiver(waiver_df)
    waiver_process_time = round((time.time() - waiver_start) * 1000, 2)
    print(f"Waiver processed: {len(processed_waiver_df)} rows")

    ### 7. Process WFN
    wfn_df = read_excel_from_s3(params["wfn_key"], header=5)
    wfn_start = time.time()
    processed_wfn_df = process_data_wfn(
        wfn_df, client_params, min_wage, state_min_wage, pay_periods_per_year
    )
    wfn_process_time = round((time.time() - wfn_start) * 1000, 2)
    print(f"WFN processed: {len(processed_wfn_df)} rows")

    ### 8. Process TA (using results from first two)
    ta_df, system_name, system_config = read_ta_excel_from_s3(
        params["ta_key"], client_id
    )
    print(
        f"Will normalize for system: {system_name}, using {system_config} for client: {client_id}"
    )
    ta_start = time.time()
    processed_ta_df, daily_df, anomalies_df_new = process_data_ta(
        ta_df,
        client_params,
        system_config,
        min_wage,
        pay_date,
        client_id,
        processed_waiver_df,
        processed_wfn_df,
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
    # Store json files for ready-to-serve front consumption
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

    # To be passed to front-end upon success via lambda_handler
    return {
        "status": "success",
        "details": {
            "annotations": annotation_result[
                "message"
            ],  # Add other success details as needed for UI
        },
        "result": result,
    }
