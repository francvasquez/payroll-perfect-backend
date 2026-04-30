from helper.aws import (
    handle_get_client_config,
    handle_save_client_config,
    handle_presigned_url_request,
    load_processed_results,
    list_pay_periods,
    save_annotations,
    load_annotations,
    delete_annotations,
    delete_pay_period,
)
from helper.db_utils import handle_query_ta_records, handle_get_ta_columns
from helper.file_processor import handle_file_upload


def route_action(action, params, event):
    # Pre extract params
    clientId = params.get("clientId")
    employeeId = params.get("employeeId")
    startDate = params.get("startDate")
    endDate = params.get("endDate")
    selectedCols = params.get("selectedCols", [])
    payDate = params.get("payDate")
    annotations = params.get("annotations")
    config = params.get("config")

    # Use ELIF to prevent the "Default Fallthrough" to file upload
    if action == "query-ta-records":
        return handle_query_ta_records(
            clientId, employeeId, startDate, endDate, selectedCols
        )
    elif action == "get-ta-columns":
        return handle_get_ta_columns(clientId)
    elif action == "get-client-config":
        return handle_get_client_config(clientId)
    elif action == "save-client-config":
        return handle_save_client_config(clientId, config)
    elif action == "list-pay-periods":
        return list_pay_periods(clientId)
    elif action == "load-processed-results":
        return load_processed_results(clientId, payDate)
    elif action == "get-upload-url":
        return handle_presigned_url_request(params)
    elif action == "save-annotations":
        return save_annotations(clientId, payDate, annotations)
    elif action == "load-annotations":
        return load_annotations(clientId, payDate)
    elif action == "delete-annotations":
        return delete_annotations(clientId, payDate)
    elif action == "delete-pay-period":
        return delete_pay_period(clientId, payDate)
    else:
        # This will pass return dictionary to lambda_handler which will convert it to API Gateway response
        return handle_file_upload(event, params)
