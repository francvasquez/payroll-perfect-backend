import json, traceback
from app_config import *
from helper.aux import parse_event_params
from helper.action_router import route_action
from exceptions import AppError


def _make_error_response(status_code: int, message: str) -> dict:
    return {
        "statusCode": status_code,
        "headers": CORS_HEADERS,
        "body": json.dumps({"error": message}),
    }


def lambda_handler(event, _):

    ### 0. CORS Preflight check - clean up once determined
    method = event.get("requestContext", {}).get("http", {}).get("method")
    if method == "OPTIONS":
        print("OPTIONS preflight detected")
        return {"statusCode": 200, "headers": CORS_HEADERS, "body": ""}

    try:
        ### 1. Check action, extract params
        params = parse_event_params(event)
        print("Action requested: ", params["action"], "with params: ", params)

        ### 2. Route based on action
        action = params.get("action")
        payload = route_action(action, params, event)

        ### 3. Wrap successful response to API Gateway
        return {
            "statusCode": 200,
            "headers": CORS_HEADERS,
            "body": json.dumps(payload, default=str),
        }

    except AppError as e:
        # Known, safe-to-expose errors (400, 404, etc.)
        print(f"App error [{e.status_code}]: {e.message}")
        return _make_error_response(e.status_code, e.message)

    except Exception as e:
        # Unexpected errors — never leak internals to the client
        print(f"Unhandled error: {e}")
        traceback.print_exc()
        return _make_error_response(500, "Internal server error")
