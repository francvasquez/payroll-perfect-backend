import json
from app_config import *
from helper.aux import parse_event_params
from helper.action_router import route_action


def lambda_handler(event, _):

    ### 0. CORS Preflight check - clean up once determined
    method = event.get("requestContext", {}).get("http", {}).get("method")
    if method == "OPTIONS":
        print("OPTIONS preflight detected")
        return {"statusCode": 200, "headers": CORS_HEADERS, "body": ""}

    try:
        ### 1. Check action, extract params
        params, contactParams = parse_event_params(event)
        print("Action requested: ", params["action"], "with params: ", params)

        ### 2. Route based on action
        action = params.get("action")
        return route_action(action, params, contactParams, event)

    except Exception as e:
        print(f"Error in lambda_handler: {str(e)}")
        import traceback

        print(traceback.format_exc())
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": str(e)}),
        }
