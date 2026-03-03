import json
from config import *
from helper.aux import parse_event_params, route_action


def lambda_handler(event):

    ### 0. CORS Preflight check - clean up once determined
    method1 = event.get("httpMethod")
    method2 = event.get("requestContext", {}).get("http", {}).get("method")
    print("Received HTTP method (httpMethod):", method1)
    print("Received HTTP method (requestContext.http.method):", method2)
    if method1 == "OPTIONS" or method2 == "OPTIONS":
        print("OPTIONS preflight detected")
        return {"statusCode": 200, "headers": CORS_HEADERS, "body": ""}

    try:
        ### 1. Check action, extract params
        params = parse_event_params(event)
        print("Action requested: ", params["action"], "with params: ", params)

        ### 2. Route based on action
        action = params.get("action")
        return route_action(action, params, event)

    except Exception as e:
        print(f"Error in lambda_handler: {str(e)}")
        import traceback

        print(traceback.format_exc())
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": str(e)}),
        }
