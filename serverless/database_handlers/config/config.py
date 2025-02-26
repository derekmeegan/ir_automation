import json
import os
import logging
from typing import Any, Dict, Optional, Union
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
TABLE_NAME: str = os.environ.get("CONFIG_TABLE")
table = dynamodb.Table(TABLE_NAME)

def build_response(status_code: int, body: Optional[Union[dict, list]] = None) -> Dict[str, Any]:
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "GET,POST,OPTIONS"
        },
        "body": json.dumps(body) if body is not None else ""
    }

def handler(event: dict, context: object) -> dict:
    try:
        method: str = event.get("httpMethod", "")
        if method == "OPTIONS":
            return build_response(200)
        elif method == "GET":
            path_params: dict = event.get("pathParameters") or {}
            ticker = path_params.get("ticker")
            if ticker:
                response = table.get_item(Key={"ticker": ticker})
                return build_response(200, response.get("Item"))
            else:
                response = table.scan()
                return build_response(200, response.get("Items", []))
        elif method == "POST":
            body: dict = json.loads(event.get("body", "{}"))
            table.put_item(Item=body)
            return build_response(201, body)
        else:
            return build_response(405, {"error": "Method Not Allowed"})
    except Exception as e:
        logger.error("Company Lambda error: %s", str(e))
        return build_response(500, {"error": "Internal Server Error"})
