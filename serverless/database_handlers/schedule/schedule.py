import json
import os
import logging
from typing import Any, Dict, Optional, Union
import boto3
import decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
TABLE_NAME: str = os.environ.get("SCHEDULE_TABLE")
table = dynamodb.Table(TABLE_NAME)

class DecimalEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, decimal.Decimal):
            return float(o) if o % 1 else int(o)
        return super().default(o)

def build_response(status_code: int, body: Optional[Union[dict, list]] = None) -> Dict[str, Any]:
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "GET,POST,PUT,OPTIONS"
        },
        "body": json.dumps(body, cls=DecimalEncoder) if body is not None else ""
    }

def handler(event: dict, context: object) -> dict:
    try:
        method: str = event.get("httpMethod", "")
        if method == "OPTIONS":
            return build_response(200)
        elif method == "GET":
            response = table.scan()
            return build_response(200, response.get("Items", []))
        elif method == "POST":
            body: dict = json.loads(event.get("body", "{}"))
            table.put_item(Item=body)
            return build_response(201, body)
        elif method == "PUT":
            body: dict = json.loads(event.get("body", "{}"))
            ticker = body.get("ticker")
            date = body.get("date")
            if not ticker or not date:
                return build_response(400, {"error": "ticker and date are required"})
            update_expr = "set is_active = :is_active, quarter = :quarter, release_time = :release_time, #yr = :year"
            expr_attr_names = {"#yr": "year"}
            expr_attr_values = {
                ":is_active": body.get("is_active"),
                ":quarter": body.get("quarter"),
                ":release_time": body.get("release_time"),
                ":year": body.get("year")
            }
            response = table.update_item(
                Key={"ticker": ticker, "date": date},
                UpdateExpression=update_expr,
                ExpressionAttributeNames=expr_attr_names,
                ExpressionAttributeValues=expr_attr_values,
                ReturnValues="ALL_NEW"
            )
            return build_response(200, response.get("Attributes", {}))
        else:
            return build_response(405, {"error": "Method Not Allowed"})
    except Exception as e:
        logger.error("Earnings Lambda error: %s", str(e))
        return build_response(500, {"error": "Internal Server Error"})
