import json
import os
import logging
import decimal
from typing import Any, Dict, Optional, Union
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
TABLE_NAME: str = os.environ.get("MESSAGES_TABLE", "")
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
            "Access-Control-Allow-Methods": "GET,POST,DELETE,PATCH,OPTIONS"
        },
        "body": json.dumps(body, cls=DecimalEncoder) if body is not None else ""
    }

def handler(event: dict, context: object) -> dict:
    try:
        method: str = event.get("httpMethod", "")
        path_params: Dict[str, Any] = event.get("pathParameters") or {}

        if method == "OPTIONS":
            return build_response(200)

        if method == "GET":
            message_id: Optional[str] = path_params.get("id")
            if message_id:
                response = table.get_item(Key={"id": message_id})
                item = response.get("Item")
                if item is None:
                    return build_response(404, {"error": "Message not found"})
                return build_response(200, item)
            else:
                response = table.scan()
                items = response.get("Items", [])
                return build_response(200, items)

        elif method == "POST":
            body: dict = json.loads(event.get("body", "{}"))
            table.put_item(Item=body)
            return build_response(201, body)

        elif method == "PATCH":
            # Expected endpoint: /messages/{id}/read to mark a message as read
            message_id: Optional[str] = path_params.get("id")
            if not message_id:
                return build_response(400, {"error": "Missing message ID"})
            table.update_item(
                Key={"id": message_id},
                UpdateExpression="set is_read = :r",
                ExpressionAttributeValues={":r": True},
                ReturnValues="UPDATED_NEW"
            )
            return build_response(200, {"message": "Message marked as read"})

        elif method == "DELETE":
            message_id: Optional[str] = path_params.get("id")
            if not message_id:
                return build_response(400, {"error": "Missing message ID"})
            table.delete_item(Key={"id": message_id})
            return build_response(200, {"message": "Message deleted"})

        else:
            return build_response(405, {"error": "Method Not Allowed"})
    except Exception as e:
        logger.error("Error processing request: %s", str(e))
        return build_response(500, {"error": "Internal Server Error"})
