import os
import boto3
import json
from decimal import Decimal
from datetime import datetime
from typing import Any, Dict
from boto3.dynamodb.conditions import Key

DYNAMO_TABLE = os.environ["TABLE_NAME"]
WORKER_IMAGE_URI = os.environ["WORKER_IMAGE_URI"]
WORKER_EXECUTION_ROLE = os.environ["WORKER_EXECUTION_ROLE"]
HISTORICAL_TABLE = os.environ["HISTORICAL_TABLE"]
CONFIG_TABLE = os.environ["CONFIG_TABLE"]
DISABLER_URL = os.environ["DISABLER_URL"]

dynamo = boto3.resource("dynamodb")
lambda_client = boto3.client("lambda")
events_client = boto3.client("events")

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    - Reads today's records from the scheduling table.
    - For each record (ticker, date, quarter, release_time),
      1) Generates a JSON file from the historical table,
         retrieves a JSON site config from the config table,
         and passes these values along with other variables.
      2) Creates/updates a dedicated worker Lambda with the additional variables.
      3) Creates/updates an EventBridge rule to ping that worker function 
         between ~5:55AM–9:30AM ET or 3:55PM–6:30PM ET, depending on release_time.
    """
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    table = dynamo.Table(DYNAMO_TABLE)

    response = table.query(
        KeyConditionExpression=Key("date").eq(today_str)
    )
    items = response.get("Items", [])
    print(f"Found {len(items)} items for {today_str}")

    created_or_updated = []

    for item in items:
        ticker = item["ticker"]
        release_time = item.get("release_time", "after").lower()  # "before" or "after"
        quarter = item.get("quarter")
        year = item.get("year")

        # 1) Generate JSON from historical data and retrieve site config.
        json_data = generate_json_for_ticker(ticker, today_str)
        site_config = get_site_config(ticker)

        rule_name = f"PingRule-{ticker}-{today_str}"
        event_id = _create_or_update_ping_rule(rule_name, function_name, release_time)

        # Compose environment variables for the worker.
        variables = {
            "QUARTER": quarter,
            "YEAR": year,
            "JSON_DATA": json_data,
            "SITE_CONFIG": site_config,
            "PING_RULE_NAME":rule_name,
            "PING_RULE_ID": event_id,
            "DISABLER_URL": DISABLER_URL,
        }

        # 2) Create or update the worker Lambda for this ticker.
        function_name = f"WorkerFunction-{ticker}"
        create_or_update_worker_function(function_name, variables)

        created_or_updated.append({"ticker": ticker, "function": function_name, "rule": rule_name})

    return {"created_or_updated": created_or_updated}

def generate_json_for_ticker(ticker: str, today_str: str) -> str:
    historical_table = dynamo.Table(HISTORICAL_TABLE)
    response = historical_table.get_item(Key={"ticker": ticker, "date": today_str})
    item = response.get("Item", {})
    if not item:
        raise ValueError(f"No historical data found for ticker {ticker}")
    return json.dumps(item, default=lambda o: float(o) if isinstance(o, Decimal) else o)

def get_site_config(ticker: str) -> str:
    """
    Query the config table for a given ticker and return the JSON configuration as a string.
    """
    config_table = dynamo.Table(CONFIG_TABLE)
    response = config_table.get_item(Key={"ticker": ticker})
    item = response.get("Item", {})
    return json.dumps(item, default=lambda o: float(o) if isinstance(o, Decimal) else o)

def create_or_update_worker_function(function_name: str, variables: dict) -> None:
    """
    Create or update a worker Lambda that uses the shared Docker image,
    passing the provided environment variables.
    """
    try:
        # Check if the function exists.
        lambda_client.get_function(FunctionName=function_name)
        print(f"Worker Lambda {function_name} found; updating code and environment...")
        # Update configuration (environment) and the container image.
        lambda_client.update_function_configuration(
            FunctionName=function_name,
            Environment={"Variables": variables}
        )
        lambda_client.update_function_code(
            FunctionName=function_name,
            ImageUri=WORKER_IMAGE_URI
        )
    except lambda_client.exceptions.ResourceNotFoundException:
        print(f"Creating worker Lambda {function_name} with image {WORKER_IMAGE_URI}...")
        lambda_client.create_function(
            FunctionName=function_name,
            Role=WORKER_EXECUTION_ROLE,
            PackageType="Image",
            Code={"ImageUri": WORKER_IMAGE_URI},
            Timeout=900,
            MemorySize=1024,
            Environment={"Variables": variables},
        )

def _create_or_update_ping_rule(rule_name: str, function_name: str, release_time: str) -> None:
    """
    Create/update an EventBridge rule & target that pings the specified Lambda 
    at intervals between ~5:55–9:30 AM ET or 3:55–6:30 PM ET.
    """
    if release_time == "before":
        cron_expr = "cron(0/15 10-13 ? * * *)"  # morning window (approx.)
    else:
        cron_expr = "cron(0/15 20-23 ? * * *)"  # afternoon window (approx.)

    events_client.put_rule(
        Name=rule_name,
        ScheduleExpression=cron_expr,
        State="ENABLED",
        Description=f"Ping {function_name} for {release_time} release"
    )

    lambda_arn = f"arn:aws:lambda:{os.environ['AWS_REGION']}:{os.environ['AWS_ACCOUNT_ID']}:function:{function_name}"
    event_id = f"{function_name}Target"
    events_client.put_targets(
        Rule=rule_name,
        Targets=[
            {
                "Id": event_id,
                "Arn": lambda_arn,
                "RoleArn": _get_event_invoke_role(),
            }
        ]
    )
    return event_id

def _get_event_invoke_role() -> str:
    """
    Return an IAM role ARN that allows EventBridge to invoke the worker Lambda.
    """
    return f"arn:aws:iam::{os.environ['AWS_ACCOUNT_ID']}:role/EventBridgeInvokeLambdaRole"
    
