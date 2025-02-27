import os
import boto3
import json
import time
import base64
import requests
from decimal import Decimal
from datetime import datetime
from typing import Any, Dict
from boto3.dynamodb.conditions import Key, Attr

DYNAMO_TABLE = os.environ["TABLE_NAME"]
WORKER_IMAGE_URI = os.environ["WORKER_IMAGE_URI"]
WORKER_EXECUTION_ROLE = os.environ["WORKER_EXECUTION_ROLE"]
HISTORICAL_TABLE = os.environ["HISTORICAL_TABLE"]
CONFIG_TABLE = os.environ["CONFIG_TABLE"]
MESSAGES_TABLE = os.environ["MESSAGES_TABLE"]
GROQ_API_SECRET_ARN = os.environ["GROQ_API_SECRET_ARN"]
DISCORD_WEBHOOK_SECRET_ARN = os.environ["DISCORD_WEBHOOK_SECRET_ARN"]
ARTIFACT_BUCKET = os.environ["ARTIFACT_BUCKET"]

dynamo = boto3.resource("dynamodb")
lambda_client = boto3.client("lambda")
events_client = boto3.client("events")
ec2_client = boto3.client("ec2")

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
    today_str = event.get('today_str', datetime.utcnow().strftime("%Y-%m-%d"))
    release_time = event.get("release_time", "after")
    table = dynamo.Table(DYNAMO_TABLE)

    response = table.query(
        KeyConditionExpression=Key("date").eq(today_str),
        FilterExpression=Attr("release_time").eq(release_time) & Attr("is_active").eq(True)
    )
    items = response.get("Items", [])
    print(f"Found {len(items)} items for {today_str}")

    instance_ids = []

    for item in items:
        ticker = item["ticker"]
        quarter = item.get("quarter")
        year = item.get("year")

        json_data = generate_json_for_ticker(ticker, today_str)
        site_config = get_site_config(ticker)

        variables = {
            "QUARTER": str(int(float(quarter))),
            "YEAR": str(int(float(year))),
            "JSON_DATA": json_data,
            "SITE_CONFIG": site_config,
            "GROQ_API_SECRET_ARN": GROQ_API_SECRET_ARN,
            "DISCORD_WEBHOOK_SECRET_ARN": DISCORD_WEBHOOK_SECRET_ARN,
            "ARTIFACT_BUCKET": ARTIFACT_BUCKET,
            "MESSAGES_TABLE": MESSAGES_TABLE
        }

        function_name = f"WorkerFunction-{ticker}"
        instance_id = create_or_update_worker_instance(function_name, variables)
        instance_ids.append(instance_id)

    poll_and_trigger(instance_ids)

    return instance_ids

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

def create_or_update_worker_instance(instance_name: str, variables: Dict[str, Any]) -> None:
    env_options = " ".join(f"-e {key}='{value}'" for key, value in variables.items())
    user_data_script = f"""#!/bin/bash
yum update -y
amazon-linux-extras install docker -y
service docker start
usermod -a -G docker ec2-user
chkconfig docker on
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin {os.environ['AWS_ACCOUNT_ID']}.dkr.ecr.us-east-1.amazonaws.com
docker pull {WORKER_IMAGE_URI}
docker run -d -p 8080:8080 --restart unless-stopped {env_options} {WORKER_IMAGE_URI}
"""
    encoded_user_data = base64.b64encode(user_data_script.encode("utf-8")).decode("utf-8")
    response = ec2_client.describe_instances(
        Filters=[
            {"Name": "tag:Name", "Values": [instance_name]},
            {"Name": "instance-state-name", "Values": ["pending", "running", "stopped"]},
        ]
    )
    instances = [
        instance
        for reservation in response.get("Reservations", [])
        for instance in reservation.get("Instances", [])
    ]
    if instances:
        instance_id = instances[0]["InstanceId"]
        print(f"EC2 instance {instance_name} ({instance_id}) found; stopping it before updating user data...")
        ec2_client.stop_instances(InstanceIds=[instance_id])
        waiter = ec2_client.get_waiter('instance_stopped')
        waiter.wait(InstanceIds=[instance_id])
        print(f"EC2 instance {instance_name} ({instance_id}) found; updating user data and rebooting...")
        ec2_client.modify_instance_attribute(
            InstanceId=instance_id,
            UserData={"Value": encoded_user_data}
        )
    else:
        print(f"Creating EC2 instance {instance_name} for running the Docker worker image...")
        response = ec2_client.run_instances(
            ImageId="ami-0c104f6f4a5d9d1d5",
            InstanceType="c5.2xlarge",
            MinCount=1,
            MaxCount=1,
            KeyName = 'ir_worker',
            IamInstanceProfile={'Name': os.environ.get("INSTANCE_PROFILE")},
            UserData=encoded_user_data,
            SubnetId=os.environ.get("SUBNET_ID"),
            SecurityGroupIds=[os.environ.get("INSTANCE_SECURITY_GROUP")],
            TagSpecifications=[
                {
                    "ResourceType": "instance",
                    "Tags": [{"Key": "Name", "Value": instance_name}],
                }
            ],
        )
        instance_id = response["Instances"][0]["InstanceId"]
        print(f"Created EC2 instance {instance_name} with ID {instance_id}.")
        waiter = ec2_client.get_waiter('instance_running')
        waiter.wait(InstanceIds=[instance_id])
        
        # Poll each instance until it has a public IP, and trigger its /process endpoint asynchronously.
    return instance_id

def wait_for_endpoint(url, timeout=600, interval=10):
    """
    Continuously attempts a GET request on the provided URL until a 200 response is received,
    or until the timeout (in seconds) is reached.
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                return True
        except Exception as e:
            # Likely connection error - the instance isn't ready yet.
            pass
        time.sleep(interval)
    return False

def poll_and_trigger(instance_ids):
    """
    Continuously polls the given instance IDs until each one has a public IP and its health-check endpoint is ready.
    Once an instance is ready, spawn a thread to send a POST request to its /process endpoint, then remove it from the list.
    """
    remaining = set(instance_ids)
    while remaining:
        for instance_id in list(remaining):
            desc = ec2_client.describe_instances(InstanceIds=[instance_id])
            public_ip = None
            for reservation in desc.get("Reservations", []):
                for inst in reservation.get("Instances", []):
                    public_ip = inst.get("PublicIpAddress")
            if public_ip:
                # Construct the health-check URL (adjust if you have a dedicated health endpoint)
                health_url = f"http://{public_ip}:8080/health"
                if wait_for_endpoint(health_url, timeout=60*5, interval=5):
                    # Now that the app is healthy, trigger the /process endpoint asynchronously.
                    url = f"http://{public_ip}:8080/process"
                    try:
                        # Attempt to make the HTTP request with a short timeout
                        requests.post(url, timeout=1)
                    except requests.exceptions.RequestException as e:
                        # Log request-specific exceptions
                        print(f"Fire and forget... get that ir info!")
                    remaining.remove(instance_id)
                else:
                    print(f"Instance {instance_id} at {public_ip} not ready yet.")
        if remaining:
            time.sleep(5)  # Wait a bit before polling again.