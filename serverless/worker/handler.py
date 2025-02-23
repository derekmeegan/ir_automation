import os
import json
import boto3
import asyncio
import requests
from typing import Any, Dict
from classes.ir import IRWorkflow
from flask import Flask, jsonify

app = Flask(__name__)

@app.route("/process", methods=["POST"])
def process() -> Any:
    deployment_type = os.environ.get("DEPLOYMENT_TYPE", "")
    config = {
        "quarter": os.environ.get("QUARTER", ""),
        "year": os.environ.get("YEAR", ""),
        "json_data": os.environ.get("JSON_DATA", ""),
        "deployment_type": deployment_type,
        "groq_api_secret_arn": os.environ.get("GROQ_API_SECRET_ARN", ""),
        "groq_api_key": os.environ.get("GROQ_API_KEY", ""),
        "discord_webhook_arn": os.environ.get("DISCORD_WEBHOOK_SECRET_ARN", ""),
        "discord_webhook_url": os.environ.get("DISCORD_WEBHOOK_URL", ""),
        **json.loads(os.environ.get("SITE_CONFIG", "{}"))
    }

    workflow = IRWorkflow(config)
    while True:
        metrics = asyncio.run(workflow.process_earnings())
        if metrics is None:
            if deployment_type != "local":
                ec2 = boto3.client("ec2", region_name="us-east-1")
                instance_id = requests.get("http://169.254.169.254/latest/meta-data/instance-id").text
                ec2.terminate_instances(InstanceIds=[instance_id])
                print(f"Instance {instance_id} is terminating.")
            break
    return jsonify('Success')

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
