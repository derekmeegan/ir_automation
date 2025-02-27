import os
import time
import json
import boto3
import asyncio
import requests
from typing import Any, Dict
from classes.ir import IRWorkflow
from flask import Flask, jsonify

app = Flask(__name__)

@app.route("/health", methods=["GET"])
def health():
    return jsonify('We running baby')

@app.route("/process", methods=["POST"])
def process() -> Any:
    os.environ["SCREEN_WIDTH"] = '1920'
    os.environ["SCREEN_HEIGHT"] = '1024'
    os.environ["SCREEN_DEPTH"] = '16'
    os.environ["MAX_CONCURRENT_CHROME_PROCESSES"]='10'
    os.environ["ENABLE_DEBUGGER"] = 'false'
    os.environ["PREBOOT_CHROME"] = 'true'
    os.environ["CONNECTION_TIMEOUT"] = '300000'
    os.environ["MAX_CONCURRENT_SESSIONS"] = '10'
    os.environ["CHROME_REFRESH_TIME"] = '600000'
    os.environ["DEFAULT_BLOCK_ADS"] = 'true'
    os.environ["DEFAULT_STEALTH"] = 'true'
    os.environ["DEFAULT_IGNORE_HTTPS_ERRORS"] = 'true'
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
        "s3_artifact_bucket": os.environ.get('ARTIFACT_BUCKET', ''),
        "messages_table": os.environ.get('MESSAGES_TABLE', ''),
        **json.loads(os.environ.get("SITE_CONFIG", "{}"))
    }

    workflow = IRWorkflow(config)
    while True:
        try:
            metrics = asyncio.run(workflow.process_earnings())
            if metrics is None:
                if deployment_type != "local":
                    time.sleep(60 * 10)
                    ec2 = boto3.client("ec2", region_name="us-east-1")
                    instance_id = requests.get("http://169.254.169.254/latest/meta-data/instance-id").text
                    ec2.terminate_instances(InstanceIds=[instance_id])
                    print(f"Instance {instance_id} is terminating.")
                break
        except Exception as e:
            print(f'workflow broke with the following error: {e}')
            raise
    return jsonify('Success')

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
