import os
import json
import asyncio
import requests
from typing import Any, Dict
from classes.ir import IRWorkflow

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda entry point for the IRWorkflow job.
    Merges default config from environment variables with any event overrides.
    """
    deployment_type = os.environ.get("DEPLOYMENT_TYPE", "")
    ping_rule_name = os.environ.get("PING_RULE_NAME", "")
    ping_rule_id = os.environ.get("PING_RULE_ID", "")
    disabler_url = os.environ.get("DISABLER_URL", "")
    config = {
        'quarter': os.environ.get("QUARTER", ""),
        'year': os.environ.get("YEAR", ""),
        'json_data': os.environ.get("JSON_DATA", ""),
        'deployment_type': deployment_type,
        'groq_api_secret_arn': os.environ.get("GROQ_API_SECRET_ARN", ""),
        'groq_api_key': os.environ.get("GROQ_API_KEY", ""),
        'discord_webhook_arn': os.environ.get("DISCORD_WEBHOOK_SECRET_ARN", ""),
        'discord_webhook_url': os.environ.get("DISCORD_WEBHOOK_URL", ""),
        **json.loads(os.environ.get("SITE_CONFIG", "{}"))
    }

    workflow = IRWorkflow(config)
    while True:
        try:
            metrics = asyncio.run(workflow.process_earnings())
            print(metrics)
            if metrics is None:
                if deployment_type != 'local':
                    payload: Dict[str, Any] = {"rule_name": ping_rule_name, "target_ids": [ping_rule_id]}
                    response = requests.post(disabler_url, json=payload)
                    response.raise_for_status()
                    return response.json()
                break
        except Exception as e:
            raise
            print({"error": str(e)})
            return {"error": str(e)}

    return metrics
