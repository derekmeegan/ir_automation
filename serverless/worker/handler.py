import os
import json
import asyncio
from typing import Any, Dict
from classes.ir import IRWorkflow

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda entry point for the IRWorkflow job.
    Merges default config from environment variables with any event overrides.
    """
    config = {
        'quarter': os.environ.get("QUARTER", ""),
        'year': os.environ.get("YEAR", ""),
        'csv_uri': os.environ.get("CSV_URI", ""),
        'deployment_type': os.environ.get("DEPLOYMENT_TYPE", ""),
        'groq_api_key': os.environ.get("GROQ_API_KEY", ""),
        **json.loads(os.environ.get("SITE_CONFIG", "{}"))
    }

    workflow = IRWorkflow(config)
    try:
        metrics = asyncio.run(workflow.process_earnings())
    except Exception as e:
        return {"error": str(e)}

    return metrics
