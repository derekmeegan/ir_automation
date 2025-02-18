import asyncio
import os
import json
from typing import Any, Dict
from classes.ir import IRWorkflow

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda entry point for the IRWorkflow job.
    Merges default config from environment variables with any event overrides.
    """
    config: Dict[str, Any] = {
        "quarter": os.environ.get("QUARTER", ""),
        "ir_url": os.environ.get("IR_URL", ""),
        "ticker": os.environ.get("TICKER", ""),
        "csv_uri": os.environ.get("CSV_URI", ""),
        "site_config": json.loads(os.environ.get("SITE_CONFIG", "{}")),
    }

    workflow = IRWorkflow(config)
    try:
        metrics = asyncio.run(workflow.process_earnings())
    except Exception as e:
        return {"error": str(e)}
    return metrics
