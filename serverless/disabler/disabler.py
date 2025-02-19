import boto3
from typing import Any, Dict, List

events_client = boto3.client("events")

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    rule_name: str = event.get("rule_name")
    target_ids: List[str] = event.get("target_ids", [])
    
    if not rule_name:
        return {"error": "Missing rule_name"}
    
    # Disable the rule.
    events_client.disable_rule(Name=rule_name)
    
    # Remove specified targets; if none provided, use a default target ID.
    if target_ids:
        events_client.remove_targets(Rule=rule_name, Ids=target_ids)
    else:
        events_client.remove_targets(Rule=rule_name, Ids=[f"{rule_name}Target"])
    
    return {"message": f"Disabled rule {rule_name} and removed targets"}
