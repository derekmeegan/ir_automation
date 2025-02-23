import boto3

ec2_client = boto3.client("ec2", region_name="us-east-1")

def lambda_handler(event, context):
    """
    Expects event with keys:
      - "action": "start" or "stop"
      - "report_type": e.g. "before" or "after"
    """
    action = event.get("action", "start")
    report_type = event.get("release_time")
    
    if not report_type:
        return {"error": "Missing report_type in event"}
    
    # Build filters to select instances by tag and state
    filters = [{"Name": "tag:ReleaseTime", "Values": [report_type]}]
    
    if action == "start":
        filters.append({"Name": "instance-state-name", "Values": ["stopped"]})
    elif action == "stop":
        filters.append({"Name": "instance-state-name", "Values": ["running"]})
    else:
        return {"error": f"Invalid action: {action}"}
    
    # Query instances matching the filters
    response = ec2_client.describe_instances(Filters=filters)
    instance_ids = []
    for reservation in response.get("Reservations", []):
        for instance in reservation.get("Instances", []):
            instance_ids.append(instance["InstanceId"])
    
    if not instance_ids:
        return {"message": f"No instances found to {action} for report_type {report_type}"}
    
    # Start or stop the instances based on the action
    if action == "start":
        ec2_client.start_instances(InstanceIds=instance_ids)
        return {"message": "Started instances", "instance_ids": instance_ids}
    elif action == "stop":
        ec2_client.stop_instances(InstanceIds=instance_ids)
        return {"message": "Stopped instances", "instance_ids": instance_ids}
