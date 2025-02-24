import boto3
import requests
import time
import threading

ec2_client = boto3.client("ec2", region_name="us-east-1")

def lambda_handler(event, context):
    """
    Expects an event with keys:
      - "action": "start" or "stop"
      - "release_time": a tag value identifying the target instances
    """
    action = event.get("action", "start")
    release_time = event.get("release_time")
    
    if not release_time:
        return {"error": "Missing release_time in event"}
    
    # Build filters to select instances by the ReleaseTime tag and desired state.
    filters = [{"Name": "tag:ReleaseTime", "Values": [release_time]}]
    if action == "start":
        filters.append({"Name": "instance-state-name", "Values": ["stopped"]})
    elif action == "stop":
        filters.append({"Name": "instance-state-name", "Values": ["running"]})
    else:
        return {"error": f"Invalid action: {action}"}
    
    # Query instances matching the filters.
    response = ec2_client.describe_instances(Filters=filters)
    instance_ids = []
    for reservation in response.get("Reservations", []):
        for instance in reservation.get("Instances", []):
            instance_ids.append(instance["InstanceId"])
    
    if not instance_ids:
        return {"message": f"No instances found to {action} for release_time {release_time}"}
    
    if action == "start":
        # Start the instances.
        ec2_client.start_instances(InstanceIds=instance_ids)
        # Wait until they reach the 'running' state.
        waiter = ec2_client.get_waiter('instance_running')
        waiter.wait(InstanceIds=instance_ids)
        
        # Poll each instance until it has a public IP, and trigger its /process endpoint asynchronously.
        poll_and_trigger(instance_ids)
        
        return {"message": "Started instances and triggered endpoints", "instance_ids": instance_ids}
    
    elif action == "stop":
        ec2_client.stop_instances(InstanceIds=instance_ids)
        return {"message": "Stopped instances", "instance_ids": instance_ids}

def poll_and_trigger(instance_ids):
    """
    Continuously polls the given instance IDs until each one has a public IP (i.e. is fully initialized).
    Once an instance is ready, it spawns a separate thread to send a POST request to its /process endpoint,
    then removes it from the polling set.
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
                url = f"http://{public_ip}:8080/process"
                # Spawn a thread to send the POST request asynchronously.
                threading.Thread(target=send_post, args=(url, instance_id)).start()
                remaining.remove(instance_id)
        if remaining:
            time.sleep(10)  # Poll every 10 seconds.

def send_post(url, instance_id):
    try:
        response = requests.post(url, json={})
        print(f"Triggered {url} for instance {instance_id}: {response.status_code}")
    except Exception as e:
        print(f"Error triggering {url} for instance {instance_id}: {e}")
