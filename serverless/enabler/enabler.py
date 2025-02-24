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
                if wait_for_endpoint(health_url, timeout=60, interval=5):
                    # Now that the app is healthy, trigger the /process endpoint asynchronously.
                    url = f"http://{public_ip}:8080/process"
                    threading.Thread(target=send_post, args=(url, instance_id)).start()
                    remaining.remove(instance_id)
                else:
                    print(f"Instance {instance_id} at {public_ip} not ready yet.")
        if remaining:
            time.sleep(5)  # Wait a bit before polling again.

def send_post(url, instance_id):
    try:
        response = requests.post(url, json={})
        print(f"Triggered {url} for instance {instance_id}: {response.status_code}")
    except Exception as e:
        print(f"Error triggering {url} for instance {instance_id}: {e}")