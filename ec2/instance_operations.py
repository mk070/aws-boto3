import boto3
import logging

logger = logging.getLogger(__name__)
ec2 = boto3.client('ec2')

def start_instance(instance_id):
    try:
        ec2.start_instances(InstanceIds=[instance_id])
        logger.info(f"Instance '{instance_id}' has been started.")
    except Exception as e:
        logger.error(f"Error starting instance '{instance_id}': {e}")

def stop_instance(instance_id):
    try:
        ec2.stop_instances(InstanceIds=[instance_id])
        logger.info(f"Instance '{instance_id}' has been stopped.")
    except Exception as e:
        logger.error(f"Error stopping instance '{instance_id}': {e}")

def reboot_instance(instance_id):
    try:
        ec2.reboot_instances(InstanceIds=[instance_id])
        logger.info(f"Instance '{instance_id}' has been rebooted.")
    except Exception as e:
        logger.error(f"Error rebooting instance '{instance_id}': {e}")

def list_all_instances():
    try:
        response = ec2.describe_instances()
        instances = []
        for reservation in response['Reservations']:
            for instance in reservation['Instances']:
                instances.append({
                    'InstanceId': instance['InstanceId'],
                    'InstanceType': instance['InstanceType'],
                    'State': instance['State']['Name'],
                    'PublicIpAddress': instance.get('PublicIpAddress', 'N/A'),
                    'PrivateIpAddress': instance.get('PrivateIpAddress', 'N/A')
                })
        
        print("\n--- EC2 Instances ---")
        for i, instance in enumerate(instances, 1):
            print(f"{i}. Instance ID: {instance['InstanceId']}, Type: {instance['InstanceType']}, "
                  f"State: {instance['State']}, Public IP: {instance['PublicIpAddress']}, "
                  f"Private IP: {instance['PrivateIpAddress']}")
        print(f"Total Instances: {len(instances)}\n")
        
        return instances
    except Exception as e:
        logger.error(f"Error listing EC2 instances: {e}")
        return []
