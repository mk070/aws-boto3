import boto3
import logging

logger = logging.getLogger(__name__)
ec2 = boto3.client('ec2')

def check_instance_status(instance_id):
    try:
        response = ec2.describe_instance_status(InstanceIds=[instance_id])
        statuses = response.get('InstanceStatuses', [])
        if statuses:
            status = statuses[0]
            logger.info(f"Instance ID: {instance_id}")
            logger.info(f"Instance State: {status['InstanceState']['Name']}")
            logger.info(f"System Status: {status['SystemStatus']['Status']}")
            logger.info(f"Instance Status: {status['InstanceStatus']['Status']}")
        else:
            logger.info(f"No status found for instance '{instance_id}'. It might be stopped or terminated.")
    except Exception as e:
        logger.error(f"Error checking status of instance '{instance_id}': {e}")
