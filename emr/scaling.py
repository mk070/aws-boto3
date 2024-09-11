import boto3
import logging

logger = logging.getLogger(__name__)
emr = boto3.client('emr')

def add_instance_group(cluster_id, instance_type, instance_count):
    try:
        response = emr.add_instance_groups(
            InstanceGroups=[
                {
                    'Name': 'Additional core nodes',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': instance_type,
                    'InstanceCount': instance_count,
                }
            ],
            JobFlowId=cluster_id
        )
        instance_group_ids = response['InstanceGroupIds']
        logger.info(f"Added {instance_count} instances of type '{instance_type}' to cluster '{cluster_id}'.")
        return instance_group_ids
    except Exception as e:
        logger.error(f"Error adding instance group to EMR cluster '{cluster_id}': {e}")

def modify_instance_group(instance_group_id, instance_count):
    try:
        emr.modify_instance_groups(
            InstanceGroups=[
                {
                    'InstanceGroupId': instance_group_id,
                    'InstanceCount': instance_count
                }
            ]
        )
        logger.info(f"Modified instance group '{instance_group_id}' to have {instance_count} instances.")
    except Exception as e:
        logger.error(f"Error modifying instance group '{instance_group_id}': {e}")
