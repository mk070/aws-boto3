import boto3
import logging

logger = logging.getLogger(__name__)
emr = boto3.client('emr')

def create_cluster(cluster_name, instance_type, instance_count, release_label='emr-6.3.0'):
    try:
        response = emr.run_job_flow(
            Name=cluster_name,
            ReleaseLabel=release_label,
            Instances={
                'InstanceGroups': [
                    {
                        'Name': 'Master nodes',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': instance_type,
                        'InstanceCount': 1,
                    },
                    {
                        'Name': 'Core nodes',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': instance_type,
                        'InstanceCount': instance_count - 1,
                    }
                ],
                'Ec2KeyName': 'your-key-pair',
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
            },
            Applications=[
                {'Name': 'Hadoop'},
                {'Name': 'Spark'},
            ],
            LogUri='s3://your-log-bucket/',
            ServiceRole='EMR_DefaultRole',
            JobFlowRole='EMR_EC2_DefaultRole',
            VisibleToAllUsers=True,
        )
        cluster_id = response['JobFlowId']
        logger.info(f"EMR cluster '{cluster_name}' created with ID '{cluster_id}'.")
        return cluster_id
    except Exception as e:
        logger.error(f"Error creating EMR cluster '{cluster_name}': {e}")

def terminate_cluster(cluster_id):
    try:
        emr.terminate_job_flows(JobFlowIds=[cluster_id])
        logger.info(f"EMR cluster '{cluster_id}' terminated.")
    except Exception as e:
        logger.error(f"Error terminating EMR cluster '{cluster_id}': {e}")

def list_clusters():
    try:
        response = emr.list_clusters(ClusterStates=['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING'])
        clusters = response.get('Clusters', [])
        if not clusters:
            logger.info("No active EMR clusters found.")
        else:
            logger.info(f"Found {len(clusters)} active EMR clusters:")
            for cluster in clusters:
                logger.info(f" - {cluster['Name']} (ID: {cluster['Id']})")
        return clusters
    except Exception as e:
        logger.error(f"Error listing EMR clusters: {e}")
