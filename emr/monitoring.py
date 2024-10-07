# boto3/emr/monitoring.py
import boto3
import pandas as pd
import datetime
import os
import logging

logger = logging.getLogger(__name__)
cloudwatch = boto3.client('cloudwatch')
emr = boto3.client('emr')


def setup_emr_alarm(cluster_id, metric_name, threshold, sns_topic_arn):
    try:
        alarm_name = f"EMR-{metric_name}-{cluster_id}"
        cloudwatch.put_metric_alarm(
            AlarmName=alarm_name,
            ComparisonOperator='GreaterThanThreshold',
            EvaluationPeriods=2,
            MetricName=metric_name,
            Namespace='AWS/ElasticMapReduce',
            Period=300,  # 5 minutes
            Statistic='Average',
            Threshold=threshold,
            ActionsEnabled=True,
            AlarmActions=[sns_topic_arn],
            Dimensions=[{'Name': 'JobFlowId', 'Value': cluster_id}],
            Unit='Percent'  # or appropriate unit for the metric
        )
        logger.info(f"{metric_name} alarm set for EMR cluster '{cluster_id}' with a threshold of {threshold}.")
    except Exception as e:
        logger.error(f"Error setting up {metric_name} alarm for EMR cluster '{cluster_id}': {e}")

def get_cluster_instance_ids(cluster_id):
    """Fetches all instance IDs (both master and worker nodes) in the EMR cluster."""
    try:
        response = emr.list_instances(
            ClusterId=cluster_id,
            InstanceGroupTypes=['MASTER', 'CORE', 'TASK']  # Includes both master and worker nodes
        )
        instance_ids = [instance['Ec2InstanceId'] for instance in response['Instances']]
        if instance_ids:
            logger.info(f"Fetched instance IDs for cluster '{cluster_id}': {instance_ids}")
        else:
            logger.warning(f"No instance IDs found for cluster '{cluster_id}'. Ensure the cluster is running and has active nodes.")
        return instance_ids
    except Exception as e:
        logger.error(f"Error fetching instance IDs for cluster '{cluster_id}': {e}")
        return []

def fetch_ec2_instance_metrics(instance_id):
    """Fetch metrics for each EC2 instance in the EMR cluster."""
    ec2_metrics = [
        'CPUUtilization', 'EBSByteBalance%', 'EBSIOBalance%', 'EBSReadBytes',
        'EBSReadOps', 'EBSWriteBytes', 'EBSWriteOps', 'NetworkIn', 'NetworkOut',
        'NetworkPacketsIn', 'NetworkPacketsOut', 'StatusCheckFailed',
        'StatusCheckFailed_Instance', 'StatusCheckFailed_System'
    ]
    data_points = []

    for metric in ec2_metrics:
        try:
            response = cloudwatch.get_metric_statistics(
                Namespace='AWS/EC2',
                MetricName=metric,
                Dimensions=[
                    {'Name': 'InstanceId', 'Value': instance_id}
                ],
                StartTime=datetime.datetime.utcnow() - datetime.timedelta(hours=24),  # Last 24 hours
                EndTime=datetime.datetime.utcnow(),
                Period=300,  # 5-minute intervals
                Statistics=['Average']
            )

            if response.get('Datapoints'):
                for point in response['Datapoints']:
                    data_points.append({
                        'Instance ID': instance_id,
                        'Metric': metric,
                        'Timestamp': point['Timestamp'],
                        'Average Value': point['Average']
                    })
                logger.info(f"Fetched data points for metric '{metric}' on instance '{instance_id}'.")
            else:
                logger.warning(f"No data points found for metric '{metric}' on instance '{instance_id}'.")

        except Exception as e:
            logger.error(f"Error fetching metric '{metric}' for instance '{instance_id}': {e}")

    return data_points

def fetch_complete_cluster_metrics(cluster_id):
    """Fetch metrics from both the master node and all worker nodes in the cluster."""
    instance_ids = get_cluster_instance_ids(cluster_id)
    all_node_data = []

    if not instance_ids:
        logger.error(f"No instances found for cluster ID '{cluster_id}'. Metric collection aborted.")
        return all_node_data

    for instance_id in instance_ids:
        node_data = fetch_ec2_instance_metrics(instance_id)
        all_node_data.extend(node_data)

    if not all_node_data:
        logger.error("No metrics data collected for any node in the cluster.")

    return all_node_data

def save_cluster_report_to_csv(cluster_metrics, file_name='Complete_EC2_Instance_Report.csv'):
    """Save the complete EC2 instance metrics report to a CSV file."""
    if not cluster_metrics:
        logger.error("No data available to write to the report. The CSV file will be empty.")
        return

    try:
        df = pd.DataFrame(cluster_metrics)
        file_path = os.path.join(os.getcwd(), file_name)
        df.to_csv(file_path, index=False)
        logger.info(f"EC2 instance metrics report saved at: {file_path}")
    except Exception as e:
        logger.error(f"Failed to save the report to CSV: {e}")
