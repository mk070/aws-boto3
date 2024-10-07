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

def get_instance_group_mapping(cluster_id):
    """Fetches instance group IDs and their types (MASTER, CORE, TASK) for the given EMR cluster."""
    group_mapping = {}
    try:
        response = emr.list_instance_groups(ClusterId=cluster_id)
        for group in response['InstanceGroups']:
            group_mapping[group['Id']] = group['InstanceGroupType']
        logger.info(f"Fetched instance group mapping for cluster '{cluster_id}': {group_mapping}")
    except Exception as e:
        logger.error(f"Error fetching instance group mapping for cluster '{cluster_id}': {e}")
    return group_mapping

def get_cluster_instance_ids(cluster_id):
    """Fetches all instance IDs in the EMR cluster with their associated node types."""
    instance_group_mapping = get_instance_group_mapping(cluster_id)
    instances = []

    try:
        response = emr.list_instances(ClusterId=cluster_id)
        for instance in response['Instances']:
            instance_id = instance['Ec2InstanceId']
            instance_group_id = instance['InstanceGroupId']
            node_type = instance_group_mapping.get(instance_group_id, 'UNKNOWN')
            instances.append({'InstanceId': instance_id, 'NodeType': node_type})
        if instances:
            logger.info(f"Fetched instance IDs and node types for cluster '{cluster_id}': {instances}")
        else:
            logger.warning(f"No instance IDs found for cluster '{cluster_id}'. Ensure the cluster is running and has active nodes.")
    except Exception as e:
        logger.error(f"Error fetching instance IDs for cluster '{cluster_id}': {e}")

    return instances

def fetch_ec2_instance_metrics(cluster_id, instance_id, node_type):
    """Fetch metrics for each EC2 instance in the EMR cluster."""
    ec2_metrics = [
        {'MetricName': 'CPUUtilization', 'Description': 'Percentage of CPU used'},
        {'MetricName': 'EBSReadBytes', 'Description': 'Amount of data read from EBS'},
        {'MetricName': 'EBSWriteBytes', 'Description': 'Amount of data written to EBS'},
        {'MetricName': 'NetworkIn', 'Description': 'Data received by the instance'},
        {'MetricName': 'NetworkOut', 'Description': 'Data sent out by the instance'},
    ]
    data_points = []

    for metric in ec2_metrics:
        try:
            response = cloudwatch.get_metric_statistics(
                Namespace='AWS/EC2',
                MetricName=metric['MetricName'],
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
                        'Cluster ID': cluster_id,
                        'Instance ID': instance_id,
                        'Node Type': node_type,
                        'Metric': metric['MetricName'],
                        'Metric Description': metric['Description'],
                        'Start Time': (point['Timestamp'] - datetime.timedelta(minutes=5)).isoformat(),  # Approximate start time
                        'End Time': point['Timestamp'].isoformat(),  # Exact end time when metric was recorded
                        'Average Value': point['Average'],
                        'Data Processed (MB/GB)': convert_bytes_to_readable_format(metric['MetricName'], point['Average']),
                        'Resource Utilization (CPU/Memory)': compute_resource_utilization(metric['MetricName'], point['Average'])
                    })
                logger.info(f"Fetched data points for metric '{metric['MetricName']}' on instance '{instance_id}'.")
            else:
                logger.warning(f"No data points found for metric '{metric['MetricName']}' on instance '{instance_id}'.")

        except Exception as e:
            logger.error(f"Error fetching metric '{metric['MetricName']}' for instance '{instance_id}': {e}")

    return data_points

def convert_bytes_to_readable_format(metric_name, value):
    """Converts byte-based metrics to a more readable format (MB/GB)."""
    if 'Bytes' in metric_name:
        if value >= 1e9:  # If the value is in GB range
            return f"{value / 1e9:.2f} GB"
        elif value >= 1e6:  # If the value is in MB range
            return f"{value / 1e6:.2f} MB"
    return '-'  # Return '-' for non-byte metrics

def compute_resource_utilization(metric_name, value):
    """Formats CPU and Memory metrics for better readability."""
    if metric_name == 'CPUUtilization':
        return f"{value:.2f}%"
    return '-'

def fetch_complete_cluster_metrics(cluster_id):
    """Fetch metrics from both the master node and all worker nodes in the cluster."""
    instances = get_cluster_instance_ids(cluster_id)
    all_node_data = []

    if not instances:
        logger.error(f"No instances found for cluster ID '{cluster_id}'. Metric collection aborted.")
        return all_node_data

    for instance in instances:
        node_data = fetch_ec2_instance_metrics(cluster_id, instance['InstanceId'], instance['NodeType'])
        all_node_data.extend(node_data)

    if not all_node_data:
        logger.error("No metrics data collected for any node in the cluster.")

    return all_node_data

def save_cluster_report_to_csv(cluster_metrics, file_name='Complete_Cluster_Report.csv'):
    """Save the complete cluster metrics report to a CSV file in the desired format."""
    if not cluster_metrics:
        logger.error("No data available to write to the report. The CSV file will be empty.")
        return

    try:
        df = pd.DataFrame(cluster_metrics)
        file_path = os.path.join(os.getcwd(), file_name)
        df.to_csv(file_path, index=False)
        logger.info(f"Cluster metrics report saved at: {file_path}")
    except Exception as e:
        logger.error(f"Failed to save the report to CSV: {e}")


