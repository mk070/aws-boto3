import boto3
import logging

logger = logging.getLogger(__name__)
cloudwatch = boto3.client('cloudwatch')

def setup_cpu_alarm(instance_id, threshold, sns_topic_arn):
    """
    Sets up a CloudWatch alarm for high CPU utilization, avoiding duplicates.
    """
    try:
        alarm_name = f"HighCPUUtilization-{instance_id}"

        # Check if the alarm already exists
        existing_alarms = cloudwatch.describe_alarms(AlarmNames=[alarm_name])
        if existing_alarms['MetricAlarms']:
            logger.info(f"CPU utilization alarm already exists for instance '{instance_id}'. Updating the threshold.")
            # If the alarm exists, update it with the new threshold
            cloudwatch.put_metric_alarm(
                AlarmName=alarm_name,
                ComparisonOperator='GreaterThanThreshold',
                EvaluationPeriods=2,
                MetricName='CPUUtilization',
                Namespace='AWS/EC2',
                Period=300,  # 5 minutes
                Statistic='Average',
                Threshold=threshold,  # Example: 10 for 10% CPU
                ActionsEnabled=True,
                AlarmActions=[sns_topic_arn],
                Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
                Unit='Percent'
            )
            logger.info(f"CPU utilization alarm updated for instance '{instance_id}' with a new threshold of {threshold}%.")
        else:
            # If the alarm does not exist, create it
            cloudwatch.put_metric_alarm(
                AlarmName=alarm_name,
                ComparisonOperator='GreaterThanThreshold',
                EvaluationPeriods=2,
                MetricName='CPUUtilization',
                Namespace='AWS/EC2',
                Period=300,  # 5 minutes
                Statistic='Average',
                Threshold=threshold,  # Example: 10 for 10% CPU
                ActionsEnabled=True,
                AlarmActions=[sns_topic_arn],
                Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
                Unit='Percent'
            )
            logger.info(f"CPU utilization alarm set for instance '{instance_id}' with a threshold of {threshold}%.")
    except Exception as e:
        logger.error(f"Error setting up CPU utilization alarm for instance '{instance_id}': {e}")

def setup_status_check_alarm(instance_id, sns_topic_arn):
    """
    Sets up a CloudWatch alarm for EC2 instance status checks.
    """
    try:
        alarm_name = f"InstanceStatusCheckFailed-{instance_id}"

        # Check if the alarm already exists
        existing_alarms = cloudwatch.describe_alarms(AlarmNames=[alarm_name])
        if existing_alarms['MetricAlarms']:
            logger.info(f"Instance status check alarm already exists for instance '{instance_id}'.")
        else:
            # If the alarm does not exist, create it
            cloudwatch.put_metric_alarm(
                AlarmName=alarm_name,
                ComparisonOperator='GreaterThanThreshold',
                EvaluationPeriods=1,
                MetricName='StatusCheckFailed_Instance',
                Namespace='AWS/EC2',
                Period=60,  # 1 minute
                Statistic='Average',
                Threshold=1,
                ActionsEnabled=True,
                AlarmActions=[sns_topic_arn],
                Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
                Unit='Count'
            )
            logger.info(f"Instance status check alarm set for instance '{instance_id}'.")
    except Exception as e:
        logger.error(f"Error setting up instance status check alarm for instance '{instance_id}': {e}")
