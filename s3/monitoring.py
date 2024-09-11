import boto3
import logging
import colorlog
import sys
import os

# Ensure the parent directory is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Set up color logging
handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(
    "%(log_color)s%(levelname)s:%(message)s",
    log_colors={
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'bold_red',
    }
))

logger = colorlog.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.INFO)

cloudwatch = boto3.client('cloudwatch')


def create_or_update_cloudwatch_alarm(bucket_name, size_threshold, object_threshold, topic_arn):
    try:
        # Alarm for BucketSizeBytes
        size_alarm_name = f"S3BucketSizeAlarm-{bucket_name}"
        # Check if the size alarm already exists
        size_alarm_response = cloudwatch.describe_alarms(AlarmNames=[size_alarm_name])

        if size_alarm_response['MetricAlarms']:
            logger.info(f"Size alarm '{size_alarm_name}' already exists, updating if necessary.")
            existing_size_alarm = size_alarm_response['MetricAlarms'][0]
            if existing_size_alarm['Threshold'] != size_threshold * 1024 * 1024:
                cloudwatch.put_metric_alarm(
                    AlarmName=size_alarm_name,
                    ComparisonOperator=existing_size_alarm['ComparisonOperator'],
                    EvaluationPeriods=existing_size_alarm['EvaluationPeriods'],
                    MetricName=existing_size_alarm['MetricName'],
                    Namespace=existing_size_alarm['Namespace'],
                    Period=86400,  # 1 day period to match the reporting frequency of BucketSizeBytes
                    Statistic=existing_size_alarm['Statistic'],
                    Threshold=size_threshold * 1024 * 1024,  # Update the threshold
                    ActionsEnabled=True,
                    AlarmActions=existing_size_alarm['AlarmActions'],
                    Dimensions=existing_size_alarm['Dimensions'],
                    Unit=existing_size_alarm['Unit']
                )
                logger.info(f"Size alarm '{size_alarm_name}' threshold updated to {size_threshold} MB.")
        else:
            # Create a new size alarm if it doesn't exist
            cloudwatch.put_metric_alarm(
                AlarmName=size_alarm_name,
                ComparisonOperator='GreaterThanThreshold',
                EvaluationPeriods=1,
                MetricName='BucketSizeBytes',
                Namespace='AWS/S3',
                Period=86400,  # 1 day period for BucketSizeBytes metric
                Statistic='Average',
                Threshold=size_threshold * 1024 * 1024,  # Convert MB to Bytes
                ActionsEnabled=True,
                AlarmActions=[topic_arn],
                Dimensions=[
                    {'Name': 'BucketName', 'Value': bucket_name},
                    {'Name': 'StorageType', 'Value': 'StandardStorage'}
                ],
                Unit='Bytes'
            )
            logger.info(f"CloudWatch size alarm created for bucket '{bucket_name}' with threshold {size_threshold} MB.")

        # Alarm for NumberOfObjects
        object_alarm_name = f"S3NumberOfObjectsAlarm-{bucket_name}"
        # Check if the object alarm already exists
        object_alarm_response = cloudwatch.describe_alarms(AlarmNames=[object_alarm_name])

        if object_alarm_response['MetricAlarms']:
            logger.info(f"Object count alarm '{object_alarm_name}' already exists, updating if necessary.")
            existing_object_alarm = object_alarm_response['MetricAlarms'][0]
            if existing_object_alarm['Threshold'] != object_threshold:
                cloudwatch.put_metric_alarm(
                    AlarmName=object_alarm_name,
                    ComparisonOperator=existing_object_alarm['ComparisonOperator'],
                    EvaluationPeriods=existing_object_alarm['EvaluationPeriods'],
                    MetricName=existing_object_alarm['MetricName'],
                    Namespace=existing_object_alarm['Namespace'],
                    Period=60,  # 1 minute period for quicker testing with NumberOfObjects metric
                    Statistic=existing_object_alarm['Statistic'],
                    Threshold=object_threshold,  # Update the threshold
                    ActionsEnabled=True,
                    AlarmActions=existing_object_alarm['AlarmActions'],
                    Dimensions=existing_object_alarm['Dimensions'],
                    Unit=existing_object_alarm['Unit']
                )
                logger.info(f"Object count alarm '{object_alarm_name}' threshold updated to {object_threshold} objects.")
        else:
            # Create a new object count alarm if it doesn't exist
            cloudwatch.put_metric_alarm(
                AlarmName=object_alarm_name,
                ComparisonOperator='GreaterThanThreshold',
                EvaluationPeriods=1,
                MetricName='NumberOfObjects',
                Namespace='AWS/S3',
                Period=60,  # 1 minute period for quicker testing
                Statistic='Average',
                Threshold=object_threshold,  # Threshold in number of objects
                ActionsEnabled=True,
                AlarmActions=[topic_arn],
                Dimensions=[
                    {'Name': 'BucketName', 'Value': bucket_name},
                    {'Name': 'StorageType', 'Value': 'AllStorageTypes'}
                ],
                Unit='Count'
            )
            logger.info(f"CloudWatch object count alarm created for bucket '{bucket_name}' with threshold {object_threshold} objects.")

    except Exception as e:
        logger.error(f"Error creating or updating CloudWatch alarm: {e}")

