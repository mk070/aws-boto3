import boto3
import logging
import colorlog
import sys
import os

# Ensure the parent directory is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from s3_operations import *
from monitoring import *
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

# Initialize AWS services
sns = boto3.client('sns')


def create_or_get_sns_topic():
    topic_name = "S3Alert"
    try:
        # Check if the topic already exists
        response = sns.list_topics()
        for topic in response['Topics']:
            if topic_name in topic['TopicArn']:
                logger.info(f"SNS Topic '{topic_name}' already exists with ARN: {topic['TopicArn']}")
                return topic['TopicArn']

        # If not, create a new one
        response = sns.create_topic(Name=topic_name)
        topic_arn = response['TopicArn']
        logger.info(f"SNS Topic '{topic_name}' created with ARN: {topic_arn}")
        return topic_arn
    except Exception as e:
        logger.error(f"Error creating or getting SNS topic: {e}")
        return None

def subscribe_to_sns(topic_arn):
    email = input("Enter the email address to receive alerts: ")
    try:
        sns.subscribe(
            TopicArn=topic_arn,
            Protocol='email',
            Endpoint=email
        )
        logger.info(f"Subscription request sent to '{email}'. Confirm the subscription in your email.")
    except Exception as e:
        logger.error(f"Error subscribing to SNS topic: {e}")

def manage_alerts():
    alert_status = input("Would you like to turn ON or OFF the alerts? (on/off): ").strip().lower()
    if alert_status not in ['on', 'off']:
        logger.error("Invalid input. Please enter 'on' or 'off'.")
        return

    if alert_status == 'on':
        size_threshold = float(input("Enter the storage size threshold in MB for alerts: "))
        object_threshold = int(input("Enter the object count threshold for alerts: "))
        topic_arn = create_or_get_sns_topic()  # Check or create SNS topic
        if topic_arn:
            subscribe_to_sns(topic_arn)
            buckets = list_buckets()
            for bucket in buckets:
                create_or_update_cloudwatch_alarm(bucket, size_threshold, object_threshold, topic_arn)  # Check or create CloudWatch alarm
    else:
        logger.info("Alerts turned OFF. To disable alarms, manual action is required in AWS Console.")
