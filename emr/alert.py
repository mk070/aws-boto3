import boto3
import logging

logger = logging.getLogger(__name__)
sns = boto3.client('sns')

def create_or_get_sns_topic(topic_name):
    try:
        response = sns.list_topics()
        for topic in response['Topics']:
            if topic_name in topic['TopicArn']:
                logger.info(f"SNS Topic '{topic_name}' already exists with ARN: {topic['TopicArn']}")
                return topic['TopicArn']

        response = sns.create_topic(Name=topic_name)
        topic_arn = response['TopicArn']
        logger.info(f"SNS Topic '{topic_name}' created with ARN: {topic_arn}")
        return topic_arn
    except Exception as e:
        logger.error(f"Error creating or retrieving SNS topic '{topic_name}': {e}")
        return None

def subscribe_to_sns(topic_arn, email):
    try:
        sns.subscribe(
            TopicArn=topic_arn,
            Protocol='email',
            Endpoint=email
        )
        logger.info(f"Subscription request sent to '{email}'. Confirm the subscription in your email.")
    except Exception as e:
        logger.error(f"Error subscribing to SNS topic: {e}")
