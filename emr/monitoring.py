import boto3
import logging

logger = logging.getLogger(__name__)
cloudwatch = boto3.client('cloudwatch')

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
