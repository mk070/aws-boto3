import boto3
import logging
import colorlog
import sys

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
s3 = boto3.client('s3')



def list_buckets():
    try:
        response = s3.list_buckets()
        buckets = [bucket['Name'] for bucket in response['Buckets']]
        print("\n--- Bucket List ---")
        for i, bucket in enumerate(buckets, 1):
            print(f"{i}. {bucket}")
        print(f"Total Buckets: {len(buckets)}\n")
        return buckets  # Return the list of bucket names
    except Exception as e:
        logger.error(f"Error listing buckets: {e}")
        return []

def create_bucket():
    bucket_name = input("Enter the bucket name: ")
    try:
        response = s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': 'ap-south-1'
            }
        )
        logger.info(f"Bucket '{bucket_name}' created successfully.")
    except Exception as e:
        logger.error(f"Error creating bucket '{bucket_name}': {e}")

def list_bucket_objects():
    bucket_name = input("Enter the bucket name to list objects: ")
    try:
        response = s3.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            print(f"\n--- Objects in Bucket '{bucket_name}' ---")
            for obj in response['Contents']:
                print(f"Object: {obj['Key']} (Size: {obj['Size']} bytes)")
            print()
        else:
            print(f"No objects found in bucket '{bucket_name}'.\n")
    except Exception as e:
        logger.error(f"Error listing objects in bucket '{bucket_name}': {e}")

def show_usage():
    buckets = list_buckets()
    for bucket in buckets:
        try:
            response = s3.list_objects_v2(Bucket=bucket)
            total_size = sum(obj['Size'] for obj in response.get('Contents', []))
            print(f"Bucket '{bucket}' total usage: {total_size / 1024 / 1024:.2f} MB\n")
        except Exception as e:
            logger.error(f"Error calculating usage for bucket '{bucket}': {e}")
