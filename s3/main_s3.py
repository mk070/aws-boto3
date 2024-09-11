import boto3
import logging
import colorlog
import sys
import os

# Ensure the parent directory is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from s3_operations import *
from alert import *

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

def main_menu():
    while True:
        print("\n----- Menu -----")
        print("1. Create Bucket")
        print("2. List Buckets")
        print("3. List Bucket Objects")
        print("4. Show Usage")
        print("5. Turn ON/OFF Alerts")
        print("6. Exit")

        choice = input("Select an option: ").strip()

        if choice == '1':
            create_bucket()
        elif choice == '2':
            list_buckets()
        elif choice == '3':
            list_bucket_objects()
        elif choice == '4':
            show_usage()
        elif choice == '5':
            manage_alerts()
        elif choice == '6':
            logger.info("Exiting...")
            sys.exit(0)
        else:
            logger.error("Invalid selection. Please choose a valid option.")

if __name__ == "__main__":
    main_menu()
