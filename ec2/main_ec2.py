import sys
import os
import logging

# Ensure the parent directory is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ec2.instance_operations import start_instance, stop_instance, reboot_instance, list_all_instances
from ec2.status_check import check_instance_status
from ec2.threshold_alarms import setup_cpu_alarm, setup_status_check_alarm
from ec2.alert import create_or_get_sns_topic, subscribe_to_sns


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main_menu():
    while True:
        print("\n----- EC2 Management Menu -----")
        print("1. Start EC2 Instance")
        print("2. Stop EC2 Instance")
        print("3. Reboot EC2 Instance")
        print("4. Check EC2 Instance Status")
        print("5. Setup CPU Utilization Alarm")
        print("6. Setup Status Check Alarm")
        print("7. List All EC2 Instances")
        print("8. Exit")

        choice = input("Select an option: ").strip()

        if choice == '1':
            instance_id = input("Enter the Instance ID to start: ")
            start_instance(instance_id)
        elif choice == '2':
            instance_id = input("Enter the Instance ID to stop: ")
            stop_instance(instance_id)
        elif choice == '3':
            instance_id = input("Enter the Instance ID to reboot: ")
            reboot_instance(instance_id)
        elif choice == '4':
            instance_id = input("Enter the Instance ID to check status: ")
            check_instance_status(instance_id)
        elif choice == '5':
            instance_id = input("Enter the Instance ID: ")
            threshold = float(input("Enter the CPU utilization threshold (%): "))
            topic_name = "HighCPUUtilizationAlerts"
            sns_topic_arn = create_or_get_sns_topic(topic_name)
            email = input("Enter the email address to receive alerts: ")
            subscribe_to_sns(sns_topic_arn, email)
            setup_cpu_alarm(instance_id, threshold, sns_topic_arn)
        elif choice == '6':
            instance_id = input("Enter the Instance ID: ")
            topic_name = "InstanceStatusCheckAlerts"
            sns_topic_arn = create_or_get_sns_topic(topic_name)
            email = input("Enter the email address to receive alerts: ")
            subscribe_to_sns(sns_topic_arn, email)
            setup_status_check_alarm(instance_id, sns_topic_arn)
        elif choice == '7':
            list_all_instances()
        elif choice == '8':
            logger.info("Exiting...")
            sys.exit(0)
        else:
            logger.error("Invalid selection. Please choose a valid option.")

if __name__ == "__main__":
    main_menu()
