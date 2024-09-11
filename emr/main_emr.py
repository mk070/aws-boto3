import sys
import os
import logging

# Ensure the parent directory is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from emr.cluster_operations import create_cluster, terminate_cluster, list_clusters
from emr.scaling import add_instance_group, modify_instance_group
from emr.monitoring import setup_emr_alarm
from emr.alert import create_or_get_sns_topic, subscribe_to_sns

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main_menu():
    while True:
        print("\n----- EMR Management Menu -----")
        print("1. Create EMR Cluster")
        print("2. Terminate EMR Cluster")
        print("3. List EMR Clusters")
        print("4. Add Instance Group")
        print("5. Modify Instance Group")
        print("6. Setup EMR Alarm")
        print("7. Exit")

        choice = input("Select an option: ").strip()

        if choice == '1':
            cluster_name = input("Enter the Cluster Name: ")
            instance_type = input("Enter the Instance Type (e.g., m5.xlarge): ")
            instance_count = int(input("Enter the Number of Instances: "))
            create_cluster(cluster_name, instance_type, instance_count)
        elif choice == '2':
            cluster_id = input("Enter the Cluster ID to terminate: ")
            terminate_cluster(cluster_id)
        elif choice == '3':
            list_clusters()
        elif choice == '4':
            cluster_id = input("Enter the Cluster ID: ")
            instance_type = input("Enter the Instance Type: ")
            instance_count = int(input("Enter the Number of Instances to Add: "))
            add_instance_group(cluster_id, instance_type, instance_count)
        elif choice == '5':
            instance_group_id = input("Enter the Instance Group ID: ")
            instance_count = int(input("Enter the New Number of Instances: "))
            modify_instance_group(instance_group_id, instance_count)
        elif choice == '6':
            cluster_id = input("Enter the Cluster ID: ")
            metric_name = input("Enter the Metric Name (e.g., CPUUtilization): ")
            threshold = float(input("Enter the Threshold for the Alarm: "))
            topic_name = f"EMR-{metric_name}-Alerts"
            sns_topic_arn = create_or_get_sns_topic(topic_name)
            email = input("Enter the Email to Receive Alerts: ")
            subscribe_to_sns(sns_topic_arn, email)
            setup_emr_alarm(cluster_id, metric_name, threshold, sns_topic_arn)
        elif choice == '7':
            logger.info("Exiting...")
            sys.exit(0)
        else:
            logger.error("Invalid selection. Please choose a valid option.")

if __name__ == "__main__":
    main_menu()
