from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get API keys from environment variables
#SEVDESK_API_KEY = os.getenv("SEVDESK_API_KEY")
SEVDESK_API_KEY = os.getenv("SEVDESK_API_KEY")

# SevDesk API base URL
base_url = "https://my.sevdesk.de/"

# Headers for authentication
headers = {
    "Authorization": SEVDESK_API_KEY,
    "Content-Type": "application/json"
}

# File to store the last sensor timestamp
sensor_file = "sensor.txt"

def get_last_sensor():
    if os.path.exists(sensor_file):
        with open(sensor_file, "r") as f:
            return f.read().strip()
    return "2000-01-01T00:00:00+00:00"  # Default to a very old date

def update_sensor(new_sensor):
    with open(sensor_file, "w") as f:
        f.write(new_sensor)

def get_email(contact_id):
    url = f"{base_url}api/v1/CommunicationWay"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        comm_data = response.json()
        if "objects" in comm_data and isinstance(comm_data["objects"], list):
            for comm in comm_data["objects"]:
                if comm.get("contact", {}).get("id") == contact_id and comm.get("type") == "EMAIL":
                    return comm.get("value", None)
    return None

def fetch_sevdesk_contacts():
    create_after = (datetime.now() - timedelta(days=1)).isoformat()
    current_timestamp = datetime.now().isoformat()

    print(f"Using createAfter filter: {create_after}")
    print(f"Current timestamp: {current_timestamp}")

    url = f"{base_url}api/v1/Contact?createAfter={create_after}"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        contacts_data = response.json()
        if "objects" in contacts_data and contacts_data["objects"]:
            contacts = contacts_data["objects"]

            print(f"+------------+-----------------------+------------+------------------------+------------------------+------------------------+------------------------+")
            print(f"| Contact ID | First Name            | Last Name | Email                  | Created at               | Sensor timestamp     | Current Timestamp       | Flag       |")
            print(f"+------------+-----------------------+------------+------------------------+------------------------+------------------------+------------------------+")

            for contact in contacts:
                contact_id = contact.get("id", "N/A")
                full_name = contact.get("name", "N/A")

                name_parts = full_name.split(" ")
                first_name = name_parts[0] if len(name_parts) > 0 else "N/A"
                last_name = name_parts[1] if len(name_parts) > 1 else "N/A"

                email = get_email(contact_id)
                created = contact.get("create", "N/A")
                
                try:
                    created_dt = datetime.fromisoformat(created).replace(tzinfo=None) if created != "N/A" else datetime.now()
                except ValueError:
                    created_dt = datetime.now()

                try:
                    create_after_dt = datetime.fromisoformat(create_after).replace(tzinfo=None)
                except ValueError:
                    create_after_dt = datetime.now()

                flag = "New" if created_dt > create_after_dt else "Old"

                print(f"| {contact_id:<10} | {first_name:<20} | {last_name:<10} | {str(email):<22} | {created:<22} | {create_after:<22} | {current_timestamp:<22} | {flag:<10} |")

            print(f"+------------+-----------------------+------------+------------------------+------------------------+------------------------+------------------------+")

            new_sensor = datetime.now().isoformat()
            update_sensor(new_sensor)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 19),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "fetch_sevdesk_contacts",
    default_args=default_args,
    description="Fetch contacts from sevDesk and log details",
    schedule_interval=timedelta(hours=1),
)

fetch_contacts_task = PythonOperator(
    task_id="fetch_sevdesk_contacts_task",
    python_callable=fetch_sevdesk_contacts,
    dag=dag,
)

fetch_contacts_task
