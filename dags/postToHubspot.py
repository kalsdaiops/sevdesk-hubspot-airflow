import requests
import json
from datetime import datetime
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
import re

# HubSpot API Token (Replace with actual HubSpot API token)
HUBSPOT_API_TOKEN = ""

# HubSpot API Headers
HUBSPOT_HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_API_TOKEN}",
    "Content-Type": "application/json"
}

# SevDesk API Token
SEVDESK_API_TOKEN = ""

# SevDesk API base URL
SEVDESK_BASE_URL = "https://my.sevdesk.de/"

# Headers for SevDesk API authentication
SEVDESK_HEADERS = {
    "Authorization": SEVDESK_API_TOKEN,
    "Content-Type": "application/json"
}

# File to store the last sensor timestamp
SENSOR_FILE = "sensor.txt"

# Placeholder Email
PLACEHOLDER_EMAIL = "no-email@example.com"

# Function to get the last sensor timestamp
def get_last_sensor():
    if os.path.exists(SENSOR_FILE):
        with open(SENSOR_FILE, "r") as f:
            return f.read().strip()
    return "2000-01-01T00:00:00+00:00"

# Function to update the sensor file
def update_sensor(new_sensor):
    with open(SENSOR_FILE, "w") as f:
        f.write(new_sensor)

# Function to validate email format
def is_valid_email(email):
    regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(regex, email) is not None

# Function to fetch contacts from SevDesk and create them in HubSpot
def list_and_create_contacts():
    url = f"{SEVDESK_BASE_URL}api/v1/Contact"
    response = requests.get(url, headers=SEVDESK_HEADERS)

    if response.status_code == 200:
        contacts_data = response.json()
        if "objects" in contacts_data and contacts_data["objects"]:
            contacts = contacts_data["objects"]

            # Get the last known sensor timestamp
            sensor = get_last_sensor()
            sensor_dt = datetime.fromisoformat(sensor).replace(tzinfo=None)

            # Get the latest creation timestamp
            sorted_contacts = sorted(contacts, key=lambda x: x.get("create", "2000-01-01T00:00:00+00:00"), reverse=True)
            new_sensor = sorted_contacts[0].get("create", sensor)

            print(f"+------------+------------------------+-----------+--------------------------+---------------------------+---------------------------+------------+")
            print(f"| Contact ID |   First Name           | Last Name |          Email           |          Created          | Sensor Timestamp          | Flag       |")
            print(f"+------------+------------------------+-----------+--------------------------+---------------------------+---------------------------+------------+")

            for contact in contacts:
                # Use sevClient ID as Contact ID
                contact_id = contact.get("sevClient", {}).get("id", "N/A")
                
                full_name = contact.get("name", "N/A")
                name_parts = full_name.split(" ")
                first_name = name_parts[0] if len(name_parts) > 0 else "N/A"
                last_name = name_parts[1] if len(name_parts) > 1 else "N/A"

                # Extract email
                email = ""
                if "communicationWays" in contact and isinstance(contact["communicationWays"], list):
                    for comm in contact["communicationWays"]:
                        if comm.get("type") == "EMAIL":
                            email = comm.get("value", "")
                            break

                # Use a placeholder if email is missing or invalid
                if not email or not is_valid_email(email):
                    print(f"⚠️ Invalid email for Contact ID: {contact_id}. Using placeholder.")
                    email = PLACEHOLDER_EMAIL  # Use a formatted placeholder email

                created = contact.get("create", "N/A")
                created_dt = datetime.fromisoformat(created).replace(tzinfo=None) if created != "N/A" else sensor_dt
                flag = "New" if created_dt > sensor_dt else "Old"

                print(f"| {contact_id:<10} | {first_name:<22} | {last_name:<9} | {email:<24} | {created:<25} | {sensor:<25} | {flag:<10} |")

                if flag == "New":
                    print(f"Creating contact in HubSpot: {contact_id}, {first_name}, {last_name}, {email}")
                    create_contact_in_hubspot(contact_id, first_name, last_name, email)

            print(f"+------------+------------------------+-----------+--------------------------+---------------------------+---------------------------+------------+")

            update_sensor(new_sensor)

# Function to create a contact in HubSpot
def create_contact_in_hubspot(contact_id, first_name, last_name, email):
    hubspot_url = "https://api.hubapi.com/crm/v3/objects/contacts"

    create_payload = {
        "properties": {
            "firstname": first_name,
            "lastname": last_name
        }
    }

    # Only include email if it is valid
    if email and email != PLACEHOLDER_EMAIL:
        create_payload["properties"]["email"] = email

    create_response = requests.post(hubspot_url, headers=HUBSPOT_HEADERS, json=create_payload)

    if create_response.status_code == 201:
        print(f"✅ Contact created in HubSpot: {create_response.json().get('id')}")
    else:
        print(f"❌ Failed to create contact in HubSpot. Status Code: {create_response.status_code}")
        print(f"Error Response: {create_response.json()}")

# Define the Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 17),
    'retries': 1,
}

dag = DAG(
    'sevdesk_to_hubspot_sync',
    default_args=default_args,
    description='Sync new contacts from SevDesk to HubSpot',
    schedule_interval=None,
    catchup=False,
)

list_and_create_task = PythonOperator(
    task_id='list_and_create_contacts',
    python_callable=list_and_create_contacts,
    dag=dag,
)

list_and_create_task
