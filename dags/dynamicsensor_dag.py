import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator
from datetime import datetime, timedelta

# API Keys
<<<<<<< HEAD
<<<<<<< HEAD
SEVDESK_API_KEY = ""
HUBSPOT_API_KEY = ""  # Replace with actual HubSpot token
=======
SEVDESK_API_KEY = "62837f0452b821e78683b111ad4d6727"
HUBSPOT_API_KEY = "pat-eu1-9b932abf-c64a-49d7-b0d5-4a4a41a73f07"  # Replace with actual HubSpot token
>>>>>>> 7a39430 (Initial commit - Added sevDesk to HubSpot Airflow DAG)
=======
SEVDESK_API_KEY = ""
HUBSPOT_API_KEY = ""  # Replace with actual HubSpot token
>>>>>>> c7f078f (Initial commit - Added sevDesk to HubSpot Airflow DAG)

# Base URLs
SEVDESK_BASE_URL = "https://my.sevdesk.de/api/v1"
HUBSPOT_BASE_URL = "https://api.hubapi.com/crm/v3/objects/contacts/search"

# Headers
sevdesk_headers = {"Authorization": SEVDESK_API_KEY, "Content-Type": "application/json"}
hubspot_headers = {"Authorization": f"Bearer {HUBSPOT_API_KEY}", "Content-Type": "application/json"}

# Function to fetch the latest contact (newly created) from SevDesk
def fetch_new_contact_from_sevdesk():
    sevdesk_response = requests.get(f"{SEVDESK_BASE_URL}/Contact", headers=sevdesk_headers)
    sevdesk_data = sevdesk_response.json()

    # Check if new contacts are available and return the first one (or adapt as needed)
    if "objects" in sevdesk_data and len(sevdesk_data["objects"]) > 0:
        new_contact = sevdesk_data["objects"][0]  # Assuming the newest contact is the first in the list
        return new_contact["id"]
    return None  # No new contact found

# Function to create a contact in HubSpot using the new CONTACT_ID
def create_contact_in_hubspot(contact_id):
    sevdesk_contact_response = requests.get(f"{SEVDESK_BASE_URL}/Contact/{contact_id}", headers=sevdesk_headers)
    sevdesk_contact_data = sevdesk_contact_response.json()

    if "objects" in sevdesk_contact_data and len(sevdesk_contact_data["objects"]) > 0:
        contact = sevdesk_contact_data["objects"][0]
        email = contact.get("email", "No Email")
        first_name = contact.get("first_name", "No First Name")
        last_name = contact.get("last_name", "No Last Name")

        create_payload = {
            "properties": {
                "email": email,
                "firstname": first_name,
                "lastname": last_name
            }
        }

        create_response = requests.post(
            "https://api.hubapi.com/crm/v3/objects/contacts",
            headers=hubspot_headers,
            json=create_payload
        )

        if create_response.status_code == 201:
            print(f"New contact created in HubSpot: {create_response.json()['id']}")
        else:
            print(f"❌ Failed to create contact in HubSpot. Status Code: {create_response.status_code}")
    else:
        print("❌ No contact data found for the given ID.")

# Define a custom sensor that detects new contacts from SevDesk
class SevDeskNewContactSensor(BaseSensorOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def poke(self, context):
        new_contact_id = fetch_new_contact_from_sevdesk()

        if new_contact_id:
            print(f"New contact found with ID: {new_contact_id}")
            return new_contact_id  # Return the new CONTACT_ID to trigger the next task
        else:
            print("No new contact found yet.")
            return False  # Keep waiting if no new contact is found

# Airflow DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 17),
    'retries': 1,
}

dag = DAG(
    'sevdesk_to_hubspot_sync_dynamic',
    default_args=default_args,
    description='Sync new contacts from SevDesk to HubSpot',
    schedule_interval=timedelta(minutes=5),  # Check for new contacts every 5 minutes
    catchup=False,
)

# Define the task to process and create the contact in HubSpot using the dynamically fetched CONTACT_ID
def process_and_create_contact(**context):
    # Get the new CONTACT_ID from the XCom value returned by the sensor
    contact_id = context['ti'].xcom_pull(task_ids='detect_new_contact_in_sevdesk')

    if contact_id:
        create_contact_in_hubspot(contact_id)
    else:
        print("❌ No new contact found to create in HubSpot.")

# Define tasks
detect_new_contact_in_sevdesk = SevDeskNewContactSensor(
    task_id='detect_new_contact_in_sevdesk',
    poke_interval=60,  # Check every 60 seconds
    timeout=600,  # Timeout after 10 minutes
    mode="poke",  # Using poke mode to periodically check for the new contact
    dag=dag,
)

process_new_contact_in_hubspot = PythonOperator(
    task_id='process_and_create_contact',
    python_callable=process_and_create_contact,
    provide_context=True,  # Needed to pass the XCom data to the task
    dag=dag,
)

# Set task dependencies
detect_new_contact_in_sevdesk >> process_new_contact_in_hubspot
