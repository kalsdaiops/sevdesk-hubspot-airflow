import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

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
HUBSPOT_BASE_URL = "https://api.hubapi.com/crm/v3/objects/contacts"

# Headers
sevdesk_headers = {"Authorization": SEVDESK_API_KEY, "Content-Type": "application/json"}
hubspot_headers = {"Authorization": f"Bearer {HUBSPOT_API_KEY}", "Content-Type": "application/json"}

# Function to fetch the latest contact from SevDesk
def fetch_new_contact_from_sevdesk():
    try:
        response = requests.get(f"{SEVDESK_BASE_URL}/Contact", headers=sevdesk_headers)
        response.raise_for_status()
        data = response.json()

        if "objects" in data and data["objects"]:
            latest_contact = data["objects"][0]  # Assuming latest contact is first in the list
            return latest_contact["id"]
    except Exception as e:
        print(f"❌ Error fetching new contact: {e}")
    return None

# Function to get contact details from SevDesk
def get_sevdesk_contact_details(contact_id):
    try:
        response = requests.get(f"{SEVDESK_BASE_URL}/Contact/{contact_id}", headers=sevdesk_headers)
        response.raise_for_status()
        contact = response.json().get("objects", [{}])[0]
        return {
            "email": contact.get("email", "No Email"),
            "firstname": contact.get("name", "No First Name"),
            "lastname": contact.get("familyname", "No Last Name"),
        }
    except Exception as e:
        print(f"❌ Error fetching contact details: {e}")
    return None

# Function to create contact in HubSpot
def create_contact_in_hubspot():
    contact_id = fetch_new_contact_from_sevdesk()
    if not contact_id:
        print("❌ No new contact found.")
        return

    contact_details = get_sevdesk_contact_details(contact_id)
    if not contact_details:
        print(f"❌ No details found for contact ID {contact_id}.")
        return

    try:
        response = requests.post(
            HUBSPOT_BASE_URL,
            headers=hubspot_headers,
            json={"properties": contact_details},
        )
        if response.status_code == 201:
            print(f"✅ Contact created in HubSpot: {response.json()['id']}")
        else:
            print(f"❌ Failed to create contact. Response: {response.json()}")
    except Exception as e:
        print(f"❌ Error creating contact in HubSpot: {e}")

# Airflow DAG definition
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 17),
    "retries": 1,
}

dag = DAG(
    "sevdesk_to_hubspot_manual",
    default_args=default_args,
    description="Manually sync new contacts from SevDesk to HubSpot",
    schedule_interval=None,  # Manual trigger only
    catchup=False,
)

sync_contact_task = PythonOperator(
    task_id="sync_new_contact",
    python_callable=create_contact_in_hubspot,
    dag=dag,
)

sync_contact_task
