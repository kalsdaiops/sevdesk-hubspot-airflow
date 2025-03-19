from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import time

# 🔹 Replace with your actual API Key
API_KEY = ""

# 🔹 sevDesk API URLs
BASE_URL = "https://my.sevdesk.de/api/v1"
GET_CONTACTS_URL = f"{BASE_URL}/Contact"
POST_CONTACT_URL = f"{BASE_URL}/Contact"
GET_EMAIL_URL = f"{BASE_URL}/CommunicationWay"
POST_EMAIL_URL = f"{BASE_URL}/CommunicationWay"

# Headers for authentication
headers = {
    "Authorization": API_KEY,
    "Content-Type": "application/json"
}

# Contact details
customer_number = "Customer-1337"
first_name = "John"
last_name = "Snow"
email = "string@gmail.com"
contact_id = None  # To be assigned later

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 18),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "sevdesk_contact_pipeline",  # Updated DAG name
    default_args=default_args,
    schedule_interval=timedelta(hours=1),  # Run every hour
    catchup=False,
)

def get_contacts():
    """Fetch contacts from sevDesk and check if the target contact exists."""
    global contact_id
    response = requests.get(GET_CONTACTS_URL, headers=headers)

    if response.status_code == 200:
        contacts = response.json().get("objects", [])
        for contact in contacts:
            if contact.get("customerNumber") == customer_number:
                contact_id = contact["id"]
                print(f"✅ Contact already exists: {contact_id}")
                return
    else:
        print(f"❌ Failed to fetch contacts: {response.text}")

def create_or_update_contact():
    """Create a new contact if not found, otherwise update existing contact."""
    global contact_id
    contact_payload = {
        "name": f"{first_name} {last_name}",
        "status": 100,
        "customerNumber": customer_number,
        "surename": first_name,
        "familyname": last_name,
        "titel": "Commander",
        "category": {"id": 3, "objectName": "Category"}
    }

    if contact_id:
        # Update existing contact
        update_url = f"{BASE_URL}/Contact/{contact_id}"
        response = requests.put(update_url, json=contact_payload, headers=headers)
        if response.status_code == 200:
            print(f"✅ Contact updated: {contact_id}")
        else:
            print(f"❌ Failed to update contact: {response.text}")
    else:
        # Create new contact
        response = requests.post(POST_CONTACT_URL, json=contact_payload, headers=headers)
        if response.status_code in [200, 201]:
            contact_id = response.json().get("objects", {}).get("id")
            print(f"✅ New contact created: {contact_id}")
        else:
            print(f"❌ Failed to create contact: {response.text}")

def add_or_update_email():
    """Add or update the email for the contact."""
    if not contact_id:
        print("⚠️ Contact ID not found, skipping email update.")
        return

    email_payload = {
        "contact": {"id": contact_id, "objectName": "Contact"},
        "type": "EMAIL",
        "value": email,
        "key": {"id": 1, "objectName": "CommunicationWayKey"},
        "main": 0
    }

    # Check if email already exists
    response = requests.get(f"{GET_EMAIL_URL}?contact.id={contact_id}", headers=headers)

    if response.status_code == 200:
        existing_emails = response.json().get("objects", [])
        if existing_emails:
            email_id = existing_emails[0]["id"]
            update_email_url = f"{POST_EMAIL_URL}/{email_id}"
            response = requests.put(update_email_url, json=email_payload, headers=headers)
            if response.status_code == 200:
                print(f"✅ Email updated for contact {contact_id}")
            else:
                print(f"❌ Failed to update email: {response.text}")
        else:
            # Create new email
            response = requests.post(POST_EMAIL_URL, json=email_payload, headers=headers)
            if response.status_code in [200, 201]:
                print(f"✅ New email added for contact {contact_id}")
            else:
                print(f"❌ Failed to add email: {response.text}")
    else:
        print(f"❌ Error fetching emails: {response.text}")

# Airflow Tasks
t1 = PythonOperator(
    task_id="get_contacts",
    python_callable=get_contacts,
    dag=dag,
)

t2 = PythonOperator(
    task_id="create_or_update_contact",
    python_callable=create_or_update_contact,
    dag=dag,
)

t3 = PythonOperator(
    task_id="add_or_update_email",
    python_callable=add_or_update_email,
    dag=dag,
)

# Task dependencies
t1 >> t2 >> t3
