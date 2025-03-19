import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# SevDesk API Configuration
<<<<<<< HEAD
<<<<<<< HEAD
API_KEY = ""
=======
API_KEY = "62837f0452b821e78683b111ad4d6727"
>>>>>>> 7a39430 (Initial commit - Added sevDesk to HubSpot Airflow DAG)
=======
API_KEY = ""
>>>>>>> c7f078f (Initial commit - Added sevDesk to HubSpot Airflow DAG)
BASE_URL = "https://my.sevdesk.de/api/v1"
CONTACT_ID = "98976708"
headers = {"Authorization": API_KEY, "Content-Type": "application/json"}

# Function to fetch contact details and email
def fetch_contact_details():
    # Fetch contact details
    contact_response = requests.get(f"{BASE_URL}/Contact/{CONTACT_ID}", headers=headers)
    contact_data = contact_response.json()

    # Extract first and last name
    first_name = contact_data.get("objects", [{}])[0].get("name", "N/A")
    last_name = contact_data.get("objects", [{}])[0].get("familyname", "N/A")

    # Fetch email
    email_response = requests.get(f"{BASE_URL}/CommunicationWay", headers=headers)
    email_data = email_response.json()

    # Extract email linked to the contact
    email = "No Email Found"
    if "objects" in email_data:
        for item in email_data["objects"]:
            if item.get("type") == "EMAIL" and item.get("contact", {}).get("id") == CONTACT_ID:
                email = item.get("value")
                break

    print(f"First Name: {first_name}")
    print(f"Last Name: {last_name}")
    print(f"Email: {email}")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 17),  # Adjust start date as needed
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'fetch_sevdesk_contact',  # Unique identifier for the DAG
    default_args=default_args,
    description='Fetch SevDesk Contact details and email',
    schedule_interval=None,  # Set to None for manual execution only
    catchup=False,
)

# Task to fetch contact details and email
fetch_contact_task = PythonOperator(
    task_id='fetch_contact_details',
    python_callable=fetch_contact_details,
    dag=dag,
)

fetch_contact_task  # Task execution

