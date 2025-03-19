import requests
import json
from datetime import datetime, timedelta  # Import timedelta here
import os

# SevDesk API Token (Replace with actual API token)
api_token = ""

# SevDesk API base URL
base_url = "https://my.sevdesk.de/"

# Headers for authentication
headers = {
    "Authorization": api_token,
    "Content-Type": "application/json"
}

# File to store the last sensor timestamp
sensor_file = "sensor.txt"

# Function to get the last sensor timestamp from the file
def get_last_sensor():
    if os.path.exists(sensor_file):
        with open(sensor_file, "r") as f:
            return f.read().strip()
    return "2000-01-01T00:00:00+00:00"  # Default to a very old date

# Function to update the sensor file with the latest timestamp
def update_sensor(new_sensor):
    with open(sensor_file, "w") as f:
        f.write(new_sensor)

# Function to fetch emails using the CommunicationWay endpoint for a given contact ID
def get_email(contact_id):
    url = f"{base_url}api/v1/CommunicationWay"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        comm_data = response.json()
        if "objects" in comm_data and isinstance(comm_data["objects"], list):
            for comm in comm_data["objects"]:
                if comm.get("contact", {}).get("id") == contact_id and comm.get("type") == "EMAIL":
                    return comm.get("value", None)  # Assign None if email is missing
    return None  # Assign None if no email is found

# Function to fetch all contacts and determine new/old status
def list_all_contacts():
    # Get the current timestamp for the filter
    create_after = (datetime.now() - timedelta(days=1)).isoformat()  # Example: filter contacts created after the last day
    current_timestamp = datetime.now().isoformat()  # Current timestamp when the request is made

    # Print the filter and current timestamp
    print(f"Using createAfter filter: {create_after}")
    print(f"Current timestamp: {current_timestamp}")

    url = f"{base_url}api/v1/Contact?createAfter={create_after}"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        contacts_data = response.json()
        if "objects" in contacts_data and contacts_data["objects"]:
            contacts = contacts_data["objects"]

            # Print table headers with fixed column widths
            print(f"+------------+-----------------------+------------+------------------------+------------------------+------------------------+------------------------+")
            print(f"| Contact ID | First Name            | Last Name | Email                  | Created at               | Sensor timestamp     | Current Timestamp       | Flag       |")
            print(f"+------------+-----------------------+------------+------------------------+------------------------+------------------------+------------------------+")

            for contact in contacts:
                contact_id = contact.get("id", "N/A")
                full_name = contact.get("name", "N/A")

                # Name Handling (First & Last Name)
                name_parts = full_name.split(" ")
                first_name = name_parts[0] if len(name_parts) > 0 else "N/A"
                last_name = name_parts[1] if len(name_parts) > 1 else "N/A"

                # Fetch email from CommunicationWay endpoint
                email = get_email(contact_id)

                # Creation Date Handling (convert to datetime object for comparison)
                created = contact.get("create", "N/A")
                try:
                    created_dt = datetime.fromisoformat(created).replace(tzinfo=None) if created != "N/A" else datetime.now()
                except ValueError:
                    created_dt = datetime.now()  # Fallback in case of invalid format

                # Convert createAfter to datetime object for comparison
                try:
                    create_after_dt = datetime.fromisoformat(create_after).replace(tzinfo=None)
                except ValueError:
                    create_after_dt = datetime.now()  # Fallback

                # Step 3: Compare creation date with the filter timestamp and set flag
                flag = "New" if created_dt > create_after_dt else "Old"

                # Print the contact details in a table format
                print(f"| {contact_id:<10} | {first_name:<20} | {last_name:<10} | {str(email):<22} | {created:<22} | {create_after:<22} | {current_timestamp:<22} | {flag:<10} |")

            print(f"+------------+-----------------------+------------+------------------------+------------------------+------------------------+------------------------+")

            # Step 4: Update the sensor file with the latest timestamp
            new_sensor = datetime.now().isoformat()  # Update sensor with current timestamp
            update_sensor(new_sensor)

# Run the function to list all contacts
list_all_contacts()