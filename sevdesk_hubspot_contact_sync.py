import requests
import json
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# 🔹 Retrieve API keys from environment variables
HUBSPOT_API_KEY = os.getenv("HUBSPOT_API_KEY")
SEVDESK_API_TOKEN = os.getenv("SEVDESK_API_TOKEN")

# 🔹 HubSpot API URLs
BASE_URL = "https://api.hubapi.com"
GET_CONTACTS_URL = f"{BASE_URL}/crm/v3/objects/contacts/search"
POST_CONTACT_URL = f"{BASE_URL}/crm/v3/objects/contacts"
UPDATE_CONTACT_URL = f"{BASE_URL}/crm/v3/objects/contacts/{{contact_id}}"

SEVDESK_BASE_URL = "https://my.sevdesk.de/"

# Headers for authentication
hubspot_headers = {
    "Authorization": f"Bearer {HUBSPOT_API_KEY}",
    "Content-Type": "application/json"
}

sevdesk_headers = {
    "Authorization": SEVDESK_API_TOKEN,
    "Content-Type": "application/json"
}

# Function to fetch emails using the CommunicationWay endpoint for a given contact ID
def get_email(contact_id):
    url = f"{SEVDESK_BASE_URL}api/v1/CommunicationWay"
    response = requests.get(url, headers=sevdesk_headers)
    if response.status_code == 200:
        comm_data = response.json()
        if "objects" in comm_data and isinstance(comm_data["objects"], list):
            for comm in comm_data["objects"]:
                if comm.get("contact", {}).get("id") == contact_id and comm.get("type") == "EMAIL":
                    return comm.get("value", None)
    return None

# Function to fetch new contacts from SevDesk
def get_new_contacts():
    fetch_timestamp = datetime.now().isoformat()  # Timestamp for when contacts are fetched
    create_after = (datetime.now() - timedelta(days=1)).isoformat()
    url = f"{SEVDESK_BASE_URL}api/v1/Contact?createAfter={create_after}"
    response = requests.get(url, headers=sevdesk_headers)
    new_contacts = []
    if response.status_code == 200:
        contacts_data = response.json()
        if "objects" in contacts_data and contacts_data["objects"]:
            print("Fetching new contacts since:", create_after)
            print("+------------+-----------------+-----------------+------------------------+------------------------+")
            print("| Contact ID | First Name      | Last Name       | Email                  | Fetched At             |")
            print("+------------+-----------------+-----------------+------------------------+------------------------+")
            for contact in contacts_data["objects"]:
                contact_id = contact.get("id", "N/A")
                full_name = contact.get("name", "N/A")
                name_parts = full_name.split(" ")
                first_name = name_parts[0] if len(name_parts) > 0 else "N/A"
                last_name = name_parts[1] if len(name_parts) > 1 else "N/A"
                email = get_email(contact_id)
                if email:
                    new_contacts.append({
                        "first_name": first_name,
                        "last_name": last_name,
                        "email": email,
                        "fetched_at": fetch_timestamp  # Include the fetched timestamp
                    })
                    print(f"| {contact_id:<10} | {first_name:<15} | {last_name:<15} | {email:<22} | {fetch_timestamp:<22} |")
            print("+------------+-----------------+-----------------+------------------------+------------------------+")
    return new_contacts

# Function to fetch all contacts from HubSpot and display in a table
def get_all_hubspot_contacts():
    hubspot_fetch_timestamp = datetime.now().isoformat()  # Timestamp for when HubSpot contacts are fetched
    url = f"{BASE_URL}/crm/v3/objects/contacts"
    all_contacts = []
    has_more = True
    after = 0  # Pagination parameter for HubSpot

    while has_more:
        params = {
            "limit": 100,  # Max results per page
            "after": after
        }
        response = requests.get(url, headers=hubspot_headers, params=params)
        if response.status_code == 200:
            data = response.json()
            contacts_data = data.get("results", [])
            has_more = data.get("paging", {}).get("next", {}).get("after", False)

            if contacts_data:
                all_contacts.extend(contacts_data)
                after = data.get("paging", {}).get("next", {}).get("after", 0)
            else:
                has_more = False
        else:
            print(f"❌ Error fetching contacts from HubSpot: {response.json()}")
            has_more = False

    # Print the fetched contacts from HubSpot
    print(f"\nAll contacts in HubSpot on: {hubspot_fetch_timestamp}")
    print("+------------+-----------------+-----------------+------------------------+------------------------+")
    print("| Contact ID | First Name      | Last Name       | Email                  | Fetched At             |")
    print("+------------+-----------------+-----------------+------------------------+------------------------+")
    for contact in all_contacts:
        contact_id = contact.get("id", "N/A") or "N/A"
        first_name = contact.get("properties", {}).get("firstname", "N/A") or "N/A"
        last_name = contact.get("properties", {}).get("lastname", "N/A") or "N/A"
        email = contact.get("properties", {}).get("email", "N/A") or "N/A"
        
        print(f"| {contact_id:<10} | {first_name:<15} | {last_name:<15} | {email:<22} | {hubspot_fetch_timestamp:<22} |")
    print("+------------+-----------------+-----------------+------------------------+------------------------+")


# Function to sync contacts to HubSpot
def sync_contacts_to_hubspot():
    new_contacts = get_new_contacts()
    print("\nStarting sync process...")
    for contact in new_contacts:
        email = contact["email"]
        synced_at = datetime.now().isoformat()  # Timestamp for when contact is synced
        print(f"Checking if {email} exists in HubSpot...")
        search_payload = {
            "filterGroups": [
                {"filters": [{"propertyName": "email", "operator": "EQ", "value": email}]}
            ],
            "properties": ["firstname", "lastname", "email"]
        }
        response = requests.post(GET_CONTACTS_URL, json=search_payload, headers=hubspot_headers)
        if response.status_code == 200:
            data = response.json()
            existing_contacts = data.get("results", [])
            if existing_contacts:
                contact_id = existing_contacts[0]["id"]
                print(f"✅ Contact found (ID: {contact_id}), updating...")
                update_url = UPDATE_CONTACT_URL.format(contact_id=contact_id)
                update_payload = {"properties": {"firstname": contact["first_name"], "lastname": contact["last_name"], "email": email}}
                update_response = requests.patch(update_url, json=update_payload, headers=hubspot_headers)
                if update_response.status_code == 200:
                    print(f"✅ Contact updated: {email}")
                else:
                    print(f"❌ Failed to update: {update_response.json()}")
            else:
                print(f"🔹 {email} not found, creating new contact...")
                new_contact_payload = {"properties": {"firstname": contact["first_name"], "lastname": contact["last_name"], "email": email}}
                post_response = requests.post(POST_CONTACT_URL, json=new_contact_payload, headers=hubspot_headers)
                if post_response.status_code in [200, 201]:
                    created_contact = post_response.json()
                    contact_id = created_contact.get("id")
                    print(f"✅ Created new contact (ID: {contact_id})")
                else:
                    print(f"❌ Failed to create contact: {post_response.json()}")
        else:
            print(f"❌ Error searching contacts: {response.json()}")
        
        # After syncing to HubSpot, print the timestamp of when it was synced
        print(f"Synced At: {synced_at}")
    
    print("\nSync process completed!")

# Run the sync function
sync_contacts_to_hubspot()

# Fetch and print all HubSpot contacts
get_all_hubspot_contacts()
