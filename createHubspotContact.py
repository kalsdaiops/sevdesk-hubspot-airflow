import requests
import time
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# 🔹 Retrieve API keys from environment variables
HUBSPOT_API_KEY = os.getenv("HUBSPOT_API_KEY")
# 🔹 HubSpot API URLs
BASE_URL = "https://api.hubapi.com"
GET_CONTACTS_URL = f"{BASE_URL}/crm/v3/objects/contacts/search"
POST_CONTACT_URL = f"{BASE_URL}/crm/v3/objects/contacts"
UPDATE_CONTACT_URL = f"{BASE_URL}/crm/v3/objects/contacts/{{contact_id}}"

# Headers for authentication
headers = {
    "Authorization": f"Bearer {HUBSPOT_API_KEY}",
    "Content-Type": "application/json"
}

# Contact data
first_name = "John"
last_name = "Snow"
email = "string@gmail.com"  # Replace with the actual email

# Contact fields to send to HubSpot
new_contact_payload = {
    "properties": {
        "firstname": first_name,
        "lastname": last_name,
        "email": email
    }
}

# Step 1: Check if the contact exists using email
search_payload = {
    "filterGroups": [
        {
            "filters": [
                {
                    "propertyName": "email",
                    "operator": "EQ",
                    "value": email
                }
            ]
        }
    ],
    "properties": ["firstname", "lastname", "email"]
}

response = requests.post(GET_CONTACTS_URL, json=search_payload, headers=headers)

if response.status_code == 200:
    data = response.json()
    existing_contacts = data.get("results", [])

    if existing_contacts:
        # Step 2: If contact exists, update it
        contact_id = existing_contacts[0]["id"]
        print(f"✅ Contact found with ID: {contact_id}, updating it...")

        update_url = UPDATE_CONTACT_URL.format(contact_id=contact_id)
        update_response = requests.patch(update_url, json=new_contact_payload, headers=headers)

        if update_response.status_code == 200:
            print(f"✅ Contact updated successfully: {email}")
        else:
            print(f"❌ Failed to update contact: {update_response.json()}")
    else:
        # Step 3: If contact doesn't exist, create a new one
        print("🔹 No existing contact found. Creating new one...")

        post_response = requests.post(POST_CONTACT_URL, json=new_contact_payload, headers=headers)

        if post_response.status_code in [200, 201]:
            created_contact = post_response.json()
            contact_id = created_contact.get("id")
            print(f"✅ New contact created with ID: {contact_id}")
        else:
            print(f"❌ Failed to create contact: {post_response.json()}")
else:
    print(f"❌ Error searching contacts: {response.json()}")