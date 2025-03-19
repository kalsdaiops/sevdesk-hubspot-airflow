import requests
import time  # Add delay between API calls
from datetime import datetime, timedelta

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

# Define the timestamp (e.g., 1 hour ago)
timestamp = (datetime.now() - timedelta(hours=1)).isoformat()  # Get contacts created in the last 1 hour

# Print the `createAfter` filter and the current timestamp
print(f"Using createAfter filter: {timestamp}")
print(f"Current timestamp: {datetime.now().isoformat()}")

# Contact data
customer_number = "Customer-1337"
first_name = "John"
last_name = "Snow"
email = "string@gmail.com"  # Replace with the actual email
contact_id = None  # To be assigned after contact creation

new_contact_payload = {
    "name": f"{first_name} {last_name}",
    "status": 100,
    "customerNumber": customer_number,
    "surename": first_name,
    "familyname": last_name,
    "titel": "Commander",
    "category": {  # Ensure this category exists in sevDesk
        "id": 3,
        "objectName": "Category"
    }
}

# Step 1: Check if the contact exists using the customer number and the createAfter parameter
GET_CONTACTS_URL_WITH_TIMESTAMP = f"{GET_CONTACTS_URL}?createAfter={timestamp}"

response = requests.get(GET_CONTACTS_URL_WITH_TIMESTAMP, headers=headers)
if response.status_code == 200:
    data = response.json()
    existing_contacts = [
        contact for contact in data.get("objects", []) if contact.get("customerNumber") == customer_number
    ]

    if existing_contacts:
        # Step 2: If contact exists, update it
        contact_id = existing_contacts[0]["id"]
        UPDATE_CONTACT_URL = f"{BASE_URL}/Contact/{contact_id}"

        update_response = requests.put(UPDATE_CONTACT_URL, json=new_contact_payload, headers=headers)
        if update_response.status_code == 200:
            print(f"✅ Contact updated successfully: {customer_number}")
        else:
            print(f"❌ Failed to update contact: {update_response.json()}")
            exit()  # Stop execution if contact update fails
    else:
        # Step 3: If contact doesn't exist, create a new one
        post_response = requests.post(POST_CONTACT_URL, json=new_contact_payload, headers=headers)
        if post_response.status_code in [200, 201]:
            created_contact = post_response.json()
            contact_id = created_contact.get("objects", {}).get("id")
            print(f"✅ New contact created: {customer_number}")
        else:
            print(f"❌ Failed to create contact: {post_response.json()}")
            exit()  # Stop execution if contact creation fails
else:
    print(f"❌ Error fetching contacts: {response.json()}")
    exit()  # Stop execution if contact lookup fails

# 🔹 Wait for the new contact to be available in the system
time.sleep(2)

# Step 4: Create or update the email for the contact
email_payload = {
    "contact": {
        "id": contact_id,
        "objectName": "Contact"
    },
    "type": "EMAIL",
    "value": email,
    "key": {
        "id": 1,  # This should be the correct key ID for EMAIL in SevDesk
        "objectName": "CommunicationWayKey"
    },
    "main": 0  # Set to 1 if this should be the main email, otherwise 0
}

# Check if the contact already has an email
email_response = requests.get(f"{GET_EMAIL_URL}?contact.id={contact_id}&objectName=CommunicationWay", headers=headers)

if email_response.status_code == 200:
    email_data = email_response.json()
   
    # Check if any email exists
    existing_emails = [obj for obj in email_data.get("objects", []) if obj.get("type") == "EMAIL"]

    if existing_emails:
        email_id = existing_emails[0]["id"]
        print(f"🔹 Existing email found: ID {email_id}")

        # Update existing email
        update_email_response = requests.put(f"{POST_EMAIL_URL}/{email_id}", json=email_payload, headers=headers)

        if update_email_response.status_code == 200:
            print(f"✅ Email updated: {email}")
            # Print the contact details (sure name, last name, email)
            print(f"Contact Details - Sure Name: {first_name}, Last Name: {last_name}, Email: {email}")
        else:
            print(f"❌ Failed to update email: {update_email_response.json()}")

    else:
        print("🔹 No existing email found. Creating new one...")

        # Create new email
        post_email_response = requests.post(POST_EMAIL_URL, json=email_payload, headers=headers)

        if post_email_response.status_code in [200, 201]:
            print(f"✅ New email added: {email}")
            # Print the contact details (sure name, last name, email)
            print(f"Contact Details - Sure Name: {first_name}, Last Name: {last_name}, Email: {email}")
        else:
            print(f"❌ Failed to add email: {post_email_response.json()}")

elif email_response.status_code == 400:
    print(f"⚠️ No email found, attempting to create one...")

    # Create new email
    post_email_response = requests.post(POST_EMAIL_URL, json=email_payload, headers=headers)

    if post_email_response.status_code in [200, 201]:
        print(f"✅ New email added: {email}")
        # Print the contact details (sure name, last name, email)
        print(f"Contact Details - Sure Name: {first_name}, Last Name: {last_name}, Email: {email}")
    else:
        print(f"❌ Failed to add email: {post_email_response.json()}")

else:
    print(f"❌ Error fetching email: {email_response.json()}")