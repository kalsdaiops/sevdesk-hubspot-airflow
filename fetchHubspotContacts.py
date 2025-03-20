import requests
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
# 🔹 Retrieve API keys from environment variables
HUBSPOT_API_KEY = os.getenv("HUBSPOT_API_KEY")
SEVDESK_API_TOKEN = os.getenv("SEVDESK_API_TOKEN")
# 🔹 HubSpot API URLs
BASE_URL = "https://api.hubapi.com"
GET_CONTACTS_URL = f"{BASE_URL}/crm/v3/objects/contacts"

# Headers for authentication
headers = {
    "Authorization": f"Bearer {HUBSPOT_API_KEY}",
    "Content-Type": "application/json"
}

# Function to fetch contacts from HubSpot
def get_all_contacts():
    # Keep track of all contacts
    all_contacts = []
    offset = None
   
    while True:
        # Construct URL with pagination if offset exists
        url = GET_CONTACTS_URL
        if offset:
            url = f"{GET_CONTACTS_URL}?after={offset}"

        # Make GET request to HubSpot API
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()

            # Add contacts to the list
            contacts = data.get("results", [])
            all_contacts.extend(contacts)

            # Check if there is a next page of contacts (pagination)
            offset = data.get("paging", {}).get("next", {}).get("after")

            # If there is no next page, break out of the loop
            if not offset:
                break
        else:
            print(f"❌ Error fetching contacts: {response.json()}")
            break

    return all_contacts

# Fetch all contacts
contacts = get_all_contacts()

# Prepare data for display
if contacts:
    print("\nContacts in HubSpot:\n")
    print(f"{'First Name':<15} {'Last Name':<15} {'Email':<30}")
    print("-" * 60)  # Separator line
   
    for contact in contacts:
        properties = contact.get("properties", {})
       
        # Replace None with "N/A" if the value is missing
        first_name = properties.get("firstname", "N/A") or "N/A"
        last_name = properties.get("lastname", "N/A") or "N/A"
        email = properties.get("email", "N/A") or "N/A"
       
        # Print each contact's details
        print(f"{first_name:<15} {last_name:<15} {email:<30}")
else:
    print("No contacts found.")