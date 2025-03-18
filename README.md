# 🚀 sevDesk to HubSpot Contact Sync with Apache Airflow

## 📌 Overview
This project automates the process of transferring newly created contacts from **sevDesk** to **HubSpot** using **Apache Airflow**. It eliminates manual data entry and ensures real-time synchronization.

## 🛠 Tech Stack
- **Apache Airflow** → For workflow automation
- **sevDesk API** → Fetches new contacts
- **HubSpot API** → Creates contacts in CRM
- **Docker** → Containerized execution
- **GitHub** → Version control
  
🛠 Tech Equivalents: AWS vs. Apache Airflow
AWS Service	Apache Airflow Equivalent	Purpose
- **Apache Airflow** → For workflow automation
- **sevDesk API** → Fetches new contacts
- **HubSpot API** → Creates contacts in CRM
- **Docker** → Containerized execution
- **GitHub** → Version control
**AWS Lambda | Airflow DAG (PythonOperator)** -	Executes the Python script that fetches data from sevDesk and pushes it to HubSpot.
**AWS EventBridge  | (Scheduler)	Airflow Scheduler (Cron/Timetable)**	 -	Triggers the DAG at defined intervals (e.g., every hour).
**AWS Secrets Manager | 	Airflow Variables & Connections	 -**	Stores API keys securely to avoid hardcoding sensitive credentials.
**AWS CloudWatch Logs | 	Airflow Task Logs (UI & Logging)**	 -	Logs task execution, errors, and success status.
**AWS Step Functions | 	Airflow DAG Task Dependencies**	 -	Defines task execution order and dependencies.

## 📂 Project Structure
```
sevdesk-hubspot-airflow/
│── dags/                 # Airflow DAGs (Python scripts)
│── plugins/              # Custom Airflow plugins
│── config/               # Configuration files
│── logs/                 # Airflow logs (ignored in Git)
│── docker-compose.yaml   # Airflow Docker setup
│── .env                  # API keys (ignored in Git)
│── README.md             # Documentation
│── .gitignore            # Ignore unnecessary files
```

## 🚀 Installation & Setup

### 1️⃣ **Clone the Repository**
```bash
git clone https://github.com/YOUR_USERNAME/sevdesk-hubspot-airflow.git
cd sevdesk-hubspot-airflow
```

### 2️⃣ **Set Up Environment Variables**
Create a `.env` file and add:
```env
SEVDESK_API_KEY="your_sevdesk_api_key"
HUBSPOT_API_KEY="your_hubspot_api_key"
```

### 3️⃣ **Start Apache Airflow with Docker**
```bash
docker-compose up -d
```
Airflow UI should be accessible at:
```
http://localhost:8080
```

### 4️⃣ **Trigger the DAG Manually**
1. Log into **Airflow UI**.
2. Find `sevdesk_to_hubspot_sync_dynamic` DAG.
3. Click **Trigger DAG**.

## 📜 API Endpoints Used
- **sevDesk**
  - `GET /Contact` → Fetch new contacts
- **HubSpot**
  - `POST /crm/v3/objects/contacts` → Create contact

## 📝 How It Works
1. The DAG **detects new contacts** in sevDesk.
2. It extracts the **email, first name, and last name**.
3. It sends the data to the **HubSpot API** to create a new contact.
4. Handles **errors & logging** automatically.

## 🔄 DAG Workflow
- `detect_new_contact_in_sevdesk`: Fetch new contact IDs from sevDesk.
- `process_and_create_contact`: Send the contact to HubSpot.
- Retries failed API calls and logs errors.

## 🛠 Future Improvements
- Implement **webhooks** for real-time sync
- Deploy to **AWS Lambda** for scalability
- Improve logging & monitoring

## 🤝 Contributing
Pull requests are welcome!

---
🚀 **Built with Apache Airflow & APIs**


