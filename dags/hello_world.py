from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 17),  # Adjust start date as needed
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'hello_world',  # Unique identifier for the DAG
    default_args=default_args,
    description='A simple test DAG that prints Hello World',
    schedule_interval='@once',  # Run only once when triggered manually
    catchup=False,
)

# Function to print Hello World
def print_hello():
    print("Hello, World!")

# Task: Run the function
hello_task = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)

hello_task  # Task execution

