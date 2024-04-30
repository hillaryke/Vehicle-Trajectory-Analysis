from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from src.extract_data import extract_data_main

# Define the DAG
with DAG(
        dag_id="traffic_flow",
        start_date=datetime(2024, 4, 29),
        schedule_interval="@daily",
) as dag:
    # Define a task to load data into the database
    load_data = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data_main,
    )