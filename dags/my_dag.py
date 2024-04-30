from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

# Define the DAG
with DAG(
    dag_id="data_loading",
    start_date=datetime(2022, 1, 1),
    schedule_interval="@daily",
) as dag:

    # Define a task to load data into the database
    load_data = BashOperator(
        task_id="load_data",
        bash_command="python /path/to/your/data_loading_script.py",
    )