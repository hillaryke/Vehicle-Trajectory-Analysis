from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Define the DAG
with DAG(
    dag_id="traffic_flow",
    start_date=datetime(2022, 1, 1),
    schedule_interval="@daily",
) as dag:

    # Define a task to load data into the database
    load_data = BashOperator(
        task_id="load_data",
        bash_command="python /path/to/your/data_loading_script.py",
    )

    # Define a task to run dbt transformations
    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command="dbt run --profiles-dir /path/to/your/dbt/profiles",
    )

    # Define a task to check data quality with dbt modules
    check_data_quality = BashOperator(
        task_id="check_data_quality",
        bash_command="dbt test --profiles-dir /path/to/your/dbt/profiles",
    )

    # Define a task to generate a report
    generate_report = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report_function,  # replace with your function
    )

    # Define the order of task execution
    load_data >> run_dbt >> check_data_quality >> generate_report