from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime

def _load_data():
    # Load data into the database
    # engine = create_engine('postgresql://postgres://postgres:postgres@localhost:5432/postgres')

    # Read the csv file
    df = pd.read_csv('/opt/airflow/data/test_data.csv')

        # Rest of your function...
    # Write the data from the DataFrame to the table
    # df.to_sql('traffic', engine, if_exists='replace', index=False)

    print("Loading data into the database")

with DAG(
    dag_id="data_loading_csv",
    start_date=datetime(2024, 4, 29),
    schedule_interval="@daily",
) as dag:
    # Define task to load data into the database
    load_data = PythonOperator(
        task_id="load_data_prod",
        python_callable=_load_data
    )

    # Define the task execution order below

