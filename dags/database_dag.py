from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
import psycopg2

def create_pg_sqlalchemy_engine(user, password, host, port, db):
    connection_string = f'postgresql://{user}:{password}@{host}:{port}/{db}'
    engine = create_engine(connection_string)
    return engine

def create_postgres_connection(user, password, host, port, db):
    connection = psycopg2.connect(
        dbname=db,
        user=user,
        password=password,
        host=host,
        port=port
    )
    return connection

def _query_data():
    # create a connection to the database using sqlalchemy
    engine = create_pg_sqlalchemy_engine('airflow', 'airflow', 'postgres', '5432', 'postgres')

    # Try to establish a connection and execute a SQL query
    try:
        with engine.connect() as connection:
            result = connection.execute("SELECT * FROM traffic LIMIT 10")
            for row in result:
                print(row)
    except Exception as e:
        print("Failed to query the database. Error: ", e)

def _load_data():
    # create a connection to the database using sqlalchemy
    engine = create_pg_sqlalchemy_engine('airflow', 'airflow', 'postgres', '5432', 'postgres')

    # Try to establish a connection and execute a simple SQL query
    try:
        with engine.connect() as connection:
            result = connection.execute("SELECT 1")
            print("Connection successful. Result: ", result.scalar())
    except Exception as e:
        print("Failed to connect to the database. Error: ", e)

    # Load data from a CSV file into a pandas DataFrame
    data = pd.read_csv('/opt/airflow/data/test_data.csv')

    # Write data to the database
    data.to_sql('traffic', con=engine, if_exists='replace', index=False)

with DAG(
    dag_id="data_loading_dev",
    start_date=datetime(2024, 4, 29),
    schedule_interval="@daily",
) as dag:
    # Define task to load data into the database
    load_data = PythonOperator(
        task_id="load_data_prod",
        python_callable=_load_data
    )

    # Define task to query data from the database
    query_data = PythonOperator(
        task_id="query_data",
        python_callable=_query_data
    )

    # Define the task execution order
    load_data >> query_data

