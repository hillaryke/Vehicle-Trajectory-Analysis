from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from random import randint

def _training_model():
    return randint(1, 10)

def _choose_best_model(ti):
    if (best_accuracy > 8):
        return "accurate"
    return "inaccurate"

# Define the DAG
with DAG(
    dag_id="data_loading",
    start_date=datetime(2024, 4, 29),
    schedule_interval="@daily",
) as dag:
    # Define a task to load data into the database
    # load_data = PostgresOperator(
    #     task_id="load_data",
    #     postgres_conn_id="postgres_conn_id",  # replace with your connection ID
    #     sql="""
    #         COPY your_table
    #         FROM '../data/test_data.csv'
    #         DELIMITER ';'
    #         CSV HEADER;
    #     """,
    # )
    training_model_A = PythonOperator(
        task_id="training_model_A",
        python_callable=_training_model,
    )

    training_model_B = PythonOperator(
        task_id="training_model_B",
        python_callable=_training_model,
    )

    training_model_C = PythonOperator(
        task_id="training_model_C",
        python_callable=_training_model,
    )

    choose_best_model = BranchPythonOperator(
        task_id="choose_best_model",
    )

    accurate = BashOperator(
        task_id="accurate",
        bash_command="echo 'accurate'",
    )

    inaccurate = BashOperator(
        task_id="inaccurate",
        bash_command="echo 'inaccurate'",
    )