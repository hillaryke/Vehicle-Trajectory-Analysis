# Import necessary modules and functions
from airflow import DAG
from airflow.datasets import Dataset
from airflow.utils.task_group import TaskGroup
from pendulum import datetime
from cosmos.providers.dbt.core.operators import (
    DbtDepsOperator,
    DbtRunOperationOperator,
    DbtSeedOperator,
)

# Define the DAG
with DAG(
    dag_id="import-seeds",  # Unique identifier for the DAG
    start_date=datetime(2023, 1, 1),  # The date at which to start running the DAG
    schedule_interval=None,  # The DAG will not be scheduled to run at regular intervals
    catchup=False,  # Airflow will not create historical DAG runs for intervals that were missed
    max_active_runs=1,  # The maximum number of active DAG runs per DAG
) as dag:

    # Define the seeds to be imported
    project_seeds = [
        {
            "project": "jaffle_shop",
            "seeds": ["20181101_d1_0830_0900"],
        }
    ]

    # Define a task to install dependencies
    deps_install = DbtDepsOperator(
        task_id="jaffle_shop_install_deps",
        project_dir=f"/usr/local/airflow/dbt/jaffle_shop",
        schema="public",
        dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
        conn_id="postgres",
    )

    # Define a task group to drop existing seed tables if they exist
    with TaskGroup(group_id="drop_seeds_if_exist") as drop_seeds:
        for project in project_seeds:
            for seed in project["seeds"]:
                DbtRunOperationOperator(
                    task_id=f"drop_{seed}_if_exists",
                    macro_name="drop_table",
                    args={"table_name": seed},
                    project_dir=f"/usr/local/airflow/dbt/{project['project']}",
                    schema="public",
                    dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
                    conn_id="postgres",
                )

    # Define a task to create the seed tables
    create_seeds = DbtSeedOperator(
        task_id=f"jaffle_shop_seed",
        project_dir=f"/usr/local/airflow/dbt/jaffle_shop",
        schema="public",
        dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
        conn_id="postgres",
        outlets=[Dataset(f"SEED://TRAFFIC_FLOW")],
     )

    # Define the order of task execution
    deps_install >> drop_seeds >> create_seeds