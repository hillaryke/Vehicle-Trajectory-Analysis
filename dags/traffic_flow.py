from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

def read_file(file_path):
    """
    This function reads a file and returns a list of lists where each list is a line from the file.
    :param file_path: The path to the file
    :return: A list of lists where each list is a line from the file
    """
    with open(file_path, 'r') as file:
        lines = file.readlines()
    return [line.strip('\n').strip().strip(';').split(';') for line in lines]


def get_max_fields(lines_as_lists):
    """
    This function returns the maximum number of fields in the lines.
    :param lines_as_lists: The lines as lists
    :return: The maximum number of fields
    """
    no_field_max = 0
    for row in lines_as_lists:
        if len(row) > no_field_max:
            no_field_max = len(row)
    return no_field_max


def get_track_and_trajectory_info(lines_as_lists, no_field_max):
    """
    This function returns the track and trajectory information.
    :param lines_as_lists: The lines as lists
    :param no_field_max: The maximum number of fields
    :return: The track and trajectory information
    """
    track_info = []
    trajectory_info = []
    for row in lines_as_lists:
        track_id = row[0]
        track_info.append(row[:4])
        remaining_values = row[4:]
        trajectory_matrix = [[track_id] + remaining_values[i:i + 6] for i in range(0, len(remaining_values), 6)]
        trajectory_info = trajectory_info + trajectory_matrix
    return track_info, trajectory_info


def create_dataframes(track_info, trajectory_info, cols):
    """
    This function creates dataframes from the track and trajectory information.
    :param track_info: The track information
    :param trajectory_info: The trajectory information
    :param cols: The columns
    :return: The dataframes
    """
    track_cols = cols[:4]
    trajectory_cols = ['track_id'] + cols[4:]
    df_track = pd.DataFrame(data=track_info, columns=track_cols)
    df_trajectory = pd.DataFrame(data=trajectory_info, columns=trajectory_cols)
    return df_track, df_trajectory


def extract_data_main():
    """
    The main function that calls the other functions and prints the results.
    """
    # Read the file
    lines_as_lists = read_file('/opt/airflow/data/test_data.csv')
    # Get the maximum number of fields
    no_field_max = get_max_fields(lines_as_lists)
    print(f"the maximum number of fields is {no_field_max}")
    largest_n = int((no_field_max - 4) / 6)
    print(f"the largest n = {largest_n}")
    cols = lines_as_lists.pop(0)
    # Get the track and trajectory information
    track_info, trajectory_info = get_track_and_trajectory_info(lines_as_lists, no_field_max)
    # Create the dataframes
    df_track, df_trajectory = create_dataframes(track_info, trajectory_info, cols)
    print(df_track.head(20))
    print(df_trajectory.head())

# Define the DAG
with DAG(
        dag_id="traffic_flow",
        start_date=datetime(2024, 4, 29),
        schedule_interval="@daily",
        tags=['traffic'],
) as dag:
    # Define a task to read the file
    read_file_task = PythonOperator(
        task_id="read_file",
        python_callable=read_file,
        op_kwargs={'file_path': '/opt/airflow/data/test_data.csv'},
    )

    # Define a task to get the maximum number of fields
    get_max_fields_task = PythonOperator(
        task_id="get_max_fields",
        python_callable=get_max_fields,
    )

    # Define a task to get the track and trajectory information
    get_track_and_trajectory_info_task = PythonOperator(
        task_id="get_track_and_trajectory_info",
        python_callable=get_track_and_trajectory_info,
    )

    # Define a task to create the dataframes
    create_dataframes_task = PythonOperator(
        task_id="create_dataframes",
        python_callable=create_dataframes,
    )

    # Define the order of the tasks
    [read_file_task >> get_max_fields_task >> get_track_and_trajectory_info_task] >> create_dataframes_task