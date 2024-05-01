from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine


def create_pg_sqlalchemy_engine(user, password, host, port, db):
    connection_string = f'postgresql://{user}:{password}@{host}:{port}/{db}'
    engine = create_engine(connection_string)
    return engine


def _load_data(df, table_name):
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
    # data = pd.read_csv('/opt/airflow/data/test_data.csv')

    # Write dataFrame to the database
    # TODO: convert the data types to the correct ones before writing to the database
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)


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
    df_vehicles = pd.DataFrame(data=track_info, columns=track_cols)
    df_trajectory = pd.DataFrame(data=trajectory_info, columns=trajectory_cols)
    return df_vehicles, df_trajectory

def strip_whitespace_from_columns(df):
    """
    This function strips whitespace from DataFrame column names.
    :param df: The DataFrame
    :return: The DataFrame with stripped column names
    """
    df.columns = df.columns.str.strip()
    return df

def define_column_types():
    """
    This function defines the column types for the vehicles and trajectory DataFrames.
    :return: The column types for the vehicles and trajectory DataFrames
    """
    column_types_trajectory = {
        'track_id': 'int64',
        'lat': 'float64',
        'lon': 'float64',
        'speed': 'float64',
        'lon_acc': 'float64',
        'lat_acc': 'float64',
        'time': 'float64'
    }

    column_types_vehicles = {
        'track_id': 'int64',
        'type': 'object',  # Assuming 'type' is a categorical variable
        'traveled_d': 'float64',  # Assuming 'traveled_d' is distance traveled in some unit, like km
        'avg_speed': 'float64'  # Assuming 'avg_speed'is average speed in km/h
    }

    return column_types_vehicles, column_types_trajectory

def convert_df_column_types(df, column_types):
    """
    This function converts the column types of a DataFrame.
    :param df: The DataFrame
    :param column_types: The column types
    :return: The DataFrame with converted column types
    """
    df = df.astype(column_types)
    return df

def convert_columns_to_correct_data_types(df_vehicles, df_trajectory):
    # Strip white spaces from the columns
    df_trajectory = strip_whitespace_from_columns(df_trajectory)
    df_vehicles = strip_whitespace_from_columns(df_vehicles)

    # Define column types
    column_types_vehicles, column_types_trajectory = define_column_types()

    # Convert to the correct data types for each column in the trajectory DataFrame
    df_trajectory = convert_df_column_types(df_trajectory, column_types_trajectory)

    # Convert to the correct data types for each column in the vehicles DataFrame
    df_vehicles = convert_df_column_types(df_vehicles, column_types_vehicles)

    return df_vehicles, df_trajectory

def generate_df(file_path: str):
    # Read the file
    lines_as_lists = read_file(file_path)
    # Get the maximum number of fields
    no_field_max = get_max_fields(lines_as_lists)
    print(f"the maximum number of fields is {no_field_max}")
    largest_n = int((no_field_max - 4) / 6)
    print(f"the largest n = {largest_n}")
    cols = lines_as_lists.pop(0)
    # Get the track and trajectory information
    track_info, trajectory_info = get_track_and_trajectory_info(lines_as_lists, no_field_max)
    # Create the dataframes
    df_vehicles, df_trajectory = create_dataframes(track_info, trajectory_info, cols)

    # Convert columns to correct data types
    df_vehicles, df_trajectory = convert_columns_to_correct_data_types(df_vehicles, df_trajectory)

    return df_vehicles, df_trajectory


def extract_and_load_data():
    # Hard code the file path but TODO: make this to read file name dynamically
    df_vehicles, df_trajectory = generate_df('/opt/airflow/data/test_data.csv')

    # Load data into the database
    _load_data(df_vehicles, 'vehicles')
    _load_data(df_trajectory, 'trajectories')


# Define the DAG
with DAG(
        dag_id="traffic_flow",
        start_date=datetime(2024, 4, 29),
        schedule_interval="@daily",
        tags=['traffic'],
) as dag:
    # Define a task to load data into the database
    extract_data = PythonOperator(
        task_id="extract_and_load_data",
        python_callable=extract_and_load_data,
    )
