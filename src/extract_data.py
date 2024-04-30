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


def load_data_main():
    """
    The main function that calls the other functions and prints the results.
    """
    # Read the file
    lines_as_lists = read_file("../data/test_data.csv")
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
