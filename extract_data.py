import pandas as pd

with open("../data/test_data.csv", 'r') as file:
    lines = file.readlines()

lines_as_lists = [line.strip('\n').strip().strip(';').split(';') for line in lines]
len(lines_as_lists)

no_field_max = 0

for row in lines_as_lists:
    if len(row) > no_field_max:
        no_field_max = len(row)

print(f"the maximum number of fields is {no_field_max}")
largest_n = int((no_field_max-4)/6)
print(f"the largest n = {largest_n}")


cols = lines_as_lists.pop(0)


track_cols = cols[:4]
trajectory_cols = ['track_id'] + cols[4:]


track_info = []
trajectory_info = []

for row in lines_as_lists:
    track_id = row[0]

    # add the first 4 values to track_info
    track_info.append(row[:4])

    remaining_values = row[4:]
    # reshape the list into a matrix and add track_id
    trajectory_matrix = [ [track_id] + remaining_values[i:i+6] for i in range(0,len(remaining_values),6)]
    # add the matrix rows to trajectory_info
    trajectory_info = trajectory_info + trajectory_matrix

df_track = pd.DataFrame(data= track_info,columns=track_cols)

df_track.head(20)

df_trajectory = pd.DataFrame(data= trajectory_info,columns=trajectory_cols)

df_trajectory.head()

