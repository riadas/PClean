import pandas as pd
from collections import defaultdict
import datetime

# Read the CSV file
df = pd.read_csv('datasets/flights_dirty.csv')

# Function to convert time strings to datetime objects
def parse_time(time_str):
    if pd.isna(time_str):
        return None
    try:
        # Handle various time formats
        if 'Dec' in str(time_str) or '12/02' in str(time_str):
            return pd.to_datetime(time_str, format='%m/%d/%Y %I:%M %p', errors='coerce')
        else:
            return pd.to_datetime(time_str, format='%I:%M %p', errors='coerce')
    except:
        return None

# Convert times to datetime objects
for col in ['sched_dep_time', 'act_dep_time', 'sched_arr_time', 'act_arr_time']:
    df[col] = df[col].apply(parse_time)

# Group by flight number
flight_groups = df.groupby('flight')

# Dictionary to store corrected times
corrected_times = defaultdict(lambda: defaultdict(list))

# Collect all non-null times for each flight
for flight, group in flight_groups:
    for col in ['sched_dep_time', 'act_dep_time', 'sched_arr_time', 'act_arr_time']:
        valid_times = group[col].dropna()
        if not valid_times.empty:
            # Get the most common time
            most_common = valid_times.mode().iloc[0]
            corrected_times[flight][col] = most_common

# Apply corrections to the dataframe
def get_corrected_time(row, col):
    if pd.isna(row[col]):
        return corrected_times[row['flight']][col]
    return row[col]

# Update the dataframe with corrected times
for col in ['sched_dep_time', 'act_dep_time', 'sched_arr_time', 'act_arr_time']:
    df[col] = df.apply(lambda row: get_corrected_time(row, col), axis=1)

# Format times back to strings
def format_time(dt):
    if pd.isna(dt):
        return ''
    return dt.strftime('%I:%M %p').lstrip('0')

# Convert times back to formatted strings
for col in ['sched_dep_time', 'act_dep_time', 'sched_arr_time', 'act_arr_time']:
    df[col] = df[col].apply(format_time)

# Save the corrected dataset
df.to_csv('datasets/flights_clean.csv', index=False)