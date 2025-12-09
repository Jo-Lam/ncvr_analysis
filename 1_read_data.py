import pandas as pd
import glob
import os

os.chdir(r"ncvr/raw_data")

# Specify the pattern for your file names
file_pattern = 'ncvoter-*.csv.gz'

# Get a list of all file names that match the pattern
file_names = glob.glob(file_pattern)

# Initialize an empty DataFrame to store the merged data
merged_df = pd.DataFrame()

# Loop through each file and merge it into the combined DataFrame
for file_name in file_names:
    # Extract the year from the file name (assuming the year is part of the file name)
    snapshot_version = file_name.split('-')[1][:4]
    # Read the CSV file into a DataFrame
    df = pd.read_csv(file_name, compression='gzip', encoding = 'latin1') #encoding utf-8 did not work.
    # keep only relevant variables
    columns_to_keep = ['ncid', 'first_name', 'middle_name', 'last_name', 'age', 'gender', 'race', 'ethnic', 'zip_code']
    df = df[columns_to_keep]
    # Merge the DataFrame into the combined DataFrame using the shared unique identifier
    # Adjust 'unique_id_column' to the actual column name of the shared unique identifier
    if file_name == 'ncvoter-20111004.csv.gz':
        merged_df = df
    else:
        # Define a suffix for the merge operation
        merge_suffix = '_' + snapshot_version
        # Merge with a suffix for this specific file
        merged_df = pd.merge(merged_df, df, on='ncid', how='outer', suffixes=('', merge_suffix))

columns_to_rename = ['first_name', 'middle_name', 'last_name', 'age', 'gender', 'race', 'ethnic', 'zip_code']
suffix_to_add = '_2011'
merged_df = merged_df.rename(columns={col: col + suffix_to_add for col in columns_to_rename})

merged_df.to_parquet("all_data_raw.parquet")