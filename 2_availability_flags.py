import pandas as pd
import os
import glob
# Function to create binary flags for missing data
def create_missing_flags(df, variable, file_suffix):
    df[f'{variable}_missing_{file_suffix}'] = df[variable].isnull().astype(int)
    return df[['ncid', f'{variable}_missing_{file_suffix}']]

# Function to extract the year from the file name
def extract_year(file_name):
    # Assuming the year is always present in the file name
    # You may need to adjust this based on your file naming convention
    return file_name.split('-')[1][:4]
    
# Function to process a single csv file
def process_csv(file_path, variables_of_interest, output_folder):
    df = pd.read_csv(file_path, compression='gzip', encoding = 'latin1')
    result_dfs = []
    file_suffix = extract_year(os.path.basename(file_path))
#
    for variable in variables_of_interest:
        result_df = create_missing_flags(df, variable, file_suffix)
        result_dfs.append(result_df)
#
    # Save individual parquet with missing flags
    result_df_combined = pd.concat(result_dfs, axis=1)
    output_file_path = os.path.join(output_folder, f'{file_suffix}_processed.parquet')
#
 # Handle potential duplicate columns
    result_df_combined = result_df_combined.loc[:, ~result_df_combined.columns.duplicated()]
    result_df_combined.to_parquet(output_file_path, index=False)
#    
    return result_df_combined

# Function to concatenate processed parquet files
def concatenate_parquets(input_folder, output_folder):
    all_files = [f for f in os.listdir(input_folder) if f.endswith('_processed.parquet')]
    dfs = []
#
    for file in all_files:
        df = pd.read_parquet(os.path.join(input_folder, file))
        dfs.append(df)
#    
# Concatenate DataFrames, handling potential duplicate columns
    result_df = pd.concat(dfs, axis=1, ignore_index=False)

# remove if ncid = NaN
    result_df = result_df.dropna(subset=['ncid'])

# Remove duplicate columns
    result_df = result_df.loc[:, ~result_df.columns.duplicated()]
#
    # Create flag for variable availability across all timepoints
    result_df['all_variables_available'] = result_df.filter(like='_missing').sum(axis=1) == 0
    # Save concatenated parquet
    output_file_path = os.path.join(output_folder, 'concatenated_result.parquet')
    result_df.to_parquet(output_file_path, index=False)
#
    return result_df

# Specify the folder containing the raw parquet files
input_folder = 'ncvr\\raw_data'
output_folder = 'ncvr\\availability_flags'

# List of variables of interest
variables_of_interest = ['first_name', 'middle_name', 'last_name', 'age', 'gender', 'race', 'ethnic', 'zip_code']


# Specify the pattern for your file names
file_pattern = 'ncvr\\raw_data\\ncvoter-*.csv.gz'

# Get a list of all file names that match the pattern
file_names = glob.glob(file_pattern)

# Loop through each parquet file in the folder
for file_name in file_names:
    process_csv(file_name, variables_of_interest, output_folder)

# Concatenate processed parquet files
concatenate_parquets(output_folder, output_folder)

