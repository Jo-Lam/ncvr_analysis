import pandas as pd
import os
import glob

os.chdir(r"ncvr/timepointPairs")

# Define the folder where the Parquet files are saved
folder_path = r'ncvr/timepointPairs'

# Specify the pattern for your file names
file_pattern = 'timepoint_pair_*.parquet'

# Get a list of all file names that match the pattern
parquet_files = glob.glob(file_pattern)

# Loop through each Parquet file
for parquet_file in parquet_files:
    # Initialize a list to store the extracted timepoints
    timepoints_list = []
    # Read the Parquet file into a DataFrame
    timepoint_pair_df = pd.read_parquet(parquet_file)
    # Extract the years from the file name
    timepoint_start, timepoint_end = parquet_file.replace('timepoint_pair_', '').replace('.parquet', '').split('_')
    # Append the years to the list
    timepoints_list.append((timepoint_start, timepoint_end))
    # Column to check
    columns_to_check = [f'first_name_{timepoint_start}', f'first_name_{timepoint_end}', f'last_name_{timepoint_start}', f'last_name_{timepoint_end}']
    # Drop rows where all specified columns are NaN or empty # worked to some extent
    timepoint_pair_df = timepoint_pair_df.dropna(subset=columns_to_check, how='all')
    # drop if name == None, work
    timepoint_pair_df = timepoint_pair_df[~timepoint_pair_df[f'first_name_{timepoint_start}'].isnull()]
    timepoint_pair_df = timepoint_pair_df[~timepoint_pair_df[f'first_name_{timepoint_end}'].isnull()]
    timepoint_pair_df = timepoint_pair_df[~timepoint_pair_df[f'last_name_{timepoint_start}'].isnull()]
    timepoint_pair_df = timepoint_pair_df[~timepoint_pair_df[f'last_name_{timepoint_end}'].isnull()]
    #timepoint_pair_df = timepoint_pair_df.drop(columns = [col for col in timepoint_pair_df.columns if col.isnull()])
    # for each pair, choose later. If later = missing, use former.
    var_list = ['age','gender','race','ethnic','zip_code']
    for var in var_list:
        var_2011 = f'{var}_{timepoint_start}'
        var_2012 = f'{var}_{timepoint_end}'
        new_var = f'{var}_clean'
        timepoint_pair_df[new_var] = timepoint_pair_df[var_2012]
        timepoint_pair_df[new_var].fillna(timepoint_pair_df[var_2011], inplace = True)
    # drop if age < 18 or age > 115
    timepoint_pair_df = timepoint_pair_df[(timepoint_pair_df['age_clean']>= 18) & (timepoint_pair_df['age_clean'] <= 115)]
    # regroup race and ethnicity
    timepoint_pair_df['ethnic_clean'] = timepoint_pair_df['ethnic_clean'].astype(str)
    timepoint_pair_df['race_clean'] = timepoint_pair_df['race_clean'].astype(str)
    timepoint_pair_df['composite_race_ethnic'] = timepoint_pair_df['race_clean'] + '_' + timepoint_pair_df['ethnic_clean']
    # timepoint_pair_df['composite_race_ethnic'] = timepoint_pair_df['composite_race_ethnic'].astype(str)
    def combine_race_ethnic(row):
        if row['composite_race_ethnic'].startswith('w') or row['composite_race_ethnic'].startswith('o'):
            return row['composite_race_ethnic']
        else:
            return row['race_clean']
    timepoint_pair_df['race_ethnic_clean'] = timepoint_pair_df.apply(combine_race_ethnic, axis=1)
    # keeping only necessary columns
    columns_to_keep = ['ncid', f'first_name_{timepoint_start}', f'first_name_{timepoint_end}', f'last_name_{timepoint_start}', f'last_name_{timepoint_end}', 'age_clean' , 'gender_clean', 'race_clean', 'ethnic_clean', 'race_ethnic_clean', 'composite_race_ethnic','zip_code_clean']
    timepoint_pair_df = timepoint_pair_df[columns_to_keep]
    # save as processed data
    timepoint_pair_df.to_parquet(f"processed_{parquet_file}")
