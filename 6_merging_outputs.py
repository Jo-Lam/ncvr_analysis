import os

import pandas as pd
import pyarrow.parquet as pq

# Specify the folder containing Parquet files
folder_path = r'C:\Users\uctvjla\OneDrive - University College London\Documents\GitHub\IPDLN-24\ncvr\output\first_name'

# Get a list of all files in the folder
all_files = os.listdir(folder_path)

# Filter files that contain "first_name" in their names
firstname_files = [file for file in all_files if "first_name" in file]

# Read Parquet files and store DataFrames in a list
dfs = []
for file in firstname_files:
    file_path = os.path.join(folder_path, file)
    table = pq.read_table(file_path)
    df = table.to_pandas()
    dfs.append(df)

# Merge the DataFrames
firstname_df = pd.concat(dfs, ignore_index=True)

# clean
similarity_score = firstname_df.filter(like='first_name_similarity_score') # no need, just keep first name similarity score!
similarity_cat = firstname_df.filter(like='first_name_similarity_flag')
firstname_df['first_name_similarity_category'] = similarity_cat.mean(axis=1)

firstnames = firstname_df.filter(like = 'first_name')
timepoints = ['2011','2012','2013','2014','2015','2016','2017','2018','2019','2020','2021']
for i in range(len(timepoints) - 1):
    t1_col = f'first_name_{timepoints[i]}'
    t2_col = f'first_name_{timepoints[i + 1]}'
#
    if t1_col in firstnames.columns and t2_col in firstnames.columns:
        mask = ~firstnames[t1_col].isnull() & ~firstnames[t2_col].isnull()
        firstname_df.loc[mask, 'first_name_t1'] = firstnames[t1_col]
        firstname_df.loc[mask, 'first_name_t2'] = firstnames[t2_col]
        
columns_to_keep_firstname = ['ncid', 'first_name_t1', 'first_name_t2', 'age_clean', 'gender_clean', 'race_clean', 'ethnic_clean', 'race_ethnic_clean', 'composite_race_ethnic', 'zip_code_clean','first_name_similarity_score','first_name_similarity_category']
firstname_df = firstname_df[columns_to_keep_firstname]

# group pacific islanders - use composite
firstname_df['race_aggre'] = firstname_df['composite_race_ethnic']
firstname_df.loc[firstname_df['race_aggre'] == 'p', 'race_aggre'] = 'o'

firstname_df.to_csv(r'output\first_name_similarity.csv')



# Specify the folder containing Parquet files
folder_path = r'C:\Users\uctvjla\OneDrive - University College London\Documents\GitHub\IPDLN-24\ncvr\output\last_name'

# Get a list of all files in the folder
all_files = os.listdir(folder_path)

# Filter files that contain "first_name" in their names
lastname_files = [file for file in all_files if "last_name" in file]

# Read Parquet files and store DataFrames in a list
dfs = []
for file in lastname_files:
    file_path = os.path.join(folder_path, file)
    table = pq.read_table(file_path)
    df = table.to_pandas()
    dfs.append(df)

# Merge the DataFrames
lastname_df = pd.concat(dfs, ignore_index=True)

"""with pd.option_context('display.max_columns', None):
    print(lastname_df.head())
"""

# clean dfs
similarity_score = lastname_df.filter(like='last_name_similarity_score') # no need, just keep similarity score!
similarity_cat = lastname_df.filter(like='last_name_similarity_flag')
lastname_df['last_name_similarity_category'] = similarity_cat.mean(axis=1)

lastnames = lastname_df.filter(like = 'last_name')
timepoints = ['2011','2012','2013','2014','2015','2016','2017','2018','2019','2020','2021']
for i in range(len(timepoints) - 1):
    t1_col = f'last_name_{timepoints[i]}'
    t2_col = f'last_name_{timepoints[i + 1]}'
#
    if t1_col in lastnames.columns and t2_col in lastnames.columns:
        mask = ~lastnames[t1_col].isnull() & ~lastnames[t2_col].isnull()
        lastname_df.loc[mask, 'last_name_t1'] = lastnames[t1_col]
        lastname_df.loc[mask, 'last_name_t2'] = lastnames[t2_col]
        
columns_to_keep_lastname = ['ncid', 'last_name_t1', 'last_name_t2', 'age_clean', 'gender_clean', 'race_clean', 'ethnic_clean', 'race_ethnic_clean', 'composite_race_ethnic', 'zip_code_clean','last_name_similarity_score','last_name_similarity_category']
lastname_df = lastname_df[columns_to_keep_lastname]

lastname_df['race_aggre'] = lastname_df['composite_race_ethnic']
lastname_df.loc[lastname_df['race_aggre'] == 'p', 'race_aggre'] = 'o'

# save data.
lastname_df.to_csv(r'output\last_name_similarity.csv')