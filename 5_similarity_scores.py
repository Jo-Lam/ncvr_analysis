import pandas as pd
from jellyfish import jaro_winkler_similarity

# define timepoints
timepoints = ['2011','2012','2013','2014','2015','2016','2017','2018','2019','2020','2021'] # up to 2021 ['2022','2023']

# Define the similarity threshold, can be adjusted as required.
similarity_threshold = 0.7

def JW_calculate_similarity(row, current_col, next_col):
    similarity_score = jaro_winkler_similarity(str(row[current_col]), str(row[next_col]))
    return similarity_score

# Define Function to calculate similarity score and create flag
def calculate_similarity_and_flag(row, current_col, next_col):
    similarity_score = jaro_winkler_similarity(str(row[current_col]), str(row[next_col]))
#
    if similarity_score > similarity_threshold and similarity_score != 1:
        return 1
    elif similarity_score < similarity_threshold:
        return 2
    elif similarity_score == 1:
        return 3
    else:
        return 0  # Default value if none of the conditions match

# Define labels for the flag
jw_flag_labels = {0: 'unknown', 1: 'JW > 0.7 < 1', 2: 'JW < 0.7', 3: 'JW == 1'}
race_labels ={'a': 'Asian',
    'b': 'Black or African American',
    'i': 'American Indian or Alaska Native',
    'm': 'Two or more races',
    'o': 'Other',
    'p': 'Native Hawaiian or Pacific Islander',
    'u': 'Undesignated',
    'w': 'White',
}

# use this - April 2025 
ethnic_race_labels = {'a': 'Asian',
    'b': 'Black or African American',
    'i': 'American Indian or Alaska Native',
    'm': 'Two or more races',
    'o_nl': 'Other, Non-Hispanic',
    'o_un': 'Other, Unknown',
    'o_hl': 'Other, Hispanic',
    'p': 'Native Hawaiian or Pacific Islander',
    'u': 'Undesignated',
    'w_nl': 'White, Non-Hispanic',
    'w_un': 'White, Unknown',
    'w_hl': 'White, Hispanic',
}

gender_labels = {
    'f':"Female",
    'm':"Male",
    'u':'Undesignated'
}

# Initialize an empty DataFrame to store crosstab results
crosstab_results = pd.DataFrame(columns=['Attribute', 'Timepoint Pair', 'Type', 'Crosstab'])

# Loop through each pair of consecutive timepoints
for i in range(len(timepoints) - 1):
    t1 = timepoints[i]
    t2 = timepoints[i + 1]
    # Construct the file name for the current timepoint pair
    file_name = fr"timepointPairs\processed_timepoint_pair_{t1}_{t2}.parquet"
    try:
        # Read the Parquet file into a DataFrame
        df = pd.read_parquet(file_name)
        # Perform other tasks with the DataFrame (replace this with your actual tasks)
        # For example, you can print the first few rows of the DataFrame
        print(f"Processing timepoint pair {t1}_{t2}:")
        print(df.head())
        for attr in ['first_name', 'last_name']:
            df[f'{attr}_similarity_{t1}_{t2}'] = 0.0
            current_col = f'{attr}_{t1}'
            next_col = f'{attr}_{t2}'
            similarity_flag = f'{attr}_similarity_flag_{t1}_{t2}'
            df[f'{attr}_similarity_score'] = df.apply(lambda row: JW_calculate_similarity(row, current_col, next_col), axis = 1)
            df[similarity_flag] = df.apply(
            lambda row: calculate_similarity_and_flag(row, current_col, next_col), axis=1
            )
            
            df['flag_label'] = df[f'{attr}_similarity_flag_{t1}_{t2}'].map(jw_flag_labels)
            df['gender_labels'] = df['gender_clean'].map(gender_labels)
            df['race_labels'] = df['race_clean'].map(race_labels)
            df['race_ethnic_labels'] = df['race_ethnic_clean'].map(ethnic_race_labels)
                        # comparison vars
            age_bins = pd.cut(df['age_clean'], bins=[18, 30, 50, 70, 100], labels=['18-29', '30-49', '50-69', '70+'])
            
        
            # keep only if non identical
            df_attr = df[df[f'{attr}_similarity_score'] != 1.0]
            
            # save df to parquets
            file_name2 = fr"output\{attr}\similarity_{attr}_{t1}_{t2}.parquet"
            df_attr.to_parquet(file_name2, index=False)
            
            
            # tabulate output
            cross_tab_gender = pd.crosstab(df[f'gender_label_{t2}'], df['flag_label'], margins=True, margins_name='Total', normalize='index')
            cross_tab_race = pd.crosstab(df[f'race_label_{t2}'],  df['flag_label'], margins=True, margins_name='Total', normalize='index')
            cross_tab_age = pd.crosstab(age_bins,  df['flag_label'], margins=True, margins_name='Total', normalize='index')
            # Append the crosstab results to the summary DataFrame
            crosstab_results = pd.concat([
                crosstab_results,
                pd.DataFrame({
                    'Attribute': [attr],
                    'Timepoint Pair': [f"{t1}_{t2}"],
                    'Type': ['Gender'],
                    'Crosstab': [cross_tab_gender]
                }),
                pd.DataFrame({
                    'Attribute': [attr],
                    'Timepoint Pair': [f"{t1}_{t2}"],
                    'Type': ['Race'],
                    'Crosstab': [cross_tab_race]
                }),
                pd.DataFrame({
                    'Attribute': [attr],
                    'Timepoint Pair': [f"{t1}_{t2}"],
                    'Type': ['Age'],
                    'Crosstab': [cross_tab_age]
                })
            ])
    except FileNotFoundError:
        # Handle the case when the file is not found
        print(f"File {file_name} not found.")
    except Exception as e:
        # Handle other potential exceptions
        print(f"Error processing {file_name}: {e}")



# Save crosstab results to CSV
crosstab_results.to_csv('crosstab_results.csv', index=False)