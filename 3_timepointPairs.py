import pandas as pd
import os

merged_df = pd.read_parquet("ncvr//raw_data//all_data_raw.parquet") # assuming we use all. Use analysis_sample.parquet for data with fn & ln for all 13 years

# Define the timepoints
timepoints = ['2011', '2012', '2013','2014','2015','2016','2017','2018','2019','2020','2021','2022','2023']

os.chdir("ncvr//timepointPairs")

# Loop through each pair of consecutive timepoints
for i in range(len(timepoints) - 1):
    start_timepoint = timepoints[i]
    end_timepoint = timepoints[i + 1]

    # Create a DataFrame for the current and next timepoint pair
    timepoint_pair_df = merged_df.filter(like='ncid').copy()
    timepoint_pair_df = pd.concat([
        timepoint_pair_df,
        merged_df.filter(like=start_timepoint),
        merged_df.filter(like=end_timepoint)
    ], axis=1)

    # Save the DataFrame to a Parquet file
    file_name = f'timepoint_pair_{start_timepoint}_{end_timepoint}.parquet'
    timepoint_pair_df.to_parquet(file_name, index=False)

    print(f"Saved {file_name}")

