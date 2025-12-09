# This file reads the combined first name/ last name files and -
# plot 1) density graphs by race
# plot 2) cumulative density plots by race.
#
# similar graphs by other characteristics is also possible, for example, by ethnicity or race

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

df_fn = pd.read_csv(r"C:\Users\uctvjla\OneDrive - University College London\Documents\GitHub\IPDLN-24\ncvr\output\name_similarity\first_name_similarity.csv")

def map_to_group(code):
    race, eth = code.split('_')
    is_hispanic = (eth == 'hl')

    if race in ['w', 'b'] and is_hispanic:
        return 'Hispanic (White or Black)'
    elif race == 'w':
        return 'Non-Hispanic White'
    elif race == 'b':
        return 'Non-Hispanic Black'
    elif race == 'a':
        return 'Asian'
    elif race in ['i', 'p']:
        return 'Indigenous or Pacific Islander'
    elif race == 'o':
        return 'Other'
    elif race == 'm':
        return 'Mixed'
    elif race == 'u':
        return 'Unknown'
    else:
        return 'Unknown'

# Apply mapping
df_fn['ethnic_group'] = df_fn['composite_race_ethnic'].apply(map_to_group)

sns.histplot(df_fn, x='first_name_similarity_score', bins = 20, hue ='ethnic_group', multiple = 'stack', edgecolor='black')
plt.xlabel("Jaro-Winkler Similarity Score")
plt.ylabel("Frequency")
plt.show()

sns.set_theme(style="whitegrid")
p = sns.kdeplot(data=df_fn, x="first_name_similarity_score", hue="ethnic_group", fill=True, common_norm=False, alpha=0.6, bw_adjust = 1,  legend=True ,palette="viridis")
# plt.legend(loc = 'upper left')
# plt.title('Density Graph - First Name Similarity Score by Race')
# control x limit
plt.xlim(0, 1)
plt.xticks([0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0])
# show the graph
plt.show()

## Last Name
df_ln = pd.read_csv(r"C:\Users\uctvjla\OneDrive - University College London\Documents\GitHub\IPDLN-24\ncvr\output\name_similarity\last_name_similarity.csv")
df_ln['ethnic_group'] = df_ln['composite_race_ethnic'].apply(map_to_group)

# Frequency Plot
sns.histplot(df_ln, x='last_name_similarity_score', bins = 20, hue ='ethnic_group', multiple = 'stack', edgecolor='black')
plt.xlabel("Jaro-Winkler Similarity Score")
plt.ylabel("Frequency")
plt.show()

sns.set_theme(style="whitegrid")
p = sns.kdeplot(data=df_ln, x="last_name_similarity_score", hue="ethnic_group", fill=True, common_norm=False, alpha=0.6, bw_adjust = 1,  legend=True ,palette="viridis")
#plt.legend(loc = 'upper left')
#plt.title('Density Graph - Last Name Similarity Score (excluding 0) by Race')
# control x limit
plt.xlim(0, 1)
plt.xticks([0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0])
# show the graph
plt.show()

sns.set_theme(style="whitegrid")
df_ln_nozero = df_ln[df_ln['last_name_similarity_score'] > 0]

p = sns.kdeplot(data=df_ln_nozero, x="last_name_similarity_score", hue="ethnic_group", fill=True, common_norm=False, alpha=0.6, bw_adjust = 1,  legend=True ,palette="viridis")
#plt.legend(loc = 'upper left')
#plt.title('Density Graph - Last Name Similarity Score (excluding 0) by Race')
# control x limit
plt.xlim(0, 1)
plt.xticks([0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0])
# show the graph
plt.show()


# export
df_fn = df_fn[['ncid', 'first_name_t1', 'first_name_t2', 'age_clean', 'ethnic_group', 'zip_code_clean',
       'first_name_similarity_score', 'first_name_similarity_category']]

df_ln = df_ln[['ncid', 'last_name_t1', 'last_name_t2', 'age_clean',
       'gender_clean', 'zip_code_clean', 'last_name_similarity_score',
       'last_name_similarity_category', 'ethnic_group']]

df_ln.to_csv("20250422_updated_ln_similarity.csv")
df_fn.to_csv("20250422_updated_fn_similarity.csv")



"""
## free graphs
df_ln['age_bins'] = pd.cut(df_ln['age_clean'], bins=[18, 30, 50, 70, 100], labels=['18-29', '30-49', '50-69', '70+'])


import numpy as np

lastname_df_07 = lastname_df[lastname_df['last_name_similarity_score'] < 0.7]
lastname_df['race_aggre'].value_counts()
lastname_df_07['race_aggre'].value_counts()

freq0 = [339937, 99345, 20144, 16632, 3601, 3354, 2794]
freq1 = [303517, 78615, 12405, 10899, 3053, 2397, 1751]

prob0 = freq0 /np.sum(freq0)
prob1 = freq1 /np.sum(freq1)

kld = entropy(prob0,prob1)

def kl_divergence(p, q):
    return np.sum(np.where(p != 0, p * np.log(p / q), 0))

kl_result = kl_divergence(prob0, prob1)



## testing: compare strings

def compare_strings(df, column1, column2):
    # Compute Jaro-Winkler similarity
    df['jaro_winkler_similarity'] = df.apply(lambda row: jellyfish.jaro_winkler(row[column1], row[column2]), axis=1)
    # Compute Levenshtein distance
    df['levenshtein_distance'] = df.apply(lambda row: jellyfish.levenshtein_distance(row[column1], row[column2]), axis=1)
    return df[['jaro_winkler_similarity', 'levenshtein_distance']]
"""