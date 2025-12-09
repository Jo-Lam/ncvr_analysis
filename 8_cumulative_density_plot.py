# cumulative_density_plot
# plot JW 0.7, 0.8, 0.9
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats
from scipy.stats import entropy
import seaborn as sns

df_fn = pd.read_csv(r"C:\Users\uctvjla\OneDrive - University College London\Documents\GitHub\IPDLN-24\ncvr\output\name_similarity\20250422_updated_fn_similarity.csv")

df_subset = df_fn[df_fn['first_name_similarity_score']>= 0.7]

plt.figure(figsize=(16, 8))
sns.ecdfplot(data = df_fn, x = 'first_name_similarity_score', hue = 'ethnic_group', legend = True, palette= "muted")
plt.xlabel('First Name Similarity Score')
plt.ylabel('Cumulative Density')
plt.xlim(0.7,1)
plt.xticks(fontsize = 14)
plt.yticks(fontsize = 14)
#plt.title("Cumulative Density of First Name Mismatches, by Ethno-Racial Categories")
#plt.savefig("fig3a.svg", format="svg",dpi=300)
plt.show()


df_ln = pd.read_csv(r"C:\Users\uctvjla\OneDrive - University College London\Documents\GitHub\IPDLN-24\ncvr\output\name_similarity\20250422_updated_ln_similarity.csv")
df_ln_nozero = df_ln[df_ln['last_name_similarity_score'] > 0]

plt.figure(figsize=(16, 8))
sns.ecdfplot(data = df_ln, x = 'last_name_similarity_score', hue = 'race_aggre', legend = True)
plt.xlabel('Last Name Similarity Score')
plt.ylabel('Cumulative Density')
plt.xlim(0.7,1)
plt.xticks(fontsize = 14)
plt.yticks(fontsize = 14)
plt.title("Cumulative Density of Last Name Mismatches, by Race")
plt.savefig("fig3b.svg", format="svg",dpi=300)
plt.show()


plt.figure(figsize=(16, 8))
sns.ecdfplot(data = df_ln_nozero, x = 'last_name_similarity_score', hue = 'ethnic_group', legend = True, palette = "muted")
plt.xlim(0.5,1)

plt.xticks(fontsize = 14)
plt.yticks(fontsize = 14)
plt.xlabel('Last Name Similarity Score')
plt.ylabel('Cumulative Density')

plt.show()
#plt.title("Cumulative Density of Last Name Mismatches, by Race")

#plt.savefig("fig3b.svg", format="svg",dpi=300)

