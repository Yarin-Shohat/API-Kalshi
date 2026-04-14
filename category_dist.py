from CONST import *
import json
import gzip
import pandas as pd

# Data file paths
markets_PATH = MARKETS_DIR + "kalshi_markets.parquet"
# taxonomy_PATH = TAXONOMY_DIR + "taxonomy.parquet"
series_by_category_PATH = SERIES_DIR + "fixed_series_by_category.json"

print("Reading data files...")
# Read files and load data into memory
print("Loading markets...")
df_markets = pd.read_parquet(markets_PATH)
print("Finished loading markets.")

print("Loading series by category...")
with open(series_by_category_PATH, 'r', encoding='utf-8') as f:
    series_by_category = json.load(f)
print("Finished loading series by category.")

categories = df_markets['category'].unique()

# Analyze category distribution df_markets
category_counts = df_markets['category'].value_counts().reset_index()
category_counts.columns = ['category', 'count']



# Analyze category distribution df_series_by_category


# Prepare category distribution by series
series_dist = []
for key in series_by_category.keys():
    series_dist.append({'category': key, 'num_series': len(series_by_category[key])})
series_dist_df = pd.DataFrame(series_dist)

# Merge the two DataFrames on 'category'
summary_df = pd.merge(category_counts, series_dist_df, on='category', how='outer')
summary_df = summary_df.rename(columns={'count': 'market count'})
summary_df = summary_df[['category', 'market count', 'num_series']]

# Save to one CSV
summary_csv_path = 'category_dist_summary.csv'
summary_df.to_csv(summary_csv_path, index=False)
print(f"\n--- Category Distribution Summary (saved to {summary_csv_path}) ---")
print(summary_df)