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

# Convert volume columns to numeric, coercing errors to NaN
df_markets['volume_fp'] = pd.to_numeric(df_markets['volume_fp'], errors='coerce')
df_markets['volume_24h_fp'] = pd.to_numeric(df_markets['volume_24h_fp'], errors='coerce')


print("Loading series by category...")
with open(series_by_category_PATH, 'r', encoding='utf-8') as f:
    series_by_category = json.load(f)
print("Finished loading series by category.")

categories = df_markets['category'].unique()

# Analyze category distribution df_markets
category_counts = df_markets['category'].value_counts().reset_index()
category_counts.columns = ['category', 'count']

# Prepare category distribution by series
series_dist = []
for key in series_by_category.keys():
    series_dist.append({'category': key, 'num_series': len(series_by_category[key])})
series_dist_df = pd.DataFrame(series_dist)

# Merge the two DataFrames on 'category'
summary_df = pd.merge(category_counts, series_dist_df, on='category', how='outer')
summary_df = summary_df.rename(columns={'count': 'market count'})
summary_df = summary_df[['category', 'market count', 'num_series']]

# Add stat about volume_fp and volume_24h_fp for every category
volume_stats = df_markets.groupby('category').agg(
    total_volume_fp=pd.NamedAgg(column='volume_fp', aggfunc='sum'),
    mean_volume_fp=pd.NamedAgg(column='volume_fp', aggfunc='mean'),
    std_volume_fp=pd.NamedAgg(column='volume_fp', aggfunc='std'),
    min_volume_fp=pd.NamedAgg(column='volume_fp', aggfunc='min'),
    max_volume_fp=pd.NamedAgg(column='volume_fp', aggfunc='max'),
    total_volume_24h_fp=pd.NamedAgg(column='volume_24h_fp', aggfunc='sum'),
    mean_volume_24h_fp=pd.NamedAgg(column='volume_24h_fp', aggfunc='mean'),
    std_volume_24h_fp=pd.NamedAgg(column='volume_24h_fp', aggfunc='std'),
    min_volume_24h_fp=pd.NamedAgg(column='volume_24h_fp', aggfunc='min'),
    max_volume_24h_fp=pd.NamedAgg(column='volume_24h_fp', aggfunc='max'),
).reset_index()

# Merge volume stats with summary DataFrame
summary_df = pd.merge(summary_df, volume_stats, on='category', how='outer')

# Save to one CSV
summary_csv_path = 'category_dist_summary.csv'
summary_df.to_csv(summary_csv_path, index=False)
print(f"\n--- Category Distribution Summary (saved to {summary_csv_path}) ---")
print(summary_df)