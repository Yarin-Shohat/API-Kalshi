from CONST import *
import json
import gzip
import pandas as pd
import os

from KalshiExtractionEngine import KalshiExtractionEngine


API_KEY = os.getenv("KALSHI_API_KEY")
API_PATH = os.getenv("KALSHI_API_KEY_PATH")

# Data file paths
# markets_PATH = MARKETS_DIR + "kalshi_markets.parquet"
# taxonomy_PATH = TAXONOMY_DIR + "taxonomy.parquet"
series_by_category_PATH = SERIES_DIR + "fixed_series_by_category.json"

engine = KalshiExtractionEngine(api_key_id=API_KEY, private_key_path=API_PATH, use_demo=False)

print("Reading data files...")
# Read files and load data into memory
# print("Loading markets...")
# df_markets = pd.read_parquet(markets_PATH)
# print("Finished loading markets.")
print("Loading series by category...")
with open(series_by_category_PATH, 'r') as f:
    series_by_category = json.load(f)
print("Finished loading series by category.")

series_without_prohibition = []

for category, series_list in series_by_category.items():
    for series in series_list:
        additional_prohibitions = series.get('additional_prohibitions', [])
        if not additional_prohibitions or len(additional_prohibitions) == 0:
            series_without_prohibition.append(series["title"])

print(f"Total series without additional prohibitions: {len(series_without_prohibition)}")
print("Sample of series without additional prohibitions:")
for title in series_without_prohibition:
    print(title)

# Save series_without_prohibition to a text file
with open("series_without_prohibition.txt", "w") as f:
    for title in series_without_prohibition:
        f.write(title + "\n")