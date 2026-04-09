from CONST import *
import json
import gzip

# Data file paths
markets_by_cartegory_PATH = MARKETS_DIR + "markets_by_category.json.gz"
markets_by_series_PATH = MARKETS_DIR + "markets_by_series.json.gz"
taxonomy_PATH = TAXONOMY_DIR + "taxonomy.json.gz"
series_by_category_PATH = SERIES_DIR + "fixed_series_by_category.json"

print("Reading data files...")
# Read files and load data into memory
print("Loading markets_by_cartegory...")
with gzip.open(markets_by_cartegory_PATH, 'rt', encoding='utf-8') as f:
    markets_by_cartegory = json.load(f)
# print("Loading markets_by_series...")
# with gzip.open(markets_by_series_PATH, 'rt', encoding='utf-8') as f:
#     markets_by_series = json.load(f)
# print("Loading taxonomy...")
# with gzip.open(taxonomy_PATH, 'rt', encoding='utf-8') as f:
#     taxonomy = json.load(f)
# print("Loading series_by_category...")
# with open(series_by_category_PATH, 'r', encoding='utf-8') as f:
#     series_by_category = json.load(f)

# Show some insights about the data
# categories = list(taxonomy.keys())
# print(f"Total Categories: {len(categories)}")
# for category in categories:
#     num_series = len(series_by_category.get(category, []))
#     num_markets = len(markets_by_cartegory.get(category, []))
#     print(f"Category: {category} | Series: {num_series} | Markets: {num_markets}")

# # Check bad categories in series_by_category
# print("\n\nChecking for bad categories in series_by_category...")

# for category, series_list in series_by_category.items():
#     if category not in taxonomy:
#         print(f"[ERROR] Bad category found in series_by_category: {category}: {len(series_list)} series of category {category} not found in taxonomy")

# for category in taxonomy.keys():
#     if category not in series_by_category:
#         print(f"[WARNING] Category {category} found in taxonomy but not in series_by_category")

# print("Check bad category classification in series_by_category")
# for category, series_list in series_by_category.items():
#     for series in series_list:
#         series_category = series.get("category")
#         if series_category != category:
#             print(f"[ERROR] Bad category classification for series {series['ticker']} in series_by_category: expected {category} but found {series_category}, series title: {series['title']}")

