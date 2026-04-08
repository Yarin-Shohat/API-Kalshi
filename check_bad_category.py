from CONST import *
import json
import gzip

# Data file paths
markets_by_cartegory_PATH = MARKETS_DIR + "markets_by_category.json.gz"
markets_by_series_PATH = MARKETS_DIR + "markets_by_series.json.gz"
taxonomy_PATH = TAXONOMY_DIR + "taxonomy.json.gz"
series_by_category_PATH = SERIES_DIR + "series_by_category.json.gz"

# Read files and load data into memory
with gzip.open(taxonomy_PATH, 'rt', encoding='utf-8') as f:
    taxonomy = json.load(f)
with gzip.open(series_by_category_PATH, 'rt', encoding='utf-8') as f:
    series_by_category = json.load(f)


categories = list(taxonomy.keys())
print(f"Total Categories: {len(categories)}")
for category in categories:
    pass

print("Exploring markets by category and series...")