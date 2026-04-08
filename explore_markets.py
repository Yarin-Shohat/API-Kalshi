from CONST import *
import json


markets_by_cartegory_PATH = MARKETS_DIR + "markets_by_category.json"
markets_by_series_PATH = MARKETS_DIR + "markets_by_series.json"
taxonomy_PATH = TAXONOMY_DIR + "taxonomy.json"
series_by_category_PATH = SERIES_DIR + "series_by_category.json"

with open(taxonomy_PATH, "r") as f:
    taxonomy = json.load(f)

with open(series_by_category_PATH, "r") as f:
    series_by_category = json.load(f)

categories = list(taxonomy.keys())
print(f"Total Categories: {len(categories)}")
for category in categories:
    pass