from CONST import *
import json
import gzip

series_by_category_PATH = SERIES_DIR + "series_by_category.json.gz"

print("Loading series_by_category...")
with gzip.open(series_by_category_PATH, 'rt', encoding='utf-8') as f:
    series_by_category = json.load(f)

new_series_by_category = {}
for category, series_list in series_by_category.items():
    for series in series_list:
        series_category = series.get("category", "Unknown")
        new_series_by_category[series_category] = new_series_by_category.get(series_category, [])
        new_series_by_category[series_category].append(series)

with open("fixed_series_by_category.json", "w") as f:
    json.dump(new_series_by_category, f, indent=4)

print("Finished fixing series_by_category.json.gz with correct category classification!")