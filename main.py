
import os
from KalshiExtractionEngine import KalshiExtractionEngine
import json

API_KEY = os.getenv("KALSHI_API_KEY")
API_PATH = os.getenv("KALSHI_API_KEY_PATH")

engine = KalshiExtractionEngine(api_key_id=API_KEY, private_key_path=API_PATH, use_demo=False)

################ GET CATEGORIES TAXONOMY AND MAP TO MONITORED TAGS ################
# print("--- Extract and Map All Platform Categories ---")
# taxonomy = engine.fetch_categories_taxonomy()
# for cat_name, tags_array in list(taxonomy.items()):
#     print(f"Parent Category: {cat_name} | Monitored Tags: {', '.join(tags_array if tags_array is not None else [])}")
# print(f"Total {len(list(taxonomy.items()))} categories")

# # Save taxonomy
# with open("taxonomy.json", "w") as f:
#     json.dump(taxonomy, f, indent=4)

################ EXTRACT HISTORICAL DATA WITH CHECKPOINTING ################
# print("Starting historical data extraction...")
# engine.extract_markets_with_checkpointing()
# print("Extraction complete!")

################# FETCH AND GROUP SERIES DATA BY CATEGORY ################
# print("Fetching and grouping series by category...")
# series_by_category = engine.fetch_and_group_series_by_category()
# with open("series_by_category.json", "w") as f:
#     json.dump(series_by_category, f, indent=4)
# print("Series grouped by category saved to series_by_category.json")

####################### FETCH AND GROUP MARKETS BY CATEGORY ################
# print("Fetching and grouping markets by category...")
# markets_by_category = engine.fetch_and_group_markets_by_category()
# with open("markets_by_category.json", "w") as f:
#     json.dump(markets_by_category, f, indent=4)
# print("Markets grouped by category saved to markets_by_category.json")

####### EXTRACT MARKETS FOR A SINGLE SERIES (EXAMPLE) ################
# markets_by_series = engine.fetch_markets_by_series("KXHIGHDEN")
# with open("markets_by_series_KXHIGHDEN.json", "w") as f:
#     json.dump(markets_by_series, f, indent=4)
# print("Markets for series KXHIGHDEN saved to markets_by_series_KXHIGHDEN.json")

####### EXTRACT MARKETS FOR ALL SERIES AND GROUP BY CATEGORY ################
# Read series by category from previously saved file
with open("series_by_category.json", "r") as f:
    series_by_category = json.load(f)

data_by_series = {}
data_by_category = {}

# Loop through each category and its series, fetch markets for each series, and organize by category
for category, series_list in series_by_category.items():
    data_by_category[category] = []
    for series in series_list:
        series_ticker = series["ticker"]
        markets_by_series = engine.fetch_markets_by_series(series_ticker)
        data_by_series[series_ticker] = markets_by_series
        data_by_category[category].append(markets_by_series)

with open("markets_by_series.json", "w") as f:
    json.dump(data_by_series, f, indent=4)
print("Markets for all series saved to markets_by_series.json")

with open("markets_by_category.json", "w") as f:
    json.dump(data_by_category, f, indent=4)
print("Markets for all categories saved to markets_by_category.json")