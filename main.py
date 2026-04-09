
import os
from KalshiExtractionEngine import KalshiExtractionEngine
import json
from CONST import *
import gzip
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


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
with open(SERIES_DIR + "fixed_series_by_category.json", "r") as f:
    series_by_category = json.load(f)

# data_by_series = {}
# data_by_category = {}

# # Loop through each category and its series, fetch markets for each series, and organize by category
# for category, series_list in series_by_category.items():
#     data_by_category[category] = []
#     for series in series_list:
#         series_ticker = series["ticker"]
#         markets_by_series = engine.fetch_markets_by_series(series_ticker)
#         data_by_series[series_ticker] = markets_by_series
#         data_by_category[category].append(markets_by_series)


# # Save data in separate jsonl files (newline-delimited JSON for efficient streaming and reading)
# # Save series data as jsonl with gzip compression
# with gzip.open("data_by_series.jsonl.gz", "wt", encoding="utf-8") as f:
#     for series_ticker, markets in data_by_series.items():
#         json.dump({"ticker": series_ticker, "markets": markets}, f)
#         f.write("\n")

# # Save category data as jsonl with gzip compression
# with gzip.open("data_by_category.jsonl.gz", "wt", encoding="utf-8") as f:
#     for category, markets_list in data_by_category.items():
#         json.dump({"category": category, "markets": markets_list}, f)
#         f.write("\n")

# print("Data saved to data_by_series.jsonl.gz and data_by_category.jsonl.gz")

print("Starting extraction of markets for all series and grouping by category...")
parquet_path = "kalshi_markets.parquet"
writer = None
first_schema = None
first_columns = None

for category, series_list in series_by_category.items():
    print(f"\n\nProcessing category: {category} with {len(series_list)} series")
    print(f"Category number {list(series_by_category.keys()).index(category) + 1} out of {len(series_by_category)}")
    for series in series_list:
        series_ticker = series["ticker"]
        
        # Calling your specific function
        markets_list = engine.fetch_markets_by_series(series_ticker)
        
        if not markets_list:
            continue
            
        # Convert the list of dicts directly to DataFrame
        df_chunk = pd.DataFrame(markets_list)
        
        # Add metadata columns so you don't lose the context
        df_chunk['category'] = category
        df_chunk['parent_series'] = series_ticker
        
        # Align columns to match the first schema
        if writer is None:
            # First chunk: save schema and columns
            table = pa.Table.from_pandas(df_chunk)
            writer = pq.ParquetWriter(parquet_path, table.schema, compression='snappy')
            first_schema = table.schema
            first_columns = df_chunk.columns.tolist()
            writer.write_table(table)
        else:
            # For subsequent chunks, align columns
            # Add missing columns
            for col in first_columns:
                if col not in df_chunk.columns:
                    df_chunk[col] = None
            # Drop extra columns
            df_chunk = df_chunk[first_columns]
            # Convert to Table with the original schema
            table = pa.Table.from_pandas(df_chunk, schema=first_schema, preserve_index=False)
            writer.write_table(table)
    print(f"Finished processing category: {category}")
if writer:
    writer.close()