import datetime

from CONST import *
import json
import gzip
import pandas as pd
import os

from KalshiExtractionEngine import KalshiExtractionEngine


API_KEY = os.getenv("KALSHI_API_KEY")
API_PATH = os.getenv("KALSHI_API_KEY_PATH")

# Data file paths
markets_PATH = MARKETS_DIR + "kalshi_markets.parquet"
# taxonomy_PATH = TAXONOMY_DIR + "taxonomy.parquet"
series_by_category_PATH = SERIES_DIR + "fixed_series_by_category.json"

engine = KalshiExtractionEngine(api_key_id=API_KEY, private_key_path=API_PATH, use_demo=False)

print("Reading data files...")
# # Read files and load data into memory
# print("Loading markets...")
# df_markets = pd.read_parquet(markets_PATH)
# print("Finished loading markets.")

# Read the fixed_series_by_category.json file
print("Loading series by category...")
with open(series_by_category_PATH, 'r') as f:
    series_by_category = json.load(f)
print("Finished loading series by category.")

# # See first market
# print("\n--- First Market ---")
# print(df_markets.iloc[0])

# # Get all pssible values from 'market_type' column
# print("\n--- Unique Market Types ---")
# print(df_markets['market_type'].unique())

# # Save the 10 rows and save in csv
# print("\n--- Sample of 10 Markets ---")
# df_markets_sample = df_markets.head(10)
# print(df_markets_sample)
# df_markets_sample.to_csv("sample_markets.csv", index=False)

# # fetch_candlesticks_with_routing
# start_date = int(datetime.datetime(2026, 4, 1).timestamp())
# end_date = int(datetime.datetime(2026, 4, 10).timestamp())
# ticket = "KXHIGHDEN-26APR09-T76"

# historical_price_action = engine.fetch_candlesticks_with_routing(
#     ticker=ticket,
#     start_unix=start_date,
#     end_unix=end_date,
#     resolution_minutes=60 
# )

# print(f"Retrieved {len(historical_price_action)} hourly candles for {ticket}!")
# Print the first candle to see the structure
# print(historical_price_action[0])

# # fetch_market_trades
# trades = engine.fetch_market_trades(
#     ticker=ticket
# )
# print(f"Retrieved {len(trades)} trades for {ticket}!")

# fetch_event_forecast_history
# event_forecast_history = engine.fetch_event_forecast_history(
#     event_ticker=ticket
# )
# print(f"Retrieved {len(event_forecast_history)} forecast history entries for {ticket}!")

# selected_event_tickers = (
# 	df_markets.groupby("category", dropna=True)["event_ticker"]
# 	.first()
# 	.dropna()
# 	.unique()
# )
# df_markets_selected = df_markets[df_markets["event_ticker"].isin(selected_event_tickers)]

# # Save the selected markets to csv file
# output_csv_path = "selected_markets.csv"
# df_markets_selected.to_csv(output_csv_path, index=False)
# print(f"Saved selected markets to {output_csv_path}")

# closed_market_ticker = "KXAPPRANKFREE3-25JUL27"

# order_book = engine.analyze_orderbook_depth(
#     market_ticker=closed_market_ticker
# )
# print(f"Order book depth analysis for {closed_market_ticker}:")
# print(order_book)

# Load selected markets from CSV file
# selected_markets_csv_path = "selected_markets.csv"
# df_selected_markets = pd.read_csv(selected_markets_csv_path)
df_selected_markets = pd.read_parquet(markets_PATH)

# Add series title to selected markets
series_lookup = {}
for category, series_list in series_by_category.items():
    if not isinstance(series_list, list):
        continue
    for series in series_list:
        ticker = series.get("ticker")
        if ticker:
            series_lookup[(category, ticker)] = {
                "main_title": series.get("title"),
                "contract_terms_url": series.get("contract_terms_url"),
                "contract_url": series.get("contract_url"),
            }

def get_series_field(row, field):
    lookup = series_lookup.get((row["category"], row["parent_series"]))
    return lookup.get(field) if lookup else None

df_selected_markets["main_title"] = df_selected_markets.apply(lambda row: get_series_field(row, "main_title"), axis=1)
df_selected_markets["contract_terms_url"] = df_selected_markets.apply(lambda row: get_series_field(row, "contract_terms_url"), axis=1)
df_selected_markets["contract_url"] = df_selected_markets.apply(lambda row: get_series_field(row, "contract_url"), axis=1)

ordered_columns = [
    # 1. Identification & Display
    'ticker', 'event_ticker', 'parent_series', 'category', 'title',
    'main_title', 'yes_sub_title', 'no_sub_title',

    # 2. Time & Scheduling
    'created_time', 'open_time', 'close_time', 'expected_expiration_time',
    'expiration_time', 'latest_expiration_time', 'updated_time',

    # 3. Trading, Prices & Orderbook
    'last_price_dollars', 'yes_bid_dollars', 'yes_bid_size_fp',
    'yes_ask_dollars', 'yes_ask_size_fp', 'no_bid_dollars',
    'no_ask_dollars', 'previous_price_dollars', 'previous_yes_bid_dollars',
    'previous_yes_ask_dollars', 'volume_fp', 'volume_24h_fp',
    'open_interest_fp', 'liquidity_dollars', 'notional_value_dollars',

    # 4. Contract Structure
    'market_type', 'strike_type', 'floor_strike', 'cap_strike',
    'tick_size', 'fractional_trading_enabled', 'price_level_structure',
    'price_ranges', 'response_price_units',

    # 5. Rules & Legal Documents
    'rules_primary', 'rules_secondary', 'can_close_early',
    'early_close_condition', 'contract_terms_url', 'contract_url',

    # 6. Settlement & Results
    'status', 'result', 'expiration_value', 'settlement_value_dollars',
    'settlement_ts', 'settlement_timer_seconds'
]

# # Save the enriched selected markets to a new CSV file
# enriched_csv_path = "enriched_selected_markets.csv"
# df_selected_markets[ordered_columns].to_csv(enriched_csv_path, index=False)
# print(f"Saved enriched selected markets to {enriched_csv_path}")

# Reorder columns and save to Parquet
enriched_parquet_path = "enriched_selected_markets.parquet"
df_selected_markets[ordered_columns].to_parquet(enriched_parquet_path, index=False)
print(f"Saved enriched selected markets to {enriched_parquet_path}")