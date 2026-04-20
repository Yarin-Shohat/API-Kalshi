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

# print("Reading data files...")
# # Read files and load data into memory
# print("Loading markets...")
# df_markets = pd.read_parquet(markets_PATH)
# print("Finished loading markets.")

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

# fetch_candlesticks_with_routing
start_date = int(datetime.datetime(2026, 4, 1).timestamp())
end_date = int(datetime.datetime(2026, 4, 10).timestamp())
ticket = "KXHIGHDEN-26APR09-T76"

historical_price_action = engine.fetch_candlesticks_with_routing(
    ticker=ticket,
    start_unix=start_date,
    end_unix=end_date,
    resolution_minutes=60 
)

print(f"Retrieved {len(historical_price_action)} hourly candles for {ticket}!")
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