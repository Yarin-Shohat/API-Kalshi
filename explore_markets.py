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


# List of columns to convert to datetime
datetime_cols = [
    'close_time', 'created_time', 'expected_expiration_time', 'expiration_time',
    'latest_expiration_time', 'open_time', 'updated_time', 'settlement_ts'
]

for col in datetime_cols:
    if col in df_markets.columns:
        df_markets[col] = pd.to_datetime(df_markets[col], format='ISO8601', errors='coerce')

# List of columns to convert to numeric
numeric_cols = [
    'last_price_dollars', 'liquidity_dollars', 'no_ask_dollars', 'no_bid_dollars',
    'notional_value_dollars', 'open_interest_fp', 'previous_price_dollars',
    'previous_yes_ask_dollars', 'previous_yes_bid_dollars', 'volume_24h_fp',
    'volume_fp', 'yes_ask_dollars', 'yes_ask_size_fp', 'yes_bid_dollars',
    'yes_bid_size_fp', 'settlement_value_dollars'
]

for col in numeric_cols:
    if col in df_markets.columns:
        df_markets[col] = pd.to_numeric(df_markets[col], errors='coerce')

# List of columns to convert to categorical
categorical_cols = [
    'market_type', 'price_level_structure', 'response_price_units', 'status',
    'strike_type', 'category', 'result', 'event_ticker', 'ticker', 'parent_series'
]

for col in categorical_cols:
    if col in df_markets.columns:
        # Check if the number of unique values is reasonable for a categorical column
        if df_markets[col].nunique() < len(df_markets) / 2: # Heuristic: less than half unique values
            df_markets[col] = df_markets[col].astype('category')


print("\n--- Overall DataFrame Information ---")
df_markets.info()


# --- Combine all summary statistics into one DataFrame ---
print("\n--- Creating Combined Summary Table ---")

# Descriptive statistics
desc_df = df_markets.describe(include='all').T  # Transpose for easier merging

# Unique value counts
problematic_object_columns = ['price_ranges']
unique_counts = []
for col in df_markets.columns:
    if col in problematic_object_columns:
        unique_counts.append("Not Calculated (Unhashable Type)")
    else:
        try:
            unique_counts.append(df_markets[col].nunique())
        except Exception:
            unique_counts.append("Error")
unique_counts_df = pd.DataFrame({'Unique Values Count': unique_counts}, index=df_markets.columns)

# Null value counts
null_counts_df = df_markets.isnull().sum().to_frame(name='Null Values Count')

# Min/Max values
min_vals = []
max_vals = []
for col in df_markets.columns:
    if pd.api.types.is_numeric_dtype(df_markets[col]) or pd.api.types.is_datetime64_any_dtype(df_markets[col]):
        min_vals.append(df_markets[col].min())
        max_vals.append(df_markets[col].max())
    else:
        min_vals.append("")
        max_vals.append("")
min_max_df = pd.DataFrame({'Min': min_vals, 'Max': max_vals}, index=df_markets.columns)


# Add dtype information
dtypes_df = pd.DataFrame({'Dtype': df_markets.dtypes.astype(str)})

# Merge all summaries into one DataFrame, including dtypes
summary_df = desc_df.join([unique_counts_df, null_counts_df, dtypes_df], how='outer')

# Save to CSV
summary_csv_path = 'markets_summary.csv'
summary_df.to_csv(summary_csv_path)
print(f"Combined summary table saved to {summary_csv_path}")

# The combined summary table is now saved as CSV. Remove individual prints for unique, null, min/max.