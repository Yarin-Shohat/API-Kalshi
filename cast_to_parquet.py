import gzip
import json
import glob
import pandas as pd
from pathlib import Path

from CONST import *

markets_by_cartegory_PATH = MARKETS_DIR + "markets_by_category.json.gz"
markets_by_series_PATH = MARKETS_DIR + "markets_by_series.json.gz"
taxonomy_PATH = TAXONOMY_DIR + "taxonomy.json.gz"
series_by_category_PATH = SERIES_DIR + "series_by_category.json.gz"


def json_gz_to_parquet(json_gz_path, parquet_path):
    """Convert JSON.GZ file to Parquet format."""
    with gzip.open(json_gz_path, 'rt') as f:
        data = json.load(f)
    
    df = pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame([data])
    df.to_parquet(parquet_path, index=False)


def convert_all_to_parquet():
    """Convert all JSON.GZ files to Parquet format."""
    conversions = [
        (markets_by_cartegory_PATH, MARKETS_DIR + "markets_by_category.parquet"),
        (markets_by_series_PATH, MARKETS_DIR + "markets_by_series.parquet"),
        (taxonomy_PATH, TAXONOMY_DIR + "taxonomy.parquet"),
        (series_by_category_PATH, SERIES_DIR + "series_by_category.parquet"),
    ]
    
    for json_gz_path, parquet_path in conversions:
        try:
            json_gz_to_parquet(json_gz_path, parquet_path)
            print(f"✓ Converted {json_gz_path} → {parquet_path}")
        except Exception as e:
            print(f"✗ Failed to convert {json_gz_path}: {e}")


if __name__ == "__main__":
    convert_all_to_parquet()

