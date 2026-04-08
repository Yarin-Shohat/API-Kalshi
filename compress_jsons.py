import gzip
import shutil
from CONST import *
import json


markets_by_cartegory_PATH = MARKETS_DIR + "markets_by_category.json"
markets_by_series_PATH = MARKETS_DIR + "markets_by_series.json"
taxonomy_PATH = TAXONOMY_DIR + "taxonomy.json"
series_by_category_PATH = SERIES_DIR + "series_by_category.json"

# Compress
with open(markets_by_cartegory_PATH, 'rb') as f_in:
    with gzip.open(markets_by_cartegory_PATH + '.gz', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

with open(markets_by_series_PATH, 'rb') as f_in:
    with gzip.open(markets_by_series_PATH + '.gz', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

with open(taxonomy_PATH, 'rb') as f_in:
    with gzip.open(taxonomy_PATH + '.gz', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

with open(series_by_category_PATH, 'rb') as f_in:
    with gzip.open(series_by_category_PATH + '.gz', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)