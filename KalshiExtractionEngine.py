import requests
import datetime
import time
from urllib.parse import urlparse
import base64
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import serialization
import os
import json

class KalshiExtractionEngine:
    """
    Integrative extraction engine for retrieving all data from the Kalshi system.
    Combines dynamic historical routing, PSS-RSA cryptographic signing, and continuous pagination.
    """

    def __init__(self, api_key_id=None, private_key_path=None, use_demo=False):
        """
        Set up the extraction environment while loading the required security keys for portfolio data retrieval.
        """
        self.base_url = "https://demo-api.kalshi.co/trade-api/v2" if use_demo \
                        else "https://api.elections.kalshi.com/trade-api/v2"
        self.api_key_id = api_key_id

        # Load private key for operations requiring strong authentication
        self.private_key = None
        if private_key_path:
            with open(private_key_path, "rb") as key_file:
                self.private_key = serialization.load_pem_private_key(
                    key_file.read(),
                    password=None,
                )

        # Store cutoff time data (Live vs Historical) in engine memory
        self.cutoffs = self._fetch_historical_cutoffs()

    def _generate_rsa_pss_signature(self, timestamp_str, method, path):
        """
        Create a strict cryptographic signature according to system standard.
        Important: The server requires signing only the base path (without query parameters like ?limit=5).
        """
        if not self.private_key:
            raise PermissionError("Private key required for authenticated operations.")

        message = (timestamp_str + method + path).encode('utf-8')
        signature = self.private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
        return base64.b64encode(signature).decode('utf-8')

    def _execute_request(self, method, endpoint, params=None, requires_auth=False, max_retries=5):
        """
        Manages the communication layer with the API, adding security headers.
        Includes an Exponential Backoff retry mechanism to gracefully handle 
        both HTTP 429 (Rate Limits) and sudden network disconnections.
        """
        url = f"{self.base_url}{endpoint}"
        headers = {'Content-Type': 'application/json'}

        if requires_auth:
            current_timestamp = str(int(datetime.datetime.now().timestamp() * 1000))
            clean_path = urlparse(url).path
            signature = self._generate_rsa_pss_signature(current_timestamp, method, clean_path)

            headers.update({
                'KALSHI-ACCESS-KEY': self.api_key_id,
                'KALSHI-ACCESS-SIGNATURE': signature,
                'KALSHI-ACCESS-TIMESTAMP': current_timestamp
            })

        for attempt in range(max_retries):
            try:
                # Added timeout (15 seconds) to prevent hanging connections
                response = requests.request(method, url, headers=headers, params=params, timeout=15)
                
                # Handle Rate Limiting specifically
                if response.status_code == 429:
                    wait_time = 2 ** attempt
                    print(f"[Rate Limit 429] Throttled by Kalshi. Sleeping for {wait_time}s (Attempt {attempt + 1}/{max_retries})...")
                    time.sleep(wait_time)
                    continue 
                
                response.raise_for_status()
                return response.json()

            except requests.exceptions.RequestException as error:
                # Catch connection drops, timeouts, and server errors (500, 502, etc.)
                wait_time = 2 ** attempt
                print(f"[Network/API Error] {error}. Retrying in {wait_time}s (Attempt {attempt + 1}/{max_retries})...")
                time.sleep(wait_time)
                continue
                
        print(f"[Extraction Failed] Maximum retries exceeded for {endpoint}.")
        return None

    def _paginate_extraction(self, endpoint, data_key, params=None, requires_auth=False):
        """
        Extracts data continuously while bypassing length limits using the cursor mechanism.
        Prevents system overload (Throttling) using reasonable delays according to tier layers.
        """
        if params is None:
            params = {}

        extracted_data = []
        params['limit'] = 100  # Request the maximum data ceiling for each "page"

        while True:
            response = self._execute_request("GET", endpoint, params, requires_auth)
            if not response or data_key not in response:
                break

            extracted_data.extend(response[data_key])

            cursor = response.get('cursor')
            if not cursor:
                break

            params['cursor'] = cursor
            # Safety delay of 50 milliseconds to prevent exceeding the 20 actions per second limit for a basic user
            time.sleep(0.2)

        return extracted_data

    def _fetch_historical_cutoffs(self):
        """ Fetch the timestamps separating the live and historical servers """
        return self._execute_request("GET", "/historical/cutoff") or {}

    # =========================================================================
    # Cluster 1: Metadata, category hierarchy, and system filter tags
    # =========================================================================

    def fetch_categories_taxonomy(self):
        """
        Retrieves the full segmentation tree: main categories linked to internal tag arrays.
        Useful for routing and determining trading domains (e.g., scanning for the "Inflation" tag).
        """
        data = self._execute_request("GET", "/search/tags_by_categories")
        return data.get('tags_by_categories', {}) if data else {}

    def fetch_series_collection(self, category_filter=None):
        """
        Extracts the master templates for all contracts in the system (Series).
        """
        params = {}
        if category_filter:
            params['category'] = category_filter

        return self._paginate_extraction("/series", "series", params=params)

    # =========================================================================
    # Cluster 2: Market discovery, trading depth, and order book mechanics
    # =========================================================================

    def fetch_market_contracts(self, series_ticker=None, market_status="open"):
        """
        Retrieve the list of active markets derived from a specific series.
        """
        params = {'status': market_status}
        if series_ticker:
            params['series_ticker'] = series_ticker

        return self._paginate_extraction("/markets", "markets", params=params)

    def analyze_orderbook_depth(self, market_ticker):
        """
        Retrieve the latest order book.
        Applies financial logic to infer "Ask" offers from "Bid" offers only,
        according to the zero-sum principle of binary contracts ($1.00 - the opposite price).
        """
        endpoint = f"/markets/{market_ticker}/orderbook"
        data = self._execute_request("GET", endpoint)

        if not data or 'orderbook' not in data:
            return None

        orderbook = data['orderbook']
        yes_bids = [float(p) for p in orderbook.get('yes_dollars',[])]
        no_bids = [float(p) for p in orderbook.get('no_dollars',[])]

        # Implied ask calculation
        best_yes_ask = 1.00 - max(no_bids) if no_bids else None
        best_no_ask = 1.00 - max(yes_bids) if yes_bids else None

        return {
            'raw_yes_bids': yes_bids,
            'raw_no_bids': no_bids,
            'implied_yes_ask': best_yes_ask,
            'implied_no_ask': best_no_ask,
            'spread_yes': (best_yes_ask - max(yes_bids)) if (best_yes_ask and yes_bids) else None
        }

    def extract_all_historical_markets(self):
        """
        Extracts all settled markets from the Kalshi API since its inception.
        Utilizes the internal pagination engine to automatically handle 
        cursor navigation and API rate limits.
        """
        # Define the parameter to filter strictly for historical data
        params = {
            'status': 'settled'
        }
        
        # The internal _paginate_extraction method overrides the limit, 
        # manages the while loop, and aggregates the returned list.
        return self._paginate_extraction("/markets", "markets", params=params)

    def extract_markets_with_checkpointing(self, data_file="kalshi_markets.jsonl", checkpoint_file="cursor_state.json"):
        """
        Extracts historical markets robustly using checkpoints.
        Saves data continuously to a JSONL file to prevent data loss during network drops.
        Resumes automatically from the last saved cursor if interrupted.
        """
        params = {
            'status': 'settled',
            'limit': 100
        }
        
        # 1. Check if a previous checkpoint exists and load the cursor
        cursor = None
        if os.path.exists(checkpoint_file):
            try:
                with open(checkpoint_file, 'r') as f_state:
                    state = json.load(f_state)
                    cursor = state.get("last_cursor")
                    print(f"[Checkpoint Found] Resuming extraction from cursor: {cursor}")
            except Exception as e:
                print(f"Failed to read checkpoint file: {e}")
        
        if cursor:
            params['cursor'] = cursor

        markets_saved_this_run = 0
        
        print(f"Starting extraction. Data will be appended to {data_file}")
        
        # 2. Open the data file in 'Append' mode ('a')
        with open(data_file, 'a', encoding='utf-8') as f_data:
            while True:
                # Use the robust _execute_request we built earlier
                response = self._execute_request("GET", "/markets", params=params)
                
                if not response or 'markets' not in response:
                    print("[Extraction Stopped] Could not retrieve more data or network failed.")
                    break
                    
                batch = response['markets']
                if not batch:
                    break
                    
                # 3. Write each market as a new line in the JSONL file
                for market in batch:
                    f_data.write(json.dumps(market) + "\n")
                    
                markets_saved_this_run += len(batch)
                print(f"Saved {len(batch)} markets. Total saved in this session: {markets_saved_this_run}")
                
                # 4. Get the next cursor
                cursor = response.get('cursor')
                if not cursor:
                    print("[Extraction Complete] Successfully reached the end of the historical data!")
                    # Clean up the checkpoint file since we are completely done
                    if os.path.exists(checkpoint_file):
                        os.remove(checkpoint_file)
                    break
                    
                # 5. Save the new cursor to the checkpoint file
                with open(checkpoint_file, 'w') as f_state:
                    json.dump({"last_cursor": cursor}, f_state)
                    
                params['cursor'] = cursor
                
                # Sleep to respect rate limits
                time.sleep(0.2)

    def fetch_and_group_series_by_category(self, taxonomy_path="taxonomy.json"):
        """
        Downloads all Series and groups them by categories as defined in taxonomy.json.
        Returns a dictionary: {category: [series, ...]}
        """
        with open(taxonomy_path, "r", encoding="utf-8") as f:
            taxonomy = json.load(f)
        result = {}
        for category in taxonomy.keys():
            # Use filter by category
            series_list = self.fetch_series_collection(category_filter=category)
            result[category] = series_list
        return result

    def fetch_and_group_markets_by_category(self, taxonomy_path="taxonomy.json", target_status="settled"):
        """
        Downloads markets and groups them by categories as defined in taxonomy.json.
        Avoids the N+1 API call problem by querying markets directly per category 
        instead of iterating through individual series tickers.

        Parameters:
            - taxonomy_path: Path to the taxonomy JSON file that maps categories to tags.
            - target_status: The market status to filter by (e.g., "open", "settled", "closed").
        """
        import json
        
        with open(taxonomy_path, "r", encoding="utf-8") as f:
            taxonomy = json.load(f)
            
        result = {}
        
        for category in taxonomy.keys():
            print(f"Fetching {target_status} markets directly for category: {category}...")
            
            # Using the direct category filter on the /markets endpoint
            params = {
                "category": category,
                "status": target_status 
            }
            
            # Utilizing the internal pagination mechanism to extract all relevant markets safely
            markets_list = self._paginate_extraction("/markets", "markets", params=params)
            
            result[category] = markets_list
            print(f"Successfully retrieved {len(markets_list)} markets for {category}.")
        return result

    def fetch_markets_by_series(self, series_ticker, target_status=None):
        """
        Retrieves all markets associated with a specific series ticker.
        Utilizes the internal pagination mechanism to automatically handle 
        cursor navigation and prevent data truncation.
        """
        print(f"Fetching markets for series: {series_ticker}...")
        
        # Define the query parameters
        params = {
            "series_ticker": series_ticker
        }
        
        # Optionally filter by status (e.g., 'open', 'settled')
        if target_status and target_status.lower() != "all":
            params["status"] = target_status
            
        # Extract all markets using the robust pagination engine
        markets_list = self._paginate_extraction("/markets", "markets", params=params)
        
        print(f"Successfully retrieved {len(markets_list)} markets for series {series_ticker}.")
        return markets_list

    # =========================================================================
    # Cluster 3: Continuous time analysis and candlestick data in a dual environment
    # =========================================================================

    def fetch_candlesticks_with_routing(self, ticker, start_unix, end_unix, resolution_minutes=60):
        """
        Retrieve continuous time data (OHLC) and create candlesticks.
        Applies automatic router logic based on the archival cutoff timestamp.
        If necessary, activates the synthetic candle to prevent discontinuity in illiquid trading environments.
        """
        params = {
            'start_ts': start_unix,
            'end_ts': end_unix,
            'period_interval': resolution_minutes,
            'include_latest_before_start': True  # Ensures time continuity when there is no liquidity
        }

        # First attempt to fetch from the live markets server
        live_endpoint = f"/markets/{ticker}/candlesticks"
        try:
            response = self._execute_request("GET", live_endpoint, params=params)
            # If the market is old (Pre-cutoff), an error or an empty array will be returned, which is not typical for a wide time window
            if response and 'candlesticks' in response and len(response['candlesticks']) > 0:
                return response['candlesticks']
        except Exception:
            pass # Quietly switch to the historical service in case of a 40X operational error from the live server

        # Alternative routing to the historical server if the live server rejected the request
        historical_endpoint = f"/historical/markets/{ticker}/candlesticks"
        hist_response = self._execute_request("GET", historical_endpoint, params=params)
        return hist_response.get('candlesticks', []) if hist_response else []

    def fetch_market_trades(self, ticker):
            """ 
            Extracts the complete, tick-by-tick trading history for a specific market.
            Provides precise volume, price, and timestamp for every transaction.
            """
            endpoint = f"/markets/{ticker}/trades"
            return self._paginate_extraction(endpoint, "trades")
    
    def fetch_event_forecast_history(self, event_ticker):
        """
        Retrieves the historical probability forecasts for a specific event.
        Note: This uses the 'event_ticker' (e.g., 'KXPRESIDENT24'), 
        not the individual market 'ticker'.
        """
        print(f"Fetching forecast history for event: {event_ticker}...")
        endpoint = f"/events/{event_ticker}/forecast_history"
        
        # This endpoint typically returns a single JSON object with arrays of historical data,
        # so we use the standard _execute_request rather than the pagination method.
        return self._execute_request("GET", endpoint)
    # =========================================================================
    # Cluster 4: Institutional portfolio data and trading accounting (Private Data)
    # =========================================================================

    def audit_portfolio_holdings(self):
        """
        Retrieve the exact account balance.
        The system returns data in cents (Integer), the function converts it back to fiat dollars (Float).
        """
        data = self._execute_request("GET", "/portfolio/balance", requires_auth=True)
        return (data['balance'] / 100.0) if data and 'balance' in data else None