import ccxt
import pandas as pd
import time
from datetime import datetime
import os
import pickle

class DataFetcher:
    """
    Handles fetching market data from an exchange using ccxt.
    """
    def __init__(self, api_key=None, secret_key=None, password=None, exchange_id='okx', is_sandbox_mode=False, proxy_url=None, proxy_type=None, force_fetch=False):
        """
        Initializes the DataFetcher.

        Args:
            api_key (str, optional): API key for the exchange.
            secret_key (str, optional): Secret key for the exchange.
            password (str, optional): API password/passphrase if required (e.g., by OKX).
            exchange_id (str, optional): ID of the exchange to connect to (default: 'okx').
            is_sandbox_mode (bool, optional): Whether to use the exchange's sandbox/testnet.
            proxy_url (str, optional): URL of the proxy server (e.g., 'http://user:pass@host:port', 'socks5h://user:pass@host:port').
            proxy_type (str, optional): Type of the proxy (e.g., 'http', 'socks5', 'socks5h'). Note: often part of proxy_url.
            force_fetch (bool, optional): Whether to force fetching data even if cache exists. Defaults to False.
        """
        self.exchange_id = exchange_id
        self.force_fetch = force_fetch
        try:
            exchange_class = getattr(ccxt, exchange_id)
        except AttributeError:
            raise ValueError(f"Exchange with ID '{exchange_id}' not found in ccxt. Please check your configuration.") from None

        config = {
            'apiKey': api_key,
            'secret': secret_key,
            'password': password,
            'verbose': False, # Ensure this is False or commented out for non-debug runs
            'rateLimit': True,
        }
        # Remove None values from config as ccxt expects them to be absent if not used
        config = {k: v for k, v in config.items() if v is not None}

        if proxy_url:
            proxies_dict = {
                'http': proxy_url,
                'https': proxy_url,
            }
            config['aiohttp_proxy'] = proxy_url      # For async (aiohttp)
            config['requests_proxy'] = proxies_dict  # For sync (requests)
            config['proxies'] = proxies_dict         # Generic/fallback for ccxt
            # proxy_type is noted, but ccxt usually derives type from proxy_url scheme (e.g. socks5h://)
            # If specific handling for proxy_type is needed for ccxt, it would be added here.

        self.exchange = exchange_class(config)

        if is_sandbox_mode:
            if hasattr(self.exchange, 'set_sandbox_mode'):
                 self.exchange.set_sandbox_mode(True)
            elif 'test' in self.exchange.urls: # Common convention for testnet URLs
                self.exchange.urls['api'] = self.exchange.urls['test']
            else:
                # Attempt common sandbox URL patterns if direct method not available
                # This is a fallback and might not work for all exchanges
                # OKX specific sandbox URL structure
                if exchange_id == 'okx':
                    self.exchange.urls['api'] = 'https://www.okx.com/api/v5' # Base URL
                    # For OKX, sandbox is typically controlled by a header or specific API key type,
                    # rather than just a different base URL for all endpoints.
                    # The `set_sandbox_mode(True)` is preferred if available, or using demo account keys.
                    # OKX uses a demo trading header: 'x-simulated-trading': '1'
                    # This needs to be added to requests, ccxt might handle it via set_sandbox_mode
                    # or specific options. If not, custom header injection would be needed.
                    # For now, we assume set_sandbox_mode or specific demo keys handle this.
                    print("Attempting to set OKX to sandbox mode. Ensure you are using demo account API keys if direct sandbox URL override is not fully effective.")
                else:
                    print(f"Warning: Exchange '{exchange_id}' does not have a standard way to set sandbox mode via ccxt. Testnet functionality may not work as expected.")


        # Load markets (optional, but good practice for some operations)
        try:
            self.exchange.load_markets()
        except ccxt.NetworkError as e:
            print(f"Error loading markets due to network issue: {e}. Some features might not work.")
        except ccxt.ExchangeError as e:
            print(f"Error loading markets due to exchange issue: {e}. Some features might not work.")


    def fetch_ohlcv(self, symbol, timeframe='1d', since=None, limit=None, params=None, force_fetch=None):
        """
        Fetches historical OHLCV (K-line) data.

        Supports caching of fetched data to speed up subsequent requests.

        If 'since' is provided, the function will attempt to fetch all available OHLCV data
        starting from the 'since' timestamp up to the most recent data, making multiple
        API calls if necessary. The 'limit' parameter, if also provided with 'since',
        will act as a cap on the total number of candles retrieved in this paginated fetch.

        If 'since' is not provided, it fetches the most recent candles, up to the number
        specified by 'limit' (or the exchange's default if 'limit' is None).

        API rate limits are automatically handled by the underlying ccxt library,
        as rate limiting is enabled in the DataFetcher's configuration.

        Args:
            symbol (str): The trading symbol (e.g., 'BTC/USDT').
            timeframe (str, optional): The timeframe for K-lines (e.g., '1m', '5m', '1h', '1d'). Defaults to '1d'.
            since (int, optional): Timestamp in milliseconds for the earliest candle to fetch.
                                   If None, fetches most recent data.
            limit (int, optional): The maximum number of candles to fetch.
                                   If 'since' is given, this is the total max.
                                   If 'since' is None, this is the limit for recent data.
                                   Defaults to None (exchange default for recent, all for historical).
            params (dict, optional): Extra parameters to pass to the exchange API.
            force_fetch (bool, optional): Overrides the instance's force_fetch setting for this specific call.
                                         If None, uses the instance's `self.force_fetch` setting.

        Returns:
            pandas.DataFrame: A DataFrame with columns ['timestamp', 'open', 'high', 'low', 'close', 'volume'],
                              with 'timestamp' as datetime objects (UTC). Returns an empty DataFrame if
                              no data is found, or None if an error occurs that prevents data retrieval.
        """
        if not self.exchange.has['fetchOHLCV']:
            print(f"Exchange {self.exchange_id} does not support fetching OHLCV data.")
            return None

        # Determine effective force_fetch state
        current_force_fetch = self.force_fetch if force_fetch is None else force_fetch

        # Cache setup
        since_str = str(since) if since is not None else "latest"
        cache_filename_parts = [
            self.exchange_id.lower(),
            symbol.replace('/', '_').lower(),
            timeframe,
            since_str
        ]
        # Filter out None or empty strings just in case, though 'since_str' handles 'since=None'
        cache_filename_parts = [part for part in cache_filename_parts if part]
        cache_filename = "_".join(cache_filename_parts) + ".pkl"

        cache_dir = ".cache"
        cache_filepath = os.path.join(cache_dir, cache_filename)

        if not current_force_fetch and os.path.exists(cache_filepath):
            try:
                with open(cache_filepath, 'rb') as f:
                    print(f"Loading OHLCV data from cache: {cache_filepath}")
                    return pickle.load(f)
            except Exception as e:
                print(f"Error loading data from cache {cache_filepath}: {e}. Fetching from exchange.")

        # Proceed to fetch from exchange if not loaded from cache
        try:
            timeframe_duration_ms = self.exchange.parse_timeframe(timeframe) * 1000
            all_ohlcv_data = [] # Initialize here for broader scope

            if since is not None:
                current_since = since
                exchange_batch_limit = 100 # Default internal batch limit

                # Calculate initial batch size considering the overall limit
                # This ensures we don't over-fetch in the first call if limit < exchange_batch_limit
                current_batch_limit = exchange_batch_limit
                if limit is not None:
                    current_batch_limit = min(limit, exchange_batch_limit)

                while True:
                    if limit is not None and len(all_ohlcv_data) >= limit:
                        break # Already fetched enough data

                    # Adjust current_batch_limit if remaining candles needed are less than exchange_batch_limit
                    if limit is not None:
                        remaining_limit = limit - len(all_ohlcv_data)
                        if remaining_limit <= 0: # Should be caught by the check above, but good for safety
                             break
                        current_batch_limit = min(remaining_limit, exchange_batch_limit)

                    if current_batch_limit <= 0 : # No more candles to fetch due to limit
                        break

                    # print(f"Fetching {current_batch_limit} candles for {symbol} from {self.exchange.iso8601(current_since)}")
                    ohlcv_batch = self.exchange.fetch_ohlcv(
                        symbol,
                        timeframe,
                        since=current_since,
                        limit=current_batch_limit, # Use adjusted batch limit
                        params=params or {}
                    )

                    if ohlcv_batch:
                        all_ohlcv_data.extend(ohlcv_batch)
                        last_timestamp_in_batch = ohlcv_batch[-1][0]
                        current_since = last_timestamp_in_batch + timeframe_duration_ms

                        # Break if fewer candles than requested were returned (end of data)
                        if len(ohlcv_batch) < current_batch_limit:
                            break
                        # If current_batch_limit was already small due to overall 'limit',
                        # and we got exactly that many, we might be at the overall 'limit'.
                        # The check at the beginning of the loop (len(all_ohlcv_data) >= limit) handles this.
                    else:
                        break # No more data returned by exchange

                # Slice data if 'limit' was provided and we fetched more (e.g. due to batching)
                if limit is not None and len(all_ohlcv_data) > limit:
                    all_ohlcv_data = all_ohlcv_data[:limit]

            else: # Original behavior if 'since' is not provided
                # If 'since' is None, ccxt fetches the most recent candles.
                # The 'limit' here directly applies to how many recent candles to get.
                ohlcv_data_raw = self.exchange.fetch_ohlcv(symbol, timeframe, since, limit, params or {})
                if ohlcv_data_raw:
                    all_ohlcv_data.extend(ohlcv_data_raw) # Use extend to be consistent, though it's just one batch

            if not all_ohlcv_data:
                # Updated message to be more generic
                print(f"No OHLCV data returned for {symbol} with timeframe {timeframe}.")
                df = pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            else:
                df = pd.DataFrame(all_ohlcv_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

                # Ensure final df respects limit if 'since' was None and limit was applied by ccxt directly
                # This is mostly for safety, as ccxt should return 'limit' items.
                # However, if 'since' was used, slicing is already done.
                if since is None and limit is not None and len(df) > limit:
                     df = df.head(limit)

            # Save to cache
            try:
                if not os.path.exists(cache_dir):
                    os.makedirs(cache_dir, exist_ok=True)
                with open(cache_filepath, 'wb') as f:
                    pickle.dump(df, f)
                    print(f"Saved OHLCV data to cache: {cache_filepath}")
            except Exception as e:
                print(f"Error saving data to cache {cache_filepath}: {e}")

            return df

        except ccxt.NetworkError as e:
            print(f"Network error while fetching OHLCV for {symbol}: {e}")
        except ccxt.ExchangeError as e:
            print(f"Exchange error while fetching OHLCV for {symbol}: {e}")
        except Exception as e:
            print(f"An unexpected error occurred while fetching OHLCV for {symbol}: {e}")
        return None

    def fetch_ticker_price(self, symbol, params=None):
        """
        Fetches the latest ticker price for a symbol.

        Args:
            symbol (str): The trading symbol (e.g., 'BTC/USDT').
            params (dict, optional): Extra parameters to pass to the exchange API.

        Returns:
            float: The last traded price, or None if an error occurs.
        """
        if not self.exchange.has['fetchTicker']:
            print(f"Exchange {self.exchange_id} does not support fetching ticker data.")
            return None
        try:
            ticker = self.exchange.fetch_ticker(symbol, params=params)
            return ticker['last'] # 'last' is the common field for the last traded price
        except ccxt.NetworkError as e:
            print(f"Network error while fetching ticker for {symbol}: {e}")
        except ccxt.ExchangeError as e:
            print(f"Exchange error while fetching ticker for {symbol}: {e}")
        except KeyError:
            print(f"Could not find 'last' price in ticker data for {symbol}. Ticker data: {ticker}")
        except Exception as e:
            print(f"An unexpected error occurred while fetching ticker for {symbol}: {e}")
        return None

    def get_account_balance(self, currency_code=None):
        """
        Fetches account balance.

        Args:
            currency_code (str, optional): The specific currency code (e.g., 'USDT', 'BTC') to get balance for.
                                           If None, returns all balances.

        Returns:
            dict or float: A dictionary of all balances, or a float for a specific currency's 'free' balance.
                           Returns None if an error occurs.
        """
        if not self.exchange.has['fetchBalance']:
            print(f"Exchange {self.exchange_id} does not support fetching balance.")
            return None
        try:
            balance = self.exchange.fetch_balance()
            if currency_code:
                return balance.get(currency_code, {}).get('free', 0.0) # Return free balance for the currency
            return balance['free'] # Return dict of free balances for all currencies
        except ccxt.NetworkError as e:
            print(f"Network error while fetching balance: {e}")
        except ccxt.ExchangeError as e:
            print(f"Exchange error while fetching balance: {e}")
        except Exception as e:
            print(f"An unexpected error occurred while fetching balance: {e}")
        return None


# Example of how to use it (optional, for testing within this file)
if __name__ == "__main__":
    print("--- Testing DataFetcher ---")
    # This test will try to connect to OKX. Without API keys, it will use public endpoints.
    # For authenticated endpoints like get_account_balance, it would fail without keys.
    # For sandbox testing, ensure your config.example.toml is copied to config.toml
    # and sandbox API keys are set if needed, or set is_sandbox_mode=True.

    # Load config to get API keys if available for testing private endpoints
    # This assumes config.py is in ../config_manager/ and we are in owl/data_fetcher
    import sys
    from pathlib import Path
    # Add project root to sys.path to import config_manager
    project_root_path = Path(__file__).resolve().parent.parent.parent
    sys.path.append(str(project_root_path))

    API_KEY = None
    API_SECRET = None
    API_PASSWORD = None
    EXCHANGE_ID = 'okx'  # Default exchange_id
    IS_SANDBOX = False   # Default sandbox mode
    PROXY_URL = None
    PROXY_TYPE = None

    try:
        from owl.config_manager.config import load_config
        config = load_config() # Expects config.toml in project root
        
        # Load API keys
        api_keys_config = config.get('api_keys', {})
        API_KEY = api_keys_config.get('okx_api_key')
        API_SECRET = api_keys_config.get('okx_secret_key')
        API_PASSWORD = api_keys_config.get('okx_password')
        
        # Load exchange settings
        exchange_settings = config.get('exchange_settings', {})
        EXCHANGE_ID = exchange_settings.get('exchange_id', 'okx')
        IS_SANDBOX = exchange_settings.get('sandbox_mode', False)

        # Load proxy settings
        proxy_settings = config.get('proxy', {})
        if not proxy_settings: # Fallback to exchange_settings for proxy info
            proxy_settings = config.get('exchange_settings', {})
        PROXY_URL = proxy_settings.get('proxy_url')
        PROXY_TYPE = proxy_settings.get('proxy_type')
        
        print("Config loaded for testing DataFetcher:")
        print(f"  API_KEY: {'Set' if API_KEY else 'Not set'}")
        print(f"  EXCHANGE_ID: {EXCHANGE_ID}")
        print(f"  IS_SANDBOX: {IS_SANDBOX}")
        print(f"  PROXY_URL: {PROXY_URL}")
        print(f"  PROXY_TYPE: {PROXY_TYPE}")

    except Exception as e:
        print(f"Could not load config for testing DataFetcher: {e}. Running with defaults and public access only.")


    # Initialize DataFetcher
    try:
        fetcher = DataFetcher(
            api_key=API_KEY,
            secret_key=API_SECRET,
            password=API_PASSWORD,
            exchange_id=EXCHANGE_ID,
            is_sandbox_mode=IS_SANDBOX,
            proxy_url=PROXY_URL,
            proxy_type=PROXY_TYPE
        )

        # Test fetch_ohlcv
        # Use a symbol relevant to the configured exchange_id, or a common one like BTC/USDT
        # For OKX, BTC/USDT (spot) or BTC/USDT/USDT (swap if defaultType is swap) can be used.
        # The exact symbol might depend on whether markets are loaded and what the default is.
        test_symbol = "BTC/USDT" 
        if EXCHANGE_ID == 'okx': # Potentially adjust symbol based on typical OKX usage or if sandbox implies demo swap
             # test_symbol = "BTC-USDT-SWAP" # if using instrument_id like in config.example for backtesting
             pass # Keep BTC/USDT as a general ccxt spot symbol

        print(f"\n--- Testing fetch_ohlcv ({test_symbol}, 1d, last 5 candles for {EXCHANGE_ID}) ---")
        # To get the last N candles, we don't set 'since'. 'limit' gives recent ones.
        # For specific historical data, 'since' is timestamp in ms.
        ohlcv_data = fetcher.fetch_ohlcv(symbol=test_symbol, timeframe='1d', limit=5)
        if ohlcv_data is not None and not ohlcv_data.empty:
            print("OHLCV Data:")
            print(ohlcv_data.head())
        else:
            print(f"Could not fetch OHLCV data for {test_symbol} or data was empty.")

        # Test fetch_ticker_price
        print(f"\n--- Testing fetch_ticker_price ({test_symbol} for {EXCHANGE_ID}) ---")
        price = fetcher.fetch_ticker_price(symbol=test_symbol)
        if price is not None:
            print(f"Current price for {test_symbol}: {price}")
        else:
            print(f"Could not fetch ticker price for {test_symbol}.")

        # Test get_account_balance (will likely fail/return empty if keys are invalid or not for sandbox)
        print(f"\n--- Testing get_account_balance (USDT on {EXCHANGE_ID}) ---")
        if API_KEY and API_SECRET: # Only attempt if keys are somewhat present
            usdt_balance = fetcher.get_account_balance(currency_code='USDT')
            if usdt_balance is not None:
                print(f"USDT Account Balance (Free): {usdt_balance}")
            else:
                print("Could not fetch USDT account balance (or error occurred). This is expected if API keys are not valid or lack permissions.")

            all_balances = fetcher.get_account_balance()
            if all_balances is not None:
                print(f"All Account Balances (Free): {all_balances}")
            else:
                print("Could not fetch all account balances.")

        else:
            print("Skipping balance check as API keys are not configured for testing.")

    except ValueError as ve:
        print(f"ValueError during DataFetcher initialization: {ve}")
    except ccxt.AuthenticationError as ae:
        print(f"CCXT Authentication Error: {ae}. Ensure API keys are correct and have permissions.")
        print("If using sandbox, ensure you have sandbox-specific API keys and password.")
    except Exception as e:
        print(f"An unexpected error occurred during DataFetcher testing: {e}")

    print("\n--- DataFetcher Test Complete ---")
