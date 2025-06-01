import ccxt
import pandas as pd
import time
from datetime import datetime

class DataFetcher:
    """
    Handles fetching market data from an exchange using ccxt.
    """
    def __init__(self, api_key=None, secret_key=None, password=None, exchange_id='okx', is_sandbox_mode=False):
        """
        Initializes the DataFetcher.

        Args:
            api_key (str, optional): API key for the exchange.
            secret_key (str, optional): Secret key for the exchange.
            password (str, optional): API password/passphrase if required (e.g., by OKX).
            exchange_id (str, optional): ID of the exchange to connect to (default: 'okx').
            is_sandbox_mode (bool, optional): Whether to use the exchange's sandbox/testnet.
        """
        self.exchange_id = exchange_id
        try:
            exchange_class = getattr(ccxt, exchange_id)
        except AttributeError:
            raise ValueError(f"Exchange with ID '{exchange_id}' not found in ccxt.")

        config = {
            'apiKey': api_key,
            'secret': secret_key,
            'password': password,
            # 'verbose': True, # Uncomment for detailed ccxt output, useful for debugging
        }
        # Remove None values from config as ccxt expects them to be absent if not used
        config = {k: v for k, v in config.items() if v is not None}

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


    def fetch_ohlcv(self, symbol, timeframe='1d', since=None, limit=None, params=None):
        """
        Fetches historical OHLCV (K-line) data.

        Args:
            symbol (str): The trading symbol (e.g., 'BTC/USDT').
            timeframe (str, optional): The timeframe for K-lines (e.g., '1m', '5m', '1h', '1d'). Defaults to '1d'.
            since (int, optional): Timestamp in milliseconds for the earliest candle. Defaults to None.
            limit (int, optional): The maximum number of candles to fetch. Defaults to None (exchange default).
            params (dict, optional): Extra parameters to pass to the exchange API.

        Returns:
            pandas.DataFrame: A DataFrame with columns ['timestamp', 'open', 'high', 'low', 'close', 'volume'],
                              or None if an error occurs.
        """
        if not self.exchange.has['fetchOHLCV']:
            print(f"Exchange {self.exchange_id} does not support fetching OHLCV data.")
            return None

        try:
            # ccxt returns OHLCV data as a list of lists
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe, since, limit, params)
            if not ohlcv:
                print(f"No OHLCV data returned for {symbol} with timeframe {timeframe}.")
                return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])


            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            # It's common for exchanges (like OKX) to return the open time of the candle.
            # For daily candles, this means 00:00 UTC of that day.
            # The design doc mentions Beijing time (UTC+8). We'll handle timezones higher up.
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
    IS_SANDBOX = False # Set to true if you have OKX demo trading keys

    try:
        from owl.config_manager.config import load_config
        config = load_config() # Expects config.toml in project root
        API_KEY = config.get('api_keys', {}).get('okx_api_key')
        API_SECRET = config.get('api_keys', {}).get('okx_secret_key')
        API_PASSWORD = config.get('api_keys', {}).get('okx_password')
        # Check if config.toml has a sandbox mode flag, e.g. in a [exchange_settings] table
        # For now, manually set IS_SANDBOX or rely on ccxt's default behavior / demo keys
        # IS_SANDBOX = config.get('mode', {}).get('okx_sandbox_mode', False) # Example
        print("Config loaded. API keys might be present.")
    except Exception as e:
        print(f"Could not load config for testing DataFetcher: {e}. Running with public access only.")


    # Initialize DataFetcher (use sandbox mode for OKX if testing with demo keys)
    # OKX sandbox: self.exchange.options['defaultType'] = 'swap' # or spot
    # And use the demo trading endpoint if not using set_sandbox_mode()
    # For OKX, `set_sandbox_mode(True)` should work if ccxt version supports it well.
    # Or one might need to use specific API keys for the demo environment.
    try:
        # Set is_sandbox_mode=True if you are using OKX demo account API keys
        fetcher = DataFetcher(api_key=API_KEY, secret_key=API_SECRET, password=API_PASSWORD, exchange_id='okx', is_sandbox_mode=IS_SANDBOX)

        # Test fetch_ohlcv
        print("\n--- Testing fetch_ohlcv (BTC/USDT, 1d, last 5 candles) ---")
        # To get the last N candles, we don't set 'since'. 'limit' gives recent ones.
        # For specific historical data, 'since' is timestamp in ms.
        # Example: Get data for the last 5 days for BTC/USDT spot market
        # Note: OKX uses BTC-USDT for spot, BTC-USDT-SWAP for perpetual swaps.
        # The design doc mentions "BTC/USDT" which usually implies spot.
        # Instrument ID from config.example.toml is "BTC-USDT-SWAP"
        instrument_id_spot = "BTC/USDT" # CCXT standard for spot
        instrument_id_swap = "BTC/USDT/USDT" # Example for OKX USDT margined swap, check ccxt docs
                                         # A common one is BTC/USDT:USDT for USDT margined swap
                                         # Or use the specific ID from exchange.markets
        # Let's try with a common spot symbol first.
        # If markets are loaded, you can find the correct symbol:
        # print(fetcher.exchange.markets.keys()) # to list all symbols

        # For OKX Spot BTC/USDT
        ohlcv_data = fetcher.fetch_ohlcv(symbol=instrument_id_spot, timeframe='1d', limit=5)
        if ohlcv_data is not None and not ohlcv_data.empty:
            print("OHLCV Data:")
            print(ohlcv_data.head())
        else:
            print(f"Could not fetch OHLCV data for {instrument_id_spot} or data was empty.")

        # Test fetch_ticker_price
        print(f"\n--- Testing fetch_ticker_price ({instrument_id_spot}) ---")
        price = fetcher.fetch_ticker_price(symbol=instrument_id_spot)
        if price is not None:
            print(f"Current price for {instrument_id_spot}: {price}")
        else:
            print(f"Could not fetch ticker price for {instrument_id_spot}.")

        # Test get_account_balance (will likely fail/return empty if keys are invalid or not for sandbox)
        print("\n--- Testing get_account_balance (USDT) ---")
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
