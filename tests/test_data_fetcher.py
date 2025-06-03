import unittest
from unittest.mock import patch, MagicMock
import os
import shutil
import pickle
import pandas as pd
from datetime import datetime

# Add project root to sys.path to allow importing owl modules
import sys
from pathlib import Path
project_root_path = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root_path))

from owl.data_fetcher.fetcher import DataFetcher

class TestDataFetcherCaching(unittest.TestCase):

    def setUp(self):
        """Set up for test methods."""
        self.cache_dir = ".cache"
        # Ensure a clean state before each test
        if os.path.exists(self.cache_dir):
            shutil.rmtree(self.cache_dir)

        # Minimal config for DataFetcher instantiation
        # No API keys needed as we'll mock the actual exchange calls
        self.fetcher = DataFetcher(exchange_id='okx') # 'okx' is a valid ccxt exchange ID

        # Sample OHLCV data that the mocked exchange will return
        self.sample_ohlcv_data_raw = [
            [datetime(2023, 1, 1, 0, 0).timestamp() * 1000, 100, 110, 90, 105, 1000],
            [datetime(2023, 1, 1, 1, 0).timestamp() * 1000, 105, 115, 95, 110, 1200],
        ]
        self.sample_ohlcv_df = pd.DataFrame(
            self.sample_ohlcv_data_raw,
            columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
        )
        self.sample_ohlcv_df['timestamp'] = pd.to_datetime(self.sample_ohlcv_df['timestamp'], unit='ms')

    def tearDown(self):
        """Clean up after test methods."""
        if os.path.exists(self.cache_dir):
            shutil.rmtree(self.cache_dir)

    def test_caching_and_force_fetch(self):
        """Test caching behavior and force_fetch functionality."""
        symbol = "TEST/USDT"
        timeframe = "1h"
        since = int(datetime(2023, 1, 1, 0, 0).timestamp() * 1000)

        # --- 1. First call: Data should be fetched from exchange and cached ---
        # Mock the underlying ccxt exchange's fetch_ohlcv method
        with patch.object(self.fetcher.exchange, 'fetch_ohlcv', return_value=self.sample_ohlcv_data_raw) as mock_exchange_fetch:
            print("Test: First call to fetch_ohlcv (fetch from exchange)")
            df_fetched = self.fetcher.fetch_ohlcv(symbol=symbol, timeframe=timeframe, since=since)

            mock_exchange_fetch.assert_called_once()
            pd.testing.assert_frame_equal(df_fetched, self.sample_ohlcv_df)

            # Verify cache file was created
            cache_filename = f"okx_test_usdt_1h_{since}.pkl" # Based on DataFetcher's naming
            cache_filepath = os.path.join(self.cache_dir, cache_filename)
            self.assertTrue(os.path.exists(cache_filepath), "Cache file was not created.")

            # Verify content of cache file
            with open(cache_filepath, 'rb') as f:
                cached_df = pickle.load(f)
            pd.testing.assert_frame_equal(cached_df, self.sample_ohlcv_df)
            print("Test: Data fetched and cached successfully.")

        # --- 2. Second call: Data should be loaded from cache ---
        with patch.object(self.fetcher.exchange, 'fetch_ohlcv', return_value=self.sample_ohlcv_data_raw) as mock_exchange_fetch_cached:
            print("Test: Second call to fetch_ohlcv (load from cache)")
            df_cached_load = self.fetcher.fetch_ohlcv(symbol=symbol, timeframe=timeframe, since=since)

            mock_exchange_fetch_cached.assert_not_called() # Should not call exchange
            pd.testing.assert_frame_equal(df_cached_load, self.sample_ohlcv_df)
            print("Test: Data loaded from cache successfully.")

        # --- 3. Third call: force_fetch=True, data should be fetched from exchange again ---
        # New sample data for this forced fetch to ensure we are not getting stale cache
        forced_ohlcv_data_raw = [
            [datetime(2023, 1, 2, 0, 0).timestamp() * 1000, 200, 210, 190, 205, 2000],
        ]
        forced_ohlcv_df = pd.DataFrame(
            forced_ohlcv_data_raw,
            columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
        )
        forced_ohlcv_df['timestamp'] = pd.to_datetime(forced_ohlcv_df['timestamp'], unit='ms')

        with patch.object(self.fetcher.exchange, 'fetch_ohlcv', return_value=forced_ohlcv_data_raw) as mock_exchange_force_fetch:
            print("Test: Third call to fetch_ohlcv (force_fetch=True)")
            df_force_fetched = self.fetcher.fetch_ohlcv(symbol=symbol, timeframe=timeframe, since=since, force_fetch=True)

            mock_exchange_force_fetch.assert_called_once()
            pd.testing.assert_frame_equal(df_force_fetched, forced_ohlcv_df)

            # Verify cache file was updated
            with open(cache_filepath, 'rb') as f:
                cached_df_after_force = pickle.load(f)
            pd.testing.assert_frame_equal(cached_df_after_force, forced_ohlcv_df)
            print("Test: Data force-fetched and cache updated successfully.")

        # --- 4. Fourth call: Data should be loaded from the updated cache ---
        with patch.object(self.fetcher.exchange, 'fetch_ohlcv', return_value=forced_ohlcv_data_raw) as mock_exchange_cached_after_force:
            print("Test: Fourth call to fetch_ohlcv (load updated cache)")
            df_cached_load_after_force = self.fetcher.fetch_ohlcv(symbol=symbol, timeframe=timeframe, since=since)

            mock_exchange_cached_after_force.assert_not_called()
            pd.testing.assert_frame_equal(df_cached_load_after_force, forced_ohlcv_df)
            print("Test: Updated data loaded from cache successfully.")

if __name__ == '__main__':
    unittest.main()
