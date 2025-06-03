# tests/test_data_fetcher.py
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import datetime

# Adjust import path as necessary if your project structure is different
# Assuming 'owl' is a top-level package and this test file is in tests/
from owl.data_fetcher.fetcher import DataFetcher

# Helper to create mock OHLCV data
def create_mock_ohlcv_candles(start_ts, timeframe_ms, count):
    return [[start_ts + i * timeframe_ms, 100+i, 110+i, 90+i, 105+i, 1000+i*10] for i in range(count)]

class TestDataFetcherFetchOHLCV(unittest.TestCase):

    def setUp(self):
        self.mock_exchange_instance = MagicMock()
        self.mock_exchange_instance.has = {'fetchOHLCV': True}
        self.timeframe_ms = 60 * 1000  # 1 minute in milliseconds
        # parse_timeframe in ccxt returns seconds, so we mock it to return seconds
        self.mock_exchange_instance.parse_timeframe = MagicMock(return_value=self.timeframe_ms / 1000)

        # Patch the specific exchange class used by DataFetcher, e.g., ccxt.okx
        # The DataFetcher defaults to 'okx'
        self.exchange_patcher = patch('ccxt.okx', return_value=self.mock_exchange_instance)

        self.mock_ccxt_exchange_class = self.exchange_patcher.start() # Start patcher

        # Initialize DataFetcher, it will use the patched exchange class
        self.fetcher = DataFetcher(exchange_id='okx')
        # Crucially, ensure the fetcher instance uses our fully mocked exchange object
        self.fetcher.exchange = self.mock_exchange_instance


    def tearDown(self):
        self.exchange_patcher.stop() # Stop patcher

    def test_fetch_ohlcv_exchange_does_not_support(self):
        self.fetcher.exchange.has['fetchOHLCV'] = False # Modify the mock directly
        result = self.fetcher.fetch_ohlcv("BTC/USDT", "1m")
        self.assertIsNone(result)
        self.fetcher.exchange.has['fetchOHLCV'] = True # Reset for other tests

    def test_fetch_ohlcv_no_since_no_limit(self):
        mock_data = create_mock_ohlcv_candles(start_ts=1672531200000, timeframe_ms=self.timeframe_ms, count=10)
        self.fetcher.exchange.fetch_ohlcv = MagicMock(return_value=mock_data)

        df = self.fetcher.fetch_ohlcv(symbol="BTC/USDT", timeframe='1m')

        self.fetcher.exchange.fetch_ohlcv.assert_called_once_with(
            "BTC/USDT", '1m', None, None, {}
        )
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 10)
        self.assertEqual(list(df.columns), ['timestamp', 'open', 'high', 'low', 'close', 'volume'])

    def test_fetch_ohlcv_no_since_with_limit(self):
        mock_data = create_mock_ohlcv_candles(start_ts=1672531200000, timeframe_ms=self.timeframe_ms, count=5)
        self.fetcher.exchange.fetch_ohlcv = MagicMock(return_value=mock_data)

        df = self.fetcher.fetch_ohlcv(symbol="BTC/USDT", timeframe='1m', limit=5)

        self.fetcher.exchange.fetch_ohlcv.assert_called_once_with(
            "BTC/USDT", '1m', None, 5, {}
        )
        self.assertEqual(len(df), 5)

    def test_fetch_ohlcv_with_since_no_limit_multiple_batches(self):
        start_ts = 1672531200000  # 2023-01-01 00:00:00 UTC

        def fetch_ohlcv_side_effect(symbol, timeframe, since, limit, params):
            # SUT internal batch limit is 100
            self.assertEqual(limit, 100)
            if since == start_ts:
                return create_mock_ohlcv_candles(start_ts, self.timeframe_ms, 100)
            elif since == start_ts + 100 * self.timeframe_ms:
                return create_mock_ohlcv_candles(since, self.timeframe_ms, 50)
            elif since == start_ts + 150 * self.timeframe_ms:
                return []
            self.fail(f"Unexpected call to fetch_ohlcv with since={since}")
            # return [] # Not needed due to self.fail

        self.fetcher.exchange.fetch_ohlcv = MagicMock(side_effect=fetch_ohlcv_side_effect)

        df = self.fetcher.fetch_ohlcv(symbol="BTC/USDT", timeframe='1m', since=start_ts)

        self.assertEqual(self.fetcher.exchange.fetch_ohlcv.call_count, 3)
        self.assertEqual(len(df), 150)
        for i in range(150):
            expected_ts = pd.to_datetime(start_ts + i * self.timeframe_ms, unit='ms')
            self.assertEqual(df['timestamp'].iloc[i], expected_ts)

    def test_fetch_ohlcv_with_since_and_limit_less_than_internal_batch(self):
        start_ts = 1672531200000
        limit_val = 5
        mock_data = create_mock_ohlcv_candles(start_ts, self.timeframe_ms, limit_val)
        self.fetcher.exchange.fetch_ohlcv = MagicMock(return_value=mock_data)

        df = self.fetcher.fetch_ohlcv(symbol="BTC/USDT", timeframe='1m', since=start_ts, limit=limit_val)

        self.fetcher.exchange.fetch_ohlcv.assert_called_once_with(
            "BTC/USDT", '1m', since=start_ts, limit=limit_val, params={}
        )
        self.assertEqual(len(df), limit_val)

    def test_fetch_ohlcv_with_since_and_limit_spanning_batches(self):
        start_ts = 1672531200000
        user_limit = 150 # User wants 150 candles
        # SUT internal batch limit is 100

        def fetch_ohlcv_side_effect(symbol, timeframe, since, limit, params):
            if since == start_ts:
                self.assertEqual(limit, 100) # First call: min(150, 100)
                return create_mock_ohlcv_candles(start_ts, self.timeframe_ms, 100)
            elif since == start_ts + 100 * self.timeframe_ms:
                self.assertEqual(limit, 50)  # Second call: min(150-100, 100)
                return create_mock_ohlcv_candles(since, self.timeframe_ms, 50)
            self.fail(f"Unexpected call to fetch_ohlcv with since={since}, limit={limit}")
            # return [] # Not needed

        self.fetcher.exchange.fetch_ohlcv = MagicMock(side_effect=fetch_ohlcv_side_effect)
        df = self.fetcher.fetch_ohlcv(symbol="BTC/USDT", timeframe='1m', since=start_ts, limit=user_limit)

        self.assertEqual(self.fetcher.exchange.fetch_ohlcv.call_count, 2)
        self.assertEqual(len(df), user_limit)
        expected_last_ts = pd.to_datetime(start_ts + (user_limit - 1) * self.timeframe_ms, unit='ms')
        self.assertEqual(df['timestamp'].iloc[-1], expected_last_ts)
        
    def test_fetch_ohlcv_with_since_and_limit_more_than_available(self):
        start_ts = 1672531200000
        user_limit = 150 # User wants 150
        # Exchange only has 120 total

        def fetch_ohlcv_side_effect(symbol, timeframe, since, limit, params):
            if since == start_ts: # Requesting min(150, 100) = 100
                self.assertEqual(limit, 100)
                return create_mock_ohlcv_candles(start_ts, self.timeframe_ms, 100) # Returns 100
            elif since == start_ts + 100 * self.timeframe_ms: # Requesting min(50, 100) = 50
                self.assertEqual(limit, 50)
                return create_mock_ohlcv_candles(since, self.timeframe_ms, 20) # Returns only 20 (end of data)
            elif since == start_ts + 120 * self.timeframe_ms: # Requesting min(150 - 100 - 20, 100) = 30
                 self.assertEqual(limit,30)
                 return [] # No more data
            self.fail(f"Unexpected call to fetch_ohlcv with since={since}, limit={limit}")
            # return [] # Not needed

        self.fetcher.exchange.fetch_ohlcv = MagicMock(side_effect=fetch_ohlcv_side_effect)
        df = self.fetcher.fetch_ohlcv(symbol="BTC/USDT", timeframe='1m', since=start_ts, limit=user_limit)
        
        self.assertEqual(self.fetcher.exchange.fetch_ohlcv.call_count, 3)
        self.assertEqual(len(df), 120)

    def test_fetch_ohlcv_no_data_returned_with_since(self):
        start_ts = 1672531200000
        self.fetcher.exchange.fetch_ohlcv = MagicMock(return_value=[])

        df = self.fetcher.fetch_ohlcv(symbol="BTC/USDT", timeframe='1m', since=start_ts)

        self.fetcher.exchange.fetch_ohlcv.assert_called_once() # Called once with current_batch_limit = 100
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(df.empty)
        self.assertEqual(list(df.columns), ['timestamp', 'open', 'high', 'low', 'close', 'volume'])

    def test_fetch_ohlcv_limit_is_zero_with_since(self):
        start_ts = 1672531200000
        self.fetcher.exchange.fetch_ohlcv = MagicMock()
        
        df = self.fetcher.fetch_ohlcv(symbol="BTC/USDT", timeframe='1m', since=start_ts, limit=0)
        
        self.fetcher.exchange.fetch_ohlcv.assert_not_called()
        self.assertTrue(df.empty)
        self.assertEqual(list(df.columns), ['timestamp', 'open', 'high', 'low', 'close', 'volume'])


    def test_fetch_ohlcv_limit_is_zero_no_since(self):
        self.fetcher.exchange.fetch_ohlcv = MagicMock(return_value=[]) # Assume ccxt returns empty for limit=0

        df = self.fetcher.fetch_ohlcv(symbol="BTC/USDT", timeframe='1m', limit=0)

        self.fetcher.exchange.fetch_ohlcv.assert_called_once_with("BTC/USDT", '1m', None, 0, {})
        self.assertTrue(df.empty)
        self.assertEqual(list(df.columns), ['timestamp', 'open', 'high', 'low', 'close', 'volume'])

if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
