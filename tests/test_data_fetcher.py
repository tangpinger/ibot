import unittest
from unittest.mock import patch, MagicMock, ANY, call
import sys
import pandas as pd
import ccxt # For ccxt.ExchangeError
from pathlib import Path

# Add project root to sys.path to allow importing owl modules
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from owl.data_fetcher.fetcher import DataFetcher # noqa E402

class TestDataFetcherProxyConfiguration(unittest.TestCase):

    @patch('ccxt.okx') # Patching 'ccxt.okx' as a common default or example
    def test_data_fetcher_with_proxy(self, MockExchangeClass):
        """
        Tests that DataFetcher initializes the ccxt exchange with correct proxy settings
        when proxy_url is provided.
        """
        mock_exchange_instance = MagicMock()
        MockExchangeClass.return_value = mock_exchange_instance

        api_key = "test_api_key"
        secret_key = "test_secret_key"
        proxy_url = "socks5h://user:pass@host:port"
        proxy_type = "socks5h" # Though type is often in URL, it's a param
        exchange_id = 'okx'

        fetcher = DataFetcher(
            api_key=api_key,
            secret_key=secret_key,
            exchange_id=exchange_id,
            proxy_url=proxy_url,
            proxy_type=proxy_type
        )

        expected_proxies_dict = {
            'http': proxy_url,
            'https': proxy_url,
        }
        
        expected_config = {
            'apiKey': api_key,
            'secret': secret_key,
            'verbose': False,
            'aiohttp_proxy': proxy_url,
            'requests_proxy': expected_proxies_dict,
            'proxies': expected_proxies_dict, # ccxt adds this too
            # 'password' is not provided, so it should not be in config
        }

        MockExchangeClass.assert_called_once_with(expected_config)
        mock_exchange_instance.load_markets.assert_called_once()
        self.assertEqual(fetcher.exchange_id, exchange_id)

    @patch('ccxt.binance') # Using a different exchange for variety
    def test_data_fetcher_without_proxy(self, MockExchangeClass):
        """
        Tests that DataFetcher initializes the ccxt exchange without proxy settings
        when proxy_url is not provided.
        """
        mock_exchange_instance = MagicMock()
        MockExchangeClass.return_value = mock_exchange_instance

        api_key = "another_api_key"
        secret_key = "another_secret_key"
        exchange_id = 'binance' # Ensure this matches the patch

        fetcher = DataFetcher(
            api_key=api_key,
            secret_key=secret_key,
            exchange_id=exchange_id
            # No proxy_url or proxy_type
        )
        
        expected_config = {
            'apiKey': api_key,
            'secret': secret_key,
            'verbose': False,
            # No proxy settings should be present
        }

        MockExchangeClass.assert_called_once_with(expected_config)
        mock_exchange_instance.load_markets.assert_called_once()
        self.assertEqual(fetcher.exchange_id, exchange_id)

    @patch('ccxt.okx')
    def test_data_fetcher_api_password_present(self, MockExchangeClass):
        """
        Tests that 'password' is included in config if provided.
        """
        mock_exchange_instance = MagicMock()
        MockExchangeClass.return_value = mock_exchange_instance
        
        api_key = "test_api_key"
        secret_key = "test_secret_key"
        password = "test_password"
        exchange_id = 'okx'

        DataFetcher(
            api_key=api_key,
            secret_key=secret_key,
            password=password,
            exchange_id=exchange_id
        )

        expected_config = {
            'apiKey': api_key,
            'secret': secret_key,
            'password': password,
            'verbose': False,
        }
        MockExchangeClass.assert_called_once_with(expected_config)
        mock_exchange_instance.load_markets.assert_called_once()

    @patch('ccxt.okx')
    def test_data_fetcher_sandbox_mode(self, MockExchangeClass):
        """
        Tests that set_sandbox_mode is called if is_sandbox_mode is True
        and the exchange object has the method.
        """
        mock_exchange_instance = MagicMock()
        # Simulate the exchange having set_sandbox_mode
        mock_exchange_instance.set_sandbox_mode = MagicMock()
        MockExchangeClass.return_value = mock_exchange_instance
        
        exchange_id = 'okx'
        fetcher = DataFetcher(exchange_id=exchange_id, is_sandbox_mode=True)

        MockExchangeClass.assert_called_once_with({'verbose': False}) # No API keys, etc.
        mock_exchange_instance.set_sandbox_mode.assert_called_once_with(True)
        mock_exchange_instance.load_markets.assert_called_once()
        self.assertTrue(fetcher.exchange.set_sandbox_mode.called)

    @patch('ccxt.kraken') # Exchange that might not have set_sandbox_mode
    def test_data_fetcher_sandbox_mode_fallback(self, MockExchangeClass):
        """
        Tests sandbox mode fallback behavior if set_sandbox_mode is not available
        but 'test' URL is.
        """
        mock_exchange_instance = MagicMock()
        # Simulate no set_sandbox_mode, but has a 'test' url
        del mock_exchange_instance.set_sandbox_mode # Ensure it's not there
        mock_exchange_instance.urls = {'api': 'real_api_url', 'test': 'test_api_url'}
        MockExchangeClass.return_value = mock_exchange_instance
        
        exchange_id = 'kraken' # Ensure this matches the patch
        fetcher = DataFetcher(exchange_id=exchange_id, is_sandbox_mode=True)

        MockExchangeClass.assert_called_once_with({'verbose': False})
        self.assertEqual(fetcher.exchange.urls['api'], 'test_api_url')
        mock_exchange_instance.load_markets.assert_called_once()

# Helper function to generate mock OHLCV data
def _generate_mock_ohlcv_data(start_ts, count, interval_ms=60000):
    """Generates a list of OHLCV data."""
    data = []
    for i in range(count):
        ts = start_ts + (i * interval_ms)
        # [timestamp, open, high, low, close, volume]
        data.append([ts, 100+i, 110+i, 90+i, 105+i, 10+i])
    return data

class TestFetchOHLCV(unittest.TestCase):
    def setUp(self):
        """Set up a DataFetcher instance for testing fetch_ohlcv."""
        # We patch 'ccxt.okx' or a generic exchange_id used for DataFetcher init,
        # then replace self.fetcher.exchange with another MagicMock for fine-grained control
        # of fetch_ohlcv calls.
        with patch('ccxt.okx') as MockExchangeClass:
            self.mock_exchange_init_instance = MagicMock()
            MockExchangeClass.return_value = self.mock_exchange_init_instance
            self.fetcher = DataFetcher(exchange_id='okx') # API keys not needed for these tests

        # This mock_exchange will be used to mock specific method calls like fetch_ohlcv
        self.fetcher.exchange = MagicMock()

    def test_fetch_ohlcv_no_since_no_looping(self):
        """Tests fetch_ohlcv behavior when 'since' is None (no looping)."""
        mock_data = _generate_mock_ohlcv_data(1678886400000, 50) # 50 candles
        self.fetcher.exchange.has = {'fetchOHLCV': True}
        self.fetcher.exchange.fetch_ohlcv.return_value = mock_data

        df = self.fetcher.fetch_ohlcv(symbol='BTC/USDT', timeframe='1d', limit=50)

        self.fetcher.exchange.fetch_ohlcv.assert_called_once_with('BTC/USDT', '1d', None, 50, {})
        self.assertEqual(len(df), 50)
        self.assertEqual(df.iloc[0]['timestamp'], pd.to_datetime(mock_data[0][0], unit='ms'))
        self.assertEqual(list(df.columns), ['timestamp', 'open', 'high', 'low', 'close', 'volume'])

    def test_fetch_ohlcv_with_since_looping_multiple_calls(self):
        """Tests the looping mechanism when 'since' is provided and multiple calls are needed."""
        self.fetcher.exchange.has = {'fetchOHLCV': True}

        start_ts = 1678886400000
        chunk1 = _generate_mock_ohlcv_data(start_ts, 100, interval_ms=60000)
        ts_after_chunk1 = chunk1[-1][0] + 1
        chunk2 = _generate_mock_ohlcv_data(ts_after_chunk1, 100, interval_ms=60000)
        ts_after_chunk2 = chunk2[-1][0] + 1
        chunk3 = _generate_mock_ohlcv_data(ts_after_chunk2, 50, interval_ms=60000)

        # Configure the mock to return different chunks on successive calls
        self.fetcher.exchange.fetch_ohlcv.side_effect = [chunk1, chunk2, chunk3]

        df = self.fetcher.fetch_ohlcv(symbol='ETH/USDT', timeframe='1h', since=start_ts)

        self.assertEqual(self.fetcher.exchange.fetch_ohlcv.call_count, 3)
        calls = [
            unittest.mock.call('ETH/USDT', '1h', start_ts, 100, {}),
            unittest.mock.call('ETH/USDT', '1h', ts_after_chunk1, 100, {}),
            unittest.mock.call('ETH/USDT', '1h', ts_after_chunk2, 100, {}),
        ]
        self.fetcher.exchange.fetch_ohlcv.assert_has_calls(calls)

        self.assertEqual(len(df), 250) # 100 + 100 + 50
        self.assertEqual(df.iloc[0]['timestamp'], pd.to_datetime(chunk1[0][0], unit='ms'))
        self.assertEqual(df.iloc[100]['timestamp'], pd.to_datetime(chunk2[0][0], unit='ms'))
        self.assertEqual(df.iloc[200]['timestamp'], pd.to_datetime(chunk3[0][0], unit='ms'))
        self.assertEqual(df.iloc[-1]['timestamp'], pd.to_datetime(chunk3[-1][0], unit='ms'))

    def test_fetch_ohlcv_with_since_looping_single_call_data_less_than_limit(self):
        """Tests looping when all data (less than fetch_limit) is fetched in the first call."""
        self.fetcher.exchange.has = {'fetchOHLCV': True}
        start_ts = 1678886400000
        mock_data = _generate_mock_ohlcv_data(start_ts, 30) # 30 candles, less than 100

        self.fetcher.exchange.fetch_ohlcv.return_value = mock_data

        df = self.fetcher.fetch_ohlcv(symbol='LTC/USDT', timeframe='5m', since=start_ts)

        self.fetcher.exchange.fetch_ohlcv.assert_called_once_with('LTC/USDT', '5m', start_ts, 100, {})
        self.assertEqual(len(df), 30)
        self.assertEqual(df.iloc[0]['timestamp'], pd.to_datetime(mock_data[0][0], unit='ms'))

    def test_fetch_ohlcv_with_since_looping_empty_return_initially(self):
        """Tests looping when the first fetch call returns no data."""
        self.fetcher.exchange.has = {'fetchOHLCV': True}
        start_ts = 1678886400000
        self.fetcher.exchange.fetch_ohlcv.return_value = [] # Empty list

        df = self.fetcher.fetch_ohlcv(symbol='ADA/USDT', timeframe='15m', since=start_ts)

        self.fetcher.exchange.fetch_ohlcv.assert_called_once_with('ADA/USDT', '15m', start_ts, 100, {})
        self.assertTrue(df.empty)
        self.assertEqual(list(df.columns), ['timestamp', 'open', 'high', 'low', 'close', 'volume'])

    def test_fetch_ohlcv_with_since_looping_empty_return_mid_loop(self):
        """Tests looping that terminates when an empty list is returned mid-sequence."""
        self.fetcher.exchange.has = {'fetchOHLCV': True}
        start_ts = 1678886400000
        chunk1 = _generate_mock_ohlcv_data(start_ts, 100)
        ts_after_chunk1 = chunk1[-1][0] + 1

        self.fetcher.exchange.fetch_ohlcv.side_effect = [chunk1, []] # First data, then empty

        df = self.fetcher.fetch_ohlcv(symbol='XRP/USDT', timeframe='1d', since=start_ts)

        self.assertEqual(self.fetcher.exchange.fetch_ohlcv.call_count, 2)
        calls = [
            unittest.mock.call('XRP/USDT', '1d', start_ts, 100, {}),
            unittest.mock.call('XRP/USDT', '1d', ts_after_chunk1, 100, {}),
        ]
        self.fetcher.exchange.fetch_ohlcv.assert_has_calls(calls)
        self.assertEqual(len(df), 100) # Only data from the first chunk
        self.assertEqual(df.iloc[0]['timestamp'], pd.to_datetime(chunk1[0][0], unit='ms'))

    def test_fetch_ohlcv_exchange_does_not_support_fetchOHLCV(self):
        """Tests behavior when the exchange does not support fetchOHLCV."""
        self.fetcher.exchange.has = {'fetchOHLCV': False} # Simulate no support

        df = self.fetcher.fetch_ohlcv(symbol='BTC/USDT', timeframe='1d')

        self.assertIsNone(df)
        self.fetcher.exchange.fetch_ohlcv.assert_not_called()

    def test_fetch_ohlcv_returns_empty_list_no_since(self):
        """Tests when fetch_ohlcv (no loop) returns an empty list."""
        self.fetcher.exchange.has = {'fetchOHLCV': True}
        self.fetcher.exchange.fetch_ohlcv.return_value = []

        df = self.fetcher.fetch_ohlcv(symbol='BTC/USDT', timeframe='1d', limit=20)

        self.fetcher.exchange.fetch_ohlcv.assert_called_once_with('BTC/USDT', '1d', None, 20, {})
        self.assertTrue(df.empty)
        self.assertEqual(list(df.columns), ['timestamp', 'open', 'high', 'low', 'close', 'volume'])

    @patch('builtins.print') # To suppress print statements during error handling tests
    def test_fetch_ohlcv_handles_exchange_error(self, mock_print):
        """Tests that exchange errors are caught and None is returned."""
        self.fetcher.exchange.has = {'fetchOHLCV': True}
        self.fetcher.exchange.fetch_ohlcv.side_effect = ccxt.ExchangeError("Test Exchange Error")

        df = self.fetcher.fetch_ohlcv(symbol='BTC/USDT', timeframe='1d', since=1678886400000)

        self.assertIsNone(df)
        # Ensure print was called with the error message
        mock_print.assert_any_call(unittest.mock.ANY) # Check if print was called for the error


if __name__ == '__main__':
    unittest.main()
