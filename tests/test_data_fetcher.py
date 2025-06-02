import unittest
from unittest.mock import patch, MagicMock, ANY
import sys
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
            'aiohttp_proxy': proxy_url,
            'requests_proxy': expected_proxies_dict,
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

        MockExchangeClass.assert_called_once_with({}) # No API keys, etc.
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

        MockExchangeClass.assert_called_once_with({})
        self.assertEqual(fetcher.exchange.urls['api'], 'test_api_url')
        mock_exchange_instance.load_markets.assert_called_once()


if __name__ == '__main__':
    unittest.main()
