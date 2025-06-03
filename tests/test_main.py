import unittest
from unittest.mock import patch, MagicMock, mock_open
import sys
import argparse
# Assuming ConfigError is accessible for import, or adjust as needed
from owl.config_manager.config import ConfigError
# Import main function if it's directly callable, or structure to call it
from owl.main import main as owl_main

class TestMainSignalGeneratorInstantiation(unittest.TestCase):

    @patch('owl.main.load_config')
    @patch('owl.main.DataFetcher')
    @patch('owl.main.SignalGenerator')
    @patch('owl.main.BacktestingEngine')
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_passes_buy_window_to_signal_generator(
            self, mock_parse_args, mock_backtesting_engine,
            mock_signal_generator, mock_data_fetcher, mock_load_config):

        # Setup mock for command line arguments
        mock_args = MagicMock()
        mock_args.mode = 'backtest'
        mock_parse_args.return_value = mock_args

        # Setup mock for load_config
        sample_config = {
            'proxy': {},
            'api_keys': {
                'okx_api_key': 'test_key',
                'okx_secret_key': 'test_secret',
                'okx_password': 'test_password'
            },
            'exchange_settings': {
                'exchange_id': 'okx',
                'sandbox_mode': True,
            },
            'strategy': {
                'n_day_high_period': 20,
                'buy_cash_percentage': 0.80,
                'risk_free_rate': 0.02
            },
            'scheduler': {
                'buy_check_time': "15:45",
                'buy_execute_time': "16:05"
            },
            'backtesting': {
                'symbol': 'BTC/USDT',
                'timeframe': '1d',
                'start_date': '2023-01-01',
                'initial_capital': 10000,
                'commission_rate': 0.001
            }
        }
        mock_load_config.return_value = sample_config

        # Mock instances of DataFetcher and BacktestingEngine
        mock_data_fetcher_instance = MagicMock()
        mock_data_fetcher.return_value = mock_data_fetcher_instance

        mock_engine_instance = MagicMock()
        mock_backtesting_engine.return_value = mock_engine_instance

        # Call the main function
        owl_main()

        # Assertions
        mock_signal_generator.assert_called_once()
        args, kwargs = mock_signal_generator.call_args

        self.assertEqual(kwargs.get('n_day_high_period'), 20)
        self.assertEqual(kwargs.get('buy_window_start_time_str'), "15:45")
        self.assertEqual(kwargs.get('buy_window_end_time_str'), "16:05")

    @patch('owl.main.load_config')
    @patch('argparse.ArgumentParser.parse_args')
    @patch('sys.exit') # Mock sys.exit
    def test_main_exits_if_buy_check_time_missing(
            self, mock_sys_exit, mock_parse_args, mock_load_config):

        mock_args = MagicMock()
        mock_args.mode = 'backtest'
        mock_parse_args.return_value = mock_args

        # Config missing 'buy_check_time'
        sample_config_missing_key = {
            'proxy': {},
            'api_keys': {'okx_api_key': 'k', 'okx_secret_key': 's', 'okx_password': 'p'},
            'exchange_settings': {'exchange_id': 'okx', 'sandbox_mode': True},
            'strategy': {'n_day_high_period': 20},
            'scheduler': {
                # 'buy_check_time': "15:45", # Missing
                'buy_execute_time': "16:05"
            },
            'backtesting': {'initial_capital': 1000, 'commission_rate': 0.001, 'symbol': 's', 'start_date':'d'}
        }
        mock_load_config.return_value = sample_config_missing_key

        # Call main
        owl_main()

        # Assert sys.exit was called
        mock_sys_exit.assert_called_with(1)

    @patch('owl.main.load_config')
    @patch('argparse.ArgumentParser.parse_args')
    @patch('sys.exit') # Mock sys.exit
    def test_main_exits_if_buy_execute_time_missing(
            self, mock_sys_exit, mock_parse_args, mock_load_config):

        mock_args = MagicMock()
        mock_args.mode = 'backtest'
        mock_parse_args.return_value = mock_args

        sample_config_missing_key = {
            'proxy': {},
            'api_keys': {'okx_api_key': 'k', 'okx_secret_key': 's', 'okx_password': 'p'},
            'exchange_settings': {'exchange_id': 'okx', 'sandbox_mode': True},
            'strategy': {'n_day_high_period': 20},
            'scheduler': {
                'buy_check_time': "15:45",
                # 'buy_execute_time': "16:05" # Missing
            },
            'backtesting': {'initial_capital': 1000, 'commission_rate': 0.001, 'symbol': 's', 'start_date':'d'}
        }
        mock_load_config.return_value = sample_config_missing_key

        owl_main()
        mock_sys_exit.assert_called_with(1)

if __name__ == '__main__':
    unittest.main()
