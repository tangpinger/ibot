import unittest
from unittest.mock import patch, MagicMock, call
import pandas as pd
from datetime import datetime
import pytz
from owl.backtesting_engine.engine import BacktestingEngine
# Need to import SignalGenerator and DataFetcher if they are type hinted in BacktestingEngine
# For mocking, direct import might not be strictly necessary if we patch their instantiation path
# from owl.signal_generator.generator import SignalGenerator # Assuming path
# from owl.data_fetcher.fetcher import DataFetcher # Assuming path

class TestBacktestingEngineTimestampAdjustment(unittest.TestCase):

    def setUp(self):
        self.mock_data_fetcher = MagicMock()
        self.mock_signal_generator = MagicMock()

        # Default config
        self.sample_config = {
            'backtesting': {
                'symbol': 'BTC/USDT',
                'timeframe': '1d',
                'start_date': '2023-01-01', # Needs to be this format for engine's date parsing
                'end_date': '2023-01-02',
                'initial_capital': 10000.0,
                'commission_rate': 0.001
            },
            'strategy': {
                'n_day_high_period': 1, # Small period to ensure signal logic is reached quickly
                'buy_cash_percentage': 0.80,
                'risk_free_rate': 0.02 # For report generation
            },
            'scheduler': {
                'buy_check_time': "15:50" # HH:MM
            },
            # Add other sections if BacktestingEngine init or methods directly access them before signal check
            'proxy': {},
            'api_keys': {},
            'exchange_settings': {}
        }

        # Sample OHLCV data. Timestamp is crucial.
        # Let's assume DataFetcher returns UTC timestamps for OHLCV data (00:00 UTC for daily candles)
        self.sample_ohlcv_data = pd.DataFrame({
            'timestamp': pd.to_datetime(['2023-01-01 00:00:00', '2023-01-02 00:00:00'], utc=True),
            'open': [100, 105],
            'high': [110, 115], # current_day_high will be this
            'low': [90, 95],
            'close': [105, 110], # current_close_price
            'volume': [1000, 1200]
        })
        self.mock_data_fetcher.fetch_ohlcv.return_value = self.sample_ohlcv_data

        # Mock the signal generator's check_breakout_signal method
        self.mock_signal_generator.check_breakout_signal = MagicMock(return_value=None) # Default to no signal


    def test_timestamp_adjustment_for_signal_generator(self):
        engine = BacktestingEngine(
            config=self.sample_config,
            data_fetcher=self.mock_data_fetcher,
            signal_generator=self.mock_signal_generator
        )
        engine.run_backtest()

        # Assertions
        # The loop runs for each row of data. We need to check calls to check_breakout_signal.
        # Signal generation is skipped for the first n_period (1 in this case). So, one call for 2023-01-02.
        self.assertTrue(self.mock_signal_generator.check_breakout_signal.called)

        # Get the arguments of the first call (and likely only call in this setup)
        args, kwargs = self.mock_signal_generator.check_breakout_signal.call_args

        # Extract the current_datetime_utc8 passed to the signal generator
        passed_datetime_utc8 = kwargs.get('current_datetime_utc8')
        self.assertIsNotNone(passed_datetime_utc8)

        # Verify its components
        # Date should be 2023-01-02 (from the second row of OHLCV data)
        self.assertEqual(passed_datetime_utc8.year, 2023)
        self.assertEqual(passed_datetime_utc8.month, 1)
        self.assertEqual(passed_datetime_utc8.day, 2)

        # Time should be 15:50 as per config['scheduler']['buy_check_time']
        self.assertEqual(passed_datetime_utc8.hour, 15)
        self.assertEqual(passed_datetime_utc8.minute, 50)
        self.assertEqual(passed_datetime_utc8.second, 0) # Explicitly set to 0 in engine

        # Ensure it's timezone-aware (Asia/Shanghai)
        self.assertEqual(passed_datetime_utc8.tzinfo.zone, 'Asia/Shanghai')

    def test_timestamp_defaults_to_data_time_if_buy_check_time_missing(self):
        # Modify config to remove buy_check_time
        del self.sample_config['scheduler']['buy_check_time']

        engine = BacktestingEngine(
            config=self.sample_config,
            data_fetcher=self.mock_data_fetcher,
            signal_generator=self.mock_signal_generator
        )

        with patch('logging.warning') as mock_log_warning:
            engine.run_backtest()

        self.assertTrue(self.mock_signal_generator.check_breakout_signal.called)
        args, kwargs = self.mock_signal_generator.check_breakout_signal.call_args
        passed_datetime_utc8 = kwargs.get('current_datetime_utc8')

        self.assertIsNotNone(passed_datetime_utc8)
        # Date should be 2023-01-02
        self.assertEqual(passed_datetime_utc8.year, 2023)
        self.assertEqual(passed_datetime_utc8.month, 1)
        self.assertEqual(passed_datetime_utc8.day, 2)

        # Time should be 00:00 (original time of the daily data, converted to UTC+8)
        # The sample_ohlcv_data is 2023-01-02 00:00:00 UTC. Converted to Asia/Shanghai is 08:00.
        # However, the code logic for timestamp conversion is:
        # current_datetime_utc8 = current_timestamp_utc.tz_localize('UTC').tz_convert('Asia/Shanghai')
        # This will convert 00:00 UTC to 08:00 Asia/Shanghai.
        # The *adjustment* part changes this. If adjustment fails, it should be this 08:00.
        # The previous subtask adjusted current_datetime_utc8 (which was 00:00 UTC for daily data)
        # to a specific time like 15:55. If 'buy_check_time' is missing, this adjustment is skipped.
        # The original `current_datetime_utc8` is derived from `current_timestamp_utc`.
        # `current_timestamp_utc` is `2023-01-02 00:00:00+00:00`.
        # `tz_convert('Asia/Shanghai')` makes it `2023-01-02 08:00:00+08:00`.
        # The change in the prior subtask was to *replace* the time component.
        # If `buy_check_time` is missing, it logs a warning and uses the *un-replaced* `current_datetime_utc8`.

        self.assertEqual(passed_datetime_utc8.hour, 8) # 00:00 UTC is 08:00 Asia/Shanghai
        self.assertEqual(passed_datetime_utc8.minute, 0)
        self.assertEqual(passed_datetime_utc8.tzinfo.zone, 'Asia/Shanghai')

        mock_log_warning.assert_any_call("Config `[scheduler][buy_check_time]` not found. Using original OHLCV data time for signal generation.")

    def test_timestamp_defaults_if_buy_check_time_malformed(self):
        self.sample_config['scheduler']['buy_check_time'] = "INVALID_TIME"

        engine = BacktestingEngine(
            config=self.sample_config,
            data_fetcher=self.mock_data_fetcher,
            signal_generator=self.mock_signal_generator
        )

        with patch('logging.warning') as mock_log_warning:
            engine.run_backtest()

        self.assertTrue(self.mock_signal_generator.check_breakout_signal.called)
        args, kwargs = self.mock_signal_generator.check_breakout_signal.call_args
        passed_datetime_utc8 = kwargs.get('current_datetime_utc8')

        self.assertIsNotNone(passed_datetime_utc8)
        self.assertEqual(passed_datetime_utc8.year, 2023)
        self.assertEqual(passed_datetime_utc8.month, 1)
        self.assertEqual(passed_datetime_utc8.day, 2)
        self.assertEqual(passed_datetime_utc8.hour, 8) # 00:00 UTC is 08:00 Asia/Shanghai
        self.assertEqual(passed_datetime_utc8.minute, 0)
        self.assertEqual(passed_datetime_utc8.tzinfo.zone, 'Asia/Shanghai')

        mock_log_warning.assert_any_call(f"Malformed `buy_check_time` string: 'INVALID_TIME'. Expected HH:MM format. Using original OHLCV data time.")


if __name__ == '__main__':
    unittest.main()
