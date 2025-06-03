import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import datetime, time
from owl.signal_generator.generator import SignalGenerator

class TestSignalGeneratorBuyWindow(unittest.TestCase):
    def setUp(self):
        # Using hourly data for tests. A span of 50 hours.
        self.base_datetime = datetime(2023, 10, 2, 0, 0) # A Monday 00:00
        self.timestamps = pd.to_datetime([self.base_datetime - pd.Timedelta(hours=i) for i in range(50, 0, -1)])
        self.data = {
            'timestamp': self.timestamps,
            'high': [100 + i + (i % 5) * 0.5 for i in range(50)], # Adjusted variations for hourly
            'low': [90 - i + (i % 3) * 0.5 for i in range(50)]   # Adjusted variations for hourly
        }
        self.historical_df = pd.DataFrame(self.data)

        # N/M periods are now in hours. E.g., 20-hour high. M-related properties removed.
        self.n_period = 20  # Look back 20 hours for high
        # self.m_period = 10 # Removed

        self.n_period_high_test = self.historical_df.tail(self.n_period)['high'].max()
        self.current_high_price = self.n_period_high_test + 1
        # self.m_period_low_test removed
        # self.current_low_price_breakdown removed
        # self.current_low_price_no_breakdown removed


        # Base datetimes for testing specific days/times, ensuring they are weekdays
        # For buy signal, use a Tuesday (weekday 1) for tests
        self.test_datetime_base_utc8 = datetime(2023, 10, 3, 0, 0) # Tuesday in UTC+8 context for tests
        # self.test_sell_datetime_base_utc8 removed

        # Default window times for tests that don't focus on them
        self.default_buy_start_str = "15:00"
        self.default_buy_end_str = "16:00"
        # self.default_sell_start_str removed
        # self.default_sell_end_str removed


    def test_valid_buy_window_and_time(self):
        sg = SignalGenerator(
            n_day_high_period=self.n_period,
            buy_window_start_time_str=self.default_buy_start_str, # 15:00
            buy_window_end_time_str=self.default_buy_end_str   # 16:00
            # Removed m_day_low_period and sell window params
        )
        # Time within window: 15:30 on a valid buy day (Tuesday)
        current_datetime_utc8 = self.test_datetime_base_utc8.replace(hour=15, minute=30, second=0)
        # Base datetimes for testing specific days/times, ensuring they are weekdays
        # For buy signal, use a Tuesday (weekday 1) for tests
        self.test_datetime_base_utc8 = datetime(2023, 10, 3, 0, 0) # Tuesday in UTC+8 context for tests
        # self.test_sell_datetime_base_utc8 removed

        # Default window times for tests that don't focus on them
        self.default_buy_start_str = "15:00"
        self.default_buy_end_str = "16:00"
        # self.default_sell_start_str removed
        # self.default_sell_end_str removed


    def test_valid_buy_window_and_time(self):
        sg = SignalGenerator(
            n_day_high_period=self.n_period,
            buy_window_start_time_str=self.default_buy_start_str, # 15:00
            buy_window_end_time_str=self.default_buy_end_str   # 16:00
            # Removed m_day_low_period and sell window params
        )
        # Time within window: 15:30 on a valid buy day (Tuesday)
        current_datetime_utc8 = self.test_datetime_base_utc8.replace(hour=15, minute=30, second=0)

        signal = sg.check_breakout_signal(
            daily_ohlcv_data=self.historical_df,
            current_day_high=self.current_high_price,
            current_datetime_utc8=current_datetime_utc8
        )
        self.assertEqual(signal, "BUY")

    def test_time_outside_buy_window(self):
        sg = SignalGenerator(
            n_day_high_period=self.n_period,
            buy_window_start_time_str=self.default_buy_start_str,
            buy_window_end_time_str=self.default_buy_end_str
        )
        # Time outside window: 14:50 on a valid buy day (Tuesday)
        current_datetime_utc8 = self.test_datetime_base_utc8.replace(hour=14, minute=50, second=0)

        signal = sg.check_breakout_signal(
            daily_ohlcv_data=self.historical_df,
            current_day_high=self.current_high_price,
            current_datetime_utc8=current_datetime_utc8
        )
        self.assertIsNone(signal)

    def test_time_at_start_of_buy_window(self):
        sg = SignalGenerator(
            n_day_high_period=self.n_period,
            buy_window_start_time_str=self.default_buy_start_str,
            buy_window_end_time_str=self.default_buy_end_str
        )
        # Time at start of window: 15:00 on a valid buy day (Tuesday)
        current_datetime_utc8 = self.test_datetime_base_utc8.replace(hour=15, minute=0, second=0)

        signal = sg.check_breakout_signal(
            daily_ohlcv_data=self.historical_df,
            current_day_high=self.current_high_price,
            current_datetime_utc8=current_datetime_utc8
        )
        self.assertEqual(signal, "BUY")

    def test_time_at_end_of_buy_window(self):
        sg = SignalGenerator(
            n_day_high_period=self.n_period,
            buy_window_start_time_str=self.default_buy_start_str,
            buy_window_end_time_str=self.default_buy_end_str
        )
        # Time at end of window: 16:00 on a valid buy day (Tuesday)
        current_datetime_utc8 = self.test_datetime_base_utc8.replace(hour=16, minute=0, second=0)

        signal = sg.check_breakout_signal(
            daily_ohlcv_data=self.historical_df,
            current_day_high=self.current_high_price,
            current_datetime_utc8=current_datetime_utc8
        )
        self.assertEqual(signal, "BUY")

    def test_malformed_start_time_string(self):
        # Suppress logging error for this specific test
        with patch('logging.error') as mock_log_error:
            sg = SignalGenerator(
                n_day_high_period=self.n_period,
                buy_window_start_time_str="invalid",
                buy_window_end_time_str=self.default_buy_end_str
            )
            current_datetime_utc8 = self.test_datetime_base_utc8.replace(hour=15, minute=30, second=0)

            signal = sg.check_breakout_signal(
                daily_ohlcv_data=self.historical_df,
                current_day_high=self.current_high_price,
                current_datetime_utc8=current_datetime_utc8
            )
            self.assertIsNone(signal)
            mock_log_error.assert_called_with("Invalid format for buy_window_start_time_str: 'invalid'. Expected HH:MM. Buy window check will be disabled.")

    def test_malformed_end_time_string(self):
        with patch('logging.error') as mock_log_error:
            sg = SignalGenerator(
                n_day_high_period=self.n_period,
                buy_window_start_time_str=self.default_buy_start_str,
                buy_window_end_time_str="invalid"
            )
            current_datetime_utc8 = self.test_datetime_base_utc8.replace(hour=15, minute=30, second=0) # Use updated base

            signal = sg.check_breakout_signal(
                daily_ohlcv_data=self.historical_df,
                current_day_high=self.current_high_price,
                current_datetime_utc8=current_datetime_utc8
            )
            self.assertIsNone(signal)
            mock_log_error.assert_called_with("Invalid format for buy_window_end_time_str: 'invalid'. Expected HH:MM. Buy window check will be disabled.")

    def test_malformed_both_time_strings(self):
        with patch('logging.error') as mock_log_error:
            sg = SignalGenerator(
                n_day_high_period=self.n_period,
                buy_window_start_time_str="bad_start",
                buy_window_end_time_str="bad_end"
            )
            current_datetime_utc8 = self.test_datetime_base_utc8.replace(hour=15, minute=30, second=0)
            signal = sg.check_breakout_signal(
                daily_ohlcv_data=self.historical_df,
                current_day_high=self.current_high_price,
                current_datetime_utc8=current_datetime_utc8
            )
            self.assertIsNone(signal)
            self.assertEqual(mock_log_error.call_count, 2) # Both buy window times are malformed

# TestSignalGeneratorSellWindow class and all its methods are removed.

if __name__ == '__main__':
    unittest.main()
