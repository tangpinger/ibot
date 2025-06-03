import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import datetime, time
from owl.signal_generator.generator import SignalGenerator

class TestSignalGeneratorBuyWindow(unittest.TestCase):
    def setUp(self):
        # Dummy historical data that would cause a breakout
        self.base_date = datetime(2023, 10, 1)
        self.data = {
            'timestamp': pd.to_datetime([self.base_date - pd.Timedelta(days=i) for i in range(30, 0, -1)]),
            'high': [100 + i + (i % 5) * 5 for i in range(30)]
        }
        self.historical_df = pd.DataFrame(self.data)
        self.n_period = 20
        self.n_day_high_test = self.historical_df.tail(self.n_period)['high'].max()
        self.current_high_price = self.n_day_high_test + 1  # Ensure breakout

        # Valid buy day (Friday)
        self.valid_buy_day_datetime_base = datetime(2023, 10, 27) # A Friday

    def test_valid_buy_window_and_time(self):
        sg = SignalGenerator(
            n_day_high_period=self.n_period,
            buy_window_start_time_str="15:55",
            buy_window_end_time_str="16:00"
        )
        # Time within window: 15:58 on a valid buy day (Friday)
        current_datetime_utc8 = self.valid_buy_day_datetime_base.replace(hour=15, minute=58, second=0)

        signal = sg.check_breakout_signal(
            daily_ohlcv_data=self.historical_df,
            current_day_high=self.current_high_price,
            current_datetime_utc8=current_datetime_utc8
        )
        self.assertEqual(signal, "BUY")

    def test_time_outside_buy_window(self):
        sg = SignalGenerator(
            n_day_high_period=self.n_period,
            buy_window_start_time_str="15:55",
            buy_window_end_time_str="16:00"
        )
        # Time outside window: 15:50 on a valid buy day (Friday)
        current_datetime_utc8 = self.valid_buy_day_datetime_base.replace(hour=15, minute=50, second=0)

        signal = sg.check_breakout_signal(
            daily_ohlcv_data=self.historical_df,
            current_day_high=self.current_high_price,
            current_datetime_utc8=current_datetime_utc8
        )
        self.assertIsNone(signal)

    def test_time_at_start_of_buy_window(self):
        sg = SignalGenerator(
            n_day_high_period=self.n_period,
            buy_window_start_time_str="15:55",
            buy_window_end_time_str="16:00"
        )
        # Time at start of window: 15:55 on a valid buy day (Friday)
        current_datetime_utc8 = self.valid_buy_day_datetime_base.replace(hour=15, minute=55, second=0)

        signal = sg.check_breakout_signal(
            daily_ohlcv_data=self.historical_df,
            current_day_high=self.current_high_price,
            current_datetime_utc8=current_datetime_utc8
        )
        self.assertEqual(signal, "BUY")

    def test_time_at_end_of_buy_window(self):
        sg = SignalGenerator(
            n_day_high_period=self.n_period,
            buy_window_start_time_str="15:55",
            buy_window_end_time_str="16:00"
        )
        # Time at end of window: 16:00 on a valid buy day (Friday)
        current_datetime_utc8 = self.valid_buy_day_datetime_base.replace(hour=16, minute=0, second=0)

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
                buy_window_end_time_str="16:00"
            )
            current_datetime_utc8 = self.valid_buy_day_datetime_base.replace(hour=15, minute=58, second=0)

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
                buy_window_start_time_str="15:55",
                buy_window_end_time_str="invalid"
            )
            current_datetime_utc8 = self.valid_buy_day_datetime_base.replace(hour=15, minute=58, second=0)

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
            current_datetime_utc8 = self.valid_buy_day_datetime_base.replace(hour=15, minute=58, second=0)
            signal = sg.check_breakout_signal(
                daily_ohlcv_data=self.historical_df,
                current_day_high=self.current_high_price,
                current_datetime_utc8=current_datetime_utc8
            )
            self.assertIsNone(signal)
            self.assertEqual(mock_log_error.call_count, 2)


if __name__ == '__main__':
    unittest.main()
