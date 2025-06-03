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

        # n_period is now in DAYS due to resampling logic in SignalGenerator.
        # self.historical_df has 50 hours of data, which is approx 2 full days and a bit of a third.
        # So, after resampling, we will have 3 daily data points.
        self.n_period = 2  # Use 2-day high for these tests.

        # Calculate n_period_high_test based on how SignalGenerator will resample
        # Ensure 'timestamp' is datetime for resampling
        df_copy = self.historical_df.copy()
        df_copy['timestamp'] = pd.to_datetime(df_copy['timestamp'])
        resampled_for_setup = df_copy.set_index('timestamp').resample('D').agg({'high': 'max'}).reset_index()

        # Ensure resampled_for_setup has enough data for n_period days
        if len(resampled_for_setup) >= self.n_period:
            self.n_period_high_test = resampled_for_setup.tail(self.n_period)['high'].max()
        else:
            # Fallback or raise error if not enough data even for small N,
            # though 50 hours should give 3 daily points.
            self.n_period_high_test = resampled_for_setup['high'].max() if not resampled_for_setup.empty else 150 # Default fallback

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

    # --- New tests for resampling ---

    def test_check_breakout_with_hourly_data_resampling_buy_signal(self):
        """
        Tests that a BUY signal is generated when hourly data is provided,
        resampled to daily, and a breakout occurs against the N-day high.
        """
        n_days_for_test = 5  # For an N-day high period of 5 days
        sg = SignalGenerator(
            n_day_high_period=n_days_for_test,
            buy_window_start_time_str="09:00", # Valid buy window
            buy_window_end_time_str="17:00"
        )

        # Create (n_days_for_test + 2) days of hourly data
        total_days_of_data = n_days_for_test + 2 # 7 days for a 5-day lookback
        # Start on a Monday to make day progression predictable for valid buy days
        start_date_for_data = datetime(2023, 1, 2, 0, 0) # Monday

        hourly_timestamps = []
        hourly_highs = []

        # Historical data will span Day 0 to Day (total_days_of_data - 2), e.g., Day 0 to Day 5 if total_days_of_data = 7
        # N-day high will be calculated from the last N days of this historical set.
        # Example: N=5. Historical data for D0, D1, D2, D3, D4, D5.
        # Resampled daily highs: D0=50, D1=51, D2=52, D3=53, D4=60 (peak), D5=55
        # tail(N=5) of these daily values: [D1,D2,D3,D4,D5] -> [51,52,53,60,55]. Max is 60.
        # So, the N-day high is 60.

        for day_idx in range(total_days_of_data -1): # Day 0 to Day 5
            daily_base_high = 50 + day_idx
            # Make the Nth day within the lookback period have the actual N-day high
            # If N=5, data is D0-D5. Lookback for N-day high considers D1-D5. Peak should be in D1-D5.
            # Let Day 4 (index 4) be the peak day.
            if day_idx == (n_days_for_test -1) : # This is Day 4, which will be the N-th day in the N-day window [D0,D1,D2,D3,D4] or [D1,D2,D3,D4,D5]
                                            # The N-day high calculation takes the last N days from the provided historical data.
                                            # If historical data is D0..D5, and N=5, it takes D1,D2,D3,D4,D5. Max of these.
                daily_base_high = 60 # Peak for the 5-day high

            for hour_num in range(24):
                dt = start_date_for_data + pd.Timedelta(days=day_idx, hours=hour_num)
                hourly_timestamps.append(dt)
                if hour_num == 12: # Make one hour the daily max
                    hourly_highs.append(daily_base_high)
                else:
                    hourly_highs.append(daily_base_high - 0.5)

        historical_hourly_df = pd.DataFrame({
            'timestamp': pd.to_datetime(hourly_timestamps),
            'high': hourly_highs,
            # Add other required columns if SignalGenerator uses them, even if dummy for this test
            'low': [h - 1 for h in hourly_highs],
            'open': [h - 0.5 for h in hourly_highs],
            'close': [h + 0.2 for h in hourly_highs],
            'volume': [100 for _ in hourly_highs]
        })

        # "Current day" for signal evaluation: Monday, 2023-01-09 (Day 7, if D0 was 2023-01-02)
        # Historical data up to Day 5 (Saturday, 2023-01-07).
        # N-day high from D1,D2,D3,D4,D5 (Mon-Fri values: 51,52,53,60,55). Max is 60.
        current_eval_datetime = datetime(2023, 1, 9, 10, 0, 0) # Monday, 10:00 AM (Valid buy day/time)

        current_breakout_high = 61 # This should be a breakout
        signal_buy = sg.check_breakout_signal(historical_hourly_df.copy(), current_breakout_high, current_eval_datetime)
        # Log what the resampled daily highs would look like for manual verification if test fails
        # resampled_debug = historical_hourly_df.set_index('timestamp').resample('D').agg({'high':'max'})
        # print(f"\nDebug Resampled Data for test_check_breakout_with_hourly_data_resampling_buy_signal:\n{resampled_debug.tail(n_days_for_test + 1)}")
        self.assertEqual(signal_buy, "BUY", f"Expected BUY signal. N-day high from resampled data should be 60. Got signal: {signal_buy}")

    def test_check_breakout_with_hourly_data_resampling_no_signal(self):
        """
        Tests that NO signal is generated when hourly data is resampled,
        but the current high does not break the N-day high.
        """
        n_days_for_test = 5
        sg = SignalGenerator(
            n_day_high_period=n_days_for_test,
            buy_window_start_time_str="09:00",
            buy_window_end_time_str="17:00"
        )
        start_date_for_data = datetime(2023, 1, 2, 0, 0) # Monday
        total_days_of_data = n_days_for_test + 2
        hourly_timestamps = []
        hourly_highs = []
        for day_idx in range(total_days_of_data -1): # Day 0 to Day 5
            daily_base_high = 50 + day_idx
            if day_idx == (n_days_for_test - 1): # Day 4
                daily_base_high = 60 # Peak for the 5-day high
            for hour_num in range(24):
                dt = start_date_for_data + pd.Timedelta(days=day_idx, hours=hour_num)
                hourly_timestamps.append(dt)
                if hour_num == 12: hourly_highs.append(daily_base_high)
                else: hourly_highs.append(daily_base_high - 0.5)

        historical_hourly_df = pd.DataFrame({
            'timestamp': pd.to_datetime(hourly_timestamps), 'high': hourly_highs,
            'low': [h - 1 for h in hourly_highs], 'open': [h - 0.5 for h in hourly_highs],
            'close': [h + 0.2 for h in hourly_highs], 'volume': [100 for _ in hourly_highs]
        })
        current_eval_datetime = datetime(2023, 1, 9, 10, 0, 0) # Monday, 10:00 AM

        current_no_breakout_high = 59.5 # Not a breakout (N-day high is 60)
        signal_none = sg.check_breakout_signal(historical_hourly_df.copy(), current_no_breakout_high, current_eval_datetime)
        self.assertIsNone(signal_none, f"Expected None signal. N-day high from resampled data should be 60. Got signal: {signal_none}")

    def test_check_breakout_with_daily_data_no_resampling_buy_signal(self):
        """
        Tests that a BUY signal is generated correctly with already daily data.
        This ensures the non-resampling path remains functional.
        """
        n_days_for_test = 5
        sg = SignalGenerator(
            n_day_high_period=n_days_for_test,
            buy_window_start_time_str="09:00",
            buy_window_end_time_str="17:00"
        )

        # Create 6 days of daily historical data (D0 to D5)
        # N-day high (N=5) will be from D1,D2,D3,D4,D5. Max should be 60 on D4.
        daily_timestamps = []
        daily_highs = []
        start_date_for_data = datetime(2023, 1, 2, 0, 0) # Monday

        for day_idx in range(n_days_for_test + 1): # Day 0 to Day 5
            dt = start_date_for_data + pd.Timedelta(days=day_idx)
            daily_timestamps.append(dt)
            current_high = 50 + day_idx
            if day_idx == (n_days_for_test -1): # Day 4
                current_high = 60 # Peak for N-day high
            daily_highs.append(current_high)

        historical_daily_df = pd.DataFrame({
            'timestamp': pd.to_datetime(daily_timestamps),
            'high': daily_highs,
            'low': [h - 1 for h in daily_highs], 'open': [h - 0.5 for h in daily_highs],
            'close': [h + 0.2 for h in daily_highs], 'volume': [100 for _ in daily_highs]
        })
        # print(f"\nDebug Daily Data for test_check_breakout_with_daily_data_no_resampling_buy_signal:\n{historical_daily_df.tail(n_days_for_test + 1)}")

        current_eval_datetime = datetime(2023, 1, 9, 10, 0, 0) # Monday, 10:00 AM

        current_breakout_high = 61 # Breakout (N-day high is 60)
        signal_buy = sg.check_breakout_signal(historical_daily_df.copy(), current_breakout_high, current_eval_datetime)
        self.assertEqual(signal_buy, "BUY", f"Expected BUY signal with daily data. N-day high should be 60. Got signal: {signal_buy}")


# TestSignalGeneratorSellWindow class and all its methods are removed.

if __name__ == '__main__':
    unittest.main()
