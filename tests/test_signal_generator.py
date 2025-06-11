import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import datetime, time
from owl.signal_generator.generator import SignalGenerator

class TestSignalGenerator(unittest.TestCase): # Renamed class
    def setUp(self):
        # Data for N-day high calculation (N days before "previous_day")
        # Let's create 5 days of daily data for N-day high calculation
        self.n_period = 3 # Use 3-day high for tests
        base_hist_date = datetime(2023, 9, 25) # End date for historical data generation for N-day high

        hist_timestamps = []
        hist_highs = []
        for i in range(self.n_period + 2, 0, -1): # Create 5 days of data (self.n_period + 2)
            # Example: if n_period = 3, creates 5 days.
            # Timestamps: 2023-09-21, 2023-09-22, 2023-09-23, 2023-09-24, 2023-09-25
            # Highs:      100,        101,        102 (peak for n=3 from these 3), 103, 104
            # If historical_df is these 5 days, and N=3, it will take the *last* 3: 102,103,104. Max = 104.
            # Let's adjust to make the N-th day the peak.
            # Data for N-day high: D(N-1), D(N-2), ..., D(0)
            # historical_df should contain data such that .tail(self.n_period) gives the correct N days.
            # If self.historical_df is data for [Day1, Day2, Day3, Day4, Day5]
            # and N=3, then tail(3) is [Day3, Day4, Day5]. Max of these is N-day high.
            dt = base_hist_date - pd.Timedelta(days=i-1)
            hist_timestamps.append(dt)
            # Let high on Day3 be the peak for N=3 using D3,D4,D5
            if i == 3: # Corresponds to base_hist_date - pd.Timedelta(days=2)
                 hist_highs.append(105) # This will be the N-day high if N=3 and data is Day1..Day5
            else:
                 hist_highs.append(100 + i -1)


        self.historical_df = pd.DataFrame({
            'timestamp': pd.to_datetime(hist_timestamps),
            'high': hist_highs,
            'low': [h - 5 for h in hist_highs],
            'open': [h - 2 for h in hist_highs],
            'close': [h - 1 for h in hist_highs],
            'volume': [1000 for _ in hist_highs]
        })
        # self.historical_df now contains 5 days of data.
        # If N=3, tail(3) is the last 3 days.
        # Timestamps: ..., 2023-09-23 (102), 2023-09-24 (105), 2023-09-25 (104)
        # N-day high from these 3 is 105.
        self.n_period_high_test = self.historical_df.tail(self.n_period)['high'].max()

        # This is the high of the day *after* self.historical_df's last day.
        # This is the "previous_day_high" that the signal checks against.
        self.sample_previous_day_high_breakout = self.n_period_high_test + 1  # Breakout
        self.sample_previous_day_high_no_breakout = self.n_period_high_test - 1 # No breakout

        # current_datetime_utc8 is for context (e.g., "today" when the signal is checked)
        # This would be the day *after* "previous_day".
        # If historical_df ends 09-25, previous_day is 09-26, then context_datetime is 09-27.
        self.context_datetime_utc8 = datetime(2023, 9, 27, 10, 0, 0) # Arbitrary time, not used for filtering by SG

        # Buy window strings are not used by SignalGenerator logic anymore but are part of __init__
        self.dummy_buy_start_str = "00:00"
        self.dummy_buy_end_str = "23:59"

    def test_breakout_signal_triggers_buy(self):
        sg = SignalGenerator(
            n_day_high_period=self.n_period,
            buy_window_start_time_str=self.dummy_buy_start_str,
            buy_window_end_time_str=self.dummy_buy_end_str
        )
        signal = sg.check_breakout_signal(
            daily_ohlcv_data=self.historical_df, # N days data before previous_day
            previous_day_high=self.sample_previous_day_high_breakout, # High of previous_day
            current_datetime_utc8=self.context_datetime_utc8 # Context for "today"
        )
        self.assertEqual(signal, "BUY")

    def test_no_breakout_no_signal(self):
        sg = SignalGenerator(
            n_day_high_period=self.n_period,
            buy_window_start_time_str=self.dummy_buy_start_str,
            buy_window_end_time_str=self.dummy_buy_end_str
        )
        signal = sg.check_breakout_signal(
            daily_ohlcv_data=self.historical_df,
            previous_day_high=self.sample_previous_day_high_no_breakout,
            current_datetime_utc8=self.context_datetime_utc8
        )
        self.assertIsNone(signal)

    # Obsolete tests for time windows and malformed time strings are removed.
    # test_valid_buy_window_and_time -> combined into test_breakout_signal_triggers_buy
    # test_time_outside_buy_window -> removed
    # test_time_at_start_of_buy_window -> removed
    # test_time_at_end_of_buy_window -> removed
    # test_malformed_start_time_string -> removed
    # test_malformed_end_time_string -> removed
    # test_malformed_both_time_strings -> removed

    def test_check_breakout_with_hourly_data_resampling_buy_signal(self):
        """
        Tests that a BUY signal is generated when hourly data (representing N days before "previous day")
        is provided, resampled to daily, and "previous day's high" breaks out.
        """
        n_days_for_sg_instance = 3 # N=3 for this SG instance
        sg = SignalGenerator(
            n_day_high_period=n_days_for_sg_instance,
            buy_window_start_time_str=self.dummy_buy_start_str,
            buy_window_end_time_str=self.dummy_buy_end_str
        )

        # Create hourly data that, when resampled, forms the N-day history.
        # This hourly data should span `n_days_for_sg_instance` days.
        # Example: N=3. We need 3 days of hourly data.
        # Let these days be D1, D2, D3. Their resampled daily highs will be H1, H2, H3.
        # The N-day high will be max(H1, H2, H3).
        # `previous_day_high_for_test` will be compared against this.

        hourly_hist_timestamps = []
        hourly_hist_highs = []
        # Let's make data for 2023-09-21, 2023-09-22, 2023-09-23
        day1_date = datetime(2023,9,21)
        day2_date = datetime(2023,9,22)
        day3_date = datetime(2023,9,23)

        # Day 1: max high 100
        for h in range(24): hourly_hist_timestamps.append(day1_date + pd.Timedelta(hours=h)); hourly_hist_highs.append(98 if h!=12 else 100)
        # Day 2: max high 102
        for h in range(24): hourly_hist_timestamps.append(day2_date + pd.Timedelta(hours=h)); hourly_hist_highs.append(100 if h!=12 else 102)
        # Day 3: max high 101
        for h in range(24): hourly_hist_timestamps.append(day3_date + pd.Timedelta(hours=h)); hourly_hist_highs.append(99 if h!=12 else 101)

        # This hourly_historical_df is the `daily_ohlcv_data` argument.
        # After resampling, daily highs: 2023-09-21:100, 2023-09-22:102, 2023-09-23:101.
        # N=3 high from these is 102.
        hourly_historical_df_for_signal = pd.DataFrame({
            'timestamp': pd.to_datetime(hourly_hist_timestamps),
            'high': hourly_hist_highs, 'low': [h-1 for h in hourly_hist_highs], # Dummy other cols
            'open': [h-0.5 for h in hourly_hist_highs], 'close': [h-0.5 for h in hourly_hist_highs],
            'volume': [10 for _ in hourly_hist_highs]
        })

        n_day_high_from_hourly_resample = 102.0
        previous_day_high_for_test = n_day_high_from_hourly_resample + 1 # Breakout (103)

        # context_datetime is "today", so previous_day was 2023-09-24
        context_datetime_for_resample_test = datetime(2023, 9, 25, 10, 0, 0)

        signal_buy = sg.check_breakout_signal(
            daily_ohlcv_data=hourly_historical_df_for_signal.copy(),
            previous_day_high=previous_day_high_for_test,
            current_datetime_utc8=context_datetime_for_resample_test
        )
        self.assertEqual(signal_buy, "BUY", f"Expected BUY. N-day high from resampled hourly: {n_day_high_from_hourly_resample}. Prev day high: {previous_day_high_for_test}")

    def test_check_breakout_with_hourly_data_resampling_no_signal(self):
        n_days_for_sg_instance = 3
        sg = SignalGenerator(
            n_day_high_period=n_days_for_sg_instance,
            buy_window_start_time_str=self.dummy_buy_start_str,
            buy_window_end_time_str=self.dummy_buy_end_str
        )
        # Using same hourly data setup as above test, N-day high is 102.0
        day1_date = datetime(2023,9,21); day2_date = datetime(2023,9,22); day3_date = datetime(2023,9,23)
        hourly_hist_timestamps = []
        hourly_hist_highs = []
        for h in range(24): hourly_hist_timestamps.append(day1_date + pd.Timedelta(hours=h)); hourly_hist_highs.append(98 if h!=12 else 100)
        for h in range(24): hourly_hist_timestamps.append(day2_date + pd.Timedelta(hours=h)); hourly_hist_highs.append(100 if h!=12 else 102)
        for h in range(24): hourly_hist_timestamps.append(day3_date + pd.Timedelta(hours=h)); hourly_hist_highs.append(99 if h!=12 else 101)
        hourly_historical_df_for_signal = pd.DataFrame({
            'timestamp': pd.to_datetime(hourly_hist_timestamps), 'high': hourly_hist_highs, 'low': [h-1 for h in hourly_hist_highs],
            'open': [h-0.5 for h in hourly_hist_highs], 'close': [h-0.5 for h in hourly_hist_highs], 'volume': [10 for _ in hourly_hist_highs]
        })

        n_day_high_from_hourly_resample = 102.0
        previous_day_high_for_test = n_day_high_from_hourly_resample -1 # No Breakout (101)
        context_datetime_for_resample_test = datetime(2023, 9, 25, 10, 0, 0)

        signal_none = sg.check_breakout_signal(
            daily_ohlcv_data=hourly_historical_df_for_signal.copy(),
            previous_day_high=previous_day_high_for_test,
            current_datetime_utc8=context_datetime_for_resample_test
        )
        self.assertIsNone(signal_none, f"Expected None. N-day high from resampled hourly: {n_day_high_from_hourly_resample}. Prev day high: {previous_day_high_for_test}")


    def test_check_breakout_with_daily_data_no_resampling_buy_signal(self):
        """
        Tests that a BUY signal is generated correctly with already daily data.
        `self.historical_df` is daily data for N days. `self.sample_previous_day_high_breakout` is the breakout high.
        """
        sg = SignalGenerator( # Uses self.n_period from setUp (which is 3)
            n_day_high_period=self.n_period,
            buy_window_start_time_str=self.dummy_buy_start_str,
            buy_window_end_time_str=self.dummy_buy_end_str
        )
        # self.historical_df is already set up with N=3 days of daily data, N-day high is self.n_period_high_test (105)
        # self.sample_previous_day_high_breakout is self.n_period_high_test + 1 (106)
        # self.context_datetime_utc8 is also from setUp
        signal_buy = sg.check_breakout_signal(
            daily_ohlcv_data=self.historical_df.copy(), # This is the N days data
            previous_day_high=self.sample_previous_day_high_breakout, # This is D-1 high
            current_datetime_utc8=self.context_datetime_utc8 # This is D (context)
        )
        self.assertEqual(signal_buy, "BUY", f"Expected BUY with daily data. N-day high: {self.n_period_high_test}. PrevDayHigh: {self.sample_previous_day_high_breakout}")


if __name__ == '__main__':
    unittest.main()
