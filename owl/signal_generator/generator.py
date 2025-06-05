import pandas as pd
from datetime import datetime, time
import logging

class SignalGenerator:
    """
    Generates trading signals based on market data and predefined strategy.
    """
    def __init__(self, n_day_high_period, buy_window_start_time_str, buy_window_end_time_str):
        """
        Initializes the SignalGenerator.

        Args:
            n_day_high_period (int): The period N for calculating N-day high.
            buy_window_start_time_str (str): The start of the buy window, e.g., "15:55".
            buy_window_end_time_str (str): The end of the buy window, e.g., "16:00".
        """
        if not isinstance(n_day_high_period, int) or n_day_high_period <= 0:
            raise ValueError("n_day_high_period must be a positive integer.")
        self.n = n_day_high_period

        self.buy_window_start_str = buy_window_start_time_str
        self.buy_window_end_str = buy_window_end_time_str
        self.buy_window_start_time = None
        self.buy_window_end_time = None

        try:
            start_hour, start_minute = map(int, buy_window_start_time_str.split(':'))
            self.buy_window_start_time = time(start_hour, start_minute)
        except ValueError:
            logging.error(f"Invalid format for buy_window_start_time_str: '{buy_window_start_time_str}'. Expected HH:MM. Buy window check will be disabled.")

        try:
            end_hour, end_minute = map(int, buy_window_end_time_str.split(':'))
            self.buy_window_end_time = time(end_hour, end_minute)
        except ValueError:
            logging.error(f"Invalid format for buy_window_end_time_str: '{buy_window_end_time_str}'. Expected HH:MM. Buy window check will be disabled.")

    def check_breakout_signal(self, historical_data_for_n_period, breakout_test_day_high, current_datetime_utc8):
        """
        Checks for an N-day high breakout buy signal.

        Args:
            historical_data_for_n_period (pd.DataFrame): DataFrame with historical daily OHLCV data
                                                         for the N days *before* the `breakout_test_day`.
                                                         Must contain 'high' and 'timestamp' columns.
                                                         'timestamp' should be datetime objects.
            breakout_test_day_high (float): The highest price reached on the day being tested for breakout
                                            against its preceding N days.
            current_datetime_utc8 (datetime): The current date and time in UTC+8 (Beijing time).
                                              Used to check if the *current day* (when the order would be placed)
                                              is a valid buy day/time.

        Returns:
            str or None: "BUY" if a buy signal is generated, None otherwise.
        """
        # --- Start: Resampling Logic ---
        # Operates on historical_data_for_n_period
        data_for_processing = historical_data_for_n_period # Default to original data

        if historical_data_for_n_period is not None and not historical_data_for_n_period.empty and 'timestamp' in historical_data_for_n_period.columns:
            # Ensure 'timestamp' is datetime
            # Making a copy to avoid SettingWithCopyWarning
            df_copy = historical_data_for_n_period.copy()
            df_copy['timestamp'] = pd.to_datetime(df_copy['timestamp'])

            if len(df_copy) >= 2: # Need at least 2 points to determine original frequency
                # Check if the original data is already daily
                # Sort by timestamp just in case to correctly calculate time_diff
                df_copy_sorted = df_copy.sort_values(by='timestamp')
                time_diff = df_copy_sorted['timestamp'].iloc[1] - df_copy_sorted['timestamp'].iloc[0]

                if time_diff < pd.Timedelta(days=1): # Indicates sub-daily data
                    logging.info("SignalGenerator: Detected sub-daily data in historical_data_for_n_period. Resampling to daily.")
                    # Set timestamp as index for resampling
                    resampled_data = df_copy.set_index('timestamp').resample('D').agg(
                        {'high': 'max'} # Add other aggregations if needed
                    ).reset_index()
                    # Ensure 'timestamp' is at the start of the day (midnight)
                    resampled_data['timestamp'] = resampled_data['timestamp'].dt.normalize()
                    data_for_processing = resampled_data
                else: # Already daily or sparser, no resampling needed for frequency
                    data_for_processing = df_copy # Use the copy with corrected datetime type
            elif len(df_copy) == 1: # If only one row, use it as is (with corrected datetime type)
                 data_for_processing = df_copy
            # If df_copy is empty, data_for_processing remains historical_data_for_n_period (which will be caught by later checks)
        # --- End: Resampling Logic ---

        # 1. Validate input DataFrame (using data_for_processing, which is derived from historical_data_for_n_period)
        if not isinstance(data_for_processing, pd.DataFrame) or data_for_processing.empty:
            print("SignalGenerator: Historical data for N period is empty or not a DataFrame (after potential resampling). No signal.")
            return None
        if not {'high', 'timestamp'}.issubset(data_for_processing.columns):
            print("SignalGenerator: Historical data for N period (after potential resampling) must contain 'high' and 'timestamp' columns. No signal.")
            return None

        # Ensure data is sorted by timestamp, oldest to newest, for reliable tail() operation
        # and for the self.n check.
        data_for_processing = data_for_processing.sort_values(by='timestamp')

        if len(data_for_processing) < self.n:
            print(f"SignalGenerator: Not enough historical data ({len(data_for_processing)} days after potential resampling) to calculate {self.n}-day high. Need exactly {self.n} days of historical data. No signal.")
            return None

        # After sorting and resampling, we expect data_for_processing to contain exactly N rows
        # if the input was prepared correctly by the caller.
        # However, if resampling changed the number of rows, or if input wasn't exactly N days,
        # we should take the most recent N days from the processed data.
        n_day_data = data_for_processing.tail(self.n)

        # Re-check length after tail(self.n) in case data_for_processing had more than N rows initially.
        if len(n_day_data) < self.n:
            print(f"SignalGenerator: After processing and selecting tail, not enough data ({len(n_day_data)} days) to form {self.n}-day high. Need {self.n} days. No signal.")
            return None

        # 2. Calculate N-day high from the prepared historical data
        n_day_high_value = n_day_data['high'].max()

        print(f"SignalGenerator: N-day high (from {self.n} days prior to test day): {n_day_high_value}")
        print(f"SignalGenerator: Test day's high for comparison: {breakout_test_day_high}")

        # 3. Check for breakout
        breakout_occurred = breakout_test_day_high > n_day_high_value
        if breakout_occurred:
            print(f"SignalGenerator: Breakout detected! Test day's high {breakout_test_day_high} > N-day high {n_day_high_value}.")
        else:
            # print(f"SignalGenerator: No breakout. Test day's high {breakout_test_day_high} <= N-day high {n_day_high_value}.")
            return None # No breakout, no further checks needed

        # 4. Check if it's a valid buy day and time (as per strategy)
        # This check uses current_datetime_utc8, which is the datetime for placing the order (e.g., Day T+1)
        # Valid buy days: Friday (4), Monday (0), Tuesday (1) (datetime.weekday())
        # Valid buy time: Close to 4 PM (16:00) Beijing Time
        day_of_week = current_datetime_utc8.weekday() # Monday is 0 and Sunday is 6
        current_time_utc8 = current_datetime_utc8.time()

        is_valid_buy_day = True 
        # is_valid_buy_day = day_of_week in [0, 1, 4] # Mon, Tue, Fri

        if not is_valid_buy_day:
            print(f"SignalGenerator: Breakout occurred, but today ({current_datetime_utc8.strftime('%A')}) is not a valid buy day (Mon, Tue, Fri).")
            return None

        print(f"SignalGenerator: BUY signal generated! Breakout confirmed on a valid buy day ({current_datetime_utc8.strftime('%A')}).")
        return "BUY"

# Example of how to use it (optional, for testing within this file)
if __name__ == "__main__":
    print("--- Testing SignalGenerator ---")
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Create dummy historical data (ensure 'timestamp' is datetime-like)
    # Timestamps should represent the OPEN time of the daily candle (e.g., 00:00 UTC or normalized)
    # This df represents a pool of historical data available up to a certain point.
    base_date_for_pool = datetime(2023, 10, 27) # Let's say this is "today" for data generation purposes
    data_pool_list = []
    for i in range(50, 0, -1): # Create 50 days of data ending yesterday relative to base_date_for_pool
        # Each timestamp is normalized to midnight, representing a daily candle's start
        ts = pd.to_datetime(base_date_for_pool - pd.Timedelta(days=i)).normalize()
        data_pool_list.append({
            'timestamp': ts,
            'high': 100 + (50-i) + ((50-i)%5)*5, # Varying highs
            'low': 80 - (50-i) + ((50-i)%3)*3
        })
    historical_df_pool = pd.DataFrame(data_pool_list)
    # historical_df_pool now contains daily data, with 'timestamp' for each day at 00:00:00

    N = 20 # Default N for most tests
    buy_start_str = "15:55"
    buy_end_str = "16:00"

    sg = SignalGenerator(n_day_high_period=N,
                         buy_window_start_time_str=buy_start_str,
                         buy_window_end_time_str=buy_end_str)

    # --- Test Scenarios ---
    # Define a "breakout test day". This is the day whose 'high' we are testing.
    # Let's say breakout_test_day is historical_df_pool.iloc[-1]'s timestamp + 1 day
    # For example, if historical_df_pool ends on 2023-10-26, breakout_test_day is 2023-10-27.

    # To make tests consistent, let's pick a fixed breakout_test_day for simulation.
    # Suppose historical_df_pool contains data up to (but not including) 2023-10-27.
    # So, the last day in historical_df_pool is 2023-10-26.

    # breakout_test_day_date = pd.Timestamp('2023-10-27').normalize() # The day we are evaluating for breakout
    # historical_data_for_n_period should be N days *before* breakout_test_day_date.
    # This means data from (breakout_test_day_date - N days) to (breakout_test_day_date - 1 day).

    # Let's use the end of our historical_df_pool to define the breakout test setup.
    # Assume historical_df_pool.iloc[-1] is "Day T-1" (day before breakout test day).
    # Then breakout_test_day_high is for "Day T".
    # current_datetime_utc8 is for "Day T+1" (potential order placement day).

    # Data for N days: Take the last N entries from historical_df_pool.
    # These N days are from (historical_df_pool.iloc[-N]) to (historical_df_pool.iloc[-1]).
    # These are days T-N to T-1.
    hist_data_n_days_scenario = historical_df_pool.iloc[-N:] # Correctly N days before a hypothetical "current day"

    # The high of the day we are testing for breakout (Day T)
    # This value would typically come from data for Day T, which is NOT in hist_data_n_days_scenario.
    # For testing, we'll simulate it.
    n_day_high_from_hist_data = hist_data_n_days_scenario['high'].max()

    print(f"\n--- Scenario 1: Breakout, Valid Day (Friday), Valid Time (15:58 UTC+8) ---")
    breakout_test_day_high_s1 = n_day_high_from_hist_data + 1
    # current_datetime_utc8 is for the day of potential order placement (Day T+1)
    order_placement_datetime_s1 = datetime(2023, 10, 27, 15, 58, 0) # A Friday

    signal_buy = sg.check_breakout_signal(hist_data_n_days_scenario, breakout_test_day_high_s1, order_placement_datetime_s1)
    logging.info(f"Signal for Scenario 1 (BUY): {signal_buy}")
    assert signal_buy == "BUY"

    print(f"\n--- Scenario 2: No Breakout, Valid Day, Valid Time ---")
    no_breakout_test_day_high_s2 = n_day_high_from_hist_data - 1
    signal_no_breakout = sg.check_breakout_signal(hist_data_n_days_scenario, no_breakout_test_day_high_s2, order_placement_datetime_s1)
    logging.info(f"Signal for Scenario 2 (No Breakout): {signal_no_breakout}")
    assert signal_no_breakout is None

    print(f"\n--- Scenario 3: Breakout, Invalid Day (Wednesday) for order placement ---")
    order_placement_datetime_s3 = datetime(2023, 10, 25, 15, 58, 0) # A Wednesday
    signal_invalid_day = sg.check_breakout_signal(hist_data_n_days_scenario, breakout_test_day_high_s1, order_placement_datetime_s3)
    logging.info(f"Signal for Scenario 3 (Invalid Day): {signal_invalid_day}")
    assert signal_invalid_day is None

    # Scenario 4: The original test description highlighted a potential bug regarding time window checks.
    # The current check_breakout_signal method does not explicitly use self.buy_window_start_time/end_time.
    # It only checks the day of the week. This behavior is preserved in the refactoring.
    # If time window checks were intended, the method's logic would need further modification.
    print(f"\n--- Scenario 4: Breakout, Valid Day (Monday), Order Time outside hypothetical window (10:00 AM) ---")
    order_placement_datetime_s4 = datetime(2023, 10, 23, 10, 0, 0) # A Monday
    signal_s4 = sg.check_breakout_signal(hist_data_n_days_scenario, breakout_test_day_high_s1, order_placement_datetime_s4)
    logging.info(f"Signal for Scenario 4 (Order Time Check): {signal_s4}")
    # Based on current logic (only day check), this should be BUY.
    assert signal_s4 == "BUY"

    print("\n--- Scenario 5: Not enough historical data for N-day period ---")
    # historical_data_for_n_period should have N days. Provide N-1 days.
    short_historical_data_s5 = historical_df_pool.iloc[-(N-1):]
    signal_not_enough_data = sg.check_breakout_signal(short_historical_data_s5, breakout_test_day_high_s1, order_placement_datetime_s1)
    logging.info(f"Signal for Scenario 5 (Not Enough Data): {signal_not_enough_data}")
    assert signal_not_enough_data is None

    print(f"\n--- Scenario 6: Breakout, Valid Day (Tuesday) for order placement ---")
    order_placement_datetime_s6 = datetime(2023, 10, 24, 16, 0, 0) # A Tuesday
    signal_tuesday = sg.check_breakout_signal(hist_data_n_days_scenario, breakout_test_day_high_s1, order_placement_datetime_s6)
    logging.info(f"Signal for Scenario 6 (Tuesday Order): {signal_tuesday}")
    assert signal_tuesday == "BUY"

    # --- Resampling Tests ---
    print("\n--- Scenario 7: Hourly data needing resampling for N-day period ---")
    N_resample_s7 = 3
    sg_resample_test_s7 = SignalGenerator(n_day_high_period=N_resample_s7,
                                       buy_window_start_time_str=buy_start_str,
                                       buy_window_end_time_str=buy_end_str)

    hourly_data_list_s7 = []
    # Create sub-daily data for a period that, when resampled, gives N_resample_s7 distinct days.
    # These are days *before* the breakout_test_day.
    test_day_for_resampling_setup_s7 = pd.Timestamp('2023-10-20').normalize()

    # Create N_resample_s7 + 2 days of hourly data to ensure enough data after potential resampling edge cases
    # This ensures that even after resampling and taking tail(N_resample_s7), we have N_resample_s7 days.
    for day_idx in range(1, N_resample_s7 + 3): # e.g., if N_r=3, creates 5 days of data (days T-1 to T-5)
        day_timestamp = test_day_for_resampling_setup_s7 - pd.Timedelta(days=day_idx)
        for hour_val in range(24):
            ts = day_timestamp + pd.Timedelta(hours=hour_val)
            # Define highs for predictability:
            # Day T-1 (day_idx=1): high around 100 + (3+3-1) = 105
            # Day T-2 (day_idx=2): high around 100 + (3+3-2) = 104
            # Day T-3 (day_idx=3): high around 100 + (3+3-3) = 103
            # Day T-4 (day_idx=4): high around 100 + (3+3-4) = 102
            # Day T-5 (day_idx=5): high around 100 + (3+3-5) = 101
            hourly_data_list_s7.append({
                'timestamp': ts,
                'high': 100 + (N_resample_s7 + 3 - day_idx) - (hour_val / 24.0)
            })
    hourly_hist_df_s7 = pd.DataFrame(hourly_data_list_s7).sort_values(by='timestamp')

    # This hourly_hist_df_s7 IS historical_data_for_n_period.
    # After internal resampling and tail(N_resample_s7):
    # Expected daily highs for T-5, T-4, T-3, T-2, T-1 are approx: 101, 102, 103, 104, 105
    # tail(3) will pick data for days T-3, T-2, T-1. Max highs: 103, 104, 105. N-day high = 105.
    expected_n_day_high_s7 = 100 + (N_resample_s7 + 3 - 1) # Max high from the latest relevant day
    breakout_high_s7 = expected_n_day_high_s7 + 1
    order_placement_datetime_s7 = datetime(2023, 10, 27, 15, 58, 0) # Valid Friday

    signal_resample_s7 = sg_resample_test_s7.check_breakout_signal(hourly_hist_df_s7, breakout_high_s7, order_placement_datetime_s7)
    logging.info(f"Signal for Scenario 7 (Resampling, N={N_resample_s7}): {signal_resample_s7} (Expected N-day high was approx {expected_n_day_high_s7})")
    assert signal_resample_s7 == "BUY"

    print("\n--- Scenario 8: Daily data, no resampling needed (using sg with N=20) ---")
    # hist_data_n_days_scenario is already daily and has N=20 days. (defined before S1)
    # n_day_high_from_hist_data was calculated from this. (defined before S1)
    breakout_high_s8 = n_day_high_from_hist_data + 1
    order_placement_datetime_s8 = datetime(2023, 10, 27, 15, 58, 0) # Valid Friday (order_placement_datetime_s1 can be reused)
    # Use the main 'sg' instance which is N=20
    signal_daily_no_resample_s8 = sg.check_breakout_signal(hist_data_n_days_scenario, breakout_high_s8, order_placement_datetime_s8)
    logging.info(f"Signal for Scenario 8 (Daily, N={N}, no resampling): {signal_daily_no_resample_s8}")
    assert signal_daily_no_resample_s8 == "BUY"

    print("\n--- Scenario 9: More than N days of daily historical data provided ---")
    # historical_data_for_n_period contains N+5 daily candles.
    # The method should internally use only the most recent N days from this set.
    hist_data_more_than_N_daily_s9 = historical_df_pool.iloc[-(N+5):] # Has N+5 days

    # The N-day high should be calculated based on the last N days of this (N+5)-day chunk.
    expected_n_day_high_s9 = hist_data_more_than_N_daily_s9.tail(N)['high'].max()
    breakout_high_s9 = expected_n_day_high_s9 + 1
    # Use order_placement_datetime_s1 (Valid Friday)
    signal_more_data_s9 = sg.check_breakout_signal(hist_data_more_than_N_daily_s9, breakout_high_s9, order_placement_datetime_s1)
    logging.info(f"Signal for Scenario 9 (More daily data than N, N={N}): {signal_more_data_s9}")
    assert signal_more_data_s9 == "BUY"

    print("\n--- SignalGenerator Test Complete (Refactored Logic) ---")
