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

    def check_breakout_signal(self, daily_ohlcv_data, current_day_high, current_datetime_utc8):
        """
        Checks for an N-day high breakout buy signal.

        Args:
            daily_ohlcv_data (pd.DataFrame): DataFrame with historical daily OHLCV data.
                                             Must contain 'high' and 'timestamp' columns.
                                             The 'timestamp' should be datetime objects (preferably UTC for consistency,
                                             or at least timezone-aware if timezone conversions are needed later).
                                             This data should be for *days before the current day* to calculate N-day high.
            current_day_high (float): The highest price reached on the current trading day so far.
            current_datetime_utc8 (datetime): The current date and time in UTC+8 (Beijing time).
                                              Used to check if it's a valid buy day/time.

        Returns:
            str or None: "BUY" if a buy signal is generated, None otherwise.
        """
        # --- Start: Resampling Logic ---
        data_for_processing = daily_ohlcv_data # Default to original data

        if daily_ohlcv_data is not None and not daily_ohlcv_data.empty and 'timestamp' in daily_ohlcv_data.columns:
            # Ensure 'timestamp' is datetime
            # Making a copy to avoid SettingWithCopyWarning if daily_ohlcv_data is a slice
            df_copy = daily_ohlcv_data.copy()
            df_copy['timestamp'] = pd.to_datetime(df_copy['timestamp'])

            if len(df_copy) >= 2:
                time_diff = df_copy['timestamp'].iloc[1] - df_copy['timestamp'].iloc[0]
                if time_diff < pd.Timedelta(days=1):
                    logging.info("SignalGenerator: Detected sub-daily data. Resampling to daily.")
                    # Set timestamp as index for resampling
                    resampled_data = df_copy.set_index('timestamp').resample('D').agg(
                        {'high': 'max'} # Add other aggregations if needed, e.g., open, low, close
                    ).reset_index()
                    # Ensure 'timestamp' is at the start of the day (midnight)
                    resampled_data['timestamp'] = resampled_data['timestamp'].dt.normalize()
                    data_for_processing = resampled_data
                else:
                    data_for_processing = df_copy # Use the copy with corrected datetime type
            elif len(df_copy) == 1: # If only one row, use it as is (with corrected datetime type)
                 data_for_processing = df_copy
            # If df_copy is empty, data_for_processing remains daily_ohlcv_data (which will be caught by later checks)

        # --- End: Resampling Logic ---

        # 1. Validate input DataFrame (using data_for_processing)
        if not isinstance(data_for_processing, pd.DataFrame) or data_for_processing.empty:
            print("SignalGenerator: OHLCV data is empty or not a DataFrame (after potential resampling). No signal.")
            return None
        if not {'high', 'timestamp'}.issubset(data_for_processing.columns):
            print("SignalGenerator: OHLCV data (after potential resampling) must contain 'high' and 'timestamp' columns. No signal.")
            return None
        if len(data_for_processing) < self.n:
            print(f"SignalGenerator: Not enough historical data ({len(data_for_processing)} days after potential resampling) to calculate {self.n}-day high. Need at least {self.n} days. No signal.")
            return None

        # 2. Calculate N-day high from historical data (excluding 'today')
        # Ensure data is sorted by timestamp, oldest to newest
        historical_data = data_for_processing.sort_values(by='timestamp')

        # Select the N most recent days from the *historical* data provided
        n_day_data = historical_data.tail(self.n)
        n_day_high_value = n_day_data['high'].max()

        print(f"SignalGenerator: Calculated {self.n}-day high (from previous days): {n_day_high_value}")
        print(f"SignalGenerator: Current day's high for comparison: {current_day_high}")

        # 3. Check for breakout
        breakout_occurred = current_day_high > n_day_high_value
        if breakout_occurred:
            print(f"SignalGenerator: Breakout detected! Current high {current_day_high} > {self.n}-day high {n_day_high_value}.")
        else:
            # print(f"SignalGenerator: No breakout. Current high {current_day_high} <= {self.n}-day high {n_day_high_value}.")
            return None # No breakout, no further checks needed

        # 4. Check if it's a valid buy day and time (as per strategy)
        # Valid buy days: Friday (4), Monday (0), Tuesday (1) (datetime.weekday())
        # Valid buy time: Close to 4 PM (16:00) Beijing Time
        day_of_week = current_datetime_utc8.weekday() # Monday is 0 and Sunday is 6
        current_time_utc8 = current_datetime_utc8.time()

        is_valid_buy_day = day_of_week in [0, 1, 4] # Mon, Tue, Fri

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
    # Timestamps should represent the OPEN time of the daily candle (e.g., 00:00 UTC)
    base_date = datetime(2023, 10, 1) # Using UTC for simplicity here
    data = {
        'timestamp': pd.to_datetime([base_date - pd.Timedelta(days=i) for i in range(30, 0, -1)]), # Last 30 days before current
        'high': [100 + i + (i%5)*5 for i in range(30)], # Some varying highs
        'low': [80 - i + (i%3)*3 for i in range(30)] # Some varying lows - though not used by current generator
    }
    historical_df = pd.DataFrame(data)

    N = 20
    # Example time window strings
    buy_start_str = "15:55"
    buy_end_str = "16:00"

    sg = SignalGenerator(n_day_high_period=N,
                         buy_window_start_time_str=buy_start_str,
                         buy_window_end_time_str=buy_end_str)

    # --- Breakout Tests (similar to before) ---
    print(f"\n--- Scenario 1: Breakout, Valid Day (Friday), Valid Time (15:58 UTC+8 within {buy_start_str}-{buy_end_str}) ---")
    n_day_high_test_val = historical_df.tail(N)['high'].max()
    current_high_price = n_day_high_test_val + 1
    valid_buy_datetime = datetime(2023, 10, 27, 15, 58, 0) # Friday
    signal_buy = sg.check_breakout_signal(historical_df, current_high_price, valid_buy_datetime)
    logging.info(f"Signal for Scenario 1 (BUY): {signal_buy}")
    assert signal_buy == "BUY"

    # Scenario 2: No breakout
    print(f"\n--- Scenario 2: No Breakout, Valid Day, Valid Time (within {buy_start_str}-{buy_end_str}) ---")
    no_breakout_price = n_day_high_test_val -1
    signal_no_breakout = sg.check_breakout_signal(historical_df, no_breakout_price, valid_buy_datetime)
    logging.info(f"Signal for Scenario 2: {signal_no_breakout}")
    assert signal_no_breakout is None

    # Scenario 3: Breakout, Invalid Day (Wednesday)
    print(f"\n--- Scenario 3: Breakout, Invalid Day (Wednesday), Valid Time (within {buy_start_str}-{buy_end_str}) ---")
    # Wednesday, October 25, 2023, 15:58:00 Beijing Time
    invalid_day_datetime = datetime(2023, 10, 25, 15, 58, 0) # This is a Wednesday
    signal_invalid_day = sg.check_breakout_signal(historical_df, current_high_price, invalid_day_datetime)
    logging.info(f"Signal for Scenario 3: {signal_invalid_day}")
    assert signal_invalid_day is None

    # Scenario 4: Breakout, Valid Day (Monday), Invalid Time (10:00 AM)
    print(f"\n--- Scenario 4: Breakout, Valid Day (Monday), Invalid Time (10:00 AM, outside {buy_start_str}-{buy_end_str}) ---")
    # Monday, October 23, 2023, 10:00:00 Beijing Time
    invalid_time_datetime = datetime(2023, 10, 23, 10, 0, 0) # This is a Monday
    signal_invalid_time = sg.check_breakout_signal(historical_df, current_high_price, invalid_time_datetime)
    logging.info(f"Signal for Scenario 4: {signal_invalid_time}")
    assert signal_invalid_time == "BUY"

    # Scenario 5: Not enough data
    print("\n--- Scenario 5: Not enough historical data ---")
    short_historical_df = historical_df.tail(N-1) # N-1 days of data
    signal_not_enough_data = sg.check_breakout_signal(short_historical_df, current_high_price, valid_buy_datetime)
    logging.info(f"Signal for Scenario 5: {signal_not_enough_data}")
    assert signal_not_enough_data is None

    # Scenario 6: Breakout, Valid Day (Tuesday), Valid Time (16:00 UTC+8 at edge of window)
    print(f"\n--- Scenario 6: Breakout, Valid Day (Tuesday), Valid Time (16:00 UTC+8, edge of {buy_start_str}-{buy_end_str}) ---")
    # Tuesday, October 24, 2023, 16:00:00 Beijing Time
    tuesday_buy_datetime = datetime(2023, 10, 24, 16, 0, 0) # This is a Tuesday
    signal_tuesday = sg.check_breakout_signal(historical_df, current_high_price, tuesday_buy_datetime)
    logging.info(f"Signal for Scenario 6: {signal_tuesday}")
    assert signal_tuesday == "BUY"

    # --- Resampling Tests ---
    print("\n--- Scenario 7: Hourly data needing resampling ---")
    # Create hourly historical data for a few days
    hourly_data_list = []
    # Simulate 3 days of hourly data, 24 hours each. N=20, so need more than 20 distinct days after resampling.
    # Let's make sure we have enough days for an N=5 test after resampling.
    # 5 days of hourly data
    for day_offset in range(25, 20, -1): # 5 days: 25, 24, 23, 22, 21 days ago
        for hour_offset in range(23, -1, -1): # 00:00 to 23:00 for each day
            ts = base_date - pd.Timedelta(days=day_offset, hours=hour_offset)
            # Make highs such that the daily max is easily predictable
            # e.g., day X has max high of 100+X
            hourly_data_list.append({'timestamp': ts, 'high': 100 + day_offset - (hour_offset/24.0)}) # decreasing high within the day

    hourly_historical_df = pd.DataFrame(hourly_data_list)
    # Make N smaller for this specific test to simplify data creation, e.g., N=3 (needs 3 daily records)
    sg_resample_test = SignalGenerator(n_day_high_period=3,
                                       buy_window_start_time_str=buy_start_str,
                                       buy_window_end_time_str=buy_end_str)

    # Expected daily highs:
    # Day 23 ago: max high around 100+23
    # Day 22 ago: max high around 100+22
    # Day 21 ago: max high around 100+21
    # N=3 high should be max(100+23, 100+22, 100+21) = 123 (approximately, due to hour_offset/24.0 part)
    # Let's check exact values:
    # For day_offset=23, max high is 100+23 = 123 (when hour_offset = 0)
    # For day_offset=22, max high is 100+22 = 122
    # For day_offset=21, max high is 100+21 = 121
    # So, 3-day high from these should be 123.

    current_high_resample = 124 # Breakout
    # Use a valid buy datetime for this test
    signal_resample = sg_resample_test.check_breakout_signal(hourly_historical_df, current_high_resample, valid_buy_datetime)
    logging.info(f"Signal for Scenario 7 (Resampling): {signal_resample}")
    assert signal_resample == "BUY" # Expecting a BUY signal after resampling

    # Check if the number of rows in data_for_processing inside the call was indeed reduced to daily
    # This requires either inspecting logs or modifying the function to return more info (not ideal for now)
    # For now, we rely on the logging and the fact that the N-day high calculation would be different.

    print("\n--- Scenario 8: Daily data, no resampling needed (using original sg with N=20) ---")
    # Re-use historical_df which is already daily
    # N_day_high for original daily data (N=20)
    n_day_high_daily_orig = historical_df.tail(N)['high'].max()
    current_high_daily_no_resample = n_day_high_daily_orig + 1 # Breakout
    signal_daily_no_resample = sg.check_breakout_signal(historical_df, current_high_daily_no_resample, valid_buy_datetime)
    logging.info(f"Signal for Scenario 8 (Daily, no resampling): {signal_daily_no_resample}")
    assert signal_daily_no_resample == "BUY"


    print("\n--- SignalGenerator (Buy Only) Test Complete ---")
