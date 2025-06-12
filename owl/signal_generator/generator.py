import pandas as pd
from datetime import datetime, time
import logging

class SignalGenerator:
    """
    Generates trading signals based on market data and predefined strategy.
    The strategy is purely based on N-day high breakout of the previous day's high.
    Time-based windowing or day-of-week restrictions are handled by the BacktestingEngine.
    """
    def __init__(self, n_day_high_period):
        """
        Initializes the SignalGenerator.

        Args:
            n_day_high_period (int): The period N for calculating N-day high.
        """
        if not isinstance(n_day_high_period, int) or n_day_high_period <= 0:
            raise ValueError("n_day_high_period must be a positive integer.")
        self.n = n_day_high_period
        # Removed buy_window_start_str and buy_window_end_str attributes

    def check_breakout_signal(self, daily_ohlcv_data, previous_day_high, previous_day_timestamp_utc, current_timestamp_utc):
        """
        Checks for an N-day high breakout buy signal based on the previous day's high.

        Args:
            daily_ohlcv_data (pd.DataFrame): DataFrame with historical daily OHLCV data
                                             for the N days *preceding* the "previous day".
                                             Must contain 'high' and 'timestamp' columns.
                                             The 'timestamp' should be datetime objects (UTC).
            previous_day_high (float): The highest price reached on the "previous day".
            previous_day_timestamp_utc (datetime): The current date and time in UTC+8, primarily for logging context.
                                              This parameter is not used for signal filtering logic within this method.

        Returns:
            str or None: "BUY" if a buy signal is generated, None otherwise.
        """
        data_for_processing = daily_ohlcv_data # Default to original data

        # 1. Validate input DataFrame (using data_for_processing)
        if not isinstance(data_for_processing, pd.DataFrame) or data_for_processing.empty:
            print("SignalGenerator: OHLCV data is empty or not a DataFrame (after potential resampling). No signal.")
            return None
        if not {'high', 'timestamp'}.issubset(data_for_processing.columns):
            print("SignalGenerator: OHLCV data (after potential resampling) must contain 'high' and 'timestamp' columns. No signal.")
            return None

        # 2. Calculate N-day high from historical data (excluding 'today')
        # Ensure data is sorted by timestamp, oldest to newest
        historical_data = data_for_processing.sort_values(by='timestamp')

        # Select the N most recent days from the *historical* data provided
        n_day_data = historical_data.tail(self.n)
        n_day_high_value = n_day_data['high'].max()

        # print(f"SignalGenerator: Calculated {self.n}-day high (from data before previous day): {n_day_high_value}")
        # print(f"SignalGenerator: Previous day's high for comparison: {previous_day_high}")

        # 3. Check for breakout
        breakout_occurred = previous_day_high > n_day_high_value
        if breakout_occurred:
            print(f"\n\nSignalGenerator: Breakout detected! Previous day's high {previous_day_high} > {self.n}-day high {n_day_high_value}. previous day {previous_day_timestamp_utc.strftime('%Y-%m-%d')}, current day {current_timestamp_utc.strftime('%Y-%m-%d')}")
            return "BUY"
        else:
            # print(f"SignalGenerator: No breakout. Previous day's high {previous_day_high} <= {self.n}-day high {n_day_high_value}.")
            return None # No breakout

        # Time-based and day-of-week restrictions have been removed.
        # Signal is now purely based on breakout of previous_day_high against N-day high.

# Example of how to use it (optional, for testing within this file)
if __name__ == "__main__":
    print("--- Testing SignalGenerator (Simplified Logic) ---")
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
    # Buy window strings are no longer passed to SignalGenerator constructor
    sg = SignalGenerator(n_day_high_period=N)

    # --- Breakout Tests (Simplified) ---
    # The historical_df represents N days *before* the "previous day".
    # So, if "previous day" is Oct 26th, historical_df is data up to Oct 25th.
    # N-day high is calculated from historical_df.
    # previous_day_high is the high of Oct 26th.

    # Data for N-day high calculation (days before "previous_day")
    # Let's assume historical_df contains data for the 20 days ending on 2023-09-30.
    # So, N-day high is calculated from this data.
    n_day_high_from_hist = historical_df.tail(N)['high'].max()
    print(f"N-day high calculated from historical_df.tail({N}) ending {historical_df['timestamp'].max().strftime('%Y-%m-%d')}: {n_day_high_from_hist}")

    # This is the high of the day *after* historical_df's last day.
    # This is the "previous_day_high" that the signal checks against.
    previous_day_high_breakout = n_day_high_from_hist + 1  # Breakout
    previous_day_high_no_breakout = n_day_high_from_hist - 1 # No breakout

    # previous_day_timestamp_utc is for context (e.g., "today" when the signal is checked)
    # This would be the day *after* "previous_day".
    # E.g., if previous_day_high was for Oct 26th, previous_day_timestamp_utc is for Oct 27th.
    context_datetime = datetime(2023, 10, 27, 10, 0, 0) # Arbitrary time, not used for filtering

    print(f"\n--- Scenario 1: Breakout ---")
    # Data for N-day high: historical_df
    # Previous day's high: previous_day_high_breakout
    signal_buy = sg.check_breakout_signal(historical_df, previous_day_high_breakout, context_datetime)
    logging.info(f"Signal for Scenario 1 (Breakout): {signal_buy}")
    assert signal_buy == "BUY"

    print(f"\n--- Scenario 2: No Breakout ---")
    # Data for N-day high: historical_df
    # Previous day's high: previous_day_high_no_breakout
    signal_no_breakout = sg.check_breakout_signal(historical_df, previous_day_high_no_breakout, context_datetime)
    logging.info(f"Signal for Scenario 2 (No Breakout): {signal_no_breakout}")
    assert signal_no_breakout is None

    # Day-of-week and time-of-day checks are removed.
    # Scenario 3 (formerly invalid day) should now be a BUY if breakout occurs.
    print(f"\n--- Scenario 3: Breakout (formerly invalid day check) ---")
    # Wednesday, October 25, 2023, 15:58:00 Beijing Time (as context_datetime)
    # The signal depends only on historical_df and previous_day_high_breakout.
    context_formerly_invalid_day = datetime(2023, 10, 25, 15, 58, 0)
    signal_invalid_day_removed = sg.check_breakout_signal(historical_df, previous_day_high_breakout, context_formerly_invalid_day)
    logging.info(f"Signal for Scenario 3 (Breakout, day/time restrictions removed): {signal_invalid_day_removed}")
    assert signal_invalid_day_removed == "BUY"

    # Scenario 4 (formerly invalid time) should now be a BUY if breakout occurs.
    print(f"\n--- Scenario 4: Breakout (formerly invalid time check) ---")
    context_formerly_invalid_time = datetime(2023, 10, 23, 10, 0, 0)
    signal_invalid_time_removed = sg.check_breakout_signal(historical_df, previous_day_high_breakout, context_formerly_invalid_time)
    logging.info(f"Signal for Scenario 4 (Breakout, day/time restrictions removed): {signal_invalid_time_removed}")
    assert signal_invalid_time_removed == "BUY"


    print("\n--- Scenario 5: Not enough historical data ---")
    short_historical_df = historical_df.tail(N-1) # N-1 days of data
    signal_not_enough_data = sg.check_breakout_signal(short_historical_df, previous_day_high_breakout, context_datetime)
    logging.info(f"Signal for Scenario 5 (Not enough data): {signal_not_enough_data}")
    assert signal_not_enough_data is None

    # Scenario 6 (formerly edge case for time window) is now just a breakout check.
    print(f"\n--- Scenario 6: Breakout (formerly time window edge case) ---")
    context_formerly_edge_case = datetime(2023, 10, 24, 16, 0, 0)
    signal_edge_case_removed = sg.check_breakout_signal(historical_df, previous_day_high_breakout, context_formerly_edge_case)
    logging.info(f"Signal for Scenario 6 (Breakout, day/time restrictions removed): {signal_edge_case_removed}")
    assert signal_edge_case_removed == "BUY"


    # --- Resampling Tests (logic remains, interpretation of inputs changes) ---
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
    # Make N smaller for this specific test to simplify data creation, e.g., N=3 (needs 3 daily records from hourly_historical_df)
    sg_resample_test = SignalGenerator(n_day_high_period=3) # N=3

    # hourly_historical_df contains data for days 25, 24, 23, 22, 21 days ago.
    # If N=3, it will use the resampled daily highs from days 23, 22, 21 days ago.
    # Max high for day_offset=23 is 123.
    # Max high for day_offset=22 is 122.
    # Max high for day_offset=21 is 121.
    # So, 3-day high from these (resampled from hourly_historical_df) should be 123.
    n_high_from_resampled = 123 # Based on calculation in original test code

    previous_day_high_for_resample_test = n_high_from_resampled + 1 # Breakout
    # context_datetime can be any relevant "current day" context
    signal_resample = sg_resample_test.check_breakout_signal(hourly_historical_df, previous_day_high_for_resample_test, context_datetime)
    logging.info(f"Signal for Scenario 7 (Resampling with N=3): {signal_resample}")
    assert signal_resample == "BUY"

    print("\n--- Scenario 8: Daily data, no resampling needed (using original sg with N=20) ---")
    # historical_df is daily. N=20.
    # n_day_high_from_hist was calculated earlier from historical_df.tail(20)
    previous_day_high_for_daily_test = n_day_high_from_hist + 1 # Breakout
    signal_daily_no_resample = sg.check_breakout_signal(historical_df, previous_day_high_for_daily_test, context_datetime)
    logging.info(f"Signal for Scenario 8 (Daily, N=20, no resampling): {signal_daily_no_resample}")
    assert signal_daily_no_resample == "BUY"

    print("\n--- SignalGenerator (Simplified Logic) Test Complete ---")
