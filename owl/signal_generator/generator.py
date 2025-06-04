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

    def check_breakout_signal(self, hourly_data_for_24h_lookback, daily_data_for_n_day_high, current_datetime_utc8):
        """
        Checks for an N-day high breakout buy signal using 24h lookback high and N-day historical high.

        Args:
            hourly_data_for_24h_lookback (pd.DataFrame): DataFrame with hourly OHLCV data for the last 24 hours.
                                                         Must contain a 'high' column.
            daily_data_for_n_day_high (pd.DataFrame): DataFrame with historical daily OHLCV data
                                                      for calculating N-day high. Must contain 'high' and 'timestamp' columns.
                                                      Timestamps should be sorted (most recent last).
            current_datetime_utc8 (datetime): The current date and time in UTC+8 (Beijing time).
                                              Used to check if it's a valid buy day/time.

        Returns:
            str or None: "BUY" if a buy signal is generated, None otherwise.
        """
        # 1. Input Validation for hourly_data_for_24h_lookback
        if not isinstance(hourly_data_for_24h_lookback, pd.DataFrame) or hourly_data_for_24h_lookback.empty:
            logging.warning("SignalGenerator: Hourly data for 24h lookback is empty or not a DataFrame. No signal.")
            return None
        if 'high' not in hourly_data_for_24h_lookback.columns:
            logging.warning("SignalGenerator: Hourly data for 24h lookback must contain 'high' column. No signal.")
            return None

        # 2. Input Validation for daily_data_for_n_day_high
        if not isinstance(daily_data_for_n_day_high, pd.DataFrame) or daily_data_for_n_day_high.empty:
            logging.warning("SignalGenerator: Daily data for N-day high is empty or not a DataFrame. No signal.")
            return None
        if not {'high', 'timestamp'}.issubset(daily_data_for_n_day_high.columns):
            logging.warning("SignalGenerator: Daily data for N-day high must contain 'high' and 'timestamp' columns. No signal.")
            return None
        if len(daily_data_for_n_day_high) < self.n:
            logging.warning(f"SignalGenerator: Not enough historical daily data ({len(daily_data_for_n_day_high)} days) to calculate {self.n}-day high. Need at least {self.n} days. No signal.")
            return None

        # 3. Calculate high_over_24h_lookback
        high_over_24h_lookback = hourly_data_for_24h_lookback['high'].max()
        logging.info(f"SignalGenerator: Calculated 24h lookback high: {high_over_24h_lookback}")

        # 4. Calculate n_day_high_value
        # Assuming daily_data_for_n_day_high is already sorted by timestamp, most recent last.
        # If not, uncomment: daily_data_for_n_day_high = daily_data_for_n_day_high.sort_values(by='timestamp')
        n_day_data = daily_data_for_n_day_high.tail(self.n) # Select the most recent N days
        n_day_high_value = n_day_data['high'].max()
        logging.info(f"SignalGenerator: Calculated {self.n}-day historical high: {n_day_high_value} from {len(n_day_data)} records.")

        # 5. Breakout Condition
        breakout_occurred = high_over_24h_lookback > n_day_high_value
        if not breakout_occurred:
            logging.info(f"SignalGenerator: No breakout. 24h lookback high {high_over_24h_lookback} <= {self.n}-day historical high {n_day_high_value}.")
            return None
        logging.info(f"SignalGenerator: Breakout detected! 24h lookback high {high_over_24h_lookback} > {self.n}-day historical high {n_day_high_value}.")

        # 6. Valid Buy Day/Time Check (existing logic)
        # Valid buy days: Friday (4), Monday (0), Tuesday (1) (datetime.weekday())
        # Valid buy time: Close to 4 PM (16:00) Beijing Time (original logic, can be refined if start/end times are used)
        day_of_week = current_datetime_utc8.weekday() # Monday is 0 and Sunday is 6
        # current_time_utc8_timeobj = current_datetime_utc8.time() # Not directly used in this version of day/time check

        is_valid_buy_day = day_of_week in [0, 1, 4] # Mon, Tue, Fri

        if not is_valid_buy_day:
            logging.info(f"SignalGenerator: Breakout occurred, but today ({current_datetime_utc8.strftime('%A')}) is not a valid buy day (Mon, Tue, Fri).")
            return None

        # TODO: Add buy window check if self.buy_window_start_time and self.buy_window_end_time are valid
        # if self.buy_window_start_time and self.buy_window_end_time:
        #     if not (self.buy_window_start_time <= current_time_utc8_timeobj <= self.buy_window_end_time):
        #         logging.info(f"SignalGenerator: Breakout on valid day, but current time {current_time_utc8_timeobj.strftime('%H:%M:%S')} is outside buy window [{self.buy_window_start_str}-{self.buy_window_end_str}].")
        #         return None

        logging.info(f"SignalGenerator: BUY signal generated! Breakout confirmed on a valid buy day ({current_datetime_utc8.strftime('%A')}).")
        return "BUY"

# Example of how to use it (optional, for testing within this file)
if __name__ == "__main__":
    print("--- Testing SignalGenerator (New Method Signature) ---")
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    N = 20 # N-day high period
    buy_start_str = "15:55" # Example, not strictly enforced in this version of check_breakout_signal's day/time check
    buy_end_str = "16:00"   # Example

    sg = SignalGenerator(n_day_high_period=N,
                         buy_window_start_time_str=buy_start_str,
                         buy_window_end_time_str=buy_end_str)

    # --- Setup Dummy Data ---
    # current_datetime_utc8 for example tests
    # Friday, October 27, 2023, 15:58:00 Beijing Time
    valid_buy_datetime_fri = datetime(2023, 10, 27, 15, 58, 0)
    # Wednesday, October 25, 2023, 15:58:00 Beijing Time
    invalid_day_datetime_wed = datetime(2023, 10, 25, 15, 58, 0)

    # Dummy daily_data_for_n_day_high (N days + buffer)
    # Timestamps should be for previous days, e.g., midnight UTC
    daily_timestamps = pd.to_datetime([valid_buy_datetime_fri - pd.Timedelta(days=i) for i in range(N + 10, 0, -1)]).normalize()
    daily_highs = [100 + i for i in range(N + 10)] # Increasing highs for predictability
    dummy_daily_data = pd.DataFrame({'timestamp': daily_timestamps, 'high': daily_highs})
    # N-day high will be the max of the last N days from this set
    expected_n_day_high = dummy_daily_data.tail(N)['high'].max() # Max of [100+10, ..., 100+10+N-1] = 100+10+N-1

    # Dummy hourly_data_for_24h_lookback
    # Should lead up to the `current_datetime_utc8`
    hourly_timestamps = pd.to_datetime([valid_buy_datetime_fri - pd.Timedelta(hours=i) for i in range(23, -1, -1)]) # Last 24 hours


    print(f"\n--- Test Setup ---")
    logging.info(f"N = {N}")
    logging.info(f"Expected N-day high based on dummy daily data: {expected_n_day_high}")
    logging.info(f"Dummy daily data shape: {dummy_daily_data.shape}")
    logging.info(f"Dummy daily data tail(3):\n{dummy_daily_data.tail(3)}")


    # --- Scenario 1: Breakout, Valid Day ---
    print(f"\n--- Scenario 1: Breakout, Valid Day (Friday) ---")
    hourly_highs_breakout = [expected_n_day_high - 5 + i for i in range(24)] # Some variation
    hourly_highs_breakout[-1] = expected_n_day_high + 1 # Ensure last hour's high causes breakout
    dummy_hourly_data_breakout = pd.DataFrame({'timestamp': hourly_timestamps, 'high': hourly_highs_breakout})

    signal_s1 = sg.check_breakout_signal(dummy_hourly_data_breakout, dummy_daily_data, valid_buy_datetime_fri)
    logging.info(f"Signal for Scenario 1 (Breakout, Valid Day): {signal_s1}")
    assert signal_s1 == "BUY"

    # --- Scenario 2: No Breakout, Valid Day ---
    print(f"\n--- Scenario 2: No Breakout, Valid Day (Friday) ---")
    hourly_highs_no_breakout = [expected_n_day_high - 10 + i for i in range(24)]
    hourly_highs_no_breakout[-1] = expected_n_day_high -1 # Max hourly high is below N-day high
    dummy_hourly_data_no_breakout = pd.DataFrame({'timestamp': hourly_timestamps, 'high': hourly_highs_no_breakout})

    signal_s2 = sg.check_breakout_signal(dummy_hourly_data_no_breakout, dummy_daily_data, valid_buy_datetime_fri)
    logging.info(f"Signal for Scenario 2 (No Breakout, Valid Day): {signal_s2}")
    assert signal_s2 is None

    # --- Scenario 3: Breakout, Invalid Day ---
    print(f"\n--- Scenario 3: Breakout, Invalid Day (Wednesday) ---")
    # Using dummy_hourly_data_breakout which causes a breakout
    signal_s3 = sg.check_breakout_signal(dummy_hourly_data_breakout, dummy_daily_data, invalid_day_datetime_wed)
    logging.info(f"Signal for Scenario 3 (Breakout, Invalid Day): {signal_s3}")
    assert signal_s3 is None

    # --- Scenario 4: Not enough daily data ---
    print(f"\n--- Scenario 4: Not enough daily data ---")
    short_daily_data = dummy_daily_data.tail(N - 1)
    signal_s4 = sg.check_breakout_signal(dummy_hourly_data_breakout, short_daily_data, valid_buy_datetime_fri)
    logging.info(f"Signal for Scenario 4 (Not enough daily data): {signal_s4}")
    assert signal_s4 is None

    # --- Scenario 5: Empty hourly data ---
    print(f"\n--- Scenario 5: Empty hourly data ---")
    empty_hourly_df = pd.DataFrame({'high': []}) # Or some other way to make it empty / invalid
    signal_s5 = sg.check_breakout_signal(empty_hourly_df, dummy_daily_data, valid_buy_datetime_fri)
    logging.info(f"Signal for Scenario 5 (Empty hourly data): {signal_s5}")
    assert signal_s5 is None

    # --- Scenario 6: Hourly data missing 'high' column ---
    print(f"\n--- Scenario 6: Hourly data missing 'high' column ---")
    hourly_missing_high_col = pd.DataFrame({'timestamp': hourly_timestamps, 'value': hourly_highs_breakout})
    signal_s6 = sg.check_breakout_signal(hourly_missing_high_col, dummy_daily_data, valid_buy_datetime_fri)
    logging.info(f"Signal for Scenario 6 (Hourly missing 'high'): {signal_s6}")
    assert signal_s6 is None

    # --- Scenario 7: Daily data missing 'high' column ---
    print(f"\n--- Scenario 7: Daily data missing 'high' column ---")
    daily_missing_high_col = pd.DataFrame({'timestamp': daily_timestamps, 'value': daily_highs})
    signal_s7 = sg.check_breakout_signal(dummy_hourly_data_breakout, daily_missing_high_col, valid_buy_datetime_fri)
    logging.info(f"Signal for Scenario 7 (Daily missing 'high'): {signal_s7}")
    assert signal_s7 is None

    # --- Scenario 8: Daily data missing 'timestamp' column ---
    print(f"\n--- Scenario 8: Daily data missing 'timestamp' column ---")
    daily_missing_ts_col = pd.DataFrame({'high': daily_highs, 'value': daily_highs})
    signal_s8 = sg.check_breakout_signal(dummy_hourly_data_breakout, daily_missing_ts_col, valid_buy_datetime_fri)
    logging.info(f"Signal for Scenario 8 (Daily missing 'timestamp'): {signal_s8}")
    assert signal_s8 is None

    print("\n--- SignalGenerator (New Method) Test Complete ---")
