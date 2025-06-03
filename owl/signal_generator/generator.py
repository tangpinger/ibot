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
        # 1. Validate input DataFrame
        if not isinstance(daily_ohlcv_data, pd.DataFrame) or daily_ohlcv_data.empty:
            print("SignalGenerator: OHLCV data is empty or not a DataFrame. No signal.")
            return None
        if not {'high', 'timestamp'}.issubset(daily_ohlcv_data.columns):
            print("SignalGenerator: OHLCV data must contain 'high' and 'timestamp' columns. No signal.")
            return None
        if len(daily_ohlcv_data) < self.n:
            print(f"SignalGenerator: Not enough historical data ({len(daily_ohlcv_data)} days) to calculate {self.n}-day high. Need at least {self.n} days. No signal.")
            return None

        # 2. Calculate N-day high from historical data (excluding 'today')
        # Ensure data is sorted by timestamp, oldest to newest
        historical_data = daily_ohlcv_data.sort_values(by='timestamp')

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

        is_valid_buy_time_window = False
        if self.buy_window_start_time and self.buy_window_end_time:
            is_valid_buy_time_window = self.buy_window_start_time <= current_time_utc8 <= self.buy_window_end_time
        else:
            logging.warning("SignalGenerator: Buy window start or end time is not properly configured. Buy time check will fail.")

        if not is_valid_buy_day:
            print(f"SignalGenerator: Breakout occurred, but today ({current_datetime_utc8.strftime('%A')}) is not a valid buy day (Mon, Tue, Fri).")
            return None

        if not is_valid_buy_time_window:
            print(f"SignalGenerator: Breakout occurred on a valid day, but current time ({current_datetime_utc8.strftime('%H:%M')}) is not within the configured buy window ({self.buy_window_start_str}-{self.buy_window_end_str}).")
            return None

        print(f"SignalGenerator: BUY signal generated! Breakout confirmed on a valid buy day ({current_datetime_utc8.strftime('%A')}) and time window ({current_datetime_utc8.strftime('%H:%M')}).")
        return "BUY"


# Example of how to use it (optional, for testing within this file)
if __name__ == "__main__":
    print("--- Testing SignalGenerator ---")

    # Create dummy historical data (ensure 'timestamp' is datetime-like)
    # Timestamps should represent the OPEN time of the daily candle (e.g., 00:00 UTC)
    base_date = datetime(2023, 10, 1) # Using UTC for simplicity here
    data = {
        'timestamp': pd.to_datetime([base_date - pd.Timedelta(days=i) for i in range(30, 0, -1)]), # Last 30 days before current
        'high': [100 + i + (i%5)*5 for i in range(30)] # Some varying highs
    }
    historical_df = pd.DataFrame(data)

    N = 20
    # Example time window strings
    buy_start_str = "15:55"
    buy_end_str = "16:00"
    sg = SignalGenerator(n_day_high_period=N,
                         buy_window_start_time_str=buy_start_str,
                         buy_window_end_time_str=buy_end_str)

    # Scenario 1: Breakout on a valid day and time
    print(f"\n--- Scenario 1: Breakout, Valid Day (Friday), Valid Time (15:58 UTC+8 within {buy_start_str}-{buy_end_str}) ---")
    n_day_high_test = historical_df.tail(N)['high'].max()
    print(f"Test: Expected {N}-day high from dummy data: {n_day_high_test}")

    current_high_price = n_day_high_test + 1 # Ensure breakout
    # Friday, October 27, 2023, 15:58:00 Beijing Time
    valid_buy_datetime = datetime(2023, 10, 27, 15, 58, 0) # This is a Friday
    signal = sg.check_breakout_signal(historical_df, current_high_price, valid_buy_datetime)
    print(f"Signal for Scenario 1: {signal}")
    assert signal == "BUY"

    # Scenario 2: No breakout
    print(f"\n--- Scenario 2: No Breakout, Valid Day, Valid Time (within {buy_start_str}-{buy_end_str}) ---")
    no_breakout_price = n_day_high_test -1
    signal_no_breakout = sg.check_breakout_signal(historical_df, no_breakout_price, valid_buy_datetime)
    print(f"Signal for Scenario 2: {signal_no_breakout}")
    assert signal_no_breakout is None

    # Scenario 3: Breakout, Invalid Day (Wednesday)
    print(f"\n--- Scenario 3: Breakout, Invalid Day (Wednesday), Valid Time (within {buy_start_str}-{buy_end_str}) ---")
    # Wednesday, October 25, 2023, 15:58:00 Beijing Time
    invalid_day_datetime = datetime(2023, 10, 25, 15, 58, 0) # This is a Wednesday
    signal_invalid_day = sg.check_breakout_signal(historical_df, current_high_price, invalid_day_datetime)
    print(f"Signal for Scenario 3: {signal_invalid_day}")
    assert signal_invalid_day is None

    # Scenario 4: Breakout, Valid Day (Monday), Invalid Time (10:00 AM)
    print(f"\n--- Scenario 4: Breakout, Valid Day (Monday), Invalid Time (10:00 AM, outside {buy_start_str}-{buy_end_str}) ---")
    # Monday, October 23, 2023, 10:00:00 Beijing Time
    invalid_time_datetime = datetime(2023, 10, 23, 10, 0, 0) # This is a Monday
    signal_invalid_time = sg.check_breakout_signal(historical_df, current_high_price, invalid_time_datetime)
    print(f"Signal for Scenario 4: {signal_invalid_time}")
    assert signal_invalid_time is None

    # Scenario 5: Not enough data
    print("\n--- Scenario 5: Not enough historical data ---")
    short_historical_df = historical_df.tail(N-1) # N-1 days of data
    signal_not_enough_data = sg.check_breakout_signal(short_historical_df, current_high_price, valid_buy_datetime)
    print(f"Signal for Scenario 5: {signal_not_enough_data}")
    assert signal_not_enough_data is None

    # Scenario 6: Breakout, Valid Day (Tuesday), Valid Time (16:00 UTC+8 at edge of window)
    print(f"\n--- Scenario 6: Breakout, Valid Day (Tuesday), Valid Time (16:00 UTC+8, edge of {buy_start_str}-{buy_end_str}) ---")
    # Tuesday, October 24, 2023, 16:00:00 Beijing Time
    tuesday_buy_datetime = datetime(2023, 10, 24, 16, 0, 0) # This is a Tuesday
    signal_tuesday = sg.check_breakout_signal(historical_df, current_high_price, tuesday_buy_datetime)
    print(f"Signal for Scenario 6: {signal_tuesday}")
    assert signal_tuesday == "BUY"

    # Scenario 7: Malformed time strings for window
    print(f"\n--- Scenario 7: Malformed time strings for window ---")
    sg_malformed_window = SignalGenerator(n_day_high_period=N,
                                          buy_window_start_time_str="INVALID",
                                          buy_window_end_time_str="16:00")
    signal_malformed = sg_malformed_window.check_breakout_signal(historical_df, current_high_price, valid_buy_datetime)
    print(f"Signal for Scenario 7 (malformed start time): {signal_malformed}")
    assert signal_malformed is None # Should not generate BUY if window is invalid

    sg_malformed_window_2 = SignalGenerator(n_day_high_period=N,
                                          buy_window_start_time_str="15:55",
                                          buy_window_end_time_str="INVALID")
    signal_malformed_2 = sg_malformed_window_2.check_breakout_signal(historical_df, current_high_price, valid_buy_datetime)
    print(f"Signal for Scenario 7 (malformed end time): {signal_malformed_2}")
    assert signal_malformed_2 is None # Should not generate BUY if window is invalid


    print("\n--- SignalGenerator Test Complete ---")
