import pandas as pd
from datetime import datetime

class SignalGenerator:
    """
    Generates trading signals based on market data and predefined strategy.
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
        hour_of_day = current_datetime_utc8.hour
        minute_of_day = current_datetime_utc8.minute

        is_valid_buy_day = day_of_week in [0, 1, 4] # Mon, Tue, Fri
        # Check if time is within a window, e.g., 15:55 to 16:00 as per design doc for final check
        # This can be made more flexible, e.g., by passing target time and window from config
        is_valid_buy_time_window = (hour_of_day == 15 and minute_of_day >= 55) or (hour_of_day == 16 and minute_of_day == 0)

        if not is_valid_buy_day:
            print(f"SignalGenerator: Breakout occurred, but today ({current_datetime_utc8.strftime('%A')}) is not a valid buy day (Mon, Tue, Fri).")
            return None

        # The design doc implies the "日内已发生突破" (breakout happened during the day) is one condition,
        # and the buy is executed at 4 PM. So, this function might be called at 4 PM.
        # If it's called periodically during the day, the time check is for the "日内突破监控".
        # For now, assume this function is called when a decision is needed (e.g. near 4pm).
        if not is_valid_buy_time_window:
            print(f"SignalGenerator: Breakout occurred on a valid day, but current time ({current_datetime_utc8.strftime('%H:%M')}) is not the designated buy execution window (e.g., 15:55-16:00).")
            # Depending on how Scheduler uses this, this might be an INFO log rather than preventing signal
            # if the goal is just to check "has breakout happened today".
            # However, the function name "check_breakout_SIGNAL" implies it's for an actionable signal.
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
    # print("Sample Historical Data (last 5 days):")
    # print(historical_df.tail())

    N = 20
    sg = SignalGenerator(n_day_high_period=N)

    # Scenario 1: Breakout on a valid day and time
    print("\n--- Scenario 1: Breakout, Valid Day (Friday), Valid Time (15:58 UTC+8) ---")
    # Last 20 days from historical_df: highest high would be for i=29 (100+29+(29%5)*5 = 129+4*5 = 149)
    # Or, if we take .tail(20), it's index 10 to 29. Max of (100+i+(i%5)*5) for i in 10..29
    n_day_high_test = historical_df.tail(N)['high'].max()
    print(f"Test: Expected {N}-day high from dummy data: {n_day_high_test}")

    current_high_price = n_day_high_test + 1 # Ensure breakout
    # Friday, October 27, 2023, 15:58:00 Beijing Time
    valid_buy_datetime = datetime(2023, 10, 27, 15, 58, 0) # This is a Friday
    signal = sg.check_breakout_signal(historical_df, current_high_price, valid_buy_datetime)
    print(f"Signal for Scenario 1: {signal}")
    assert signal == "BUY"

    # Scenario 2: No breakout
    print("\n--- Scenario 2: No Breakout, Valid Day, Valid Time ---")
    no_breakout_price = n_day_high_test -1
    signal_no_breakout = sg.check_breakout_signal(historical_df, no_breakout_price, valid_buy_datetime)
    print(f"Signal for Scenario 2: {signal_no_breakout}")
    assert signal_no_breakout is None

    # Scenario 3: Breakout, Invalid Day (Wednesday)
    print("\n--- Scenario 3: Breakout, Invalid Day (Wednesday), Valid Time ---")
    # Wednesday, October 25, 2023, 15:58:00 Beijing Time
    invalid_day_datetime = datetime(2023, 10, 25, 15, 58, 0) # This is a Wednesday
    signal_invalid_day = sg.check_breakout_signal(historical_df, current_high_price, invalid_day_datetime)
    print(f"Signal for Scenario 3: {signal_invalid_day}")
    assert signal_invalid_day is None

    # Scenario 4: Breakout, Valid Day (Monday), Invalid Time (10:00 AM)
    print("\n--- Scenario 4: Breakout, Valid Day (Monday), Invalid Time (10:00 AM) ---")
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

    # Scenario 6: Breakout, Valid Day (Tuesday), Valid Time (16:00 UTC+8)
    print("\n--- Scenario 6: Breakout, Valid Day (Tuesday), Valid Time (16:00 UTC+8) ---")
    # Tuesday, October 24, 2023, 16:00:00 Beijing Time
    tuesday_buy_datetime = datetime(2023, 10, 24, 16, 0, 0) # This is a Tuesday
    signal_tuesday = sg.check_breakout_signal(historical_df, current_high_price, tuesday_buy_datetime)
    print(f"Signal for Scenario 6: {signal_tuesday}")
    assert signal_tuesday == "BUY"

    print("\n--- SignalGenerator Test Complete ---")
