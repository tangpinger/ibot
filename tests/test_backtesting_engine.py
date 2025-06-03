import unittest
from unittest.mock import patch, MagicMock, call
import pandas as pd
from datetime import datetime
import pytz
from owl.backtesting_engine.engine import BacktestingEngine
# Explicitly import SignalGenerator for patching its instantiation by the engine
from owl.signal_generator.generator import SignalGenerator as ActualSignalGenerator

# Path for patching SignalGenerator where it's used by BacktestingEngine
PATCH_PATH_SG = 'owl.backtesting_engine.engine.SignalGenerator'

class TestBacktestingEngineBehavior(unittest.TestCase): # Renamed for broader scope

    def setUp(self):
        self.mock_data_fetcher = MagicMock()
        # self.mock_signal_generator is no longer directly passed if engine creates its own.
        # We will use patch to control SignalGenerator instantiation by the engine.

        # Default config
        self.sample_config = {
            'backtesting': {
                'symbol': 'BTC/USDT',
                'timeframe': '1h',
                'start_date': '2023-01-01',
                'end_date': '2023-01-04', # Extended to D+3 for holding period=1 test
                'initial_capital': 10000.0,
                'commission_rate': 0.001
            },
            'strategy': {
                'n_day_high_period': 20,
                'buy_cash_percentage': 0.80,
                'risk_free_rate': 0.02,
                # m_day_low_period, sell_window_start_time, sell_window_end_time removed
                'sell_asset_percentage': 1.0,
                'holding_period_days': 1, # Added for new strategy
                'buy_window_start_time': "09:00", # This can remain, not directly used by price pickup logic
                'buy_window_end_time': "16:00", # Target buy execution time UTC+8
            },
            'scheduler': {}, # Empty as per previous changes
            'proxy': {}, 'api_keys': {}, 'exchange_settings': {}
        }

        # Sample OHLCV data: 3 full days of hourly data + 1 more hour = 73 hours
        # Start from 2023-01-01 00:00:00 UTC up to 2023-01-04 00:00:00 UTC
        self.sample_daily_ohlcv_data = pd.DataFrame({
            'timestamp': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04'], utc=True),
            'open': [100, 110, 120, 130],
            'high': [105, 115, 125, 135], # Ensure breakout can occur on 2023-01-02
            'low': [95, 105, 115, 125],
            'close': [102, 112, 122, 132], # Daily close for 2023-01-02 is 112
            'volume': [1000, 1000, 1000, 1000]
        })

        # Hourly data setup
        # Buy decision time (when signal is checked): 2023-01-02 10:00:00 UTC+8 (02:00:00 UTC)
        # Buy price pickup time (based on buy_window_end_time="16:00"): 2023-01-02 16:00:00 UTC+8 (08:00:00 UTC)
        self.hourly_open_price_for_buy_at_1600 = 111.88 # New specific price for BUY order

        hourly_timestamps_jan1 = pd.date_range(start='2023-01-01 00:00:00', end='2023-01-01 23:00:00', freq='h', tz='UTC')
        # Generate full hourly data for Jan 2nd to ensure 08:00 UTC is present
        hourly_timestamps_jan2 = pd.date_range(start='2023-01-02 00:00:00', end='2023-01-02 23:00:00', freq='h', tz='UTC')
        hourly_timestamps_jan3 = pd.date_range(start='2023-01-03 00:00:00', end='2023-01-03 23:00:00', freq='h', tz='UTC')
        hourly_timestamps_jan4 = pd.date_range(start='2023-01-04 00:00:00', end='2023-01-04 00:00:00', freq='h', tz='UTC') # Just one candle for the end date

        all_hourly_timestamps = hourly_timestamps_jan1.union(hourly_timestamps_jan2).union(hourly_timestamps_jan3).union(hourly_timestamps_jan4)

        self.sample_hourly_ohlcv_data = pd.DataFrame({
            'timestamp': all_hourly_timestamps,
            'open': [100 + i*0.01 for i in range(len(all_hourly_timestamps))],
            'high': [100.05 + i*0.01 for i in range(len(all_hourly_timestamps))],
            'low': [99.95 + i*0.01 for i in range(len(all_hourly_timestamps))],
            'close': [100 + i*0.01 for i in range(len(all_hourly_timestamps))], # Close prices will be generic, open is targeted
            'volume': [100 + i for i in range(len(all_hourly_timestamps))]
        })

        # Set the specific OPEN for the target BUY candle (2023-01-02 08:00:00 UTC, which is 16:00 UTC+8)
        target_buy_price_pickup_utc = pd.Timestamp('2023-01-02 08:00:00', tz='UTC')
        if target_buy_price_pickup_utc in self.sample_hourly_ohlcv_data['timestamp'].values:
            self.sample_hourly_ohlcv_data.loc[self.sample_hourly_ohlcv_data['timestamp'] == target_buy_price_pickup_utc, 'open'] = self.hourly_open_price_for_buy_at_1600
        else:
            # This else block should ideally not be needed if hourly_timestamps_jan2 covers the full day
            new_buy_price_row = pd.DataFrame([{
                'timestamp': target_buy_price_pickup_utc,
                'open': self.hourly_open_price_for_buy_at_1600,
                'high': 112, 'low': 111, 'close': 111.90, 'volume': 50
            }])
            self.sample_hourly_ohlcv_data = pd.concat([self.sample_hourly_ohlcv_data, new_buy_price_row])

        # Old setup for hourly_close_price_for_buy (at 02:00 UTC) is no longer primary for buy price.
        # self.hourly_close_price_for_buy = 111.75
        # target_hourly_buy_candle_time = pd.Timestamp('2023-01-02 02:00:00', tz='UTC')
        # self.sample_hourly_ohlcv_data.loc[self.sample_hourly_ohlcv_data['timestamp'] == target_hourly_buy_candle_time, 'close'] = self.hourly_close_price_for_buy


        # Set the specific open for the target SELL candle (2023-01-03 02:00:00 UTC which is 10:00 UTC+8)
        self.hourly_open_price_for_sell = 120.25
        target_hourly_sell_candle_time = pd.Timestamp('2023-01-03 02:00:00', tz='UTC')
        if target_hourly_sell_candle_time in self.sample_hourly_ohlcv_data['timestamp'].values:
            self.sample_hourly_ohlcv_data.loc[self.sample_hourly_ohlcv_data['timestamp'] == target_hourly_sell_candle_time, 'open'] = self.hourly_open_price_for_sell
        else:
            # Add the row if it doesn't exist (e.g. if hourly_timestamps_jan3 didn't include 02:00 explicitly)
            new_sell_row = pd.DataFrame([{
                'timestamp': target_hourly_sell_candle_time,
                'open': self.hourly_open_price_for_sell,
                'high': self.sample_hourly_ohlcv_data['high'].mean(), # Placeholder
                'low': self.sample_hourly_ohlcv_data['low'].mean(),   # Placeholder
                'close': self.sample_hourly_ohlcv_data['close'].mean(),# Placeholder
                'volume': self.sample_hourly_ohlcv_data['volume'].mean()# Placeholder
            }])
            self.sample_hourly_ohlcv_data = pd.concat([self.sample_hourly_ohlcv_data, new_sell_row])

        # Ensure data is sorted by timestamp if any manual additions happened out of order
        self.sample_hourly_ohlcv_data = self.sample_hourly_ohlcv_data.sort_values('timestamp').reset_index(drop=True)

        def mock_fetch_ohlcv_se(symbol, timeframe, since, limit=None, params=None):
            # print(f"Mock fetch_ohlcv called with: timeframe={timeframe}, since={since}") # For debugging tests
            if timeframe == '1d':
                # Filter daily data by since, similar to how engine might if 'since' was for daily
                daily_data_copy = self.sample_daily_ohlcv_data.copy()
                if since is not None:
                    since_ts = pd.Timestamp(since, unit='ms', tz='UTC')
                    daily_data_copy = daily_data_copy[daily_data_copy['timestamp'] >= since_ts.normalize()] # Compare date part for daily
                return daily_data_copy
            elif timeframe == '1h':
                hourly_data_copy = self.sample_hourly_ohlcv_data.copy()
                if since is not None:
                    since_ts = pd.Timestamp(since, unit='ms', tz='UTC')
                    hourly_data_copy = hourly_data_copy[hourly_data_copy['timestamp'] >= since_ts]
                return hourly_data_copy
            return pd.DataFrame()

        self.mock_data_fetcher.fetch_ohlcv.side_effect = mock_fetch_ohlcv_se
        # self.mock_data_fetcher.fetch_ohlcv.return_value is now managed by side_effect

    @patch(PATCH_PATH_SG)
    @patch('owl.backtesting_engine.engine.logging')
    def test_buy_and_sell_trade_execution_with_hourly_buy_price(self, mock_logging, MockSignalGenerator):
        mock_sg_instance = MockSignalGenerator.return_value
        self.sample_config['strategy']['n_day_high_period'] = 1 # Breakout on 2023-01-02 (115 > 105)
        self.sample_config['backtesting']['timeframe'] = '1d' # Ensure engine knows main timeframe is daily

        # --- Scenario Setup ---
        # Buy signal on 2023-01-02 (daily candle). Daily high 115 > prev daily high 105.
        # SignalGenerator check_breakout_signal is called with current_datetime_utc8 = 2023-01-02 10:00:00 UTC+8.
        # BacktestingEngine then uses 'buy_window_end_time' ("16:00" UTC+8) from config to find price.
        # Target hourly candle for BUY price: 2023-01-02 16:00:00 UTC+8 (which is 08:00:00 UTC).
        # Expected BUY price: self.hourly_open_price_for_buy_at_1600 (111.88).
        # Daily candle timestamp for trade record: 2023-01-02 00:00:00 UTC.

        buy_trigger_daily_ts_utc = pd.Timestamp('2023-01-02 00:00:00', tz='UTC')
        buy_decision_time_utc8_hour = 10 # 10:00 UTC+8 (This is when signal is checked based on daily data)

        # Expected sell day: holding_period_days = 1. Buy on Jan 2. Sell on Jan 3 at 10:00 Beijing Time.
        sell_day_utc = pd.Timestamp('2023-01-03 00:00:00', tz='UTC')
        sell_trigger_utc_timestamp = sell_day_utc.replace(hour=2) # 02:00 UTC is 10:00 Beijing Time (for sell)

        def check_breakout_side_effect_for_hourly(*args, **kwargs):
            daily_ohlcv_data_arg = kwargs.get('daily_ohlcv_data')
            current_day_high_arg = kwargs.get('current_day_high')
            dt_utc8_arg = kwargs.get('current_datetime_utc8')

            # Trigger BUY on 2023-01-02 at 10:00 UTC+8
            if dt_utc8_arg.date() == pd.Timestamp('2023-01-02').date() and \
               dt_utc8_arg.hour == buy_decision_time_utc8_hour:

                # Verify data passed to check_breakout_signal is daily
                # Check if mostly daily: diff between timestamps should be >= 1 day, or only 1 record
                is_daily_data = True
                if len(daily_ohlcv_data_arg) > 1:
                    min_diff = daily_ohlcv_data_arg['timestamp'].diff().min()
                    if not (min_diff >= pd.Timedelta(days=1) - pd.Timedelta(seconds=1)): # allow for minor precision issues
                        is_daily_data = False
                self.assertTrue(is_daily_data, "Data passed to check_breakout_signal doesn't appear to be daily.")

                expected_high_for_buy_day = self.sample_daily_ohlcv_data[
                    self.sample_daily_ohlcv_data['timestamp'] == buy_trigger_daily_ts_utc
                ]['high'].iloc[0]
                self.assertEqual(current_day_high_arg, expected_high_for_buy_day)
                return "BUY"
            return None

        mock_sg_instance.check_breakout_signal.side_effect = check_breakout_side_effect_for_hourly

        engine = BacktestingEngine(
            config=self.sample_config,
            data_fetcher=self.mock_data_fetcher,
            signal_generator=None # Engine will create its own, which is mocked by PATCH_PATH_SG
        )

        with patch.object(engine, '_simulate_order', wraps=engine._simulate_order) as spy_simulate_order:
            engine.run_backtest()

        # --- Assertions for Data Fetching ---
        fetch_calls = self.mock_data_fetcher.fetch_ohlcv.call_args_list
        self.assertEqual(len(fetch_calls), 2, "fetch_ohlcv should be called twice (daily and hourly)")
        self.assertEqual(fetch_calls[0].kwargs['timeframe'], '1d')
        self.assertEqual(fetch_calls[1].kwargs['timeframe'], '1h')
        # Check if 'since' was passed correctly (example for hourly)
        self.assertIsNotNone(fetch_calls[1].kwargs['since'], "'since' should be passed for hourly fetch")


        # --- Assertions for BUY order ---
        buy_order_call = None
        for call_item in spy_simulate_order.call_args_list:
            if call_item.kwargs.get('order_type') == 'BUY':
                buy_order_call = call_item
                break
        self.assertIsNotNone(buy_order_call, "BUY order was not simulated")

        buy_kwargs = buy_order_call.kwargs
        self.assertEqual(buy_kwargs['timestamp'], buy_trigger_daily_ts_utc, "BUY order timestamp should be the daily candle's timestamp")
        self.assertEqual(buy_kwargs['price'], self.hourly_open_price_for_buy_at_1600, "BUY order price should be the hourly OPEN price at 16:00 UTC+8 target")

        qty_bought = buy_kwargs['quantity']

        # --- Assertions for SELL order (Holding Period) ---
        sell_order_call = None
        for call_item in spy_simulate_order.call_args_list:
            if call_item.kwargs.get('order_type') == 'SELL':
                sell_order_call = call_item
                break
        self.assertIsNotNone(sell_order_call, "SELL order was not simulated due to holding period")

        sell_kwargs = sell_order_call.kwargs

        # sell_trigger_utc_timestamp was defined as Jan 3, 02:00 UTC (for the 10:00 UTC+8 check)
        # The actual order timestamp in _simulate_order should be the beginning of that daily candle
        expected_sell_order_daily_ts = pd.Timestamp('2023-01-03 00:00:00', tz='UTC') # This is self.sample_daily_ohlcv_data.iloc[2]['timestamp']

        self.assertEqual(sell_kwargs['timestamp'], expected_sell_order_daily_ts, "SELL order timestamp should be the daily candle's timestamp for the sell day")
        self.assertEqual(sell_kwargs['price'], self.hourly_open_price_for_sell, "SELL order price should be the specific hourly open price")
        self.assertAlmostEqual(sell_kwargs['quantity'], qty_bought * self.sample_config['strategy']['sell_asset_percentage'], places=4)


    @patch(PATCH_PATH_SG)
    def test_buy_restriction_when_holding_asset(self, MockSignalGenerator): # Needs update for daily data
        mock_sg_instance = MockSignalGenerator.return_value
        self.sample_config['strategy']['n_day_high_period'] = 1 # Adjusted for simpler daily data
        self.sample_config['backtesting']['timeframe'] = '1d'


        # Configure to BUY on the first possible signal (2023-01-02)
        # n_day_high_period = 1, so index 1 (2023-01-02) is first possible buy day
        first_buy_trigger_daily_ts = self.sample_daily_ohlcv_data['timestamp'][1] # 2023-01-02 00:00:00 UTC

        # Signal generator expects datetime in Asia/Shanghai for its window check
        first_buy_trigger_dt_utc8 = first_buy_trigger_daily_ts.replace(hour=10).tz_localize('UTC').tz_convert('Asia/Shanghai')


        def breakout_side_effect(*args, **kwargs):
            dt_utc8 = kwargs.get('current_datetime_utc8')
            # Check if the date part matches and if the hour is within the configured buy window
            if dt_utc8.date() == first_buy_trigger_dt_utc8.date() and \
               self.sample_config['strategy']['buy_window_start_time'] <= dt_utc8.strftime("%H:%M") <= self.sample_config['strategy']['buy_window_end_time']:
                return "BUY"
            return None
        mock_sg_instance.check_breakout_signal.side_effect = breakout_side_effect

        engine = BacktestingEngine(
            config=self.sample_config,
            data_fetcher=self.mock_data_fetcher,
            signal_generator=None # Engine will create its own
        )

        with patch.object(engine, '_simulate_order', wraps=engine._simulate_order) as spy_simulate_order:
            engine.run_backtest()

        buy_orders = [c for c in spy_simulate_order.call_args_list if c.kwargs.get('order_type') == 'BUY']
        self.assertEqual(len(buy_orders), 1, "Should only execute one BUY order even if signals persist")

        # Verify check_breakout_signal call count
        # n_period = 1. Loop starts.
        # Index 0: current_idx < n_period (0 < 1), no call.
        # Index 1 (2023-01-02): current_idx >= n_period (1 >= 1). Call check_breakout_signal. BUY signal. Asset bought.
        # Index 2 (2023-01-03): asset_qty > 0. No call to check_breakout_signal.
        # Index 3 (2023-01-04): asset_qty > 0 (assuming sell happens later or not at all in this test's scope for buy restriction).
        # So, check_breakout_signal should be called once.
        # Simplified breakout_side_effect:
        # The original side effect was:
        # if dt_utc8.date() == first_buy_trigger_dt_utc8.date() and \
        #    self.sample_config['strategy']['buy_window_start_time'] <= dt_utc8.strftime("%H:%M") <= self.sample_config['strategy']['buy_window_end_time']:
        # This was already good as SignalGenerator itself doesn't do window check, but the mock was too specific.
        # The actual check_breakout_signal call from engine uses current_datetime_utc8 derived from daily candle + fixed hour.
        # The mock should just confirm it's the right day and hour for the *signal detection point*.

        # Re-check call count logic:
        # n_period = 1.
        # Daily loop:
        # idx 0 (2023-01-01): current_idx (0) < n_period (1). No call.
        # idx 1 (2023-01-02): current_idx (1) >= n_period (1). Call. BUY. Asset bought.
        # idx 2 (2023-01-03): asset_qty > 0. No call. (Sell logic might run, but no buy signal check)
        # idx 3 (2023-01-04): asset_qty > 0 or 0 if sold. If 0, would call again.
        # If holding_period_days = 1, sell happens on Jan 3.
        # So on Jan 4, asset_qty would be 0. check_breakout_signal would be called.
        # Let's adjust holding_period_days for this test to ensure asset is still held.
        # Or, simplify the side effect to only fire once.

        # For this test, the key is that *after a buy*, subsequent buy signals on other days (if they occurred) are ignored.
        # The current side effect fires on 2023-01-02 10:00 UTC+8.
        # If it fired again on 2023-01-03 10:00 UTC+8 and we still held asset, it would be ignored.
        # If we sold on 2023-01-03, then on 2023-01-04, a new BUY signal would be processed.
        # The current assertion of 1 call is correct IF the test ensures asset is held for the remainder.
        # With holding_period_days=1, buy on Jan 2, sell on Jan 3.
        # So on Jan 4, check_breakout_signal IS called.
        # Let's make the side_effect only return "BUY" for the first relevant call.

        # Redefine side effect for test_buy_restriction_when_holding_asset
        # Ensure it only returns "BUY" once for the intended buy day.

        # Store the original side_effect if it was more complex, or just redefine:
        # The original `breakout_side_effect` was:
        # if dt_utc8.date() == first_buy_trigger_dt_utc8.date() and \
        #    self.sample_config['strategy']['buy_window_start_time'] <= dt_utc8.strftime("%H:%M") <= self.sample_config['strategy']['buy_window_end_time']:
        # This is fine, as the signal is checked at a specific time (10:00 UTC+8 in the test).
        # The SignalGenerator itself doesn't use the window for its internal logic anymore.
        # The number of calls to check_breakout_signal will be:
        # Day 1 (idx 0): No (n_period)
        # Day 2 (idx 1): Yes. BUY.
        # Day 3 (idx 2): No (asset held). Sell happens based on holding period (e.g. 10:00 UTC+8).
        # Day 4 (idx 3): Yes (asset sold).
        # So, check_breakout_signal.call_count would be 2 if it runs to Day 4 and sells on Day 3.
        # The test's end_date is '2023-01-04'.
        # Daily data: 01-01, 01-02, 01-03, 01-04.
        # Buy on 01-02. Sell on 01-03 (holding period 1 day).
        # On 01-04, portfolio is empty, so check_breakout_signal is called.
        self.assertEqual(mock_sg_instance.check_breakout_signal.call_count, 2, "check_breakout_signal should be called on 2023-01-02 and 2023-01-04")


    @patch(PATCH_PATH_SG)
    @patch('owl.backtesting_engine.engine.logging')
    def test_buy_price_fallback_to_next_available_hourly_candle(self, mock_logging, MockSignalGenerator):
        mock_sg_instance = MockSignalGenerator.return_value
        self.sample_config['strategy']['n_day_high_period'] = 1
        self.sample_config['strategy']['buy_window_end_time'] = "16:00" # Explicitly set for clarity

        buy_trigger_daily_ts_utc = pd.Timestamp('2023-01-02 00:00:00', tz='UTC')
        # Target buy time: 2023-01-02 16:00 UTC+8 -> 08:00 UTC
        exact_target_hourly_utc = pd.Timestamp('2023-01-02 08:00:00', tz='UTC')
        # Fallback hourly candle: 2023-01-02 09:00 UTC
        fallback_hourly_candle_utc = pd.Timestamp('2023-01-02 09:00:00', tz='UTC')
        expected_fallback_hourly_open_price = 111.55

        # Modify hourly data for this test
        original_hourly_data_in_setup = self.sample_hourly_ohlcv_data.copy() # Preserve data from setUp
        temp_hourly_data = self.sample_hourly_ohlcv_data.copy()

        # Ensure exact target is missing
        temp_hourly_data = temp_hourly_data[temp_hourly_data['timestamp'] != exact_target_hourly_utc]

        # Ensure fallback candle exists and has the target open price
        if fallback_hourly_candle_utc in temp_hourly_data['timestamp'].values:
            temp_hourly_data.loc[temp_hourly_data['timestamp'] == fallback_hourly_candle_utc, 'open'] = expected_fallback_hourly_open_price
        else:
            new_fallback_row = pd.DataFrame([{
                'timestamp': fallback_hourly_candle_utc, 'open': expected_fallback_hourly_open_price,
                'high': 112, 'low': 111, 'close': 111.60, 'volume': 60 # Example values
            }])
            temp_hourly_data = pd.concat([temp_hourly_data, new_fallback_row]).sort_values('timestamp').reset_index(drop=True)

        # Update DataFetcher mock to use this temporary hourly data
        def temp_mock_fetch_ohlcv_se(symbol, timeframe, since, limit=None, params=None):
            if timeframe == '1d':
                daily_data_copy = self.sample_daily_ohlcv_data.copy()
                if since is not None:
                    since_ts = pd.Timestamp(since, unit='ms', tz='UTC')
                    daily_data_copy = daily_data_copy[daily_data_copy['timestamp'] >= since_ts.normalize()]
                return daily_data_copy
            elif timeframe == '1h':
                hourly_data_copy = temp_hourly_data.copy() # Use the modified data
                if since is not None:
                    since_ts = pd.Timestamp(since, unit='ms', tz='UTC')
                    hourly_data_copy = hourly_data_copy[hourly_data_copy['timestamp'] >= since_ts]
                return hourly_data_copy
            return pd.DataFrame()

        original_fetch_side_effect = self.mock_data_fetcher.fetch_ohlcv.side_effect
        self.mock_data_fetcher.fetch_ohlcv.side_effect = temp_mock_fetch_ohlcv_se

        # Mock SignalGenerator's check_breakout_signal
        buy_decision_time_utc8_hour = 10 # When signal is detected on daily data
        def check_breakout_side_effect(*args, **kwargs):
            dt_utc8_arg = kwargs.get('current_datetime_utc8')
            if dt_utc8_arg.date() == buy_trigger_daily_ts_utc.date() and dt_utc8_arg.hour == buy_decision_time_utc8_hour:
                return "BUY"
            return None
        mock_sg_instance.check_breakout_signal.side_effect = check_breakout_side_effect

        engine = BacktestingEngine(config=self.sample_config, data_fetcher=self.mock_data_fetcher, signal_generator=None)

        with patch.object(engine, '_simulate_order', wraps=engine._simulate_order) as spy_simulate_order:
            engine.run_backtest()

        # Restore original side effect for DataFetcher and hourly data for other tests
        self.mock_data_fetcher.fetch_ohlcv.side_effect = original_fetch_side_effect
        self.sample_hourly_ohlcv_data = original_hourly_data_in_setup # Restore for other tests that rely on setUp's version

        # Assertions
        buy_order_call = next((c for c in spy_simulate_order.call_args_list if c.kwargs.get('order_type') == 'BUY'), None)
        self.assertIsNotNone(buy_order_call, "BUY order was not simulated")

        buy_kwargs = buy_order_call.kwargs
        self.assertEqual(buy_kwargs['price'], expected_fallback_hourly_open_price,
                         "BUY order price should be the open of the next available hourly candle")
        self.assertEqual(buy_kwargs['timestamp'], buy_trigger_daily_ts_utc,
                         "BUY order timestamp should be the daily candle's timestamp")

        # Check for logging
        log_found = any(
            "BUY signal: Using alternative HOURLY OPEN price" in call_args[0][0]
            for call_args in mock_logging.info.call_args_list
        )
        self.assertTrue(log_found, "Expected log for fallback to alternative hourly candle not found.")


    @patch(PATCH_PATH_SG)
    @patch('owl.backtesting_engine.engine.logging') # Patch logging for checking warnings
    def test_buy_price_fallback_to_daily_if_hourly_missing(self, mock_logging, MockSignalGenerator):
        mock_sg_instance = MockSignalGenerator.return_value
        self.sample_config['strategy']['n_day_high_period'] = 1
        self.sample_config['backtesting']['timeframe'] = '1d'

        # --- Scenario Setup ---
        # Trigger BUY on 2023-01-02, but ensure no suitable hourly data exists for the buy decision time.
        buy_trigger_daily_ts_utc = pd.Timestamp('2023-01-02 00:00:00', tz='UTC')
        buy_decision_time_utc8_hour = 10 # 10:00 UTC+8 (02:00 UTC)

        # Modify hourly data to be missing for the target time
        # For example, remove all hourly data for 2023-01-02
        # To ensure this test is independent and doesn't affect others, copy and modify.
        original_hourly_data_in_setup = self.sample_hourly_ohlcv_data.copy()
        temp_hourly_data_for_this_test = self.sample_hourly_ohlcv_data.copy()
        temp_hourly_data_for_this_test = temp_hourly_data_for_this_test[
            temp_hourly_data_for_this_test['timestamp'].dt.date != pd.Timestamp('2023-01-02').date()
        ]

        # Update DataFetcher mock to use this temporary hourly data for this test only
        def temp_fetch_ohlcv_for_daily_fallback(symbol, timeframe, since, limit=None, params=None):
            if timeframe == '1d':
                daily_data_copy = self.sample_daily_ohlcv_data.copy()
                if since is not None:
                    since_ts = pd.Timestamp(since, unit='ms', tz='UTC')
                    daily_data_copy = daily_data_copy[daily_data_copy['timestamp'] >= since_ts.normalize()]
                return daily_data_copy
            elif timeframe == '1h':
                hourly_data_copy = temp_hourly_data_for_this_test.copy() # Use the specific modified data
                if since is not None:
                    since_ts = pd.Timestamp(since, unit='ms', tz='UTC')
                    hourly_data_copy = hourly_data_copy[hourly_data_copy['timestamp'] >= since_ts]
                return hourly_data_copy
            return pd.DataFrame()

        original_fetch_side_effect = self.mock_data_fetcher.fetch_ohlcv.side_effect
        self.mock_data_fetcher.fetch_ohlcv.side_effect = temp_fetch_ohlcv_for_daily_fallback


        def check_breakout_side_effect_for_fallback(*args, **kwargs):
            dt_utc8_arg = kwargs.get('current_datetime_utc8')
            if dt_utc8_arg.date() == pd.Timestamp('2023-01-02').date() and \
               dt_utc8_arg.hour == buy_decision_time_utc8_hour: # buy_decision_time_utc8_hour is 10
                return "BUY"
            return None
        mock_sg_instance.check_breakout_signal.side_effect = check_breakout_side_effect_for_fallback

        engine = BacktestingEngine(
            config=self.sample_config,
            data_fetcher=self.mock_data_fetcher,
            signal_generator=None
        )

        with patch.object(engine, '_simulate_order', wraps=engine._simulate_order) as spy_simulate_order:
            engine.run_backtest()

        # Restore original fetch side effect and sample_hourly_ohlcv_data
        self.mock_data_fetcher.fetch_ohlcv.side_effect = original_fetch_side_effect
        self.sample_hourly_ohlcv_data = original_hourly_data_in_setup


        # --- Assertions for logging ---
        fallback_log_found = False
        for log_call in mock_logging.warning.call_args_list:
            args, _ = log_call
            if args and "Falling back to daily close price" in args[0]:
                fallback_log_found = True
                break
        self.assertTrue(fallback_log_found, "Expected fallback warning log was not found.")

        # --- Assertions for BUY order ---
        buy_order_call = None
        for call_item in spy_simulate_order.call_args_list:
            if call_item.kwargs.get('order_type') == 'BUY':
                buy_order_call = call_item
                break
        self.assertIsNotNone(buy_order_call, "BUY order was not simulated in fallback test")

        buy_kwargs = buy_order_call.kwargs
        self.assertEqual(buy_kwargs['timestamp'], buy_trigger_daily_ts_utc)

        # Expected price is the daily close for 2023-01-02
        expected_daily_close_price = self.sample_daily_ohlcv_data[
            self.sample_daily_ohlcv_data['timestamp'] == buy_trigger_daily_ts_utc
        ]['close'].iloc[0] # This is 112
        self.assertEqual(buy_kwargs['price'], expected_daily_close_price, "BUY order price should be the daily close price due to fallback")

    @patch(PATCH_PATH_SG)
    @patch('owl.backtesting_engine.engine.logging')
    def test_sell_price_fallback_to_daily_if_hourly_missing(self, mock_logging, MockSignalGenerator):
        mock_sg_instance = MockSignalGenerator.return_value
        self.sample_config['strategy']['n_day_high_period'] = 1
        self.sample_config['strategy']['holding_period_days'] = 1
        self.sample_config['backtesting']['timeframe'] = '1d'

        # --- Scenario Setup ---
        # 1. BUY on 2023-01-02 (using hourly close price as per other test, or daily if that test is separate)
        buy_trigger_daily_ts_utc = pd.Timestamp('2023-01-02 00:00:00', tz='UTC')
        buy_decision_time_utc8_hour = 10 # 10:00 UTC+8

        def check_breakout_side_effect_for_buy(*args, **kwargs):
            dt_utc8_arg = kwargs.get('current_datetime_utc8')
            if dt_utc8_arg.date() == buy_trigger_daily_ts_utc.date() and \
               dt_utc8_arg.hour == buy_decision_time_utc8_hour:
                return "BUY"
            return None
        mock_sg_instance.check_breakout_signal.side_effect = check_breakout_side_effect_for_buy

        # 2. Modify hourly data to ensure no hourly candle for SELL decision time
        # Sell decision: 2023-01-03 (due to holding period 1 day), at 10:00 UTC+8 (02:00 UTC)
        sell_decision_target_utc = pd.Timestamp('2023-01-03 02:00:00', tz='UTC')

        original_hourly_data = self.sample_hourly_ohlcv_data.copy() # Preserve for other tests
        self.sample_hourly_ohlcv_data = self.sample_hourly_ohlcv_data[
            self.sample_hourly_ohlcv_data['timestamp'] < sell_decision_target_utc # Remove candles at or after target
        ]
        # The mock_fetch_ohlcv_se in setUp will now use this modified self.sample_hourly_ohlcv_data for this test run.

        engine = BacktestingEngine(
            config=self.sample_config,
            data_fetcher=self.mock_data_fetcher,
            signal_generator=None
        )

        with patch.object(engine, '_simulate_order', wraps=engine._simulate_order) as spy_simulate_order:
            engine.run_backtest()

        # Restore hourly data for subsequent tests
        self.sample_hourly_ohlcv_data = original_hourly_data

        # --- Assertions for logging ---
        fallback_log_found = False
        expected_log_message_part = "SELL condition: No hourly candle found at or after"
        for log_call in mock_logging.warning.call_args_list:
            args, _ = log_call
            if args and expected_log_message_part in args[0]:
                fallback_log_found = True
                break
        self.assertTrue(fallback_log_found, f"Expected SELL fallback warning log (containing '{expected_log_message_part}') not found.")

        # --- Assertions for SELL order ---
        sell_order_call = None
        for call_item in spy_simulate_order.call_args_list:
            if call_item.kwargs.get('order_type') == 'SELL':
                sell_order_call = call_item
                break
        self.assertIsNotNone(sell_order_call, "SELL order was not simulated in fallback test")

        sell_kwargs = sell_order_call.kwargs

        # Expected sell order timestamp is the daily candle of 2023-01-03
        expected_sell_order_daily_ts = pd.Timestamp('2023-01-03 00:00:00', tz='UTC')
        self.assertEqual(sell_kwargs['timestamp'], expected_sell_order_daily_ts)

        # Expected price is the daily close for 2023-01-03
        expected_daily_fallback_price = self.sample_daily_ohlcv_data[
            self.sample_daily_ohlcv_data['timestamp'] == expected_sell_order_daily_ts
        ]['close'].iloc[0] # Should be 122
        self.assertEqual(sell_kwargs['price'], expected_daily_fallback_price, "SELL order price should be the daily close price due to fallback")


if __name__ == '__main__':
    unittest.main()
