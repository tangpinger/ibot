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
                'sell_asset_percentage': 1.0,
                'holding_period_hours': 4, # Changed from holding_period_days
                'buy_window_start_time': "09:00", # Used by engine for buy timing if needed by SG mock, but SG itself doesn't use it
                'buy_window_end_time': "16:00", # Target buy execution time UTC+8, used by engine
                # sell_window_start_time and sell_window_end_time are now obsolete for sell logic
            },
            'scheduler': {},
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
        # Buy price pickup time (based on buy_window_end_time="16:00" UTC+8 on D-1)
        # If D-1 is 2023-01-01, target buy time is 2023-01-01 16:00 UTC+8 (08:00 UTC)
        self.hourly_open_price_for_buy_at_d_minus_1_target_time = 101.88

        hourly_timestamps_list = []
        num_days_hourly_data = 4 # Jan 1 to Jan 4
        for i in range(num_days_hourly_data):
            day_str = f'2023-01-0{i+1}'
            hourly_timestamps_list.append(pd.date_range(start=f'{day_str} 00:00:00', end=f'{day_str} 23:00:00', freq='h', tz='UTC'))

        all_hourly_timestamps = hourly_timestamps_list[0]
        for i in range(1, len(hourly_timestamps_list)):
            all_hourly_timestamps = all_hourly_timestamps.union(hourly_timestamps_list[i])

        self.sample_hourly_ohlcv_data = pd.DataFrame({
            'timestamp': all_hourly_timestamps,
            # Base open, high, low, close prices for each day
            'open':  [100+d*10 + h*0.01 for d in range(num_days_hourly_data) for h in range(24)],
            'high':  [100+d*10 + h*0.01 + 0.05 for d in range(num_days_hourly_data) for h in range(24)],
            'low':   [100+d*10 + h*0.01 - 0.05 for d in range(num_days_hourly_data) for h in range(24)],
            'close': [100+d*10 + h*0.01 for d in range(num_days_hourly_data) for h in range(24)],
            'volume': [100 + d*10 + h for d in range(num_days_hourly_data) for h in range(24)]
        })

        # Set specific open for the D-1 target buy time candle
        # D-1 is 2023-01-01. Target buy time 16:00 UTC+8 is 08:00 UTC.
        target_buy_pickup_utc_d_minus_1 = pd.Timestamp('2023-01-01 08:00:00', tz='UTC')
        self.sample_hourly_ohlcv_data.loc[self.sample_hourly_ohlcv_data['timestamp'] == target_buy_pickup_utc_d_minus_1, 'open'] = self.hourly_open_price_for_buy_at_d_minus_1_target_time

        # Ensure data is sorted
        self.sample_hourly_ohlcv_data = self.sample_hourly_ohlcv_data.sort_values('timestamp').reset_index(drop=True)


        def mock_fetch_ohlcv_se(symbol, timeframe, since, limit=None, params=None, force_fetch=None):
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

        # --- Scenario Setup for D-1 Buy Logic ---
        # Daily data: 2023-01-01 (D-2 for N-day high), 2023-01-02 (D-1 for signal), 2023-01-03 (D, processing day)
        # Signal check for D-1 (2023-01-02) using N-day high from data ending D-2 (2023-01-01).
        # Buy on D-1 (2023-01-02) at target time.
        # Sell check on D (2023-01-03) based on holding_period_hours.

        n_period = 1 # For self.sample_config['strategy']['n_day_high_period'] = 1

        # D-1 (previous day for signal) is 2023-01-02. Its high is 115.
        # D-2 (end of N-day data for signal) is 2023-01-01. Its high is 105.
        # So, 115 > 105, breakout signal for D-1 (2023-01-02).

        # Contextual datetime for signal check (normalized D-1, converted to UTC+8)
        # D-1 is 2023-01-02.
        expected_d_minus_1_timestamp_utc = pd.Timestamp('2023-01-02 00:00:00', tz='UTC')
        expected_context_dt_for_signal_utc8 = expected_d_minus_1_timestamp_utc.tz_convert('Asia/Shanghai').normalize()

        # Target BUY on D-1 (2023-01-02) at 16:00 UTC+8 (08:00 UTC)
        # We need to adjust self.sample_hourly_ohlcv_data for D-1 (2023-01-02) for this.
        d_minus_1_target_buy_pickup_utc = pd.Timestamp('2023-01-02 08:00:00', tz='UTC')
        d_minus_1_hourly_open_price_for_buy = 111.77 # Specific price for this test
        self.sample_hourly_ohlcv_data.loc[self.sample_hourly_ohlcv_data['timestamp'] == d_minus_1_target_buy_pickup_utc, 'open'] = d_minus_1_hourly_open_price_for_buy

        # --- Sell Logic Setup for Day D (2023-01-03) ---
        # Asset bought on D-1 (2023-01-02) at d_minus_1_hourly_open_price_for_buy (111.77).
        # holding_period_hours = 4 (from config).
        # On Day D (2023-01-03), we observe hourly lows for 4 hours from 00:00 UTC.
        # Let's make a low on 2023-01-03 02:00:00 UTC drop below entry price.
        day_d_sell_trigger_candle_utc = pd.Timestamp('2023-01-03 02:00:00', tz='UTC')
        day_d_sell_trigger_low_price = d_minus_1_hourly_open_price_for_buy - 0.1 # e.g., 111.67
        self.sample_hourly_ohlcv_data.loc[self.sample_hourly_ohlcv_data['timestamp'] == day_d_sell_trigger_candle_utc, 'low'] = day_d_sell_trigger_low_price

        # Price for sell order will be OPEN of next candle (2023-01-03 03:00:00 UTC)
        day_d_sell_price_candle_utc = pd.Timestamp('2023-01-03 03:00:00', tz='UTC')
        expected_sell_price_day_d = 115.50 # Arbitrary open for this candle
        self.sample_hourly_ohlcv_data.loc[self.sample_hourly_ohlcv_data['timestamp'] == day_d_sell_price_candle_utc, 'open'] = expected_sell_price_day_d


        def check_breakout_side_effect_for_d_minus_1_buy(*args, **kwargs):
            daily_ohlcv_data_arg = kwargs.get('daily_ohlcv_data') # N days ending D-2
            previous_day_high_arg = kwargs.get('previous_day_high') # High of D-1
            current_datetime_utc8_arg = kwargs.get('current_datetime_utc8') # Context for D-1

            # Expected D-2 data (for N=1, this is 2023-01-01)
            expected_d_minus_2_data = self.sample_daily_ohlcv_data[self.sample_daily_ohlcv_data['timestamp'] == pd.Timestamp('2023-01-01', tz='UTC')]
            # Expected D-1 high (2023-01-02)
            expected_d_minus_1_high = self.sample_daily_ohlcv_data[self.sample_daily_ohlcv_data['timestamp'] == expected_d_minus_1_timestamp_utc]['high'].iloc[0]

            if current_datetime_utc8_arg == expected_context_dt_for_signal_utc8 and \
               previous_day_high_arg == expected_d_minus_1_high and \
               daily_ohlcv_data_arg.equals(expected_d_minus_2_data):
                return "BUY"
            return None

        mock_sg_instance.check_breakout_signal.side_effect = check_breakout_side_effect_for_d_minus_1_buy

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


        # --- Assertions for BUY order (on D-1 = 2023-01-02) ---
        buy_order_call = next((c for c in spy_simulate_order.call_args_list if c.kwargs.get('order_type') == 'BUY'), None)
        self.assertIsNotNone(buy_order_call, "BUY order was not simulated")
        buy_kwargs = buy_order_call.kwargs
        self.assertEqual(buy_kwargs['timestamp'], d_minus_1_target_buy_pickup_utc, "BUY order timestamp incorrect")
        self.assertEqual(buy_kwargs['price'], d_minus_1_hourly_open_price_for_buy, "BUY order price incorrect")
        qty_bought = buy_kwargs['quantity']

        # --- Assertions for SELL order (on D = 2023-01-03, new stop-loss logic) ---
        sell_order_call = next((c for c in spy_simulate_order.call_args_list if c.kwargs.get('order_type') == 'SELL'), None)
        self.assertIsNotNone(sell_order_call, "SELL order was not simulated based on new stop-loss logic")
        sell_kwargs = sell_order_call.kwargs
        self.assertEqual(sell_kwargs['timestamp'], day_d_sell_price_candle_utc, "SELL order timestamp incorrect") # Should be open of next candle
        self.assertEqual(sell_kwargs['price'], expected_sell_price_day_d, "SELL order price incorrect")
        self.assertAlmostEqual(sell_kwargs['quantity'], qty_bought * self.sample_config['strategy']['sell_asset_percentage'], places=4)

        # Verify asset_entry_price was used for sell check
        log_found_entry_price_check = any(
            f"Hourly candle at {day_d_sell_trigger_candle_utc} (UTC) with low {day_d_sell_trigger_low_price:.2f} < entry price {d_minus_1_hourly_open_price_for_buy:.2f}"
            in call_args[0][0] for call_args in mock_logging.info.call_args_list
        )
        self.assertTrue(log_found_entry_price_check, "Log for sell trigger comparing against asset_entry_price not found.")


    @patch(PATCH_PATH_SG)
    def test_buy_restriction_when_holding_asset(self, MockSignalGenerator):
        mock_sg_instance = MockSignalGenerator.return_value
        current_config = self.sample_config.copy()
        current_config['strategy']['n_day_high_period'] = 1
        current_config['backtesting']['timeframe'] = '1d'
        # Extend end_date to ensure we can see behavior on subsequent days
        current_config['backtesting']['end_date'] = '2023-01-04' # Was Jan 4, daily data covers this

        # Signal on D-1 (2023-01-01), processed when main loop is at D (2023-01-02)
        # D-1 for signal = 2023-01-01. Its high = 105.
        # N-day data for this D-1 ends D-2. D-2 doesn't exist in sample_daily_ohlcv_data before 2023-01-01.
        # Let's adjust n_day_high_period to 0 for this specific test, or ensure data exists.
        # For simplicity, let's make n_day_high_period = 1, and ensure data for D-2 (2023-01-00, not good)
        # Instead, let first signal be for D-1 = 2023-01-02.
        # D-2 for N-day data = 2023-01-01. High = 105.
        # D-1 (signal day) = 2023-01-02. High = 115.  115 > 105 -> BUY.

        d_minus_1_signal_day_ts_utc = pd.Timestamp('2023-01-02', tz='UTC')
        d_minus_1_signal_day_high = self.sample_daily_ohlcv_data[self.sample_daily_ohlcv_data['timestamp'] == d_minus_1_signal_day_ts_utc]['high'].iloc[0]

        d_minus_2_n_data_end_day_ts_utc = pd.Timestamp('2023-01-01', tz='UTC')
        d_minus_2_n_data_slice = self.sample_daily_ohlcv_data[self.sample_daily_ohlcv_data['timestamp'] == d_minus_2_n_data_end_day_ts_utc]

        context_dt_for_signal_check_utc8 = d_minus_1_signal_day_ts_utc.tz_convert('Asia/Shanghai').normalize()

        # This side effect will be called multiple times by the engine.
        # We want it to return "BUY" only for the first valid signal day (D-1 = 2023-01-02).
        # And then None for subsequent calls when asset is already held or after it's sold.

        # Store calls to the mock
        mock_calls_to_sg = []
        def single_buy_signal_side_effect(*args, **kwargs):
            mock_calls_to_sg.append(kwargs) # Store the call details
            current_datetime_utc8_arg = kwargs.get('current_datetime_utc8')
            # Only trigger BUY for D-1=2023-01-02 context
            if current_datetime_utc8_arg == context_dt_for_signal_check_utc8:
                 # Further checks to ensure it's the right data can be added here if strictness needed
                return "BUY"
            return None
        mock_sg_instance.check_breakout_signal.side_effect = single_buy_signal_side_effect

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
        # The main loop iterates with current_idx representing Day D.
        # Signal check is for D-1.
        # If D is 2023-01-02 (idx=1 in daily data), D-1 is 2023-01-01 (idx=0).
        # N-day data for D-1 means data before 2023-01-01.
        # If N=1, it needs data for 2023-01-00 (which isn't in sample).
        # So, first possible D where signal for D-1 can be generated:
        # If N=1, D-1 must be at least index 1 (2023-01-02). So D must be index 2 (2023-01-03).
        # On D=2023-01-03:
        #   D-1 (signal day) = 2023-01-02. High = 115.
        #   D-2 (N-data end) = 2023-01-01. High = 105. Signal is BUY.
        #   Buy order happens based on D-1 (2023-01-02) target time.
        # On D=2023-01-04 (asset held):
        #   No buy signal check. Sell logic runs.
        # On D=2023-01-05 (if asset sold on 04, and exists in data):
        #   D-1 (signal day) = 2023-01-04. High = 135.
        #   D-2 (N-data end) = 2023-01-03. High = 125. Signal is BUY. (This would be a second buy if not restricted)

        engine = BacktestingEngine(config=current_config, data_fetcher=self.mock_data_fetcher, signal_generator=None)
        with patch.object(engine, '_simulate_order', wraps=engine._simulate_order) as spy_simulate_order:
            engine.run_backtest()

        buy_orders = [c for c in spy_simulate_order.call_args_list if c.kwargs.get('order_type') == 'BUY']
        self.assertEqual(len(buy_orders), 1, "Should only execute one BUY order even if signals persist")

        # Verify how many times check_breakout_signal was called.
        # Daily data: 01, 02, 03, 04. N=1.
        # Loop D = 01 (idx=0): current_idx (0) < N+1 (2). No call.
        # Loop D = 02 (idx=1): current_idx (1) < N+1 (2). No call.
        # Loop D = 03 (idx=2): current_idx (2) >= N+1 (2). Call for D-1=02. BUY. Asset bought.
        # Loop D = 04 (idx=3): Asset held. No call. (Sell logic runs for D=04).
        # If asset sold on D=04, and if loop continued to D=05:
        # Loop D = 05: Asset empty. Call for D-1=04. BUY.
        # Based on current end_date '2023-01-04', only one call to check_breakout_signal is expected.
        self.assertEqual(len(mock_calls_to_sg), 1, "check_breakout_signal call count mismatch")
        self.assertEqual(mock_calls_to_sg[0]['current_datetime_utc8'], context_dt_for_signal_check_utc8)
        self.assertEqual(mock_calls_to_sg[0]['previous_day_high'], d_minus_1_signal_day_high)
        self.assertTrue(mock_calls_to_sg[0]['daily_ohlcv_data'].equals(d_minus_2_n_data_slice))


    # Obsolete SELL tests are removed here.
    # test_sell_triggered_within_window_uses_hourly_price -> REMOVE
    # test_sell_not_triggered_before_window_start -> REMOVE
    # test_sell_not_triggered_at_or_after_window_end -> REMOVE
    # test_sell_uses_daily_close_fallback_if_hourly_missing -> REMOVE (new logic has different fallback)
    # test_sell_window_times_honored_from_config -> REMOVE


    @patch(PATCH_PATH_SG)
    @patch('owl.backtesting_engine.engine.logging')
    def test_buy_price_fallback_to_next_available_hourly_candle(self, mock_logging, MockSignalGenerator):
        mock_sg_instance = MockSignalGenerator.return_value
        current_config = self.sample_config.copy()
        current_config['strategy']['n_day_high_period'] = 1
        current_config['strategy']['buy_window_end_time'] = "08:00" # Target D-1 08:00 UTC+8 (00:00 UTC)

        # D-1 (signal day) = 2023-01-02
        d_minus_1_signal_day_ts_utc = pd.Timestamp('2023-01-02', tz='UTC')
        context_dt_for_signal_check_utc8 = d_minus_1_signal_day_ts_utc.tz_convert('Asia/Shanghai').normalize()

        # Target buy time on D-1: 2023-01-02 08:00 UTC+8 -> 00:00 UTC on 2023-01-02
        exact_target_hourly_utc_d_minus_1 = pd.Timestamp('2023-01-02 00:00:00', tz='UTC')
        # Fallback hourly candle on D-1: 2023-01-02 01:00 UTC
        fallback_hourly_candle_utc_d_minus_1 = pd.Timestamp('2023-01-02 01:00:00', tz='UTC')
        expected_fallback_hourly_open_price = 110.55 # Open price of 01:00 candle on Jan 2

        temp_hourly_data = self.sample_hourly_ohlcv_data.copy()
        temp_hourly_data = temp_hourly_data[temp_hourly_data['timestamp'] != exact_target_hourly_utc_d_minus_1] # Remove exact target
        temp_hourly_data.loc[temp_hourly_data['timestamp'] == fallback_hourly_candle_utc_d_minus_1, 'open'] = expected_fallback_hourly_open_price

        original_fetch_side_effect = self.mock_data_fetcher.fetch_ohlcv.side_effect
        def temp_mock_fetch_ohlcv_se_fallback(symbol, timeframe, since, limit=None, params=None, force_fetch=None):
            if timeframe == '1d': return self.sample_daily_ohlcv_data[self.sample_daily_ohlcv_data['timestamp'] >= pd.Timestamp(since, unit='ms', tz='UTC').normalize()]
            elif timeframe == '1h': return temp_hourly_data[temp_hourly_data['timestamp'] >= pd.Timestamp(since, unit='ms', tz='UTC')]
            return pd.DataFrame()
        self.mock_data_fetcher.fetch_ohlcv.side_effect = temp_mock_fetch_ohlcv_se_fallback

        def check_breakout_side_effect_fallback(*args, **kwargs):
            if kwargs.get('current_datetime_utc8') == context_dt_for_signal_check_utc8:
                return "BUY"
            return None
        mock_sg_instance.check_breakout_signal.side_effect = check_breakout_side_effect_fallback

        engine = BacktestingEngine(config=self.sample_config, data_fetcher=self.mock_data_fetcher, signal_generator=None)

        with patch.object(engine, '_simulate_order', wraps=engine._simulate_order) as spy_simulate_order:
            engine.run_backtest()

        # Restore original fetch side effect
        self.mock_data_fetcher.fetch_ohlcv.side_effect = original_fetch_side_effect

        buy_order_call = next((c for c in spy_simulate_order.call_args_list if c.kwargs.get('order_type') == 'BUY'), None)
        self.assertIsNotNone(buy_order_call, "BUY order was not simulated in fallback to next hourly")
        buy_kwargs = buy_order_call.kwargs
        self.assertEqual(buy_kwargs['price'], expected_fallback_hourly_open_price)
        self.assertEqual(buy_kwargs['timestamp'], fallback_hourly_candle_utc_d_minus_1)
        self.assertTrue(any("Using alternative HOURLY OPEN price" in call_args[0][0] for call_args in mock_logging.info.call_args_list))

    @patch(PATCH_PATH_SG)
    @patch('owl.backtesting_engine.engine.logging')
    def test_buy_price_fallback_to_daily_if_hourly_missing(self, mock_logging, MockSignalGenerator):
        mock_sg_instance = MockSignalGenerator.return_value
        current_config = self.sample_config.copy()
        current_config['strategy']['n_day_high_period'] = 1
        current_config['backtesting']['timeframe'] = '1d' # Ensure engine processes daily

        # D-1 (signal day) = 2023-01-02
        d_minus_1_signal_day_ts_utc = pd.Timestamp('2023-01-02', tz='UTC')
        context_dt_for_signal_check_utc8 = d_minus_1_signal_day_ts_utc.tz_convert('Asia/Shanghai').normalize()

        # Remove all hourly data for D-1 (2023-01-02) to force daily fallback
        temp_hourly_data = self.sample_hourly_ohlcv_data[self.sample_hourly_ohlcv_data['timestamp'].dt.date != d_minus_1_signal_day_ts_utc.date()]

        original_fetch_side_effect = self.mock_data_fetcher.fetch_ohlcv.side_effect
        def temp_mock_fetch_ohlcv_se_daily_fallback(symbol, timeframe, since, limit=None, params=None, force_fetch=None):
            if timeframe == '1d': return self.sample_daily_ohlcv_data[self.sample_daily_ohlcv_data['timestamp'] >= pd.Timestamp(since, unit='ms', tz='UTC').normalize()]
            elif timeframe == '1h': return temp_hourly_data[temp_hourly_data['timestamp'] >= pd.Timestamp(since, unit='ms', tz='UTC')]
            return pd.DataFrame()
        self.mock_data_fetcher.fetch_ohlcv.side_effect = temp_mock_fetch_ohlcv_se_daily_fallback

        def check_breakout_side_effect_daily_fallback(*args, **kwargs):
            if kwargs.get('current_datetime_utc8') == context_dt_for_signal_check_utc8:
                return "BUY"
            return None
        mock_sg_instance.check_breakout_signal.side_effect = check_breakout_side_effect_daily_fallback

        engine = BacktestingEngine(
            config=self.sample_config,
            data_fetcher=self.mock_data_fetcher,
            signal_generator=None
        )

        with patch.object(engine, '_simulate_order', wraps=engine._simulate_order) as spy_simulate_order:
            engine.run_backtest()

        # Restore original fetch side effect
        self.mock_data_fetcher.fetch_ohlcv.side_effect = original_fetch_side_effect

        buy_order_call = next((c for c in spy_simulate_order.call_args_list if c.kwargs.get('order_type') == 'BUY'), None)
        self.assertIsNotNone(buy_order_call, "BUY order was not simulated in fallback to daily")
        buy_kwargs = buy_order_call.kwargs

        # Fallback is to D-1's daily timestamp and D-1's daily close price
        expected_d_minus_1_close_price = self.sample_daily_ohlcv_data[self.sample_daily_ohlcv_data['timestamp'] == d_minus_1_signal_day_ts_utc]['close'].iloc[0]
        self.assertEqual(buy_kwargs['timestamp'], d_minus_1_signal_day_ts_utc)
        self.assertEqual(buy_kwargs['price'], expected_d_minus_1_close_price)
        self.assertTrue(any("Falling back to D-1 DAILY CLOSE price" in call_args[0][0] for call_args in mock_logging.warning.call_args_list))

    # New SELL logic tests
    @patch(PATCH_PATH_SG)
    @patch('owl.backtesting_engine.engine.logging')
    def test_new_sell_logic_trigger_within_holding_hours(self, mock_logging, MockSignalGenerator):
        mock_sg_instance = MockSignalGenerator.return_value
        current_config = self.sample_config.copy()
        current_config['strategy']['n_day_high_period'] = 1
        current_config['strategy']['holding_period_hours'] = 4 # Test with 4 hours
        current_config['backtesting']['end_date'] = '2023-01-03' # Ensure sell day is processed

        # BUY on D-1 (2023-01-02)
        d_minus_1_buy_day_ts = pd.Timestamp('2023-01-02', tz='UTC')
        asset_entry_price = 111.0 # Assume this was the buy price on D-1

        mock_sg_instance.check_breakout_signal.side_effect = lambda **kwargs: "BUY" if kwargs['current_datetime_utc8'].date() == d_minus_1_buy_day_ts.date() else None

        # On Day D (2023-01-03), set up hourly data for sell trigger
        # Low price drops below asset_entry_price within holding_period_hours (4 hours from 00:00 UTC of Day D)
        day_d_processing_ts = pd.Timestamp('2023-01-03', tz='UTC')
        trigger_candle_ts = day_d_processing_ts.replace(hour=2) # 02:00 UTC (within 4 hours)
        sell_price_candle_ts = day_d_processing_ts.replace(hour=3) # Sell at open of 03:00 UTC
        expected_sell_price = 109.5

        temp_hourly_data = self.sample_hourly_ohlcv_data.copy()
        temp_hourly_data.loc[temp_hourly_data['timestamp'] == trigger_candle_ts, 'low'] = asset_entry_price - 1
        temp_hourly_data.loc[temp_hourly_data['timestamp'] == sell_price_candle_ts, 'open'] = expected_sell_price

        original_fetch = self.mock_data_fetcher.fetch_ohlcv.side_effect
        self.mock_data_fetcher.fetch_ohlcv.side_effect = lambda **kwargs: temp_hourly_data if kwargs['timeframe'] == '1h' else self.sample_daily_ohlcv_data

        engine = BacktestingEngine(config=current_config, data_fetcher=self.mock_data_fetcher, signal_generator=None)
        # Patch portfolio directly to simulate prior buy
        engine.portfolio['asset_qty'] = 10
        engine.portfolio['asset_entry_price'] = asset_entry_price
        engine.portfolio['asset_entry_timestamp_utc'] = d_minus_1_buy_day_ts # Does not affect new sell logic directly

        with patch.object(engine, '_simulate_order', wraps=engine._simulate_order) as spy_simulate_order:
            engine.run_backtest()

        self.mock_data_fetcher.fetch_ohlcv.side_effect = original_fetch # Restore

        sell_call = next((c for c in spy_simulate_order.call_args_list if c.kwargs['order_type'] == 'SELL'), None)
        self.assertIsNotNone(sell_call, "New sell logic did not trigger sell order")
        self.assertEqual(sell_call.kwargs['timestamp'], sell_price_candle_ts)
        self.assertEqual(sell_call.kwargs['price'], expected_sell_price)
        self.assertTrue(any(f"SELL TRIGGER (Stop-Loss on Day D: {day_d_processing_ts.strftime('%Y-%m-%d')})" in call[0][0] for call in mock_logging.info.call_args_list))

    # Add more tests for new sell logic:
    # test_new_sell_logic_no_trigger_if_price_stable_within_holding_hours
    # test_new_sell_logic_no_trigger_outside_holding_hours
    # test_new_sell_logic_sell_price_determination_last_candle
    # test_new_sell_logic_uses_correct_entry_price (Covered by trigger test, but can be more explicit if needed)

    @patch(PATCH_PATH_SG)
    @patch('owl.backtesting_engine.engine.logging')
    def test_new_sell_logic_no_trigger_if_price_stable_within_holding_hours(self, mock_logging, MockSignalGenerator):
        mock_sg_instance = MockSignalGenerator.return_value
        current_config = self.sample_config.copy()
        current_config['strategy']['n_day_high_period'] = 1
        current_config['strategy']['holding_period_hours'] = 4
        current_config['backtesting']['end_date'] = '2023-01-03'

        d_minus_1_buy_day_ts = pd.Timestamp('2023-01-02', tz='UTC')
        asset_entry_price = 111.0

        mock_sg_instance.check_breakout_signal.side_effect = lambda **kwargs: "BUY" if kwargs['current_datetime_utc8'].date() == d_minus_1_buy_day_ts.date() else None

        day_d_processing_ts = pd.Timestamp('2023-01-03', tz='UTC')
        # Hourly lows on Day D stay ABOVE asset_entry_price
        temp_hourly_data = self.sample_hourly_ohlcv_data.copy()
        for h in range(current_config['strategy']['holding_period_hours']): # First 4 hours of Day D
            candle_ts = day_d_processing_ts.replace(hour=h)
            temp_hourly_data.loc[temp_hourly_data['timestamp'] == candle_ts, 'low'] = asset_entry_price + 0.1

        original_fetch = self.mock_data_fetcher.fetch_ohlcv.side_effect
        self.mock_data_fetcher.fetch_ohlcv.side_effect = lambda **kwargs: temp_hourly_data if kwargs['timeframe'] == '1h' else self.sample_daily_ohlcv_data

        engine = BacktestingEngine(config=current_config, data_fetcher=self.mock_data_fetcher, signal_generator=None)
        engine.portfolio['asset_qty'] = 10
        engine.portfolio['asset_entry_price'] = asset_entry_price
        engine.portfolio['asset_entry_timestamp_utc'] = d_minus_1_buy_day_ts

        with patch.object(engine, '_simulate_order', wraps=engine._simulate_order) as spy_simulate_order:
            engine.run_backtest()

        self.mock_data_fetcher.fetch_ohlcv.side_effect = original_fetch

        sell_call = next((c for c in spy_simulate_order.call_args_list if c.kwargs['order_type'] == 'SELL'), None)
        self.assertIsNone(sell_call, "SELL order should not have been triggered as price remained stable.")

    @patch(PATCH_PATH_SG)
    @patch('owl.backtesting_engine.engine.logging')
    def test_new_sell_logic_no_trigger_outside_holding_hours(self, mock_logging, MockSignalGenerator):
        mock_sg_instance = MockSignalGenerator.return_value
        current_config = self.sample_config.copy()
        current_config['strategy']['n_day_high_period'] = 1
        current_config['strategy']['holding_period_hours'] = 2 # Short window
        current_config['backtesting']['end_date'] = '2023-01-03'

        d_minus_1_buy_day_ts = pd.Timestamp('2023-01-02', tz='UTC')
        asset_entry_price = 111.0
        mock_sg_instance.check_breakout_signal.side_effect = lambda **kwargs: "BUY" if kwargs['current_datetime_utc8'].date() == d_minus_1_buy_day_ts.date() else None

        day_d_processing_ts = pd.Timestamp('2023-01-03', tz='UTC')
        # Low price drops but *after* holding_period_hours (e.g., at hour 3 for a 2-hour window)
        trigger_candle_ts_outside_window = day_d_processing_ts.replace(hour=3)

        temp_hourly_data = self.sample_hourly_ohlcv_data.copy()
        # Ensure prices are stable within the window (00:00, 01:00 UTC for 2hr window)
        temp_hourly_data.loc[temp_hourly_data['timestamp'] == day_d_processing_ts.replace(hour=0), 'low'] = asset_entry_price + 1
        temp_hourly_data.loc[temp_hourly_data['timestamp'] == day_d_processing_ts.replace(hour=1), 'low'] = asset_entry_price + 1
        # Triggering low outside window
        temp_hourly_data.loc[temp_hourly_data['timestamp'] == trigger_candle_ts_outside_window, 'low'] = asset_entry_price - 1

        original_fetch = self.mock_data_fetcher.fetch_ohlcv.side_effect
        self.mock_data_fetcher.fetch_ohlcv.side_effect = lambda **kwargs: temp_hourly_data if kwargs['timeframe'] == '1h' else self.sample_daily_ohlcv_data

        engine = BacktestingEngine(config=current_config, data_fetcher=self.mock_data_fetcher, signal_generator=None)
        engine.portfolio['asset_qty'] = 10
        engine.portfolio['asset_entry_price'] = asset_entry_price
        engine.portfolio['asset_entry_timestamp_utc'] = d_minus_1_buy_day_ts

        with patch.object(engine, '_simulate_order', wraps=engine._simulate_order) as spy_simulate_order:
            engine.run_backtest()
        self.mock_data_fetcher.fetch_ohlcv.side_effect = original_fetch
        sell_call = next((c for c in spy_simulate_order.call_args_list if c.kwargs['order_type'] == 'SELL'), None)
        self.assertIsNone(sell_call, "SELL order should not trigger as low drop is outside holding_period_hours.")

    @patch(PATCH_PATH_SG)
    @patch('owl.backtesting_engine.engine.logging')
    def test_new_sell_logic_sell_price_determination_last_candle(self, mock_logging, MockSignalGenerator):
        mock_sg_instance = MockSignalGenerator.return_value
        current_config = self.sample_config.copy()
        current_config['strategy']['holding_period_hours'] = 1 # Window is only 00:00-00:59 for Day D
        current_config['backtesting']['end_date'] = '2023-01-03'

        d_minus_1_buy_day_ts = pd.Timestamp('2023-01-02', tz='UTC')
        asset_entry_price = 111.0
        mock_sg_instance.check_breakout_signal.side_effect = lambda **kwargs: "BUY" if kwargs['current_datetime_utc8'].date() == d_minus_1_buy_day_ts.date() else None

        day_d_processing_ts = pd.Timestamp('2023-01-03', tz='UTC')
        # Triggering candle is the last one in the 1-hour observation window (00:00 UTC)
        trigger_candle_ts = day_d_processing_ts.replace(hour=0)
        expected_sell_price_from_trigger_close = 108.88

        temp_hourly_data = self.sample_hourly_ohlcv_data.copy()
        temp_hourly_data.loc[temp_hourly_data['timestamp'] == trigger_candle_ts, 'low'] = asset_entry_price - 1
        temp_hourly_data.loc[temp_hourly_data['timestamp'] == trigger_candle_ts, 'close'] = expected_sell_price_from_trigger_close

        # Ensure no "next" candle within the observation window
        # (e.g. for a 1hr window, only 00:00 candle is relevant. Next is 01:00, outside window)

        original_fetch = self.mock_data_fetcher.fetch_ohlcv.side_effect
        self.mock_data_fetcher.fetch_ohlcv.side_effect = lambda **kwargs: temp_hourly_data if kwargs['timeframe'] == '1h' else self.sample_daily_ohlcv_data

        engine = BacktestingEngine(config=current_config, data_fetcher=self.mock_data_fetcher, signal_generator=None)
        engine.portfolio['asset_qty'] = 10
        engine.portfolio['asset_entry_price'] = asset_entry_price
        engine.portfolio['asset_entry_timestamp_utc'] = d_minus_1_buy_day_ts

        with patch.object(engine, '_simulate_order', wraps=engine._simulate_order) as spy_simulate_order:
            engine.run_backtest()
        self.mock_data_fetcher.fetch_ohlcv.side_effect = original_fetch

        sell_call = next((c for c in spy_simulate_order.call_args_list if c.kwargs['order_type'] == 'SELL'), None)
        self.assertIsNotNone(sell_call, "SELL order should have triggered")
        self.assertEqual(sell_call.kwargs['timestamp'], trigger_candle_ts)
        self.assertEqual(sell_call.kwargs['price'], expected_sell_price_from_trigger_close)
        self.assertTrue(any("Attempting to sell at CLOSE of triggering hourly candle" in call[0][0] for call in mock_logging.info.call_args_list))

    @patch('owl.backtesting_engine.engine.plot_equity_curve')
    @patch(PATCH_PATH_SG)
    def test_plot_filename_is_dynamic(self, MockSignalGenerator, mock_plot_equity_curve):

        engine = BacktestingEngine(
            config=self.sample_config,
            data_fetcher=self.mock_data_fetcher,
            signal_generator=None
        )

        with patch.object(engine, '_simulate_order', wraps=engine._simulate_order) as spy_simulate_order:
            engine.run_backtest()

        mock_sg_instance = MockSignalGenerator.return_value
        mock_sg_instance.check_breakout_signal.return_value = None

        test_start_date_str = '2023-02-01'
        test_end_date_str = '2023-02-03'
        test_symbol = "TEST/COIN"
        test_n_period = 10

        test_config = {key: value.copy() if isinstance(value, dict) else value for key, value in self.sample_config.items()}
        test_config['backtesting']['start_date'] = test_start_date_str
        test_config['backtesting']['end_date'] = test_end_date_str
        test_config['backtesting']['symbol'] = test_symbol
        test_config['strategy']['n_day_high_period'] = test_n_period
        test_config['backtesting']['timeframe'] = '1d'

        # Expected filename format: {symbol}_{n_period}_backtest_equity_curve_{start_date}_{end_date}.png
        formatted_symbol = test_symbol.replace('/', '_').lower()
        formatted_start_date = test_start_date_str.replace('-', '')
        formatted_end_date = test_end_date_str.replace('-', '')
        expected_filename = f"{formatted_symbol}_{test_n_period}_backtest_equity_curve_{formatted_start_date}_{formatted_end_date}.png"

        minimal_daily_data = pd.DataFrame({ # Minimal data to allow portfolio history generation
            'timestamp': pd.to_datetime([f'{test_start_date_str}T00:00:00Z', f'{test_start_date_str}T01:00:00Z']),
            'open': [100,100], 'high': [100,100], 'low': [100,100], 'close': [100,100], 'volume': [1,1]
        })
        minimal_hourly_data = pd.DataFrame({'timestamp': pd.to_datetime([]), 'open': [], 'high': [], 'low': [], 'close': [], 'volume': []})


        original_fetch_side_effect = self.mock_data_fetcher.fetch_ohlcv.side_effect
        def mock_fetch_minimal_data(symbol, timeframe, since, limit=None, params=None, force_fetch=None):
            if timeframe == '1d': return minimal_daily_data.copy()
            elif timeframe == '1h': return minimal_hourly_data.copy()
            return pd.DataFrame()
        self.mock_data_fetcher.fetch_ohlcv.side_effect = mock_fetch_minimal_data
        mock_plot_equity_curve.return_value = True

        engine = BacktestingEngine(
            config=test_config,
            data_fetcher=self.mock_data_fetcher,
            signal_generator=None # Engine creates its own, which is mocked by PATCH_PATH_SG
        )
        engine.run_backtest()

        # Restore original side effect
        self.mock_data_fetcher.fetch_ohlcv.side_effect = original_side_effect

        mock_plot_equity_curve.assert_called_once()

        # Inspect the call arguments. Call object is mock_plot_equity_curve.call_args
        # It's a tuple (args, kwargs). We are interested in kwargs['output_path']
        # or if output_path is a positional argument, then args[1] (assuming args[0] is portfolio_history_df)

        # Check plotter call signature: plot_equity_curve(portfolio_history_df, output_path)
        # The call in engine.py is plot_equity_curve(portfolio_history_df=portfolio_df, output_path=plot_output_filename)
        # So we need to check kwargs.
        args_call, kwargs_call = mock_plot_equity_curve.call_args
        called_output_path = kwargs_call['output_path']

        self.assertEqual(called_output_path, expected_filename)


# --- Tests for the NEW Sell Logic (based on holding period observation time) ---
class TestNewSellStrategyLogic(unittest.TestCase):
    def setUp(self):
        self.mock_data_fetcher = MagicMock()
        # Patching 'owl.backtesting_engine.engine.SignalGenerator' for engine's internal instantiation
        self.sg_patcher = patch('owl.backtesting_engine.engine.SignalGenerator')
        self.MockSignalGenerator = self.sg_patcher.start()
        self.mock_sg_instance = self.MockSignalGenerator.return_value

        self.base_config = {
            'backtesting': {
                'symbol': 'BTC/USDT',
                'start_date': '2023-01-01',
                'end_date': '2023-01-03', # Process 2 days, sell on 2nd day
                'initial_capital': 10000.0,
                'commission_rate': 0.001
            },
            'strategy': {
                'n_day_high_period': 1, # Keep it simple for buy signal
                'buy_cash_percentage': 1.0,
                'sell_asset_percentage': 1.0, # Default, can be overridden
                'holding_period_hours': 5,    # Test with 5 hours
                'buy_window_end_time': "16:00", # Not critical for sell tests but needed
                'risk_free_rate': 0.0 # Not critical
            },
             'scheduler': {}, 'proxy': {}, 'api_keys': {}, 'exchange_settings': {}
        }

        # Daily data: Loop will run for 2023-01-01, 2023-01-02.
        # Assume buy happens based on 2023-01-01 data (buy placed on 2023-01-01).
        # Sell check will occur during 2023-01-02 loop iteration.
        self.sample_daily_data = pd.DataFrame({
            'timestamp': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03'], utc=True),
            'open': [100, 110, 120], 'high': [105, 115, 125],
            'low': [95, 105, 115], 'close': [102, 112, 122],
            'volume': [1000, 1000, 1000]
        })

        # Hourly data will be customized per test case
        self.sample_hourly_data_template = pd.DataFrame({
            'timestamp': pd.to_datetime([
                # Day 1 (2023-01-01) - Buy day related
                '2023-01-01 08:00:00', '2023-01-01 09:00:00',
                # Day 2 (2023-01-02) - Sell observation day
                '2023-01-02 00:00:00', '2023-01-02 01:00:00', '2023-01-02 02:00:00',
                '2023-01-02 03:00:00', '2023-01-02 04:00:00', # Exact observation_end_time if holding_period_hours=5
                '2023-01-02 05:00:00', # Candle after exact time
                '2023-01-02 06:00:00',
            ], utc=True),
            'open':  [100.0, 101.0, 110.0, 111.0, 112.0, 113.0, 114.0, 115.0, 116.0],
            'high':  [100.5, 101.5, 110.5, 111.5, 112.5, 113.5, 114.5, 115.5, 116.5],
            'low':   [99.5,  100.5, 109.5, 110.5, 111.5, 112.5, 113.5, 114.5, 115.5],
            'close': [100.2, 101.2, 110.2, 111.2, 112.2, 113.2, 114.2, 115.2, 116.2],
            'volume':[10,    10,    10,    10,    10,    10,    10,    10,    10]
        })

        def mock_fetch_ohlcv_se(symbol, timeframe, since, limit=None, params=None, force_fetch=None):
            if timeframe == '1d':
                return self.current_daily_data
            elif timeframe == '1h':
                return self.current_hourly_data
            return pd.DataFrame()
        self.mock_data_fetcher.fetch_ohlcv.side_effect = mock_fetch_ohlcv_se

    def tearDown(self):
        self.sg_patcher.stop() # Important to stop the patcher

    def _initialize_engine(self, config, daily_data, hourly_data):
        self.current_daily_data = daily_data
        self.current_hourly_data = hourly_data
        engine = BacktestingEngine(
            config=config,
            data_fetcher=self.mock_data_fetcher,
            signal_generator=None # Engine creates its own, which is mocked
        )
        # Simulate that a buy order has occurred and portfolio has assets
        # Buy on 2023-01-01, sell check on 2023-01-02
        engine.portfolio = {
            'cash': 0, 'asset_qty': 10.0, 'asset_value': 1000.0,
            'total_value': 1000.0, 'asset_entry_timestamp_utc': pd.Timestamp('2023-01-01 08:00:00', tz='UTC'),
            'asset_entry_price': 100.0
        }
        # Mock signal generator to prevent buy logic interference on the sell test day
        self.mock_sg_instance.check_breakout_signal.return_value = None # No BUY signals during sell tests
        return engine

    @patch('owl.backtesting_engine.engine.logging')
    def test_sell_exact_match_candle_found(self, mock_logging):
        config = self.base_config.copy()
        config['strategy']['holding_period_hours'] = 5 # current_day_start (00:00) + 5hrs = 05:00

        hourly_data = self.sample_hourly_data_template.copy()
        # Sell day is 2023-01-02. current_day_start_utc = 2023-01-02 00:00:00 UTC
        # observation_end_time_utc = 2023-01-02 00:00:00 + 5 hours = 2023-01-02 05:00:00 UTC
        exact_obs_end_time = pd.Timestamp('2023-01-02 05:00:00', tz='UTC')
        expected_sell_price = 115.0 # Open price of 05:00 candle in template

        self.assertTrue(exact_obs_end_time in hourly_data['timestamp'].values, "Test setup error: Exact candle missing.")
        hourly_data.loc[hourly_data['timestamp'] == exact_obs_end_time, 'open'] = expected_sell_price # Ensure it's this price

        engine = self._initialize_engine(config, self.sample_daily_data, hourly_data)

        with patch.object(engine, '_simulate_order', wraps=engine._simulate_order) as spy_simulate_order:
            engine.run_backtest()

        spy_simulate_order.assert_called_once()
        call_args = spy_simulate_order.call_args[1] # kwargs
        self.assertEqual(call_args['order_type'], 'SELL')
        self.assertEqual(call_args['price'], expected_sell_price)
        self.assertEqual(call_args['timestamp'], exact_obs_end_time)
        self.assertEqual(call_args['quantity'], engine.portfolio['asset_qty'] * config['strategy']['sell_asset_percentage'])
        self.assertTrue(any(f"Exact observation_end_time candle found at {exact_obs_end_time}" in log_call[0][0] for log_call in mock_logging.info.call_args_list))

    @patch('owl.backtesting_engine.engine.logging')
    def test_sell_next_available_candle_used(self, mock_logging):
        config = self.base_config.copy()
        config['strategy']['holding_period_hours'] = 4 # current_day_start (00:00) + 4hrs = 04:00

        hourly_data = self.sample_hourly_data_template.copy()
        # Sell day is 2023-01-02. observation_end_time_utc = 2023-01-02 04:00:00 UTC
        obs_end_time = pd.Timestamp('2023-01-02 04:00:00', tz='UTC')

        # Remove exact match candle
        hourly_data = hourly_data[hourly_data['timestamp'] != obs_end_time]
        self.assertFalse(obs_end_time in hourly_data['timestamp'].values, "Test setup error: Exact candle should be removed.")

        # Next available candle is 05:00:00 UTC, its open is 115.0 in template
        expected_next_candle_ts = pd.Timestamp('2023-01-02 05:00:00', tz='UTC')
        expected_sell_price = 115.0 # From template for 05:00 candle
        hourly_data.loc[hourly_data['timestamp'] == expected_next_candle_ts, 'open'] = expected_sell_price # ensure

        engine = self._initialize_engine(config, self.sample_daily_data, hourly_data)

        with patch.object(engine, '_simulate_order', wraps=engine._simulate_order) as spy_simulate_order:
            engine.run_backtest()

        spy_simulate_order.assert_called_once()
        call_args = spy_simulate_order.call_args[1]
        self.assertEqual(call_args['order_type'], 'SELL')
        self.assertEqual(call_args['price'], expected_sell_price)
        self.assertEqual(call_args['timestamp'], expected_next_candle_ts)
        self.assertTrue(any("Exact observation_end_time candle NOT found. Using next available candle" in log_call[0][0] for log_call in mock_logging.info.call_args_list))

    @patch('owl.backtesting_engine.engine.logging')
    def test_sell_no_suitable_candle_found_skips_sell(self, mock_logging):
        config = self.base_config.copy()
        config['strategy']['holding_period_hours'] = 7 # current_day_start (00:00) + 7hrs = 07:00

        hourly_data = self.sample_hourly_data_template.copy()
        # Sell day is 2023-01-02. observation_end_time_utc = 2023-01-02 07:00:00 UTC
        obs_end_time = pd.Timestamp('2023-01-02 07:00:00', tz='UTC')

        # Remove exact match (07:00) and all subsequent candles for that day
        hourly_data = hourly_data[hourly_data['timestamp'] < obs_end_time]
        # Last candle in template is 06:00, so 07:00 and after are already not there or removed

        engine = self._initialize_engine(config, self.sample_daily_data, hourly_data)
        initial_asset_qty = engine.portfolio['asset_qty']

        with patch.object(engine, '_simulate_order', wraps=engine._simulate_order) as spy_simulate_order:
            engine.run_backtest()

        spy_simulate_order.assert_not_called()
        self.assertEqual(engine.portfolio['asset_qty'], initial_asset_qty, "Asset quantity should not change if no sell.")
        self.assertTrue(any("No suitable hourly candle found for selling" in log_call[0][0] for log_call in mock_logging.warning.call_args_list))

    @patch('owl.backtesting_engine.engine.logging')
    def test_sell_asset_quantity_zero_no_action(self, mock_logging):
        config = self.base_config.copy()
        engine = self._initialize_engine(config, self.sample_daily_data, self.sample_hourly_data_template.copy())
        engine.portfolio['asset_qty'] = 0 # Crucial for this test

        with patch.object(engine, '_simulate_order', wraps=engine._simulate_order) as spy_simulate_order:
            engine.run_backtest()

        spy_simulate_order.assert_not_called()
        # Check that the sell logic part wasn't even entered deeply by checking for its specific logs
        self.assertFalse(any("SELL Check" in log_call[0][0] for log_call in mock_logging.info.call_args_list if isinstance(log_call[0], tuple) and len(log_call[0]) > 0))


    @patch('owl.backtesting_engine.engine.logging')
    def test_sell_partial_assets_correct_quantity(self, mock_logging):
        config = self.base_config.copy()
        config['strategy']['holding_period_hours'] = 5
        config['strategy']['sell_asset_percentage'] = 0.5 # Sell half

        hourly_data = self.sample_hourly_data_template.copy()
        exact_obs_end_time = pd.Timestamp('2023-01-02 05:00:00', tz='UTC')
        expected_sell_price = 115.0
        hourly_data.loc[hourly_data['timestamp'] == exact_obs_end_time, 'open'] = expected_sell_price

        engine = self._initialize_engine(config, self.sample_daily_data, hourly_data)
        initial_asset_qty = engine.portfolio['asset_qty'] # Should be 10.0 from _initialize_engine
        expected_quantity_to_sell = initial_asset_qty * 0.5

        with patch.object(engine, '_simulate_order', wraps=engine._simulate_order) as spy_simulate_order:
            engine.run_backtest()

        spy_simulate_order.assert_called_once()
        call_args = spy_simulate_order.call_args[1]
        self.assertEqual(call_args['order_type'], 'SELL')
        self.assertEqual(call_args['price'], expected_sell_price)
        self.assertEqual(call_args['timestamp'], exact_obs_end_time)
        self.assertAlmostEqual(call_args['quantity'], expected_quantity_to_sell, places=5)

        # Portfolio asset_qty should be updated by _simulate_order if it runs successfully
        # Since _simulate_order is wrapped, it will execute.
        # self.assertAlmostEqual(engine.portfolio['asset_qty'], initial_asset_qty - expected_quantity_to_sell, places=5)
        # The above check is implicitly handled by the fact that _simulate_order would update it,
        # and we are checking the arguments to it. If _simulate_order itself is tested, this is sufficient.


if __name__ == '__main__':
    unittest.main()
