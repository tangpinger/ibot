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
                'buy_window_start_time': "09:00",
                'buy_window_end_time': "17:00",
            },
            'scheduler': {}, # Empty as per previous changes
            'proxy': {}, 'api_keys': {}, 'exchange_settings': {}
        }

        # Sample OHLCV data: 3 full days of hourly data + 1 more hour = 73 hours
        # Start from 2023-01-01 00:00:00 UTC up to 2023-01-04 00:00:00 UTC
        # Start from 2023-01-01 00:00:00 UTC up to 2023-01-04 00:00:00 UTC
        timestamps = pd.date_range(start='2023-01-01 00:00:00', end='2023-01-04 00:00:00', freq='h', tz='UTC')
        self.sample_ohlcv_data = pd.DataFrame({
            'timestamp': timestamps,
            'open': [100 + i*0.1 for i in range(len(timestamps))],
            'high': [105 + i*0.1 + (i%3)*0.2 for i in range(len(timestamps))],
            'low': [95 + i*0.1 - (i%3)*0.2 for i in range(len(timestamps))],
            'close': [100 + i*0.1 + ( (i%5)-2 )*0.1 for i in range(len(timestamps))],
            'volume': [1000 + i*10 for i in range(len(timestamps))]
        })
        self.mock_data_fetcher.fetch_ohlcv.return_value = self.sample_ohlcv_data

    @patch(PATCH_PATH_SG)
    @patch('owl.backtesting_engine.engine.logging')
    def test_buy_and_sell_trade_execution_holding_period(self, mock_logging, MockSignalGenerator):
        mock_sg_instance = MockSignalGenerator.return_value

        # --- Scenario Setup ---
        # Config: holding_period_days = 1. Buy window 09:00-17:00. Sell at 10:00 Beijing time.
        # Buy on 2023-01-02 (Monday) at 10:00 Beijing time (02:00 UTC).
        # Expected sell: Day of buy + 1 day (holding_period_days) = Day D+1. Sell occurs on Day D+1 at 10:00.
        # So, if buy is Mon 10:00, hold Tue, sell Wed 10:00. (days_passed will be 2)
        # Corrected understanding: days_passed >= holding_period_days (1) means sell on Tue 10:00.

        buy_day_utc = pd.Timestamp('2023-01-02 00:00:00', tz='UTC') # Monday
        buy_hour_utc8 = 10 # Within 09:00-17:00 buy window
        buy_trigger_utc_timestamp = buy_day_utc.replace(hour=2) # 02:00 UTC is 10:00 Beijing Time

        # Expected sell day: holding_period_days = 1. Buy on day X. Sell on day X+1 at 10:00.
        # Buy on Jan 2 (Monday). Sell on Jan 3 (Tuesday) at 10:00 Beijing Time.
        sell_day_utc = pd.Timestamp('2023-01-03 00:00:00', tz='UTC') # Tuesday
        sell_trigger_utc_timestamp = sell_day_utc.replace(hour=2) # 02:00 UTC is 10:00 Beijing Time

        def check_breakout_side_effect(*args, **kwargs):
            dt_utc8 = kwargs.get('current_datetime_utc8')
            # Trigger BUY at the specific hour on the buy day
            if dt_utc8.year == buy_trigger_utc_timestamp.year and \
               dt_utc8.month == buy_trigger_utc_timestamp.month and \
               dt_utc8.day == buy_trigger_utc_timestamp.day and \
               dt_utc8.hour == buy_hour_utc8: # buy_hour_utc8 = 10
                return "BUY"
            return None

        mock_sg_instance.check_breakout_signal.side_effect = check_breakout_side_effect
        # check_breakdown_signal no longer exists on the instance

        engine = BacktestingEngine(
            config=self.sample_config,
            data_fetcher=self.mock_data_fetcher,
            signal_generator=None
        )

        with patch.object(engine, '_simulate_order', wraps=engine._simulate_order) as spy_simulate_order:
            engine.run_backtest()

        # Check SignalGenerator was instantiated correctly (with new, reduced signature)
        MockSignalGenerator.assert_called_once_with(
            n_day_high_period=self.sample_config['strategy']['n_day_high_period'],
            buy_window_start_time_str=self.sample_config['strategy']['buy_window_start_time'],
            buy_window_end_time_str=self.sample_config['strategy']['buy_window_end_time']
            # No m_day_low_period or sell_window args
        )

        # --- Assertions for BUY order ---
        buy_order_call = None
        for call_args_list_item in spy_simulate_order.call_args_list:
            if call_args_list_item.kwargs.get('order_type') == 'BUY':
                buy_order_call = call_args_list_item
                break
        self.assertIsNotNone(buy_order_call, "BUY order was not simulated")

        buy_kwargs = buy_order_call.kwargs
        self.assertEqual(buy_kwargs['timestamp'], buy_trigger_utc_timestamp)
        buy_price_actual = self.sample_ohlcv_data[self.sample_ohlcv_data['timestamp'] == buy_trigger_utc_timestamp]['close'].iloc[0]
        self.assertEqual(buy_kwargs['price'], buy_price_actual)

        # Check portfolio state after BUY
        self.assertEqual(engine.portfolio['asset_entry_timestamp_utc'], buy_trigger_utc_timestamp)
        self.assertEqual(engine.portfolio['asset_entry_price'], buy_price_actual)
        qty_bought = buy_kwargs['quantity'] # Store for sell check

        # --- Assertions for SELL order (Holding Period) ---
        sell_order_call = None
        for call_args_list_item in spy_simulate_order.call_args_list:
            if call_args_list_item.kwargs.get('order_type') == 'SELL':
                sell_order_call = call_args_list_item
                break
        self.assertIsNotNone(sell_order_call, "SELL order was not simulated due to holding period")

        sell_kwargs = sell_order_call.kwargs
        self.assertEqual(sell_kwargs['timestamp'], sell_trigger_utc_timestamp)
        sell_price_actual = self.sample_ohlcv_data[self.sample_ohlcv_data['timestamp'] == sell_trigger_utc_timestamp]['close'].iloc[0]
        self.assertEqual(sell_kwargs['price'], sell_price_actual)
        self.assertAlmostEqual(sell_kwargs['quantity'], qty_bought * self.sample_config['strategy']['sell_asset_percentage'], places=4)

        # Check portfolio state after SELL
        self.assertIsNone(engine.portfolio['asset_entry_timestamp_utc'])
        self.assertEqual(engine.portfolio['asset_entry_price'], 0.0)
        self.assertAlmostEqual(engine.portfolio['asset_qty'], 0, places=4)

    @patch(PATCH_PATH_SG)
    def test_buy_restriction_when_holding_asset(self, MockSignalGenerator):
        mock_sg_instance = MockSignalGenerator.return_value

        # Configure to BUY on the first possible signal
        first_buy_trigger_ts = self.sample_ohlcv_data['timestamp'][self.sample_config['strategy']['n_day_high_period']]
        first_buy_trigger_dt_utc8 = first_buy_trigger_ts.tz_convert('Asia/Shanghai')

        def breakout_side_effect(*args, **kwargs):
            dt_utc8 = kwargs.get('current_datetime_utc8')
            if dt_utc8 == first_buy_trigger_dt_utc8:
                 # Ensure this time is within buy window for the test config
                if self.sample_config['strategy']['buy_window_start_time'] <= dt_utc8.strftime("%H:%M") <= self.sample_config['strategy']['buy_window_end_time']:
                    return "BUY"
            return None
        mock_sg_instance.check_breakout_signal.side_effect = breakout_side_effect

        engine = BacktestingEngine(
            config=self.sample_config,
            data_fetcher=self.mock_data_fetcher,
            signal_generator=None
        )

        with patch.object(engine, '_simulate_order', wraps=engine._simulate_order) as spy_simulate_order:
            engine.run_backtest()

        # Assert that check_breakout_signal was called initially
        self.assertGreater(mock_sg_instance.check_breakout_signal.call_count, 0)
        initial_call_count = mock_sg_instance.check_breakout_signal.call_count

        # Count BUY orders
        buy_orders = [c for c in spy_simulate_order.call_args_list if c.kwargs.get('order_type') == 'BUY']
        self.assertEqual(len(buy_orders), 1, "Should only execute one BUY order even if signals persist")

        # Further run a few steps to see if check_breakout_signal is still called
        # This part is tricky because run_backtest consumes the generator.
        # The core check is that only one BUY order is placed.
        # The engine logic is: if self.portfolio['asset_qty'] == 0: then check buy signal.
        # So, after the first buy, asset_qty > 0, and check_breakout_signal should not be called.

        # To verify check_breakout_signal call count properly, we'd need to know exactly when it stops being called.
        # It's called until a BUY happens. After BUY, asset_qty > 0.
        # On subsequent candles, the condition self.portfolio['asset_qty'] == 0 fails.
        # So, check_breakout_signal call count should be number of candles until first BUY (where idx >= n_period)

        first_buy_candle_index = -1
        for idx, row_ts in enumerate(self.sample_ohlcv_data['timestamp']):
            if row_ts == first_buy_trigger_ts:
                first_buy_candle_index = idx
                break

        expected_calls_to_check_breakout = first_buy_candle_index - self.sample_config['strategy']['n_day_high_period'] + 1
        self.assertEqual(mock_sg_instance.check_breakout_signal.call_count, expected_calls_to_check_breakout)


if __name__ == '__main__':
    unittest.main()
