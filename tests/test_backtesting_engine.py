import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
from datetime import datetime
import pytz # For timezone handling

from owl.backtesting_engine.engine import BacktestingEngine
from owl.data_fetcher.fetcher import DataFetcher # For spec in MagicMock
from owl.signal_generator.generator import SignalGenerator # For spec in MagicMock

class TestBacktestingEngine(unittest.TestCase):

    def setUp(self):
        """Set up test fixtures, including mock objects and configuration."""
        self.mock_config = {
            'backtesting': {
                'symbol': 'TEST/USD',
                'timeframe': '1d',
                'start_date': '2023-01-01',
                'end_date': '2023-01-05', # Full period for most tests
                'initial_capital': 10000.0,
                'commission_rate': 0.001 # 0.1%
            },
            'strategy': {
                'n_day_high_period': 3, # Small period for testing (e.g. needs 3 prior days)
                'buy_cash_percentage': 0.5,
                'risk_free_rate': 0.0
            }
        }

        self.mock_data_fetcher = MagicMock(spec=DataFetcher)
        self.mock_signal_generator = MagicMock(spec=SignalGenerator)

        # Sample historical data
        self.sample_data = pd.DataFrame({
            'timestamp': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05']),
            'open': [100, 102, 101, 103, 105],
            'high': [103, 104, 103, 106, 107], # Day 4 high (idx 3) is 106
            'low': [99, 101, 100, 102, 104],
            'close': [102, 101, 103, 105, 106], # Day 4 close (idx 3) is 105
            'volume': [10, 12, 11, 13, 14]
        })
        # Ensure timestamps are timezone-aware (UTC) as engine might expect
        self.sample_data['timestamp'] = self.sample_data['timestamp'].apply(lambda x: x.tz_localize('UTC') if x.tzinfo is None else x.tz_convert('UTC'))

        self.mock_data_fetcher.fetch_ohlcv.return_value = self.sample_data.copy() # Use a copy

        # Instantiate the engine for most tests
        # Errors in __init__ due to config are tested implicitly by just creating it
        self.engine = BacktestingEngine(
            config=self.mock_config,
            data_fetcher=self.mock_data_fetcher,
            signal_generator=self.mock_signal_generator
        )

    def test_initialization(self):
        """Test that the engine initializes portfolio and commission rate correctly."""
        # initial_capital is directly taken from config in __init__
        self.assertEqual(self.engine.portfolio['cash'], 10000.0)
        self.assertEqual(self.engine.commission_rate, 0.001)
        self.assertEqual(self.engine.portfolio['asset_qty'], 0.0)

    def test_simulate_buy_order_sufficient_cash(self):
        """Test simulating a BUY order with enough cash."""
        order_time = pd.Timestamp('2023-01-01', tz='UTC')
        success = self.engine._simulate_order(
            timestamp=order_time,
            order_type='BUY',
            symbol='TEST/USD',
            price=100.0,
            quantity=10
        )
        self.assertTrue(success)
        expected_cost = 100.0 * 10
        expected_commission = expected_cost * 0.001
        expected_cash_after_buy = 10000.0 - expected_cost - expected_commission

        self.assertAlmostEqual(self.engine.portfolio['cash'], expected_cash_after_buy)
        self.assertEqual(self.engine.portfolio['asset_qty'], 10)
        self.assertEqual(len(self.engine.trades), 1)
        trade = self.engine.trades[0]
        self.assertEqual(trade['type'], 'BUY')
        self.assertEqual(trade['price'], 100.0)
        self.assertEqual(trade['quantity'], 10)

    def test_simulate_buy_order_insufficient_cash(self):
        """Test simulating a BUY order without enough cash."""
        order_time = pd.Timestamp('2023-01-01', tz='UTC')
        success = self.engine._simulate_order(
            timestamp=order_time,
            order_type='BUY',
            symbol='TEST/USD',
            price=1000.0, # High price
            quantity=11 # Results in 11000 cost, more than initial_capital
        )
        self.assertFalse(success)
        self.assertEqual(self.engine.portfolio['cash'], 10000.0) # Unchanged
        self.assertEqual(self.engine.portfolio['asset_qty'], 0)   # Unchanged
        self.assertEqual(len(self.engine.trades), 0)             # No trade recorded

    def test_run_backtest_data_filtering_and_portfolio_history(self):
        """Test that run_backtest filters data by end_date and records portfolio history."""
        # Modify config for this specific test to have a shorter end_date
        test_config = self.mock_config.copy()
        test_config['backtesting'] = self.mock_config['backtesting'].copy()
        test_config['backtesting']['end_date'] = '2023-01-03' # Should include 01-01, 01-02, 01-03

        # Create a new data fetcher mock for this engine to avoid side effects on self.mock_data_fetcher
        local_mock_df = MagicMock(spec=DataFetcher)
        local_mock_df.fetch_ohlcv.return_value = self.sample_data.copy()

        engine_filtered = BacktestingEngine(
            config=test_config,
            data_fetcher=local_mock_df,
            signal_generator=self.mock_signal_generator # Can reuse signal generator mock
        )

        self.mock_signal_generator.check_breakout_signal.return_value = None # Ensure no trades
        engine_filtered.run_backtest()

        local_mock_df.fetch_ohlcv.assert_called_once()
        # Portfolio history should have one entry for each day up to and including end_date
        # Data: 01-01, 01-02, 01-03. So 3 entries.
        self.assertEqual(len(engine_filtered.portfolio_history), 3)
        self.assertEqual(engine_filtered.portfolio_history[0]['timestamp'], pd.Timestamp('2023-01-01', tz='UTC'))
        self.assertEqual(engine_filtered.portfolio_history[-1]['timestamp'], pd.Timestamp('2023-01-03', tz='UTC'))

    def test_run_backtest_with_buy_signal(self):
        """Test run_backtest execution path when a BUY signal is generated."""
        # Signal generator will return "BUY".
        # n_period = 3. First possible signal check is on index 3 (4th day, '2023-01-04')
        # Data for signal: '2023-01-01', '2023-01-02', '2023-01-03'
        # Current day high for signal: self.sample_data['high'].iloc[3] = 106
        # Timestamp for signal: self.sample_data['timestamp'].iloc[3] = '2023-01-04'
        # Execution price: self.sample_data['close'].iloc[3] = 105

        self.mock_signal_generator.check_breakout_signal.return_value = "BUY"

        # We need to mock _simulate_order to check its call without re-implementing its logic here
        # Or, we can check the effects (portfolio change, trades log)

        self.engine.run_backtest() # Uses the full sample_data (5 days)

        # Signal check should happen for days at index 3 and 4 (total 2 times)
        # N_period is 3. Data length is 5.
        # Iteration 0 (idx 0): skip signal (current_idx < n_period)
        # Iteration 1 (idx 1): skip signal
        # Iteration 2 (idx 2): skip signal
        # Iteration 3 (idx 3): check_breakout_signal called. BUY. Order simulated.
        # Iteration 4 (idx 4): check_breakout_signal called. BUY. Order simulated.
        self.assertEqual(self.mock_signal_generator.check_breakout_signal.call_count, 2) # Days at index 3 and 4

        # Check if trades occurred (due to BUY signals)
        self.assertTrue(len(self.engine.trades) > 0)
        self.assertTrue(self.engine.portfolio['asset_qty'] > 0)

        # Example: First BUY trade details (occurs on 2023-01-04 data)
        # Price = 105. Cash to spend = 10000 * 0.5 = 5000. Qty = 5000 / 105 approx 47.619
        if self.engine.trades:
            first_trade = self.engine.trades[0]
            self.assertEqual(first_trade['type'], 'BUY')
            self.assertEqual(first_trade['price'], 105) # close of 2023-01-04
            expected_qty = (self.mock_config['backtesting']['initial_capital'] *
                            self.mock_config['strategy']['buy_cash_percentage']) / 105
            self.assertAlmostEqual(first_trade['quantity'], expected_qty)

    @patch('owl.backtesting_engine.engine.generate_performance_report') # Patching at source
    def test_full_run_generates_report(self, mock_generate_report):
        """Test that run_backtest calls generate_performance_report."""
        self.mock_signal_generator.check_breakout_signal.return_value = None # No trades
        mock_generate_report.return_value = {"test_metric": 123, "sharpe_ratio": 1.5} # Sample report

        # Capture print output to check if report is printed (optional, more complex)
        # with patch('builtins.print') as mock_print:
        self.engine.run_backtest()

        mock_generate_report.assert_called_once()
        # Check some args passed to report generator
        args, kwargs = mock_generate_report.call_args
        self.assertEqual(kwargs['initial_capital'], self.mock_config['backtesting']['initial_capital'])
        self.assertEqual(len(kwargs['portfolio_history']), len(self.sample_data))

    @patch('owl.backtesting_engine.engine.plot_equity_curve') # Path to plot_equity_curve as imported in engine.py
    @patch('owl.backtesting_engine.engine.generate_performance_report') # Path to generate_performance_report
    def test_full_run_generates_report_and_calls_plotting(self, mock_generate_report, mock_plot_equity_curve):
        # Arrange
        self.mock_signal_generator.check_breakout_signal.return_value = None # No trades for simplicity
        mock_generate_report.return_value = {"test_metric": 123, "final_portfolio_value": 10000} # Example report
        mock_plot_equity_curve.return_value = True # Simulate successful plot generation

        # Act
        self.engine.run_backtest()

        # Assert
        mock_generate_report.assert_called_once()

        # Check if portfolio_history is not empty before asserting plot call
        # self.sample_data has 5 rows, so portfolio_history should not be empty
        self.assertTrue(self.engine.portfolio_history, "Portfolio history should not be empty with sample data.")
        mock_plot_equity_curve.assert_called_once()

        # Optionally, check arguments passed to plot_equity_curve
        args, kwargs = mock_plot_equity_curve.call_args
        self.assertIn('portfolio_history_df', kwargs)
        self.assertIsInstance(kwargs['portfolio_history_df'], pd.DataFrame)
        self.assertEqual(kwargs['output_path'], "backtest_equity_curve.png")
        self.assertEqual(len(kwargs['portfolio_history_df']), len(self.sample_data))


    @patch('owl.backtesting_engine.engine.plot_equity_curve')
    @patch('owl.backtesting_engine.engine.generate_performance_report')
    def test_run_backtest_empty_history_skips_plotting(self, mock_generate_report, mock_plot_equity_curve):
        # Arrange
        # Ensure no data is fetched or it's empty to create empty portfolio_history
        empty_df = pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        # Ensure timestamp column is of datetime type even when empty for consistency
        empty_df['timestamp'] = pd.to_datetime(empty_df['timestamp'])

        # Setup a data fetcher that returns an empty DataFrame for this specific test
        local_mock_df = MagicMock(spec=DataFetcher)
        local_mock_df.fetch_ohlcv.return_value = empty_df

        engine_empty_hist = BacktestingEngine(
            config=self.mock_config,
            data_fetcher=local_mock_df,
            signal_generator=self.mock_signal_generator # Can reuse, won't be called if no data
        )
        mock_generate_report.return_value = {"test_metric": 0, "final_portfolio_value": self.mock_config['backtesting']['initial_capital']}

        # Act
        engine_empty_hist.run_backtest()

        # Assert
        mock_generate_report.assert_called_once() # Report function is still called
        mock_plot_equity_curve.assert_not_called() # Plotting should be skipped


if __name__ == '__main__':
    unittest.main()
