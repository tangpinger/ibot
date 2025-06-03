# owl/backtesting_engine/engine.py
import pandas as pd
from datetime import datetime
import time
import pytz

# Assuming DataFetcher is in owl.data_fetcher.fetcher
# from owl.data_fetcher.fetcher import DataFetcher # Placeholder if direct type hint is needed
# from owl.signal_generator.generator import SignalGenerator # For type hinting if needed, instance is passed
from owl.signal_generator.generator import SignalGenerator # Make sure this is imported
from owl.analytics_reporting.reporter import generate_performance_report
from owl.analytics_reporting.plotter import plot_equity_curve
# pandas as pd is already imported at the top of the file
import logging

class BacktestingEngine:
    """
    Orchestrates the backtesting process.
    """
    def __init__(self, config, data_fetcher, signal_generator):
        """
        Initializes the BacktestingEngine.

        Args:
            config: The configuration object.
            data_fetcher: An instance of the data fetcher.
            signal_generator: An instance of the signal generator.
        """
        self.config = config
        self.data_fetcher = data_fetcher
        # self.signal_generator = signal_generator # Instantiation moved below

        # Portfolio and trade logging - Critical backtesting parameters
        try:
            bt_config = self.config['backtesting']
            strategy_conf = self.config['strategy']

            initial_capital = float(bt_config['initial_capital'])
            self.commission_rate = float(bt_config['commission_rate'])

            n_day_high_period = int(strategy_conf['n_day_high_period'])
            # self.m_day_low_period removed
            self.sell_asset_percentage = float(strategy_conf.get('sell_asset_percentage', 1.0)) # Default to 1.0
            self.holding_period_days = int(strategy_conf.get('holding_period_days', 1)) # Default to 1 day

            buy_window_start_time_str = strategy_conf.get('buy_window_start_time')
            buy_window_end_time_str = strategy_conf.get('buy_window_end_time')
            # sell_window_start_time_str and sell_window_end_time_str removed

            if not all([buy_window_start_time_str, buy_window_end_time_str]):
                raise ValueError("Missing buy_window_start_time or buy_window_end_time in [strategy]")

        except KeyError as e:
            raise ValueError(f"Error: Missing critical key '{e}' in configuration.") from e
        except ValueError as e:
            raise ValueError(f"Error processing configuration values: {e}") from e
        except TypeError:
             raise ValueError("Error: Configuration section is missing or malformed.")

        self.signal_generator = SignalGenerator(
            n_day_high_period=n_day_high_period,
            buy_window_start_time_str=buy_window_start_time_str,
            buy_window_end_time_str=buy_window_end_time_str
            # m_day_low_period and sell window times removed
        )

        self.portfolio = {
            'cash': initial_capital,
            'asset_qty': 0.0,
            'asset_value': 0.0,
            'total_value': initial_capital,
            'asset_entry_timestamp_utc': None, # New field
            'asset_entry_price': 0.0          # New field
        }
        self.trades = []
        self.historical_data = None
        self.portfolio_history = []

    def _update_portfolio_value(self, current_price, timestamp):
        """
        Updates the portfolio's asset value and total value based on the current price,
        and records the total value in history.

        Args:
            current_price (float): The current market price of the asset.
            timestamp: The current timestamp.
        """
        asset_value = self.portfolio['asset_qty'] * current_price
        self.portfolio['asset_value'] = asset_value
        self.portfolio['total_value'] = self.portfolio['cash'] + self.portfolio['asset_value']
        self.portfolio_history.append({
            'timestamp': timestamp,
            'total_value': self.portfolio['total_value'],
            'price': current_price  # Add this line
        })

    def _simulate_order(self, timestamp, order_type, symbol, price, quantity):
        """
        Simulates executing a trade (BUY or SELL).

        Args:
            timestamp: Timestamp of the order.
            order_type (str): 'BUY' or 'SELL'.
            symbol (str): The trading symbol (e.g., 'BTC/USDT').
            price (float): Execution price.
            quantity (float): Quantity to trade.

        Returns:
            bool: True if the order was executed successfully, False otherwise.
        """
        if quantity <= 0:
            print(f"Warning: Order quantity must be positive. Received {quantity}.")
            return False

        if order_type.upper() == 'BUY':
            cost = price * quantity
            commission = cost * self.commission_rate
            total_cost = cost + commission
            if self.portfolio['cash'] >= total_cost:
                self.portfolio['cash'] -= total_cost
                self.portfolio['asset_qty'] += quantity
                self.trades.append({
                    'timestamp': timestamp, 'type': 'BUY', 'symbol': symbol,
                    'price': price, 'quantity': quantity, 'commission': commission,
                    'cost': cost
                })
                # Record entry timestamp and price
                self.portfolio['asset_entry_timestamp_utc'] = timestamp
                self.portfolio['asset_entry_price'] = price
                print(f"Simulated BUY: {quantity} {symbol} at {price:.2f}. Cost: {cost:.2f}, Comm: {commission:.2f}")
                return True
            else:
                print(f"Warning: Not enough cash to execute BUY order for {quantity} {symbol} at {price:.2f}. Required: {total_cost:.2f}, Available: {self.portfolio['cash']:.2f}")
                return False
        elif order_type.upper() == 'SELL':
            if self.portfolio['asset_qty'] >= quantity:
                proceeds = price * quantity
                commission = proceeds * self.commission_rate
                total_proceeds = proceeds - commission

                self.portfolio['cash'] += total_proceeds
                self.portfolio['asset_qty'] -= quantity
                self.trades.append({
                    'timestamp': timestamp, 'type': 'SELL', 'symbol': symbol,
                    'price': price, 'quantity': quantity, 'commission': commission,
                    'proceeds': proceeds
                })
                # Reset entry timestamp and price
                self.portfolio['asset_entry_timestamp_utc'] = None
                self.portfolio['asset_entry_price'] = 0.0
                print(f"Simulated SELL: {quantity} {symbol} at {price:.2f}. Proceeds: {proceeds:.2f}, Comm: {commission:.2f}")
                return True
            else:
                print(f"Warning: Not enough assets to execute SELL order for {quantity} {symbol}. Required: {quantity}, Available: {self.portfolio['asset_qty']:.2f}")
                return False
        else:
            print(f"Warning: Unknown order type '{order_type}'. Must be 'BUY' or 'SELL'.")
            return False

    def run_backtest(self):
        """
        Runs the backtesting simulation.
        """
        print("Starting backtest run...")

        # Retrieve data fetching parameters from [backtesting] config
        bt_config = self.config.get('backtesting', {})
        symbol = bt_config.get('symbol')
        timeframe = bt_config.get('timeframe', '1d') # Default to '1d' if not specified
        start_date_str = bt_config.get('start_date')
        # end_date_str for filtering after fetch is handled later in the method

        if not symbol:
            print("Error: 'symbol' is not specified in [backtesting] config.")
            return # Critical error, cannot proceed
        if not start_date_str:
            print("Error: 'start_date' is not specified in [backtesting] config.")
            return # Critical error, cannot proceed

        # Convert start_date to POSIX timestamp in milliseconds
        try:
            # Ensure start_date_str is parsed as naive, then localized to UTC for consistent timestamp representation
            # Timestamps from exchanges are typically UTC.
            dt_object_naive = datetime.strptime(start_date_str, "%Y-%m-%d")
            dt_object_utc = pytz.utc.localize(dt_object_naive) # Localize naive datetime to UTC
            since_timestamp = int(dt_object_utc.timestamp() * 1000) # POSIX timestamp in milliseconds
        except ValueError as e:
            print(f"Error: Invalid date format for start_date '{start_date_str}'. Expected YYYY-MM-DD. Details: {e}")
            return
        except Exception as e: # Catch other potential errors like pytz failing
            print(f"Error processing start_date '{start_date_str}': {e}")
            return

        print(f"Fetching historical data for {symbol} ({timeframe}) since {start_date_str} (Timestamp: {since_timestamp}ms UTC)...")

        # Fetch data
        try:
            # We'll assume limit is handled by the fetcher or we fetch all and filter later
            self.historical_data = self.data_fetcher.fetch_ohlcv(
                symbol=symbol,
                timeframe=timeframe,
                since=since_timestamp
            )
        except Exception as e:
            print(f"Error during data fetching: {e}")
            self.historical_data = None # Ensure it's None on error

        # Process fetched data
        if self.historical_data is None or self.historical_data.empty:
            # Error message already printed by fetch_ohlcv or previous checks
            print(f"No data fetched for {symbol} ({timeframe}) since {start_date_str}. Backtest cannot proceed.")
            return

        # Implement end_date filtering
        end_date_str = bt_config.get('end_date')
        if end_date_str:
            try:
                # Convert end_date_str to a timezone-aware Timestamp (UTC)
                # Assuming timestamps in historical_data are UTC or will be compared as such
                end_date_ts = pd.Timestamp(end_date_str, tz='UTC')
                # Ensure historical_data timestamps are also UTC for comparison
                # If they are naive, localize them. If they are already UTC, this is fine.
                # Most fetchers should return UTC timestamps or allow specifying it.
                # For this example, let's assume historical_data['timestamp'] are pandas Timestamps.
                # If they are naive, they should be localized upon creation or here.
                # If historical_data['timestamp'] is already localized to UTC:
                if self.historical_data['timestamp'].dt.tz is None:
                     self.historical_data['timestamp'] = self.historical_data['timestamp'].dt.tz_localize('UTC')

                original_rows = len(self.historical_data)
                self.historical_data = self.historical_data[self.historical_data['timestamp'] <= end_date_ts]
                print(f"Filtered historical data up to end_date {end_date_str}. Rows changed from {original_rows} to {len(self.historical_data)}.")

                if self.historical_data.empty:
                    print(f"No data remains after filtering for end_date {end_date_str}. Backtest cannot proceed.")
                    return
            except ValueError as e:
                print(f"Error: Invalid date format for end_date '{end_date_str}'. Expected YYYY-MM-DD. Details: {e}")
                # Decide if to proceed without end_date filtering or stop. For now, proceed.
            except Exception as e:
                print(f"Error processing end_date '{end_date_str}': {e}. Proceeding without end_date filtering.")

        print("Successfully prepared historical data. Starting simulation loop...")
        # print(self.historical_data.head()) # Optional: keep for debugging

        # Strategy parameters from [strategy] config
        strategy_config = self.config.get('strategy', {})
        n_period = strategy_config.get('n_day_high_period')
        buy_cash_percentage = strategy_config.get('buy_cash_percentage')
        # risk_free_rate for reporter is fetched later

        if n_period is None:
            print("Error: 'n_day_high_period' is not specified in [strategy] config.")
            return # Critical for this strategy
        if buy_cash_percentage is None:
            print("Error: 'buy_cash_percentage' is not specified in [strategy] config.")
            return # Critical for portfolio management

        # The 'symbol' for trading operations is the one from [backtesting] config
        # It's already assigned to the 'symbol' variable earlier.

        # Main data loop
        for current_idx, row in enumerate(self.historical_data.itertuples(index=False)): # index=False if current_idx is simple enum
            try:
                # Ensure 'timestamp', 'high', 'low', 'close' are actual column names in your DataFrame
                current_timestamp_utc = getattr(row, 'timestamp')
                current_day_high = getattr(row, 'high')
                current_day_low = getattr(row, 'low', None) # Use None if 'low' is not present
                current_close_price = getattr(row, 'close')
            except AttributeError as e:
                print(f"Error accessing data in row (index {getattr(row, 'Index', 'N/A')}): {row}. Missing required OHLCV attribute. Details: {e}")
                print("Make sure historical_data DataFrame has 'timestamp', 'high', 'low', and 'close' columns.")
                if hasattr(row, 'close') and hasattr(row, 'timestamp'):
                     self._update_portfolio_value(current_price=getattr(row, 'close'), timestamp=getattr(row, 'timestamp'))
                continue

            if current_day_low is None:
                logging.warning(f"Timestamp {current_timestamp_utc}: 'low' price data is missing. Skipping sell signal check for this period.")
                # Update portfolio and continue, as sell signal cannot be evaluated
                self._update_portfolio_value(current_price=current_close_price, timestamp=current_timestamp_utc)
                continue

            # BUY Signal Logic (remains largely the same)
            buy_signal = None
            if self.portfolio['asset_qty'] == 0: # Only check for buy if we don't hold assets
                if current_idx >= n_period:
                    historical_data_for_signal = self.historical_data.iloc[:current_idx]
                    try:
                        if current_timestamp_utc.tzinfo is None:
                            current_datetime_utc8 = current_timestamp_utc.tz_localize('UTC').tz_convert('Asia/Shanghai')
                        else:
                            current_datetime_utc8 = current_timestamp_utc.tz_convert('Asia/Shanghai')

                        buy_signal = self.signal_generator.check_breakout_signal(
                            daily_ohlcv_data=historical_data_for_signal,
                            current_day_high=current_day_high,
                            current_datetime_utc8=current_datetime_utc8
                        )
                    except Exception as e:
                        logging.error(f"Error during BUY signal generation prep at {current_timestamp_utc}: {e}")
                        buy_signal = None

            # Process BUY Signal
            if buy_signal == "BUY":
                # Ensure we are not already holding assets before buying
                if self.portfolio['asset_qty'] == 0:
                    cash_to_spend_on_buy = self.portfolio['cash'] * buy_cash_percentage
                    if current_close_price > 0:
                        quantity_to_buy = cash_to_spend_on_buy / current_close_price
                        if quantity_to_buy > 0:
                            logging.info(f"Timestamp {current_timestamp_utc}: BUY signal. Attempting to buy {quantity_to_buy:.4f} {symbol} at {current_close_price:.2f}")
                            self._simulate_order(
                                timestamp=current_timestamp_utc, order_type='BUY',
                                symbol=symbol, price=current_close_price, quantity=quantity_to_buy
                            )
                else:
                    logging.info(f"Timestamp {current_timestamp_utc}: BUY signal received, but already holding assets. Skipping buy.")


            # SELL Logic (Holding Period Based)
            # This logic is independent of SignalGenerator and checked on each iteration if assets are held.
            if self.portfolio['asset_qty'] > 0 and self.portfolio.get('asset_entry_timestamp_utc') is not None:
                entry_ts_utc = self.portfolio['asset_entry_timestamp_utc']
                # Ensure current_timestamp_utc and entry_ts_utc are pandas Timestamps and UTC localized
                if not isinstance(entry_ts_utc, pd.Timestamp):
                    entry_ts_utc = pd.Timestamp(entry_ts_utc, tz='UTC')
                if current_timestamp_utc.tzinfo is None:
                    current_ts_utc_localized = current_timestamp_utc.tz_localize('UTC')
                else:
                    current_ts_utc_localized = current_timestamp_utc.tz_convert('UTC')

                # Calculate days passed. Sell on the day *after* the holding period.
                # E.g., hold_period=1 day. Buy Mon. Hold Tue. Sell Wed.
                # Mon (day 0). Tue (day 1). Wed (day 2). Sell if days_passed >= hold_period + 1
                # If hold_period=0 days. Buy Mon. Sell Tue. Sell if days_passed >= 1
                days_passed = (current_ts_utc_localized.normalize() - entry_ts_utc.normalize()).days

                # Target sell time: 10 AM Beijing Time on the sell day
                current_dt_utc8_for_sell_check = current_ts_utc_localized.tz_convert('Asia/Shanghai')

                is_target_sell_day = days_passed >= self.holding_period_days
                is_target_sell_hour = current_dt_utc8_for_sell_check.hour == 10

                if is_target_sell_day and is_target_sell_hour:
                    logging.info(f"Timestamp {current_timestamp_utc}: Holding period sell condition met. Holding period: {self.holding_period_days} days. Days passed: {days_passed}. Current time UTC+8: {current_dt_utc8_for_sell_check.strftime('%Y-%m-%d %H:%M')}.")
                    quantity_to_sell = self.portfolio['asset_qty'] * self.sell_asset_percentage
                    if quantity_to_sell > 0:
                        logging.info(f"Attempting to SELL {quantity_to_sell:.4f} {symbol} at {current_close_price:.2f}")
                        self._simulate_order(
                            timestamp=current_timestamp_utc, order_type='SELL',
                            symbol=symbol, price=current_close_price, quantity=quantity_to_sell
                        )

            self._update_portfolio_value(current_price=current_close_price, timestamp=current_timestamp_utc)

        print("\nBacktest simulation complete.")
        print(f"Final portfolio state: {self.portfolio}")

        if self.portfolio_history:
            print("\nPortfolio history (first 5 entries):")
            # Ensure entries are printed in a readable way, dicts can be verbose
            for entry in self.portfolio_history[:5]:
                print(f"Timestamp: {entry['timestamp']}, Total Value: {entry['total_value']:.2f}")

            if len(self.portfolio_history) > 5:
                print("\nPortfolio history (last 5 entries):")
                for entry in self.portfolio_history[-5:]:
                     print(f"Timestamp: {entry['timestamp']}, Total Value: {entry['total_value']:.2f}")
        else:
            print("\nPortfolio history is empty.")

        # print(f"\nTotal trades made: {len(self.trades)}") # Optional: print trades summary
        # for trade in self.trades: # Optional: print all trades
        #     print(trade)

        # Generate and print performance report
        # initial_capital was already validated in __init__
        # We need to re-fetch it here or pass it from __init__ if we want to avoid re-parsing config
        # For simplicity, re-accessing, but in a larger app, it might be stored on self directly.
        try:
            parsed_initial_capital = float(self.config.get('backtesting', {}).get('initial_capital'))
        except (ValueError, TypeError):
            print("Error: Could not parse initial_capital for report generation. Using portfolio's start if available, or 0.")
            parsed_initial_capital = self.portfolio_history[0]['total_value'] if self.portfolio_history else 0.0

        risk_free_rate = self.config.get('strategy', {}).get('risk_free_rate', 0.0)

        report = generate_performance_report(
            portfolio_history=self.portfolio_history,
            trades_log=self.trades,
            initial_capital=parsed_initial_capital, # Use the value from config
            risk_free_rate=risk_free_rate
        )

        print("\n--- Backtest Performance Report ---")
        if report and "error" not in report:
            for key, value in report.items():
                # Format percentage values
                if "Percentage" in key.title() and isinstance(value, (float, int)):
                    print(f"{key.replace('_', ' ').title()}: {value:.2f}%")
                elif isinstance(value, float):
                    print(f"{key.replace('_', ' ').title()}: {value:.2f}")
                else:
                    print(f"{key.replace('_', ' ').title()}: {value}")
        elif "error" in report:
            print(f"Could not generate full report: {report['error']}")
        else:
            print("Could not generate report or report is empty.")

        # Plot equity curve
        print("\nAttempting to generate equity curve plot...")
        if self.portfolio_history:
            # Convert portfolio_history (list of dicts) to DataFrame
            portfolio_df = pd.DataFrame(self.portfolio_history)

            # Ensure 'timestamp' column is in datetime format
            # The plotter also does this, but good practice to ensure here as well
            try:
                portfolio_df['timestamp'] = pd.to_datetime(portfolio_df['timestamp'])

                plot_output_path = "backtest_equity_curve.png"
                # In a more advanced setup, output_path could be derived from config or include a timestamp

                plot_success = plot_equity_curve(
                    portfolio_history_df=portfolio_df,
                    output_path=plot_output_path
                )
                # plot_equity_curve function already prints success or error messages
                if plot_success:
                    print(f"Equity curve generation process completed. Check {plot_output_path}")
                else:
                    print("Equity curve generation process encountered an issue (see plotter errors above).")

            except Exception as e: # Catch errors during DataFrame conversion or unexpected issues
                print(f"Error preparing data for plotting or during plotting call: {e}")
        else:
            print("Portfolio history is empty, skipping equity curve plot generation.")

        print("\nBacktest run finished.")


if __name__ == '__main__':
    # Example usage (optional, for testing purposes)
    # This part might be removed or modified later
    print("Backtesting Engine module direct execution (for testing).")
    # mock_config = {} # Replace with actual or mock config
    # mock_fetcher = None # Replace with actual or mock fetcher
    # mock_generator = None # Replace with actual or mock generator
    # engine = BacktestingEngine(mock_config, mock_fetcher, mock_generator)
    # engine.run_backtest()
