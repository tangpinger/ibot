# owl/backtesting_engine/engine.py
import pandas as pd
from datetime import datetime
import time
import pytz

# Assuming DataFetcher is in owl.data_fetcher.fetcher
# from owl.data_fetcher.fetcher import DataFetcher # Placeholder if direct type hint is needed
# from owl.signal_generator.generator import SignalGenerator # For type hinting if needed, instance is passed
from owl.analytics_reporting.reporter import generate_performance_report
from owl.analytics_reporting.plotter import plot_equity_curve
# pandas as pd is already imported at the top of the file

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
        self.signal_generator = signal_generator

        # Portfolio and trade logging - Critical backtesting parameters
        try:
            bt_config = self.config['backtesting']
            initial_capital = float(bt_config['initial_capital'])
            self.commission_rate = float(bt_config['commission_rate'])
        except KeyError as e:
            # More specific error for critical params
            raise ValueError(f"Error: Missing critical key '{e}' in [backtesting] configuration.") from e
        except ValueError as e:
            # More specific error for critical params
            raise ValueError(f"Error: Invalid numerical format for initial_capital or commission_rate in [backtesting] config: {e}") from e
        except TypeError: # Handles if self.config['backtesting'] is None or not a dict
             raise ValueError("Error: [backtesting] configuration section is missing or malformed.")


        self.portfolio = {
            'cash': initial_capital,
            'asset_qty': 0.0,
            'asset_value': 0.0, # Will be updated based on current price
            'total_value': initial_capital
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
            'total_value': self.portfolio['total_value']
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
                # Ensure 'timestamp', 'high', 'close' are actual column names in your DataFrame
                # Pandas itertuples names fields based on column names. If names have spaces/special chars, access might differ.
                current_timestamp_utc = getattr(row, 'timestamp')
                current_day_high = getattr(row, 'high')
                current_close_price = getattr(row, 'close')
            except AttributeError as e:
                print(f"Error accessing data in row (index {getattr(row, 'Index', 'N/A')}): {row}. Missing required OHLCV attribute. Details: {e}")
                print("Make sure historical_data DataFrame has 'timestamp', 'high', and 'close' columns.")
                # Update portfolio with current close if available, then skip to next iteration
                if 'close' in row._fields and 'timestamp' in row._fields:
                     self._update_portfolio_value(current_price=getattr(row, 'close'), timestamp=getattr(row, 'timestamp'))
                continue

            # Signal Generation
            signal = None
            if current_idx < n_period:
                # print(f"Timestamp {current_timestamp_utc}: Not enough data for signal generation (need {n_period} periods, have {current_idx}).")
                pass # Not enough data yet
            else:
                historical_data_for_signal = self.historical_data.iloc[:current_idx] # Data up to, but NOT including, the current day

                # Convert current_timestamp_utc to UTC+8 for the signal generator
                # Assuming current_timestamp_utc is a pandas Timestamp (often timezone-naive from CSVs or some fetchers)
                try:
                    if current_timestamp_utc.tzinfo is None:
                        current_datetime_utc8 = current_timestamp_utc.tz_localize('UTC').tz_convert('Asia/Shanghai')
                    else: # If already timezone-aware (e.g., UTC)
                        current_datetime_utc8 = current_timestamp_utc.tz_convert('Asia/Shanghai')
                # else: # current_timestamp_utc is already tz-aware, this should not happen if data is consistently handled
                #    current_datetime_utc8 = current_timestamp_utc.tz_convert('Asia/Shanghai')

                except Exception as e:
                    print(f"Error converting timestamp {current_timestamp_utc} to UTC+8: {e}")
                    current_datetime_utc8 = None # Mark as unavailable for signal generation

                # Generate Signal
                if current_datetime_utc8: # Proceed only if timestamp conversion was successful
                    try:
                        signal = self.signal_generator.check_breakout_signal(
                            daily_ohlcv_data=historical_data_for_signal,
                            current_day_high=current_day_high,
                            current_datetime_utc8=current_datetime_utc8
                        )
                    except Exception as e:
                        print(f"Error during signal generation at {current_timestamp_utc}: {e}")
                        signal = None # Ensure signal is None on error
                else:
                    signal = None # Skip signal generation

            # Process Signal & Execute Order
            if signal == "BUY":
                cash_to_spend_on_buy = self.portfolio['cash'] * buy_cash_percentage
                if current_close_price > 0:
                    quantity_to_buy = cash_to_spend_on_buy / current_close_price
                    if quantity_to_buy > 0:
                        print(f"Timestamp {current_timestamp_utc}: BUY signal received. Attempting to buy {quantity_to_buy:.4f} {symbol} at {current_close_price:.2f}")
                        self._simulate_order(
                            timestamp=current_timestamp_utc,
                            order_type='BUY',
                            symbol=symbol,
                            price=current_close_price,
                            quantity=quantity_to_buy
                        )
                    else:
                        print(f"Timestamp {current_timestamp_utc}: BUY signal. Quantity to buy is zero (Cash: {self.portfolio['cash']:.2f}, Price: {current_close_price:.2f}).")
                else:
                    print(f"Timestamp {current_timestamp_utc}: BUY signal. Price is zero or negative ({current_close_price:.2f}), cannot calculate quantity.")

            # TODO: Implement SELL logic if signal == "SELL" or other conditions met (e.g., stop-loss, take-profit).
            # Example:
            # elif signal == "SELL" and self.portfolio['asset_qty'] > 0:
            #     quantity_to_sell = self.portfolio['asset_qty'] # Sell all
            #     self._simulate_order(timestamp=current_timestamp_utc, order_type='SELL', symbol=symbol, price=current_close_price, quantity=quantity_to_sell)

            # Update portfolio value at the end of the period, after any trades for this period
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
