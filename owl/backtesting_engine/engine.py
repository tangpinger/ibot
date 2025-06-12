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
    def __init__(self, config, data_fetcher, signal_generator, force_fetch=False):
        """
        Initializes the BacktestingEngine.

        Args:
            config: The configuration object.
            data_fetcher: An instance of the data fetcher.
            signal_generator: An instance of the signal generator.
            force_fetch (bool, optional): Whether to force fetching data from the exchange,
                                         ignoring cache. Defaults to False.
        """
        self.config = config
        self.data_fetcher = data_fetcher
        self.force_fetch = force_fetch
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
            try:
                self.holding_period_hours = int(strategy_conf['holding_period_hours'])
            except KeyError:
                raise ValueError("Missing critical key 'holding_period_hours' in [strategy] configuration.")
            except ValueError as e: # Catches if conversion to int fails
                raise ValueError(f"Error processing 'holding_period_hours': {e}. It must be an integer.") from e
            if self.holding_period_hours <= 0:
                raise ValueError("'holding_period_hours' must be a positive integer.")

            # buy_window_start_time_str = strategy_conf.get('buy_window_start_time') # No longer used by engine or SG
            buy_window_end_time_str = strategy_conf.get('buy_window_end_time')

            self.sell_window_start_str = strategy_conf.get('sell_window_start_time', "09:00") # Default from problem desc
            self.sell_window_end_str = strategy_conf.get('sell_window_end_time', "10:00")   # Default from problem desc

            # if not all([buy_window_start_time_str, buy_window_end_time_str]): # sell times have defaults
            #     raise ValueError("Missing buy_window_start_time or buy_window_end_time in [strategy]")
            # Updated validation:
            if not buy_window_end_time_str:
                raise ValueError("Missing critical key 'buy_window_end_time' in [strategy] configuration.")

            try:
                self.sell_window_start_time = datetime.strptime(self.sell_window_start_str, "%H:%M").time()
                self.sell_window_end_time = datetime.strptime(self.sell_window_end_str, "%H:%M").time()
            except ValueError as e_time:
                logging.error(f"Invalid format for sell_window_start_time ('{self.sell_window_start_str}') or sell_window_end_time ('{self.sell_window_end_str}'). Expected HH:MM. Error: {e_time}. Falling back to defaults 09:00 and 10:00.")
                self.sell_window_start_time = datetime.strptime("09:00", "%H:%M").time()
                self.sell_window_end_time = datetime.strptime("10:00", "%H:%M").time()

        except KeyError as e:
            raise ValueError(f"Error: Missing critical key '{e}' in configuration.") from e
        except ValueError as e: # Catches general value errors, including from float/int conversions
            raise ValueError(f"Error processing configuration values: {e}") from e
        except TypeError: # Catches errors if config sections like 'strategy' or 'backtesting' are missing
             raise ValueError("Error: Configuration section is missing or malformed.")

        self.signal_generator = SignalGenerator(
            n_day_high_period=n_day_high_period
        )

        # Store buy_window_end_time_str for determining target buy time in engine
        self.buy_window_end_config_str = buy_window_end_time_str
        # buy_window_start_time_str is no longer retrieved or stored if not used.

        self.portfolio = {
            'cash': initial_capital,
            'asset_qty': 0.0,
            'asset_value': 0.0,
            'total_value': initial_capital,
            'asset_entry_timestamp_utc': None, # New field
            'asset_entry_price': 0.0          # New field
        }
        self.trades = []
        self.daily_historical_data = None  # Renamed
        self.hourly_historical_data = None # Added for hourly data
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
                print(f"Simulated BUY: {quantity} {symbol} at {price:.2f}. Cost: {cost:.2f}, Comm: {commission:.2f}, Timestamp: {timestamp.tz_convert('Asia/Shanghai')}")
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
                print(f"Simulated SELL: {quantity} {symbol} at {price:.2f}. Proceeds: {proceeds:.2f}, Comm: {commission:.2f}, Balance: {self.portfolio['cash']:.2f}, Timestamp: {timestamp.tz_convert('Asia/Shanghai')}")
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
        # timeframe = bt_config.get('timeframe', '1d') # Default to '1d' if not specified - Now fetched explicitly
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

        # print(f"Fetching historical data for {symbol} ({timeframe}) since {start_date_str} (Timestamp: {since_timestamp}ms UTC)...") # Old message

        # Fetch daily data
        print(f"Fetching DAILY historical data for {symbol} (1d) since {start_date_str} (Timestamp: {since_timestamp}ms UTC)...")
        try:
            self.daily_historical_data = self.data_fetcher.fetch_ohlcv(
                symbol=symbol,
                timeframe='1d', # Explicitly '1d'
                since=since_timestamp,
                force_fetch=self.force_fetch
            )
        except Exception as e:
            print(f"Error during DAILY data fetching: {e}")
            self.daily_historical_data = None

        if self.daily_historical_data is None or self.daily_historical_data.empty:
            print(f"No DAILY data fetched for {symbol} (1d) since {start_date_str}. Backtest cannot proceed.")
            return

        # Fetch hourly data
        print(f"Fetching HOURLY historical data for {symbol} (1h) since {start_date_str} (Timestamp: {since_timestamp}ms UTC)...")
        try:
            self.hourly_historical_data = self.data_fetcher.fetch_ohlcv(
                symbol=symbol,
                timeframe='1h', # Explicitly '1h'
                since=since_timestamp, # Use the same 'since' timestamp
                force_fetch=self.force_fetch
            )
        except Exception as e:
            print(f"Error during HOURLY data fetching: {e}")
            self.hourly_historical_data = None

        if self.hourly_historical_data is None or self.hourly_historical_data.empty:
            print(f"No HOURLY data fetched for {symbol} (1h) since {start_date_str}. Backtest cannot proceed.")
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
                # For this example, let's assume Timestamps are pandas Timestamps.
                # Filter daily data
                if self.daily_historical_data['timestamp'].dt.tz is None:
                     self.daily_historical_data['timestamp'] = self.daily_historical_data['timestamp'].dt.tz_localize('UTC')
                original_daily_rows = len(self.daily_historical_data)
                self.daily_historical_data = self.daily_historical_data[self.daily_historical_data['timestamp'] <= end_date_ts]
                print(f"Filtered DAILY historical data up to end_date {end_date_str}. Rows changed from {original_daily_rows} to {len(self.daily_historical_data)}.")

                if self.daily_historical_data.empty:
                    print(f"No DAILY data remains after filtering for end_date {end_date_str}. Backtest cannot proceed.")
                    return

                # Filter hourly data
                if self.hourly_historical_data['timestamp'].dt.tz is None:
                     self.hourly_historical_data['timestamp'] = self.hourly_historical_data['timestamp'].dt.tz_localize('UTC')
                original_hourly_rows = len(self.hourly_historical_data)
                self.hourly_historical_data = self.hourly_historical_data[self.hourly_historical_data['timestamp'] <= end_date_ts]
                print(f"Filtered HOURLY historical data up to end_date {end_date_str}. Rows changed from {original_hourly_rows} to {len(self.hourly_historical_data)}.")

                if self.hourly_historical_data.empty:
                    print(f"No HOURLY data remains after filtering for end_date {end_date_str}. Backtest cannot proceed.")
                    return
            except ValueError as e:
                print(f"Error: Invalid date format for end_date '{end_date_str}'. Expected YYYY-MM-DD. Details: {e}")
                # Decide if to proceed without end_date filtering or stop. For now, proceed.
            except Exception as e:
                print(f"Error processing end_date '{end_date_str}': {e}. Proceeding without end_date filtering.")

        print("Successfully prepared DAILY and HOURLY historical data. Starting simulation loop...")
        # print(self.daily_historical_data.head()) # Optional: keep for debugging
        # print(self.hourly_historical_data.head()) # Optional: keep for debugging

        # Strategy parameters from [strategy] config
        strategy_config = self.config.get('strategy', {})
        # n_period from strategy_config is used for initial check, but self.signal_generator.n should be the authoritative source for signal calculation
        n_period_from_config = strategy_config.get('n_day_high_period') # Keep for initial check if needed, or rely on signal_generator's
        buy_cash_percentage = strategy_config.get('buy_cash_percentage')
        # risk_free_rate for reporter is fetched later

        if n_period_from_config is None: # Check against the one from config for this initial validation
            print("Error: 'n_day_high_period' is not specified in [strategy] config.")
            return # Critical for this strategy
        if buy_cash_percentage is None:
            print("Error: 'buy_cash_percentage' is not specified in [strategy] config.")
            return # Critical for portfolio management

        # The 'symbol' for trading operations is the one from [backtesting] config
        # It's already assigned to the 'symbol' variable earlier.

        # Main data loop (iterates over DAILY data)
        for current_idx, row in enumerate(self.daily_historical_data.itertuples(index=False)): # index=False if current_idx is simple enum
            try:
                # Ensure 'timestamp', 'high', 'low', 'close' are actual column names in your DataFrame
                current_timestamp_utc = getattr(row, 'timestamp')
                current_close_price = getattr(row, 'close') # From daily data
            except AttributeError as e:
                print(f"Error accessing data in row (index {getattr(row, 'Index', 'N/A')}): {row}. Missing required OHLCV attribute. Details: {e}")
                print("Make sure DAILY historical_data DataFrame has 'timestamp', 'high', 'low', and 'close' columns.")
                if hasattr(row, 'close') and hasattr(row, 'timestamp'):
                     self._update_portfolio_value(current_price=getattr(row, 'close'), timestamp=getattr(row, 'timestamp'))
                continue

            # BUY Signal Logic (based on D-1's high breakout of N-day high ending D-2)
            buy_signal = None
            # current_idx is the index for D (current processing day in the loop)
            # We need at least n_period days before D-1, so D-1 needs index n_period.
            # Thus, D (current_idx) needs to be at least n_period + 1.
            if self.portfolio['asset_qty'] == 0: # Only check for buy if we don't hold assets
                # self.signal_generator.n is the authoritative N for the signal
                if current_idx >= self.signal_generator.n + 1:
                    try:
                        # Prepare data for the signal generator
                        previous_day_row = self.daily_historical_data.iloc[current_idx - 1]
                        previous_day_high = previous_day_row['high']
                        previous_day_timestamp_utc = previous_day_row['timestamp']
                        previous_day_close = previous_day_row['close'] # For fallback price

                        # Data for N-day high calculation (N days ending D-2)
                        # D-2 is current_idx - 2. D-1 is current_idx -1.
                        # End index for slice is current_idx - 1 (exclusive, so it takes up to current_idx - 2)
                        # Start index is current_idx - 1 - N
                        start_index_for_n_days = current_idx - 1 - self.signal_generator.n
                        end_index_for_n_days = current_idx - 1
                        historical_data_for_signal = self.daily_historical_data.iloc[start_index_for_n_days:end_index_for_n_days]

                        # Contextual datetime for signal check (e.g., start of D-1 in UTC+8)
                        logging.info(f"Debug: Checking buy signal for D-1 ({previous_day_timestamp_utc.strftime('%Y-%m-%d')}). N-day data ends {historical_data_for_signal['timestamp'].iloc[-1].strftime('%Y-%m-%d') if not historical_data_for_signal.empty else 'N/A'}.")
                        # The complex calculation for `effective_current_day_high` is removed as per plan.
                        # The signal is now based on `previous_day_high`.
                        buy_signal = self.signal_generator.check_breakout_signal(
                            daily_ohlcv_data=historical_data_for_signal,
                            previous_day_high=previous_day_high,
                            previous_day_timestamp_utc=previous_day_timestamp_utc, # For logging in generator
                            current_timestamp_utc=current_timestamp_utc # For logging in generator
                        )
                    except IndexError:
                        logging.warning(f"IndexError during data preparation for buy signal at current_idx {current_idx}. Not enough historical data available for the required N-day period + previous day. Skipping signal check.")
                        buy_signal = None
                    except Exception as e:
                        logging.error(f"Error during BUY signal generation prep for D-1 ({previous_day_timestamp_utc.strftime('%Y-%m-%d')}): {e}")
                        buy_signal = None

            # Process BUY Signal
            if buy_signal == "BUY":
                # If signal is BUY, it's for D-1. We try to buy at target time on D-1.
                # `timestamp_for_buy_order` should be on D-1.
                # `price_for_buy_order` is determined by hourly data on D-1.

                # Ensure we are not already holding assets before buying
                if self.portfolio['asset_qty'] == 0:
                    price_for_buy_order = previous_day_close # Default to D-1's daily close price for fallback
                    timestamp_for_buy_order = previous_day_timestamp_utc # Default to D-1's daily timestamp for fallback
                    buy_executed_at_specific_time = False

                    # Use self.buy_window_end_config_str stored from config, not from signal_generator
                    # buy_window_end_str = self.signal_generator.buy_window_end_str # Old way
                    buy_window_end_str = self.buy_window_end_config_str # e.g., "16:00"


                    try:
                        buy_hour, buy_minute = map(int, buy_window_end_str.split(':'))

                        # Base for target_buy_time is previous_day_timestamp_utc (D-1's timestamp)
                        previous_day_dt_utc8 = previous_day_timestamp_utc.tz_convert('Asia/Shanghai')
                        target_buy_datetime_utc8 = previous_day_dt_utc8.replace(hour=buy_hour, minute=buy_minute, second=0, microsecond=0)
                        target_buy_time_as_utc = target_buy_datetime_utc8.tz_convert('UTC')

                        print(f"BUY signal for D-1 ({previous_day_timestamp_utc.strftime('%Y-%m-%d')}): Attempting to find hourly candle for buy time {buy_window_end_str} UTC+8 (which is {target_buy_time_as_utc} UTC).")

                        exact_hourly_candle = self.hourly_historical_data[
                            self.hourly_historical_data['timestamp'] == target_buy_time_as_utc
                        ]

                        if not exact_hourly_candle.empty:
                            selected_hourly_candle = exact_hourly_candle.iloc[0]
                            price_for_buy_order = selected_hourly_candle['open'] # Use open of the target hour
                            timestamp_for_buy_order = selected_hourly_candle['timestamp'] # Timestamp of the hourly candle
                            buy_executed_at_specific_time = True
                            print(
                                f"BUY signal (D-1 context): Using HOURLY OPEN price {price_for_buy_order:.2f} from candle at "
                                f"{timestamp_for_buy_order} (UTC) for order (target time on D-1: {buy_window_end_str} UTC+8)."
                            )
                        else:
                            logging.warning(
                                f"BUY signal (D-1 context): Exact hourly candle for {target_buy_time_as_utc} (UTC) NOT found. "
                                f"Attempting to find first candle AT or AFTER target time on D-1."
                            )
                            # Search window: from target_buy_time_as_utc up to the end of D-1 (UTC)
                            # D-1's UTC timestamp is previous_day_timestamp_utc. End of D-1 is start of D.
                            day_after_previous_day_start_utc = (previous_day_timestamp_utc.normalize() + pd.Timedelta(days=1))

                            alternative_hourly_candles = self.hourly_historical_data[
                                (self.hourly_historical_data['timestamp'] >= target_buy_time_as_utc) &
                                (self.hourly_historical_data['timestamp'] < day_after_previous_day_start_utc) # Strictly before start of D
                            ]

                            if not alternative_hourly_candles.empty:
                                selected_hourly_candle = alternative_hourly_candles.iloc[0]
                                price_for_buy_order = selected_hourly_candle['open']
                                timestamp_for_buy_order = selected_hourly_candle['timestamp']
                                buy_executed_at_specific_time = True
                                logging.info(
                                    f"BUY signal (D-1 context): Using alternative HOURLY OPEN price {price_for_buy_order:.2f} from candle at "
                                    f"{timestamp_for_buy_order} (UTC) as primary target was missed (within D-1)."
                                )
                            else:
                                # Fallback to D-1's daily close price and timestamp
                                price_for_buy_order = previous_day_close # Already set as default
                                timestamp_for_buy_order = previous_day_timestamp_utc # Already set as default
                                logging.warning(
                                    f"BUY signal (D-1 context): No suitable alternative hourly candle found on D-1 ({previous_day_timestamp_utc.strftime('%Y-%m-%d')}) at or after {buy_window_end_str} UTC+8. "
                                    f"Falling back to D-1 DAILY CLOSE price {price_for_buy_order:.2f} from daily candle at {timestamp_for_buy_order}."
                                )
                                # buy_executed_at_specific_time remains False

                    except Exception as e:
                        # Fallback to D-1's daily close price and timestamp
                        price_for_buy_order = previous_day_close # Ensure fallback
                        timestamp_for_buy_order = previous_day_timestamp_utc # Ensure fallback
                        logging.error(f"BUY signal (D-1 context): Error determining buy price using target time ('{buy_window_end_str} UTC+8') for D-1 ({previous_day_timestamp_utc.strftime('%Y-%m-%d')}): {e}. "
                                      f"Falling back to D-1 DAILY CLOSE price {price_for_buy_order:.2f} from daily candle at {timestamp_for_buy_order}.")
                        buy_executed_at_specific_time = False


                    cash_to_spend_on_buy = self.portfolio['cash'] * buy_cash_percentage
                    if price_for_buy_order > 0:
                        quantity_to_buy = cash_to_spend_on_buy / price_for_buy_order
                        if quantity_to_buy > 0:
                            logging.info(f"BUY signal (D-1 context {previous_day_timestamp_utc.strftime('%Y-%m-%d')}): Attempting to buy {quantity_to_buy:.4f} {symbol} at determined price {price_for_buy_order:.2f} (Target time on D-1: {'Yes, hourly' if buy_executed_at_specific_time else 'No, D-1 daily fallback'}). Order timestamp: {timestamp_for_buy_order}")
                            self._simulate_order(
                                timestamp=timestamp_for_buy_order, # This is now correctly on D-1
                                order_type='BUY',
                                symbol=symbol,
                                price=price_for_buy_order,
                                quantity=quantity_to_buy
                            )
                else:
                    logging.info(f"BUY signal for D-1 ({previous_day_timestamp_utc.strftime('%Y-%m-%d')}): Signal received, but already holding assets (checked at D: {current_timestamp_utc.strftime('%Y-%m-%d')}). Skipping buy.")

            # New SELL Logic
            if self.portfolio['asset_qty'] > 0:
                # current_timestamp_utc is D's daily timestamp from the main loop
                # Ensure current_timestamp_utc is properly timezone-aware and normalized (start of the day)
                if current_timestamp_utc.tzinfo is None:
                    current_day_start_utc = pytz.utc.localize(current_timestamp_utc).normalize()
                else:
                    current_day_start_utc = current_timestamp_utc.normalize()

                observation_end_time_utc = current_day_start_utc + pd.Timedelta(hours=self.holding_period_hours)

                logging.info(f"SELL Check (Day D: {current_day_start_utc.strftime('%Y-%m-%d')}): Holding {self.portfolio['asset_qty']} units. Observation end time for sell: {observation_end_time_utc.strftime('%Y-%m-%d %H:%M:%S %Z')}")

                sell_price = 0.0
                timestamp_for_sell_order = None
                sell_reason = ""

                # 1. Find the hourly OHLCV candle where the timestamp matches observation_end_time_utc
                exact_match_candle = self.hourly_historical_data[
                    self.hourly_historical_data['timestamp'] == observation_end_time_utc
                ]

                if not exact_match_candle.empty:
                    sell_price = exact_match_candle.iloc[0]['open']
                    timestamp_for_sell_order = exact_match_candle.iloc[0]['timestamp']
                    sell_reason = f"Exact observation_end_time candle found at {timestamp_for_sell_order} UTC. Sell price (open): {sell_price:.2f}"
                    logging.info(sell_reason)
                else:
                    # 2. If no exact match, search for the first available hourly candle *after* observation_end_time_utc
                    #    but *before* the start of the next day.
                    next_day_start_utc = current_day_start_utc + pd.Timedelta(days=1)

                    alternative_candles = self.hourly_historical_data[
                        (self.hourly_historical_data['timestamp'] > observation_end_time_utc) &
                        (self.hourly_historical_data['timestamp'] < next_day_start_utc)
                    ].sort_values(by='timestamp')

                    if not alternative_candles.empty:
                        sell_price = alternative_candles.iloc[0]['open']
                        timestamp_for_sell_order = alternative_candles.iloc[0]['timestamp']
                        sell_reason = (f"Exact observation_end_time candle NOT found. "
                                       f"Using next available candle on {current_day_start_utc.strftime('%Y-%m-%d')} at {timestamp_for_sell_order} UTC. Sell price (open): {sell_price:.2f}")
                        logging.info(sell_reason)
                    else:
                        sell_reason = (f"No suitable hourly candle found for selling on {current_day_start_utc.strftime('%Y-%m-%d')}. "
                                       f"Neither exact match for {observation_end_time_utc.strftime('%Y-%m-%d %H:%M:%S %Z')} "
                                       f"nor any subsequent candle on the same day.")
                        logging.warning(sell_reason)
                        # Do not place a sell order for this day.

                if sell_price > 0 and timestamp_for_sell_order is not None:
                    quantity_to_sell = self.portfolio['asset_qty'] * self.sell_asset_percentage
                    if quantity_to_sell > 0:
                        logging.info(f"Attempting to execute SELL order: {quantity_to_sell:.4f} {symbol} at {sell_price:.2f}. Order timestamp: {timestamp_for_sell_order}. Reason: {sell_reason}")
                        self._simulate_order(
                            timestamp=timestamp_for_sell_order,
                            order_type='SELL',
                            symbol=symbol,
                            price=sell_price,
                            quantity=quantity_to_sell
                        )
                    else:
                        logging.warning(f"Calculated quantity_to_sell is not positive ({quantity_to_sell}). Sell order not placed. Sell price was {sell_price:.2f}.")
                elif timestamp_for_sell_order is not None: # Implies sell_price was <= 0
                     logging.warning(f"Sell price determined was not positive ({sell_price}). Sell order not placed. Timestamp was {timestamp_for_sell_order}.")
                # If timestamp_for_sell_order is None, it means no suitable candle was found, and a warning was already logged.


            self._update_portfolio_value(current_price=current_close_price, timestamp=current_timestamp_utc) # uses D's close and D's timestamp

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

                # Generate dynamic plot filename
                bt_config_for_plot = self.config.get('backtesting', {})
                symbol_for_fn = bt_config_for_plot.get('symbol', 'unknownsymbol')
                strategy_config_for_fn = self.config.get('strategy', {})
                n_day_high_period_for_fn = strategy_config_for_fn.get('n_day_high_period', 'unknownperiod')

                # Format symbol
                formatted_symbol_for_fn = str(symbol_for_fn).replace('/', '_').lower()

                start_date_str_plot = bt_config_for_plot.get('start_date', 'unknownstart')
                end_date_str_plot = bt_config_for_plot.get('end_date', 'unknownend')

                start_time_str_fn = "unknownstart"
                end_time_str_fn = "unknownend"

                try:
                    start_date_dt = datetime.strptime(start_date_str_plot, "%Y-%m-%d")
                    start_time_str_fn = start_date_dt.strftime('%Y%m%d')
                except ValueError:
                    print(f"Warning: Could not parse start_date '{start_date_str_plot}' for plot filename. Using default.")

                try:
                    end_date_dt = datetime.strptime(end_date_str_plot, "%Y-%m-%d")
                    end_time_str_fn = end_date_dt.strftime('%Y%m%d')
                except ValueError:
                    print(f"Warning: Could not parse end_date '{end_date_str_plot}' for plot filename. Using default.")

                plot_output_filename = f"{formatted_symbol_for_fn}_{n_day_high_period_for_fn}_backtest_equity_curve_{start_time_str_fn}_{end_time_str_fn}.png"
                print(f"Generated plot filename: {plot_output_filename}")

                plot_success = plot_equity_curve(
                    portfolio_history_df=portfolio_df,
                    output_path=plot_output_filename
                )
                # plot_equity_curve function already prints success or error messages
                if plot_success:
                    print(f"Equity curve generation process completed. Check {plot_output_filename}")
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
