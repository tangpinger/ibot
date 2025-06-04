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
            self.holding_period_days = int(strategy_conf.get('holding_period_days', 1)) # Default to 1 day

            buy_window_start_time_str = strategy_conf.get('buy_window_start_time')
            buy_window_end_time_str = strategy_conf.get('buy_window_end_time')

            self.sell_window_start_str = strategy_conf.get('sell_window_start_time', "09:00") # Default from problem desc
            self.sell_window_end_str = strategy_conf.get('sell_window_end_time', "10:00")   # Default from problem desc

            if not all([buy_window_start_time_str, buy_window_end_time_str]): # sell times have defaults
                raise ValueError("Missing buy_window_start_time or buy_window_end_time in [strategy]")

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
            n_day_high_period=n_day_high_period,
            buy_window_start_time_str=buy_window_start_time_str,
            buy_window_end_time_str=buy_window_end_time_str
            # m_day_low_period and sell window times are engine-specific for sell logic
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

        # Main data loop (iterates over DAILY data)
        for current_idx, row in enumerate(self.daily_historical_data.itertuples(index=False)): # index=False if current_idx is simple enum
            try:
                # Ensure 'timestamp', 'high', 'low', 'close' are actual column names in your DataFrame
                current_timestamp_utc = getattr(row, 'timestamp')
                daily_high_for_signal_fallback = getattr(row, 'high') # From daily data
                current_close_price = getattr(row, 'close') # From daily data
            except AttributeError as e:
                print(f"Error accessing data in row (index {getattr(row, 'Index', 'N/A')}): {row}. Missing required OHLCV attribute. Details: {e}")
                print("Make sure DAILY historical_data DataFrame has 'timestamp', 'high', 'low', and 'close' columns.")
                if hasattr(row, 'close') and hasattr(row, 'timestamp'):
                     self._update_portfolio_value(current_price=getattr(row, 'close'), timestamp=getattr(row, 'timestamp'))
                continue

            # BUY Signal Logic (remains largely the same)
            buy_signal = None
            if self.portfolio['asset_qty'] == 0: # Only check for buy if we don't hold assets
                # current_timestamp_utc is the START of the current daily candle (e.g., 00:00 UTC for that day)
                current_daily_candle_timestamp_utc = current_timestamp_utc # Alias for clarity

                # Determine Time Windows
                current_processing_day_dt_utc8 = current_daily_candle_timestamp_utc.tz_convert('Asia/Shanghai')

                buy_window_end_hour, buy_window_end_minute = map(int, self.signal_generator.buy_window_end_str.split(':'))
                buy_window_end_dt_utc8 = current_processing_day_dt_utc8.replace(
                    hour=buy_window_end_hour, minute=buy_window_end_minute, second=0, microsecond=0
                )
                buy_window_end_dt_utc = buy_window_end_dt_utc8.tz_convert('UTC')
                start_of_24h_lookback_utc = buy_window_end_dt_utc - pd.Timedelta(hours=24)

                logging.info(f"Processing for daily candle: {current_daily_candle_timestamp_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC")
                logging.info(f"  Buy window end (UTC+8): {buy_window_end_dt_utc8.strftime('%Y-%m-%d %H:%M:%S')}")
                logging.info(f"  Buy window end (UTC):   {buy_window_end_dt_utc.strftime('%Y-%m-%d %H:%M:%S')}")
                logging.info(f"  24h lookback starts (UTC): {start_of_24h_lookback_utc.strftime('%Y-%m-%d %H:%M:%S')}")

                # Slice Data for Signal Generation
                hourly_data_for_24h_lookback = self.hourly_historical_data[
                    (self.hourly_historical_data['timestamp'] >= start_of_24h_lookback_utc) &
                    (self.hourly_historical_data['timestamp'] < buy_window_end_dt_utc) # Candles *before* the one starting at buy_window_end_dt_utc
                ]
                logging.info(f"  Hourly data for 24h lookback: Fetched {len(hourly_data_for_24h_lookback)} rows from {start_of_24h_lookback_utc.strftime('%Y-%m-%d %H:%M:%S')} to {buy_window_end_dt_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC.")

                # Daily data for N-day high (days *strictly before* start_of_24h_lookback_utc.normalize())
                # Normalize to ensure comparison is at day level
                daily_lookback_cutoff_utc = start_of_24h_lookback_utc.normalize()

                # Filter for days strictly before the cutoff
                historical_daily_for_n_high = self.daily_historical_data[
                    self.daily_historical_data['timestamp'] < daily_lookback_cutoff_utc
                ]
                # Then take the tail N records from this filtered data
                daily_data_for_n_day_high = historical_daily_for_n_high.tail(self.signal_generator.n)

                log_daily_n_high_range = "N/A"
                if not daily_data_for_n_day_high.empty:
                    log_daily_n_high_range = f"{daily_data_for_n_day_high['timestamp'].min().strftime('%Y-%m-%d')} to {daily_data_for_n_day_high['timestamp'].max().strftime('%Y-%m-%d')}"

                logging.info(f"  Daily data for N-day high ({self.signal_generator.n} days): Fetched {len(daily_data_for_n_day_high)} rows. Data range (UTC): {log_daily_n_high_range}. Cutoff was < {daily_lookback_cutoff_utc.strftime('%Y-%m-%d')}.")

                # Preliminary checks before calling signal generator
                signal_gen_possible = True
                if hourly_data_for_24h_lookback.empty:
                    logging.warning(f"  Skipping signal generation: hourly_data_for_24h_lookback is empty for {current_daily_candle_timestamp_utc}.")
                    signal_gen_possible = False

                if len(daily_data_for_n_day_high) < self.signal_generator.n:
                    logging.warning(f"  Skipping signal generation: daily_data_for_n_day_high has {len(daily_data_for_n_day_high)} rows, less than required N={self.signal_generator.n} for {current_daily_candle_timestamp_utc}.")
                    signal_gen_possible = False

                if signal_gen_possible:
                    try:
                        # The current_datetime_utc8 for signal generator's day/time validity check is buy_window_end_dt_utc8
                        buy_signal = self.signal_generator.check_breakout_signal(
                            hourly_data_for_24h_lookback=hourly_data_for_24h_lookback,
                            daily_data_for_n_day_high=daily_data_for_n_day_high,
                            current_datetime_utc8=buy_window_end_dt_utc8 # This is the reference time for the signal decision
                        )
                    except Exception as e:
                        logging.error(f"Error during BUY signal generation at {current_daily_candle_timestamp_utc}: {e}")
                        buy_signal = None
                else:
                    buy_signal = None # Ensure buy_signal is None if checks fail

                # Process BUY Signal
                if buy_signal == "BUY":
                    # timestamp_for_buy_order will be the timestamp of the hourly candle at buy_window_end_dt_utc
                    # Default to daily close price for fallback
                    price_for_buy_order = current_close_price
                    buy_executed_at_specific_time = False

                    # The target_buy_datetime_utc is buy_window_end_dt_utc, this is the candle we try to buy on its open.
                    target_buy_datetime_utc = buy_window_end_dt_utc
                    timestamp_for_buy_order = target_buy_datetime_utc # Initialize with this target

                    logging.info(f"BUY signal: Attempting to find hourly candle for buy time {self.signal_generator.buy_window_end_str} UTC+8 (which is {target_buy_datetime_utc} UTC).")

                    exact_hourly_candle = self.hourly_historical_data[
                        self.hourly_historical_data['timestamp'] == target_buy_datetime_utc # This is buy_window_end_dt_utc
                    ]

                    if not exact_hourly_candle.empty:
                        selected_hourly_candle = exact_hourly_candle.iloc[0]
                        price_for_buy_order = selected_hourly_candle['open'] # Buy at the open of this candle
                        # timestamp_for_buy_order is already target_buy_datetime_utc
                        buy_executed_at_specific_time = True
                        logging.info(
                            f"BUY signal: Using HOURLY OPEN price {price_for_buy_order:.2f} from candle at "
                            f"{timestamp_for_buy_order} (UTC) for order."
                        )
                    else:
                        # Fallback: try to find the *first available* hourly candle on or after the target buy time,
                        # but still within the same logical "processing day" (e.g., before the next day's 00:00 UTC+8)
                        logging.warning(
                            f"BUY signal: Exact hourly candle for {target_buy_datetime_utc} (UTC) NOT found. "
                            f"Attempting to find first candle AT or AFTER target time on the same logical day."
                        )
                        # Define search window: from target_buy_datetime_utc up to the end of the UTC+8 day
                        # current_processing_day_dt_utc8 is the day being processed (UTC+8)
                        # end_of_processing_day_utc8 = current_processing_day_dt_utc8.replace(hour=23, minute=59, second=59, microsecond=999999)
                        # end_of_processing_day_utc = end_of_processing_day_utc8.tz_convert('UTC')

                        # Simpler: search up to next daily candle start (current_daily_candle_timestamp_utc + 1 day)
                        # This means we look for an execution within the 24h period of the current daily candle we are processing
                        search_limit_utc = current_daily_candle_timestamp_utc + pd.Timedelta(days=1)

                        alternative_hourly_candles = self.hourly_historical_data[
                            (self.hourly_historical_data['timestamp'] >= target_buy_datetime_utc) &
                            (self.hourly_historical_data['timestamp'] < search_limit_utc) # Strictly before next daily candle
                        ]

                        if not alternative_hourly_candles.empty:
                            selected_hourly_candle = alternative_hourly_candles.iloc[0] # Take the first one
                            price_for_buy_order = selected_hourly_candle['open']
                            timestamp_for_buy_order = selected_hourly_candle['timestamp'] # Update to actual execution time
                            buy_executed_at_specific_time = True # Still considered specific as it's hourly based
                            logging.info(
                                f"BUY signal: Using alternative HOURLY OPEN price {price_for_buy_order:.2f} from candle at "
                                f"{timestamp_for_buy_order} (UTC) as primary target was missed."
                            )
                        else:
                            # If no hourly candle found, fallback to current daily candle's close
                            price_for_buy_order = current_close_price # current_close_price from daily iteration
                            timestamp_for_buy_order = current_daily_candle_timestamp_utc # Fallback to daily candle timestamp
                            buy_executed_at_specific_time = False
                            logging.warning(
                                f"BUY signal: No suitable alternative hourly candle found after {target_buy_datetime_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC "
                                f"and before {search_limit_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC. "
                                f"Falling back to DAILY CLOSE price {price_for_buy_order:.2f} from daily candle at {timestamp_for_buy_order}."
                            )

                    cash_to_spend_on_buy = self.portfolio['cash'] * buy_cash_percentage
                    if price_for_buy_order > 0:
                        quantity_to_buy = cash_to_spend_on_buy / price_for_buy_order
                        if quantity_to_buy > 0:
                            logging.info(f"Attempting to buy {quantity_to_buy:.4f} {symbol} at determined price {price_for_buy_order:.2f} at {timestamp_for_buy_order} (Specific time target: {'Yes' if buy_executed_at_specific_time else 'No - Daily Fallback'}).")
                            self._simulate_order(
                                timestamp=timestamp_for_buy_order,
                                order_type='BUY',
                                    symbol=symbol,
                                    price=price_for_buy_order,
                                    quantity=quantity_to_buy
                                )
                else: # This else corresponds to 'if buy_signal == "BUY":'
                    # This log will now correctly reflect that no BUY signal was generated OR conditions for buying weren't met (e.g. already holding assets)
                    # The specific reason (no signal vs. already holding) is implicitly handled by the flow.
                    # If buy_signal was None, it means no signal. If buy_signal was "BUY" but this 'else' is hit, it's because asset_qty != 0.
                    # However, the 'if self.portfolio['asset_qty'] == 0:' already gates this.
                    # So this 'else' primarily means 'no buy signal was generated by the strategy for this period'.
                    logging.info(f"No BUY signal generated or conditions not met on {current_daily_candle_timestamp_utc.strftime('%Y-%m-%d')}.")


            # SELL Logic (Holding Period Based)
            # This logic is independent of SignalGenerator and checked on each iteration if assets are held.
            timestamp_for_sell_order = current_daily_candle_timestamp_utc # Initialize with daily timestamp

            if self.portfolio['asset_qty'] > 0 and self.portfolio.get('asset_entry_timestamp_utc') is not None:
                entry_ts_utc = self.portfolio['asset_entry_timestamp_utc']
                # Ensure current_daily_candle_timestamp_utc and entry_ts_utc are pandas Timestamps and UTC localized
                if not isinstance(entry_ts_utc, pd.Timestamp):
                    entry_ts_utc = pd.Timestamp(entry_ts_utc, tz='UTC')
                # current_daily_candle_timestamp_utc is already a pd.Timestamp from daily_historical_data
                # Ensure it's UTC (it should be from data fetcher)
                if current_daily_candle_timestamp_utc.tzinfo is None:
                    current_ts_utc_localized = current_daily_candle_timestamp_utc.tz_localize('UTC')
                else:
                    current_ts_utc_localized = current_daily_candle_timestamp_utc.tz_convert('UTC')

                # Calculate days passed. Sell on the day *after* the holding period.
                # E.g., hold_period=1 day. Buy Mon. Hold Tue. Sell Wed.
                # Mon (day 0). Tue (day 1). Wed (day 2). Sell if days_passed >= hold_period + 1
                # If hold_period=0 days. Buy Mon. Sell Tue. Sell if days_passed >= 1
                days_passed = (current_ts_utc_localized.normalize() - entry_ts_utc.normalize()).days

                current_dt_utc8_for_sell_check = current_ts_utc_localized.tz_convert('Asia/Shanghai')
                current_time_utc8 = current_dt_utc8_for_sell_check.time()

                is_target_sell_day = days_passed >= self.holding_period_days

                if is_target_sell_day:
                    logging.info(
                        f"Timestamp {current_timestamp_utc}: Holding period sell condition met. "
                        f"Holding period: {self.holding_period_days} days. Days passed: {days_passed}. "
                        f"Sell window: {self.sell_window_start_str} - {self.sell_window_end_str} UTC+8."
                    )

                    price_for_sell_order = getattr(row, 'close') # Default to current daily close price

                    # Target datetime for fetching sell price is the END of the sell window on the current day (UTC+8)
                    target_sell_datetime_utc8 = current_dt_utc8_for_sell_check.replace(
                        hour=self.sell_window_end_time.hour,
                        minute=self.sell_window_end_time.minute,
                        second=0, microsecond=0
                    )
                    target_utc_for_hourly_sell_price = target_sell_datetime_utc8.tz_convert('UTC')

                    logging.info(f"SELL logic: Target sell time for price check is {target_sell_datetime_utc8.strftime('%Y-%m-%d %H:%M:%S')} UTC+8 ({target_utc_for_hourly_sell_price.strftime('%Y-%m-%d %H:%M:%S')} UTC).")

                    # Search for the first hourly candle AT or AFTER target_utc_for_hourly_sell_price
                    # but BEFORE the start of the next day (relative to target_utc_for_hourly_sell_price.date() in UTC)
                    search_start_utc = target_utc_for_hourly_sell_price
                    # Normalize to get the beginning of the day in UTC, then add 1 day for the search boundary
                    search_end_utc = (target_utc_for_hourly_sell_price.normalize() + pd.Timedelta(days=1))

                    relevant_hourly_candles_for_sell = self.hourly_historical_data[
                        (self.hourly_historical_data['timestamp'] >= search_start_utc) &
                        (self.hourly_historical_data['timestamp'] < search_end_utc) # Strictly less than next day start
                    ]

                    if not relevant_hourly_candles_for_sell.empty:
                        selected_hourly_candle_for_sell = relevant_hourly_candles_for_sell.iloc[0]
                        price_for_sell_order = selected_hourly_candle_for_sell['open']
                        timestamp_for_sell_order = selected_hourly_candle_for_sell['timestamp'] # Update timestamp
                        logging.info(f"SELL order will use timestamp from hourly candle: {timestamp_for_sell_order}")
                        logging.info(
                            f"SELL condition: Using HOURLY OPEN price {price_for_sell_order:.2f} from candle at "
                            f"{selected_hourly_candle_for_sell['timestamp']} (UTC) for order. Searched from {search_start_utc} (UTC) within the same day."
                        )
                    else:
                        daily_close_price_for_fallback = getattr(row, 'close')
                        price_for_sell_order = daily_close_price_for_fallback
                        logging.info(f"SELL order will use timestamp from daily candle: {timestamp_for_sell_order}")
                        logging.warning(
                            f"SELL condition: No hourly candle found at or after {search_start_utc} (UTC) on {search_start_utc.date()} (UTC). "
                            f"Falling back to DAILY CLOSE price {price_for_sell_order:.2f} from daily candle at {current_timestamp_utc}."
                        )

                    quantity_to_sell = self.portfolio['asset_qty'] * self.sell_asset_percentage
                    if quantity_to_sell > 0:
                        logging.info(f"Attempting to SELL {quantity_to_sell:.4f} {symbol} at determined price {price_for_sell_order:.2f}")
                        self._simulate_order(
                            timestamp=timestamp_for_sell_order, # Use the determined timestamp for the order
                            order_type='SELL',
                            symbol=symbol,
                            price=price_for_sell_order, # Price from hourly open (or daily close fallback)
                            quantity=quantity_to_sell
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
