import ccxt
from datetime import datetime
import logging

# It's better to get the logger instance from the main application setup
# For now, we'll use a default logger for this module if not passed.
logger = logging.getLogger(__name__) # Will be owl.order_executor.executor if main logger is 'owl'
if not logger.hasHandlers(): # Basic config if no handlers are set up by main app
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class OrderExecutor:
    """
    Handles the execution of buy and sell orders, supporting dry-run and live modes.
    """
    def __init__(self, exchange_ccxt_instance, position_manager, dry_run=True, default_symbol=None, default_trade_amount=None):
        """
        Initializes the OrderExecutor.

        Args:
            exchange_ccxt_instance (ccxt.Exchange): An initialized ccxt exchange instance.
                                                     Can be None if dry_run is True and no exchange interaction is needed for simulation.
            position_manager (PositionManager): An instance of the PositionManager.
            dry_run (bool, optional): If True, simulates trades without actual execution. Defaults to True.
            default_symbol (str, optional): Default trading symbol (e.g., 'BTC/USDT').
            default_trade_amount (float, optional): Default amount of the base currency to trade (e.g., 0.001 for BTC).
        """
        self.exchange = exchange_ccxt_instance
        self.position_manager = position_manager
        self.dry_run = dry_run
        self.default_symbol = default_symbol
        self.default_trade_amount = default_trade_amount

        if not self.dry_run and self.exchange is None:
            raise ValueError("Exchange instance must be provided if not in dry_run mode.")
        if self.position_manager is None:
            raise ValueError("PositionManager instance must be provided.")

    def _get_current_price_for_simulation(self, symbol):
        """
        Helper to get current price for dry run simulation.
        In a real dry run, this might fetch the actual current price to make simulation more realistic.
        For now, it's a placeholder. In backtesting, the price comes from historical data.
        """
        if self.exchange and self.exchange.has['fetchTicker']:
            try:
                ticker = self.exchange.fetch_ticker(symbol)
                return ticker['last']
            except Exception as e:
                logger.warning(f"Dry Run: Could not fetch live price for {symbol} for simulation: {e}. Using placeholder 0.")
                return 0.0 # Placeholder
        logger.info(f"Dry Run: Exchange not available or doesn't support fetchTicker for {symbol}. Using placeholder price 0.")
        return 0.0 # Fallback placeholder

    def create_buy_order(self, symbol=None, quantity=None, order_type='market', price=None):
        """
        Creates a buy order.

        Args:
            symbol (str, optional): The trading symbol. Uses default if None.
            quantity (float, optional): The amount of the base currency to buy. Uses default if None.
            order_type (str, optional): Type of order ('market' or 'limit'). Defaults to 'market'.
            price (float, optional): Price for 'limit' orders.

        Returns:
            dict or None: Order details if successful, None otherwise.
        """
        target_symbol = symbol if symbol else self.default_symbol
        target_quantity = quantity if quantity else self.default_trade_amount

        if not target_symbol or not target_quantity:
            logger.error("OrderExecutor: Symbol and quantity must be specified or set as defaults.")
            return None

        if self.position_manager.has_position and self.position_manager.instrument_id == target_symbol:
            logger.warning(f"OrderExecutor: Already have an open position for {target_symbol}. Buy order skipped.")
            return None

        execution_time = datetime.now() # In a real scenario, use UTC from exchange or system

        if self.dry_run:
            simulated_price = price if price else self._get_current_price_for_simulation(target_symbol)
            logger.info(f"[DRY RUN] Executing BUY order: {target_quantity} of {target_symbol} at (simulated) price {simulated_price} ({order_type}) at {execution_time}")
            # Update position manager
            self.position_manager.update_position(
                instrument_id=target_symbol,
                entry_price=simulated_price,
                quantity=target_quantity,
                entry_time=execution_time,
                position_type="long"
            )
            return {
                "symbol": target_symbol, "type": order_type, "side": "buy",
                "amount": target_quantity, "price": simulated_price,
                "timestamp": execution_time.isoformat(), "status": "simulated_filled", "id": f"dryrun_buy_{int(execution_time.timestamp())}"
            }
        else:
            # --- LIVE TRADING LOGIC (to be implemented more thoroughly later) ---
            if not self.exchange:
                logger.error("OrderExecutor: Exchange not initialized for live trading.")
                return None
            try:
                logger.info(f"Executing LIVE BUY order: {target_quantity} of {target_symbol} ({order_type})")
                # Ensure symbol is in correct format for exchange if needed (e.g. using exchange.market(symbol)['id'])
                market_id = self.exchange.market(target_symbol)['id']

                order = self.exchange.create_order(market_id, order_type, 'buy', target_quantity, price)
                logger.info(f"LIVE BUY order placed: {order}")

                # Assuming order is filled immediately for market orders for now
                # In reality, need to check order status, handle partial fills etc.
                self.position_manager.update_position(
                    instrument_id=target_symbol, # Use the common symbol, not market_id
                    entry_price=order.get('price', order.get('average', self._get_current_price_for_simulation(target_symbol))), # Get actual fill price
                    quantity=order.get('filled', target_quantity), # Get actual filled quantity
                    entry_time=datetime.fromtimestamp(order['timestamp']/1000) if 'timestamp' in order else execution_time,
                    position_type="long"
                )
                return order
            except ccxt.NetworkError as e:
                logger.error(f"LIVE BUY order failed (Network Error) for {target_symbol}: {e}")
            except ccxt.ExchangeError as e:
                logger.error(f"LIVE BUY order failed (Exchange Error) for {target_symbol}: {e}")
            except Exception as e:
                logger.error(f"LIVE BUY order failed (Unexpected Error) for {target_symbol}: {e}", exc_info=True)
            return None

    def create_sell_order(self, symbol=None, quantity=None, order_type='market', price=None):
        """
        Creates a sell order.

        Args:
            symbol (str, optional): The trading symbol. Uses default if None.
            quantity (float, optional): The amount of the base currency to sell. Uses default if None.
                                        If None and position exists for symbol, sells all.
            order_type (str, optional): Type of order ('market' or 'limit'). Defaults to 'market'.
            price (float, optional): Price for 'limit' orders.

        Returns:
            dict or None: Order details if successful, None otherwise.
        """
        target_symbol = symbol if symbol else self.default_symbol

        if not target_symbol:
            logger.error("OrderExecutor: Symbol must be specified or set as default for sell order.")
            return None

        if not self.position_manager.has_position or self.position_manager.instrument_id != target_symbol:
            logger.warning(f"OrderExecutor: No open position for {target_symbol} to sell. Sell order skipped.")
            return None

        target_quantity = quantity if quantity else self.position_manager.quantity

        if target_quantity <= 0:
            logger.error("OrderExecutor: Sell quantity must be positive.")
            return None

        execution_time = datetime.now()

        if self.dry_run:
            simulated_price = price if price else self._get_current_price_for_simulation(target_symbol)
            logger.info(f"[DRY RUN] Executing SELL order: {target_quantity} of {target_symbol} at (simulated) price {simulated_price} ({order_type}) at {execution_time}")

            cleared_position = self.position_manager.clear_position()
            # Log P&L for dry run (simplified)
            if cleared_position:
                pnl = (simulated_price - cleared_position['entry_price']) * cleared_position['quantity']
                logger.info(f"[DRY RUN] Simulated P&L for trade on {target_symbol}: {pnl:.2f} (Quantity: {cleared_position['quantity']}, Entry: {cleared_position['entry_price']}, Exit: {simulated_price})")

            return {
                "symbol": target_symbol, "type": order_type, "side": "sell",
                "amount": target_quantity, "price": simulated_price,
                "timestamp": execution_time.isoformat(), "status": "simulated_filled", "id": f"dryrun_sell_{int(execution_time.timestamp())}"
            }
        else:
            # --- LIVE TRADING LOGIC (to be implemented more thoroughly later) ---
            if not self.exchange:
                logger.error("OrderExecutor: Exchange not initialized for live trading.")
                return None
            try:
                logger.info(f"Executing LIVE SELL order: {target_quantity} of {target_symbol} ({order_type})")
                market_id = self.exchange.market(target_symbol)['id']

                order = self.exchange.create_order(market_id, order_type, 'sell', target_quantity, price)
                logger.info(f"LIVE SELL order placed: {order}")

                # Assuming order is filled immediately for market orders for now
                cleared_position = self.position_manager.clear_position()
                if cleared_position:
                    exit_price = order.get('price', order.get('average', self._get_current_price_for_simulation(target_symbol)))
                    filled_quantity = order.get('filled', target_quantity)
                    pnl = (exit_price - cleared_position['entry_price']) * filled_quantity # Assuming full quantity sold matches cleared
                    logger.info(f"LIVE P&L for trade on {target_symbol}: {pnl:.2f} (Quantity: {filled_quantity}, Entry: {cleared_position['entry_price']}, Exit: {exit_price})")
                return order
            except ccxt.NetworkError as e:
                logger.error(f"LIVE SELL order failed (Network Error) for {target_symbol}: {e}")
            except ccxt.ExchangeError as e:
                logger.error(f"LIVE SELL order failed (Exchange Error) for {target_symbol}: {e}")
            except Exception as e:
                logger.error(f"LIVE SELL order failed (Unexpected Error) for {target_symbol}: {e}", exc_info=True)
            return None

# Example of how to use it (optional, for testing within this file)
if __name__ == "__main__":
    from owl.position_manager.manager import PositionManager # Assumes it's in the parent directory's sibling
    import sys
    from pathlib import Path
    # Add project root to sys.path to allow imports like owl.position_manager
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    sys.path.insert(0, str(PROJECT_ROOT))

    from owl.position_manager.manager import PositionManager
    from owl.data_fetcher.fetcher import DataFetcher # To simulate getting price in dry run

    print("--- Testing OrderExecutor (Dry Run Mode) ---")

    # Mock exchange for dry run price fetching (optional, can pass None if not fetching price)
    # In a real app, DataFetcher would be initialized properly.
    mock_exchange = None
    try:
        # Attempt to use DataFetcher for a more realistic dry run price
        # This requires ccxt and potentially network access for public endpoints
        # For fully offline tests, _get_current_price_for_simulation should be mocked or return a fixed value.
        data_fetcher_for_test = DataFetcher(exchange_id='okx') # No keys needed for public ticker
        mock_exchange = data_fetcher_for_test.exchange
        print("Mock exchange (via DataFetcher) created for dry run price simulation.")
    except Exception as e:
        print(f"Could not create DataFetcher for mock exchange: {e}. Price simulation will use placeholder.")


    pm = PositionManager()
    # Initialize OrderExecutor with the mock_exchange for dry run price simulation
    oe = OrderExecutor(exchange_ccxt_instance=mock_exchange,
                       position_manager=pm,
                       dry_run=True,
                       default_symbol="BTC/USDT",
                       default_trade_amount=0.01)

    # Scenario 1: Create a buy order
    print("\n--- Scenario 1: Create BUY order (Dry Run) ---")
    buy_order_details = oe.create_buy_order()
    if buy_order_details:
        print(f"Buy order executed (Dry Run): {buy_order_details}")
        print(f"Position Manager status: {pm.get_status()}")
        assert pm.has_position is True
        assert pm.instrument_id == "BTC/USDT"
    else:
        print("Buy order failed.")

    # Scenario 2: Try to buy again (should be skipped)
    print("\n--- Scenario 2: Attempt another BUY order for same symbol (Dry Run) ---")
    buy_order_details_2 = oe.create_buy_order() # Should be skipped
    if buy_order_details_2:
        print(f"Second buy order executed (Dry Run): {buy_order_details_2}")
    else:
        print("Second buy order likely skipped as position already exists (expected).")
    assert pm.quantity == 0.01 # Quantity should not have changed

    # Scenario 3: Create a sell order
    print("\n--- Scenario 3: Create SELL order (Dry Run) ---")
    sell_order_details = oe.create_sell_order()
    if sell_order_details:
        print(f"Sell order executed (Dry Run): {sell_order_details}")
        print(f"Position Manager status: {pm.get_status()}")
        assert pm.has_position is False
    else:
        print("Sell order failed.")

    # Scenario 4: Try to sell again (no position)
    print("\n--- Scenario 4: Attempt another SELL order (no position) (Dry Run) ---")
    sell_order_details_2 = oe.create_sell_order() # Should be skipped/fail
    if sell_order_details_2:
        print(f"Second sell order executed (Dry Run): {sell_order_details_2}")
    else:
        print("Second sell order likely skipped as no position exists (expected).")

    # Scenario 5: Buy a different asset
    print("\n--- Scenario 5: Create BUY order for ETH/USDT (Dry Run) ---")
    buy_eth_details = oe.create_buy_order(symbol="ETH/USDT", quantity=0.1)
    if buy_eth_details:
        print(f"ETH Buy order executed (Dry Run): {buy_eth_details}")
        print(f"Position Manager status: {pm.get_status()}")
        assert pm.instrument_id == "ETH/USDT"
    else:
        print("ETH Buy order failed.")

    print("\n--- OrderExecutor Test Complete ---")

    # Note: Live trading tests would require actual API keys, a sandbox environment,
    # and careful handling to avoid unintended real trades.
    # Those would typically be separate integration tests.
