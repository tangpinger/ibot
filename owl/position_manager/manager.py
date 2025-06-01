from datetime import datetime

class PositionManager:
    """
    Manages the current trading position status.
    """
    def __init__(self):
        """
        Initializes the PositionManager.
        By default, there is no position.
        """
        self.instrument_id = None
        self.has_position = False
        self.entry_price = 0.0
        self.quantity = 0.0
        self.entry_time = None
        self.position_type = None # 'long' or 'short' (though current strategy is long only)

    def update_position(self, instrument_id, entry_price, quantity, entry_time, position_type="long"):
        """
        Updates the manager when a new position is entered (e.g., after a buy).

        Args:
            instrument_id (str): The identifier of the instrument being traded (e.g., "BTC/USDT").
            entry_price (float): The price at which the position was entered.
            quantity (float): The amount of the asset bought.
            entry_time (datetime): The timestamp when the position was entered.
            position_type (str, optional): Type of position, e.g., "long" or "short". Defaults to "long".
        """
        if self.has_position:
            # Handle scenarios like averaging down or increasing position size if needed.
            # For now, we assume one position at a time as per the strategy.
            print(f"Warning: update_position called while already holding a position for {self.instrument_id}. Overwriting with new position details for {instrument_id}.")

        self.instrument_id = instrument_id
        self.entry_price = float(entry_price)
        self.quantity = float(quantity)
        self.entry_time = entry_time
        self.position_type = position_type
        self.has_position = True

        print(f"Position updated: Holding {self.quantity} of {self.instrument_id} bought at {self.entry_price} on {self.entry_time}. Type: {self.position_type}")

    def clear_position(self):
        """
        Clears the current position (e.g., after a sell).
        Returns the details of the cleared position for P&L calculation if needed.
        """
        if not self.has_position:
            print("Warning: clear_position called when no position is currently held.")
            return None

        cleared_position_details = {
            "instrument_id": self.instrument_id,
            "entry_price": self.entry_price,
            "quantity": self.quantity,
            "entry_time": self.entry_time,
            "position_type": self.position_type
        }

        print(f"Position cleared: Sold {self.quantity} of {self.instrument_id}. Entry price was {self.entry_price}.")

        self.instrument_id = None
        self.has_position = False
        self.entry_price = 0.0
        self.quantity = 0.0
        self.entry_time = None
        self.position_type = None

        return cleared_position_details

    def get_status(self):
        """
        Returns the current position status.

        Returns:
            dict: A dictionary containing details of the current position.
                  Example: {'has_position': True, 'instrument_id': 'BTC/USDT', 'entry_price': 50000, ...}
        """
        return {
            "instrument_id": self.instrument_id,
            "has_position": self.has_position,
            "entry_price": self.entry_price,
            "quantity": self.quantity,
            "entry_time": self.entry_time.isoformat() if self.entry_time else None,
            "position_type": self.position_type
        }

    def __str__(self):
        if self.has_position:
            return (f"PositionManager: Holding {self.quantity} of {self.instrument_id} "
                    f"(Type: {self.position_type}), bought at {self.entry_price} on {self.entry_time}.")
        else:
            return "PositionManager: No current position."

# Example of how to use it (optional, for testing within this file)
if __name__ == "__main__":
    print("--- Testing PositionManager ---")
    pm = PositionManager()
    print(pm) # Initial status

    # Simulate a buy
    print("\n--- Simulating a buy ---")
    buy_time = datetime.now()
    pm.update_position(instrument_id="BTC/USDT", entry_price=50000.0, quantity=0.1, entry_time=buy_time)
    print(pm)
    status = pm.get_status()
    print(f"Current status via get_status(): {status}")
    assert status['has_position'] is True
    assert status['quantity'] == 0.1

    # Simulate trying to buy again (should warn and overwrite)
    print("\n--- Simulating another buy (overwrite) ---")
    new_buy_time = datetime.now()
    pm.update_position(instrument_id="ETH/USDT", entry_price=4000.0, quantity=0.5, entry_time=new_buy_time)
    print(pm)
    status_eth = pm.get_status()
    print(f"Current status via get_status(): {status_eth}")
    assert status_eth['instrument_id'] == "ETH/USDT"


    # Simulate a sell
    print("\n--- Simulating a sell ---")
    cleared_details = pm.clear_position()
    print(pm)
    status_after_sell = pm.get_status()
    print(f"Current status via get_status(): {status_after_sell}")
    assert status_after_sell['has_position'] is False
    print(f"Details of cleared position: {cleared_details}")
    assert cleared_details['quantity'] == 0.5

    # Simulate trying to sell again (should warn)
    print("\n--- Simulating another sell (no position) ---")
    pm.clear_position()
    print(pm)

    print("\n--- PositionManager Test Complete ---")
