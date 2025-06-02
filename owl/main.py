import argparse
import sys
from owl.config_manager.config import load_config, ConfigError
from owl.data_fetcher.fetcher import DataFetcher
from owl.signal_generator.generator import SignalGenerator
from owl.backtesting_engine.engine import BacktestingEngine

def main():
    """
    Main function to run the Owl Trading Bot in different modes.
    """
    parser = argparse.ArgumentParser(description="Owl Trading Bot")
    parser.add_argument(
        "--mode",
        choices=['backtest', 'trade'],
        default='backtest', # Default to backtest mode
        help="Operating mode: 'backtest' for backtesting, 'trade' for live trading."
    )
    args = parser.parse_args()

    print("Loading configuration...")
    try:
        config = load_config()
        # print(f"Config loaded: {config}") # For debugging
    except ConfigError as e:
        print(f"Error loading configuration: {e}")
        sys.exit(1)
    except FileNotFoundError:
        print("Error: Configuration file 'config.toml' not found in the root directory.")
        print("Please ensure 'config.toml' exists. You can copy 'config.example.toml' to 'config.toml' and customize it.")
        sys.exit(1)


    if args.mode == 'backtest':
        print("Starting Owl in backtesting mode...")
        try:
            # Extract proxy settings
            proxy_settings = config.get('proxy', {})  # Try 'proxy' section first
            if not proxy_settings:  # Fallback to 'exchange_settings' if 'proxy' is empty or not found
                proxy_settings = config.get('exchange_settings', {})

            proxy_url = proxy_settings.get('proxy_url')
            proxy_type = proxy_settings.get('proxy_type')

            # Get API keys and other exchange settings
            api_keys_config = config.get('api_keys', {})
            okx_api_key = api_keys_config.get('okx_api_key')
            okx_secret_key = api_keys_config.get('okx_secret_key')
            okx_password = api_keys_config.get('okx_password')

            exchange_settings_config = config.get('exchange_settings', {})
            exchange_id = exchange_settings_config.get('exchange_id', 'okx') # Default to 'okx'
            is_sandbox_mode = exchange_settings_config.get('sandbox_mode', False)

            # Instantiate DataFetcher
            data_fetcher = DataFetcher(
                api_key=okx_api_key,
                secret_key=okx_secret_key,
                password=okx_password,
                exchange_id=exchange_id,
                is_sandbox_mode=is_sandbox_mode,
                proxy_url=proxy_url,
                proxy_type=proxy_type
            )

            # Instantiate SignalGenerator
            strategy_config = config.get('strategy', {})
            n_period = strategy_config.get('n_day_high_period')

            if n_period is None:
                print("Error: 'n_day_high_period' not found in [strategy] configuration.")
                sys.exit(1)
            try:
                n_period = int(n_period)
                if n_period <= 0:
                    raise ValueError("must be a positive integer")
            except ValueError as e:
                print(f"Error: 'n_day_high_period' in [strategy] config is invalid: {e}.")
                sys.exit(1)

            signal_generator = SignalGenerator(n_day_high_period=n_period)

            # Instantiate BacktestingEngine
            # BacktestingEngine __init__ handles critical config validation
            backtest_engine = BacktestingEngine(
                config=config,
                data_fetcher=data_fetcher,
                signal_generator=signal_generator
            )

            # Run Backtest
            print("Running backtest simulation...")
            backtest_engine.run_backtest()

        except ValueError as ve: # Catch config validation errors from engine/components
            print(f"Configuration or setup error during backtesting initialization: {ve}")
            sys.exit(1)
        except ConfigError as ce: # Should be caught by initial load, but as a safeguard
            print(f"Configuration error during backtesting: {ce}")
            sys.exit(1)
        except Exception as e:
            print(f"An unexpected error occurred during backtesting: {e}")
            # Consider adding traceback here for debugging: import traceback; traceback.print_exc()
            sys.exit(1)

    elif args.mode == 'trade':
        print("Live trading mode is not yet implemented.")
        # Future: Instantiate components for live trading (e.g., OrderManager, LiveDataStream)
        # live_engine = LiveTradingEngine(config=config, ...)
        # live_engine.start()

    else:
        # This case should not be reached if choices are correctly defined in argparse
        print(f"Unknown mode: {args.mode}")
        sys.exit(1)

    print("\nOwl Trading Bot finished.")

if __name__ == "__main__":
    main()
