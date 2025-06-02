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
            # Instantiate DataFetcher
            # Assuming DataFetcher's __init__ can handle fetching public data without explicit API keys from config for backtesting.
            # If DataFetcher needed API keys for all operations, you'd pass config.get('api_keys', {})
            data_fetcher = DataFetcher()

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
