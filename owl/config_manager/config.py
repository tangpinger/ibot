import toml
import os
from pathlib import Path

CONFIG_FILE_NAME = "config.toml"
LOCAL_CONFIG_FILE_NAME = "config.local.toml"
# Assuming the script is run from the root of the project or owl/main.py is the entry point
# Adjust if necessary, e.g., if scripts are run from within module directories
# CONFIG_FILE_PATH = Path(__file__).resolve().parent.parent / CONFIG_FILE_NAME # Should point to project root/config.toml

# In a real scenario, you might want to search in a few locations:
# 1. Current working directory
# 2. Project root (if discoverable)
# 3. User's home directory in a .owl/ folder

# For simplicity, we'll assume config.toml is in the project root,
# and this module is in owl/config_manager/
# So, to find project_root/config.toml from owl/config_manager/config.py:
# project_root = Path(__file__).resolve().parent.parent.parent
# CONFIG_FILE_PATH = project_root / CONFIG_FILE_NAME
# Let's correct this based on typical project structure where main.py is in owl/
# and owl is the package.
# If main.py is owl/main.py, then Path(__file__).parent.parent is owl/
# and Path(__file__).parent.parent.parent is the project root.

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CONFIG_FILE_PATH = PROJECT_ROOT / CONFIG_FILE_NAME
LOCAL_CONFIG_FILE_PATH = PROJECT_ROOT / LOCAL_CONFIG_FILE_NAME


class ConfigError(Exception):
    """Custom exception for configuration errors."""
    pass

def load_config():
    """
    Loads the configuration from the config.toml file.

    The config.toml file is expected to be in the project's root directory.

    Returns:
        dict: A dictionary containing the configuration settings.

    Raises:
        ConfigError: If the config file is not found or cannot be parsed.
    """
    path_to_load = None
    chosen_config_name = ""

    if LOCAL_CONFIG_FILE_PATH.exists():
        path_to_load = LOCAL_CONFIG_FILE_PATH
        chosen_config_name = LOCAL_CONFIG_FILE_NAME
        print(f"Using local configuration: {chosen_config_name}")
    elif CONFIG_FILE_PATH.exists():
        path_to_load = CONFIG_FILE_PATH
        chosen_config_name = CONFIG_FILE_NAME
        print(f"Using main configuration: {chosen_config_name}")
    else:
        raise ConfigError(
            f"Configuration file not found. Looked for '{LOCAL_CONFIG_FILE_NAME}' at {LOCAL_CONFIG_FILE_PATH} "
            f"and '{CONFIG_FILE_NAME}' at {CONFIG_FILE_PATH}. "
            f"Please ensure one of them exists or copy 'config.example.toml' to '{CONFIG_FILE_NAME}' or '{LOCAL_CONFIG_FILE_NAME}' and fill in your details."
        )

    try:
        with open(path_to_load, 'r') as f:
            config = toml.load(f)

        # Ensure strategy section and its specific keys exist with defaults
        strategy_config = config.get('strategy', {})
        strategy_config['sell_window_start_time'] = strategy_config.get('sell_window_start_time', "09:00")
        strategy_config['sell_window_end_time'] = strategy_config.get('sell_window_end_time', "10:00")
        config['strategy'] = strategy_config

        return config
    except toml.TomlDecodeError as e:
        raise ConfigError(f"Error decoding '{chosen_config_name}': {e}")
    except Exception as e:
        raise ConfigError(f"An unexpected error occurred while loading '{chosen_config_name}': {e}")

# Example of how to use it (optional, for testing within this file)
if __name__ == "__main__":
    # Define paths for dummy configs
    dummy_config_regular_path = PROJECT_ROOT / "config.toml"
    dummy_config_local_path = PROJECT_ROOT / "config.local.toml"
    created_files = []

    try:
        print("\n--- Testing with no config files ---")
        # Ensure no config files exist for this test
        if dummy_config_regular_path.exists(): os.remove(dummy_config_regular_path)
        if dummy_config_local_path.exists(): os.remove(dummy_config_local_path)
        try:
            load_config()
        except ConfigError as e:
            print(f"Caught expected error: {e}")

        print("\n--- Testing with config.toml only ---")
        with open(dummy_config_regular_path, 'w') as cf:
            cf.write("""
[settings]
source = "config.toml"
[mode]
dry_run = true
[logging]
log_level = "INFO"
            """)
        created_files.append(dummy_config_regular_path)
        print(f"Created dummy {CONFIG_FILE_NAME} for testing.")
        config_settings = load_config()
        print(f"Loaded settings: {config_settings.get('settings')}")
        assert config_settings.get('settings', {}).get('source') == "config.toml"

        print("\n--- Testing with config.local.toml only ---")
        if dummy_config_regular_path.exists(): os.remove(dummy_config_regular_path) # remove regular
        with open(dummy_config_local_path, 'w') as cf:
            cf.write("""
[settings]
source = "config.local.toml"
[mode]
dry_run = false
[logging]
log_level = "DEBUG"
            """)
        created_files.append(dummy_config_local_path)
        print(f"Created dummy {LOCAL_CONFIG_FILE_NAME} for testing.")
        config_settings = load_config()
        print(f"Loaded settings: {config_settings.get('settings')}")
        assert config_settings.get('settings', {}).get('source') == "config.local.toml"

        print("\n--- Testing with both config.toml and config.local.toml (local should be prioritized) ---")
        with open(dummy_config_regular_path, 'w') as cf: # recreate regular
            cf.write("""
[settings]
source = "config.toml"
[mode]
dry_run = true
[logging]
log_level = "INFO"
            """)
        # local_config already exists and should be prioritized
        config_settings = load_config()
        print(f"Loaded settings: {config_settings.get('settings')}")
        assert config_settings.get('settings', {}).get('source') == "config.local.toml"
        assert config_settings.get('logging', {}).get('log_level') == "DEBUG"


        print("\nAll configuration loading tests passed!")
        print(f"Dry run mode from final test: {config_settings.get('mode', {}).get('dry_run')}")
        print(f"Log level from final test: {config_settings.get('logging', {}).get('log_level')}")
        strategy_settings = config_settings.get('strategy', {}) # Ensure this still works
        print(f"Sell window start (default): {strategy_settings.get('sell_window_start_time')}")
        print(f"Sell window end (default): {strategy_settings.get('sell_window_end_time')}")

    except ConfigError as err:
        print(f"Configuration Error: {err}")
    except AssertionError as ae:
        print(f"Test Assertion Error: {ae}")
    finally:
        # Clean up dummy configs
        for f_path in created_files:
            if f_path.exists():
                os.remove(f_path)
                print(f"Removed dummy config at {f_path}.")
        # Ensure local is removed if it wasn't in created_files list during a specific test run
        if dummy_config_local_path.exists():
             os.remove(dummy_config_local_path)
             print(f"Removed dummy config at {dummy_config_local_path}.")
        if dummy_config_regular_path.exists(): # Just in case
            os.remove(dummy_config_regular_path)
            print(f"Removed dummy config at {dummy_config_regular_path}.")
