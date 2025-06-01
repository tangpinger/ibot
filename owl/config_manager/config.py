import toml
import os
from pathlib import Path

CONFIG_FILE_NAME = "config.toml"
# Assuming the script is run from the root of the project or owl/main.py is the entry point
# Adjust if necessary, e.g., if scripts are run from within module directories
CONFIG_FILE_PATH = Path(__file__).resolve().parent.parent / CONFIG_FILE_NAME # Should point to project root/config.toml

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
    if not CONFIG_FILE_PATH.exists():
        raise ConfigError(
            f"Configuration file '{CONFIG_FILE_NAME}' not found at {CONFIG_FILE_PATH}. "
            f"Please copy 'config.example.toml' to '{CONFIG_FILE_NAME}' and fill in your details."
        )

    try:
        with open(CONFIG_FILE_PATH, 'r') as f:
            config = toml.load(f)
        return config
    except toml.TomlDecodeError as e:
        raise ConfigError(f"Error decoding '{CONFIG_FILE_NAME}': {e}")
    except Exception as e:
        raise ConfigError(f"An unexpected error occurred while loading '{CONFIG_FILE_NAME}': {e}")

# Example of how to use it (optional, for testing within this file)
if __name__ == "__main__":
    try:
        # To test this, you'd need a config.toml in the project root
        # Create a dummy one if it doesn't exist for testing
        if not CONFIG_FILE_PATH.exists():
            dummy_config_path = PROJECT_ROOT / "config.toml"
            with open(dummy_config_path, 'w') as cf:
                cf.write("""
[mode]
dry_run = true
[logging]
log_level = "DEBUG"
log_file = "test_bot.log"
                """)
            print(f"Created dummy config at {dummy_config_path} for testing.")

        config_settings = load_config()
        print("Configuration loaded successfully!")
        print(f"Dry run mode: {config_settings.get('mode', {}).get('dry_run')}")
        print(f"Log level: {config_settings.get('logging', {}).get('log_level')}")

        # Clean up dummy config if created
        if 'dummy_config_path' in locals() and dummy_config_path.exists():
             os.remove(dummy_config_path)
             print(f"Removed dummy config at {dummy_config_path}.")

    except ConfigError as err:
        print(f"Configuration Error: {err}")
