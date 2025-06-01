import logging
import sys
from logging.handlers import RotatingFileHandler

# Default log settings (can be overridden by config)
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_LOG_FILE = "owl_bot.log"
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
MAX_LOG_FILE_SIZE = 10 * 1024 * 1024  # 10 MB
BACKUP_COUNT = 5

def setup_logging(log_level_str=None, log_file=None, logger_name="owl"):
    """
    Configures logging for the application.

    Args:
        log_level_str (str, optional): The desired log level (e.g., "DEBUG", "INFO").
                                       Defaults to DEFAULT_LOG_LEVEL.
        log_file (str, optional): Path to the log file. Defaults to DEFAULT_LOG_FILE.
                                  If None, file logging is disabled.
        logger_name (str, optional): The root logger name for the application.
                                     Defaults to "owl".

    Returns:
        logging.Logger: The configured logger instance.
    """
    # Get the root logger for the application
    logger = logging.getLogger(logger_name)

    # Prevent multiple handlers if setup_logging is called more than once
    if logger.hasHandlers():
        # In some testing scenarios or reloads, handlers might persist.
        # Clear them to avoid duplicate logs.
        # For production, ensure this is called only once.
        logger.handlers.clear()


    # Determine log level
    log_level_to_set = getattr(logging, str(log_level_str).upper(), None)
    if not isinstance(log_level_to_set, int):
        print(f"Warning: Invalid log level '{log_level_str}'. Defaulting to {DEFAULT_LOG_LEVEL}.")
        log_level_to_set = getattr(logging, DEFAULT_LOG_LEVEL)

    logger.setLevel(log_level_to_set)

    # Create formatter
    formatter = logging.Formatter(LOG_FORMAT)

    # Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File Handler (optional)
    actual_log_file = log_file if log_file is not None else DEFAULT_LOG_FILE

    if actual_log_file: # Proceed if a log file path is provided
        try:
            file_handler = RotatingFileHandler(
                actual_log_file,
                maxBytes=MAX_LOG_FILE_SIZE,
                backupCount=BACKUP_COUNT,
                encoding='utf-8'
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            logger.info(f"Logging initialized. Log level: {logging.getLevelName(logger.level)}. Outputting to console and file: {actual_log_file}")
        except Exception as e:
            logger.error(f"Failed to initialize file logging to {actual_log_file}: {e}", exc_info=True)
            # Continue with console logging
            logger.info(f"Logging initialized. Log level: {logging.getLevelName(logger.level)}. Outputting to console only.")
    else:
        logger.info(f"Logging initialized. Log level: {logging.getLevelName(logger.level)}. Outputting to console only (no log file specified).")


    return logger

# Example of how to use it (optional, for testing within this file)
if __name__ == "__main__":
    # Test with default settings
    print("--- Testing logger with default settings ---")
    default_logger = setup_logging(logger_name="owl_default_test")
    default_logger.debug("This is a debug message (default).")
    default_logger.info("This is an info message (default).")
    default_logger.warning("This is a warning message (default).")
    default_logger.error("This is an error message (default).")
    print(f"Default log file should be at: {DEFAULT_LOG_FILE}")
    print("\n--- Testing logger with custom settings ---")

    # Test with custom settings
    custom_logger = setup_logging(log_level_str="DEBUG", log_file="custom_test.log", logger_name="owl_custom_test")
    custom_logger.debug("This is a debug message (custom).")
    custom_logger.info("This is an info message (custom).")

    # Test invalid log level
    print("\n--- Testing logger with invalid level ---")
    invalid_logger = setup_logging(log_level_str="VERBOSEMESSAGE", log_file="invalid_level_test.log", logger_name="owl_invalid_test")
    invalid_logger.info("Info message after invalid level warning.")

    print("\n--- Testing logger with no file path ---")
    no_file_logger = setup_logging(log_level_str="INFO", log_file=None, logger_name="owl_no_file_test")
    no_file_logger.info("This message should only go to console.")

    # Clean up test log files
    import os
    for f_name in [DEFAULT_LOG_FILE, "custom_test.log", "invalid_level_test.log"]:
        if os.path.exists(f_name):
            # Before removing, close handlers if they exist for this file
            for logger_instance in [default_logger, custom_logger, invalid_logger]:
                for handler in logger_instance.handlers:
                    if isinstance(handler, logging.FileHandler) and handler.baseFilename.endswith(f_name):
                        handler.close()
            try:
                os.remove(f_name)
                print(f"Removed test log file: {f_name}")
            except PermissionError:
                print(f"Could not remove test log file due to permission error: {f_name}")
