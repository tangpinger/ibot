from apscheduler.schedulers.blocking import BlockingScheduler
# from apscheduler.schedulers.background import BackgroundScheduler # Alternative for non-blocking
from apscheduler.triggers.cron import CronTrigger
import pytz # For timezone handling
import logging
from datetime import datetime

logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class OwlScheduler:
    """
    Manages scheduled tasks for the trading bot using APScheduler.
    All job times are based on Beijing Time (UTC+8).
    """
    def __init__(self, timezone_str='Asia/Shanghai'):
        """
        Initializes the Scheduler.

        Args:
            timezone_str (str, optional): The timezone for scheduling. Defaults to 'Asia/Shanghai'.
        """
        try:
            self.timezone = pytz.timezone(timezone_str)
        except pytz.exceptions.UnknownTimeZoneError:
            logger.error(f"Unknown timezone: {timezone_str}. Defaulting to UTC.")
            self.timezone = pytz.utc # Fallback to UTC

        # Using BlockingScheduler if this scheduler is the main loop of the script.
        # If integrated into a larger application, BackgroundScheduler might be more appropriate.
        self.scheduler = BlockingScheduler(timezone=self.timezone)
        self._jobs = {} # To keep track of job instances

        logger.info(f"Scheduler initialized with timezone: {self.timezone}")

    def add_job(self, func, job_id, trigger_type='cron', **trigger_args):
        """
        Adds a job to the scheduler.

        Args:
            func (callable): The function to execute.
            job_id (str): A unique identifier for the job.
            trigger_type (str, optional): Type of trigger ('cron', 'interval', 'date'). Defaults to 'cron'.
            **trigger_args: Arguments for the trigger (e.g., hour, minute, day_of_week for cron).
        """
        if job_id in self._jobs:
            logger.warning(f"Job with ID '{job_id}' already exists. It will be replaced.")
            self.scheduler.remove_job(job_id)

        try:
            job = self.scheduler.add_job(func, trigger=trigger_type, id=job_id, **trigger_args)
            self._jobs[job_id] = job
            logger.info(f"Job '{job_id}' added with trigger: {trigger_type}, args: {trigger_args}")
        except Exception as e:
            logger.error(f"Failed to add job '{job_id}': {e}", exc_info=True)

    def _example_data_fetch_job(self):
        """Placeholder for the actual data fetching logic call."""
        logger.info(f"SCHEDULER: Triggered 'Daily Data Fetch' job at {datetime.now(self.timezone)}")
        # In real implementation, this would call something like:
        # bot_instance.fetch_daily_data()

    def _example_buy_signal_check_job(self):
        """Placeholder for the actual buy signal check and execution logic call."""
        logger.info(f"SCHEDULER: Triggered 'Buy Signal Check/Execution' job at {datetime.now(self.timezone)}")
        # In real implementation, this would call something like:
        # bot_instance.check_and_execute_buy_strategy()

    def _example_sell_execution_job(self):
        """Placeholder for the actual sell execution logic call."""
        logger.info(f"SCHEDULER: Triggered 'Sell Execution' job at {datetime.now(self.timezone)}")
        # In real implementation, this would call something like:
        # bot_instance.execute_sell_strategy()

    def setup_default_jobs(self,
                           data_fetch_func=None,
                           buy_check_func=None,
                           sell_execute_func=None,
                           config=None):
        """
        Sets up the default trading jobs based on the strategy.
        Times are from the design document (Section 4.5, 5.1). Assumed UTC+8.

        Args:
            data_fetch_func (callable, optional): Function to call for daily data fetching.
            buy_check_func (callable, optional): Function to call for checking buy signals and executing.
            sell_execute_func (callable, optional): Function to call for executing sell orders.
            config (dict, optional): Configuration dictionary, used to get specific times.
        """
        df_func = data_fetch_func if data_fetch_func else self._example_data_fetch_job
        bc_func = buy_check_func if buy_check_func else self._example_buy_signal_check_job
        se_func = sell_execute_func if sell_execute_func else self._example_sell_execution_job

        # Default times if not in config or config not provided
        daily_data_fetch_time = "10:00"
        buy_check_time = "15:55" # This is when to check, actual buy at 16:00
        # buy_execute_time = "16:00" # The buy_check_func should handle the 16:00 execution if signal
        sell_execute_time = "09:55" # Sell before 10:00 open

        if config and 'scheduler' in config:
            daily_data_fetch_time = config['scheduler'].get('daily_data_fetch_time', daily_data_fetch_time)
            buy_check_time = config['scheduler'].get('buy_check_time', buy_check_time)
            # buy_execute_time = config['scheduler'].get('buy_execute_time', buy_execute_time)
            sell_execute_time = config['scheduler'].get('sell_execute_time', sell_execute_time)

        fetch_hour, fetch_minute = map(int, daily_data_fetch_time.split(':'))
        buy_hour, buy_minute = map(int, buy_check_time.split(':'))
        sell_hour, sell_minute = map(int, sell_execute_time.split(':'))

        # 1. Daily Data Fetching (e.g., Mon-Fri at 10:00 AM UTC+8)
        # cron: day_of_week='mon-fri', hour=10, minute=0
        self.add_job(df_func, 'daily_data_fetch',
                     hour=fetch_hour, minute=fetch_minute, day_of_week='mon-fri')

        # 2. 收盘买入检查 (Buy Signal Check & Potential Execution)
        # Triggered on周五、周一、周二 (Fri, Mon, Tue) at 15:55 - 16:00 UTC+8.
        # The check_func should contain logic to only buy at 16:00 if conditions met.
        # cron: day_of_week='mon,tue,fri', hour=15, minute=55
        self.add_job(bc_func, 'buy_signal_check',
                     hour=buy_hour, minute=buy_minute, day_of_week='mon,tue,fri')

        # 3. 开盘卖出检查 (Sell Execution)
        # Triggered on 周一、周二、周三 (Mon, Tue, Wed) at 09:50 - 09:58 UTC+8 (e.g. 09:55).
        # cron: day_of_week='mon,tue,wed', hour=9, minute=55
        self.add_job(se_func, 'sell_execution',
                     hour=sell_hour, minute=sell_minute, day_of_week='mon,tue,wed')

        logger.info("Default trading jobs scheduled.")
        self.list_jobs()


    def list_jobs(self):
        """Lists all scheduled jobs."""
        if not self.scheduler.get_jobs():
            logger.info("No jobs currently scheduled.")
            return
        logger.info("Current scheduled jobs:")
        for job in self.scheduler.get_jobs():
            logger.info(f"  ID: {job.id}, Name: {job.name}, Trigger: {job.trigger}, Next Run: {job.next_run_time}")

    def start(self):
        """Starts the scheduler. This will block if using BlockingScheduler."""
        if not self.scheduler.get_jobs():
            logger.warning("Scheduler started, but no jobs are scheduled.")
        else:
            logger.info("Starting scheduler...")
        try:
            self.scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Scheduler stopped.")
        except Exception as e:
            logger.error(f"Scheduler crashed: {e}", exc_info=True)
        finally:
            if self.scheduler.running:
                self.scheduler.shutdown()

    def stop(self):
        """Stops the scheduler if it's running."""
        if self.scheduler.running:
            logger.info("Shutting down scheduler...")
            self.scheduler.shutdown()
        else:
            logger.info("Scheduler is not running.")

# Example of how to use it (optional, for testing within this file)
if __name__ == "__main__":
    import sys # Required for logging to stdout in test
    # Basic logger for testing
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    logger.info("--- Testing OwlScheduler ---")

    # Dummy config for testing job times
    dummy_config_scheduler = {
        "scheduler": {
            "daily_data_fetch_time": "00:01", # Every day at 00:01 for test
            "buy_check_time": "00:02",       # Every day at 00:02 for test
            "sell_execute_time": "00:03"     # Every day at 00:03 for test
        }
    }

    # Create scheduler instance (will use Asia/Shanghai by default)
    owl_sched = OwlScheduler(timezone_str='Asia/Shanghai') # Or 'UTC' for simpler testing across timezones

    # Define simple placeholder functions for the jobs
    def test_fetch(): logger.info("TEST JOB: Fetching data...")
    def test_buy(): logger.info("TEST JOB: Checking for buy signals...")
    def test_sell(): logger.info("TEST JOB: Executing sell orders...")

    # Setup jobs using the placeholder functions and dummy config for times
    # For testing, let's make them run more frequently or on all days
    # The default setup_default_jobs has specific days, so for a quick test,
    # we can add jobs manually or modify trigger_args in setup_default_jobs for testing.

    # Let's modify the setup_default_jobs for this test to run on all days for faster verification
    # This is a bit of a hack for the test; in real use, it would use the config/defaults.
    original_add_job = owl_sched.add_job
    def test_friendly_add_job(func, job_id, **trigger_args):
        logger.info(f"Test interceptor: Modifying trigger for {job_id} for testing purposes.")
        # Make jobs run every minute for quick testing, overriding original schedule
        # This is NOT how it should be in production.
        # For a real test, one might use `trigger='interval', seconds=X` or a specific date.
        # Or, use `override_next_run_time` if APScheduler supports it easily.
        # For this example, let's just use the times from dummy_config_scheduler and all days.

        # Use day_of_week='*' to run every day for testing these specific times
        trigger_args['day_of_week'] = '*'
        original_add_job(func, job_id, **trigger_args)

    owl_sched.add_job = test_friendly_add_job # Monkey patch for testing
    owl_sched.setup_default_jobs(test_fetch, test_buy, test_sell, config=dummy_config_scheduler)
    owl_sched.add_job = original_add_job # Restore original method

    owl_sched.list_jobs()

    logger.info("Scheduler test setup complete. Starting scheduler for a short duration (e.g., 10-15 seconds).")
    logger.info("Expect to see 'TEST JOB' messages if current time aligns with 00:01, 00:02, 00:03 in Asia/Shanghai (or UTC if changed).")
    logger.info("Since we set day_of_week='*', it will run if the H:M matches.")
    logger.info("To see jobs run, ensure the H:M in dummy_config_scheduler is slightly in the future from now.")

    # For a simple non-blocking test run if you were in a larger app:
    # from apscheduler.schedulers.background import BackgroundScheduler
    # owl_sched.scheduler = BackgroundScheduler(timezone=owl_sched.timezone)
    # owl_sched.setup_default_jobs(...)
    # owl_sched.scheduler.start()
    # time.sleep(X)
    # owl_sched.scheduler.shutdown()

    # Using BlockingScheduler, it will run until interrupted.
    # For an automated test, you'd run it in a separate thread or process and stop it.
    # Here, we'll just inform the user to manually stop it or it will run based on schedule.
    print("Starting BlockingScheduler. Press Ctrl+C to stop.")
    print("Jobs are scheduled as per dummy_config_scheduler times (00:01, 00:02, 00:03) for *every day of the week* due to test modification.")
    print(f"Current time in {owl_sched.timezone}: {datetime.now(owl_sched.timezone).strftime('%H:%M:%S')}")

    # This will block. For a CI/CD test, you'd need to run this in a thread and stop it after a few seconds.
    # For now, this example is more for interactive testing.
    # owl_sched.start()

    # Let's simulate a short run for automated testing:
    # Re-initialize with BackgroundScheduler for this part of the test
    import time
    from apscheduler.schedulers.background import BackgroundScheduler

    logger.info("--- Simulating short run with BackgroundScheduler for automated test ---")
    bg_scheduler_instance = BackgroundScheduler(timezone=owl_sched.timezone)
    owl_sched.scheduler = bg_scheduler_instance # Replace the scheduler instance

    # Add jobs again to the new scheduler instance
    original_add_job_bg = owl_sched.add_job
    def test_friendly_add_job_bg(func, job_id, **trigger_args):
        trigger_args['day_of_week'] = '*' # Run every day for testing
        original_add_job_bg(func, job_id, **trigger_args)
    owl_sched.add_job = test_friendly_add_job_bg
    owl_sched.setup_default_jobs(test_fetch, test_buy, test_sell, config=dummy_config_scheduler)
    owl_sched.add_job = original_add_job_bg # Restore

    owl_sched.list_jobs()

    try:
        logger.info(f"Starting BackgroundScheduler, will run for ~70 seconds to catch minute changes if times are set to next minute.")
        bg_scheduler_instance.start()
        # Check if current time is close to one of the hh:mm in dummy_config_scheduler
        # E.g., if dummy times are 00:01, 00:02, 00:03, and current time is 23:59, they should run soon.
        time.sleep(70) # Run for ~70 seconds
    except (KeyboardInterrupt, SystemExit):
        logger.info("Background scheduler interrupted.")
    finally:
        if bg_scheduler_instance.running:
            logger.info("Shutting down background scheduler.")
            bg_scheduler_instance.shutdown()
        else:
            logger.info("Background scheduler was not running or already shut down.")

    logger.info("--- OwlScheduler Test Complete ---")
