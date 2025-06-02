import matplotlib
matplotlib.use('Agg') # Use a non-interactive backend suitable for scripts/servers
import matplotlib.pyplot as plt
import pandas as pd

def plot_equity_curve(portfolio_history_df, output_path="equity_curve.png"):
    """
    Generates and saves an equity curve plot from portfolio history.

    Args:
        portfolio_history_df (pd.DataFrame): DataFrame with 'timestamp' and 'total_value' columns.
                                             'timestamp' should be datetime-like.
        output_path (str, optional): The path to save the plot image. Defaults to "equity_curve.png".

    Returns:
        bool: True if the plot was saved successfully, False otherwise.
    """
    # Input Validation
    if not isinstance(portfolio_history_df, pd.DataFrame):
        print("Error: portfolio_history_df must be a Pandas DataFrame.")
        return False
    if portfolio_history_df.empty:
        print("Error: portfolio_history_df is empty. Cannot generate plot.")
        return False
    if 'timestamp' not in portfolio_history_df.columns or 'total_value' not in portfolio_history_df.columns:
        print("Error: portfolio_history_df must contain 'timestamp' and 'total_value' columns.")
        return False
    if portfolio_history_df['timestamp'].isnull().any() or portfolio_history_df['total_value'].isnull().any():
        print("Error: 'timestamp' or 'total_value' columns contain NaN values.")
        return False

    try:
        # Ensure timestamp is in datetime format for plotting
        # This should ideally be handled before calling, but as a safeguard:
        df_to_plot = portfolio_history_df.copy()
        df_to_plot['timestamp'] = pd.to_datetime(df_to_plot['timestamp'])
        df_to_plot = df_to_plot.sort_values(by='timestamp')


        plt.figure(figsize=(12, 6))
        plt.plot(df_to_plot['timestamp'], df_to_plot['total_value'], label='Portfolio Value', color='blue')

        plt.title('Equity Curve', fontsize=16)
        plt.xlabel('Time', fontsize=12)
        plt.ylabel('Portfolio Value', fontsize=12)

        plt.legend(fontsize=10)
        plt.grid(True, linestyle='--', alpha=0.7)

        # Improve date formatting on x-axis if timestamps are datetime objects
        if pd.api.types.is_datetime64_any_dtype(df_to_plot['timestamp']):
            plt.gca().xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%Y-%m-%d %H:%M'))
            plt.gcf().autofmt_xdate() # Auto formats the x-axis labels (like rotation)
        else:
            plt.xticks(rotation=45) # Fallback rotation if not datetime

        plt.tight_layout() # Adjust layout to prevent labels from overlapping

        # Save Plot
        plt.savefig(output_path)
        plt.close() # Close the figure to free memory

        print(f"Plot saved to {output_path}")
        return True

    except Exception as e:
        print(f"An error occurred during plot generation: {e}")
        # Attempt to close plot if it was opened, to prevent lingering state
        if plt.gcf().get_axes(): # Check if a figure context exists
            plt.close()
        return False

if __name__ == '__main__':
    # Create dummy data for testing plotter.py directly
    dummy_history = [
        {'timestamp': pd.to_datetime('2023-01-01 10:00:00'), 'total_value': 10000},
        {'timestamp': pd.to_datetime('2023-01-02 10:00:00'), 'total_value': 10100},
        {'timestamp': pd.to_datetime('2023-01-03 10:00:00'), 'total_value': 10050},
        {'timestamp': pd.to_datetime('2023-01-04 10:00:00'), 'total_value': 10200},
        {'timestamp': pd.to_datetime('2023-01-05 10:00:00'), 'total_value': 10150},
    ]
    dummy_df = pd.DataFrame(dummy_history)
    # Ensure timestamp is datetime (already done in dummy_history but good practice for real data)
    dummy_df['timestamp'] = pd.to_datetime(dummy_df['timestamp'])

    print("Testing plot_equity_curve with valid data...")
    success = plot_equity_curve(dummy_df, output_path="test_equity_curve.png")
    if success:
        print("Test plot generated successfully: test_equity_curve.png")
    else:
        print("Test plot generation failed for valid data.")

    # Test with empty DataFrame
    print("\nTesting with empty DataFrame...")
    empty_df = pd.DataFrame(columns=['timestamp', 'total_value'])
    success_empty = plot_equity_curve(empty_df, output_path="test_empty_equity_curve.png")
    if not success_empty:
        print("Correctly handled empty DataFrame scenario (no plot generated).")
    else:
        print("Failed to handle empty DataFrame scenario correctly (plot might have been attempted or saved).")

    # Test with missing columns
    print("\nTesting with DataFrame missing 'total_value' column...")
    missing_col_df = pd.DataFrame({'timestamp': [pd.to_datetime('2023-01-01')]})
    success_missing = plot_equity_curve(missing_col_df, output_path="test_missing_col_equity_curve.png")
    if not success_missing:
        print("Correctly handled missing column scenario (no plot generated).")
    else:
        print("Failed to handle missing column scenario correctly.")

    # Test with NaN values
    print("\nTesting with DataFrame containing NaN values...")
    nan_history = [
        {'timestamp': pd.to_datetime('2023-01-01'), 'total_value': 10000},
        {'timestamp': None, 'total_value': 10100}, # NaN timestamp
        {'timestamp': pd.to_datetime('2023-01-03'), 'total_value': None}, # NaN value
    ]
    nan_df = pd.DataFrame(nan_history)
    success_nan = plot_equity_curve(nan_df, output_path="test_nan_equity_curve.png")
    if not success_nan:
        print("Correctly handled NaN values scenario (no plot generated).")
    else:
        print("Failed to handle NaN values scenario correctly.")

    # Test with non-DataFrame input
    print("\nTesting with non-DataFrame input...")
    success_invalid_type = plot_equity_curve([1, 2, 3], output_path="test_invalid_type_curve.png")
    if not success_invalid_type:
        print("Correctly handled non-DataFrame input scenario (no plot generated).")
    else:
        print("Failed to handle non-DataFrame input scenario correctly.")

    # Test output path that might be problematic (e.g. non-existent dir - though savefig might create it)
    # For now, this is a simple path test. More complex path tests would involve mocking os functions.
    print("\nTesting with a specific output path...")
    specific_path = "temp_plot_output.jpg" # Test with a different extension
    success_path = plot_equity_curve(dummy_df, output_path=specific_path)
    if success_path:
        print(f"Test plot generated successfully: {specific_path}")
        # Optional: check if file exists, then clean up
        # import os
        # if os.path.exists(specific_path):
        #     print(f"File {specific_path} exists.")
        #     # os.remove(specific_path) # Clean up test file
        # else:
        #     print(f"File {specific_path} was NOT created despite success=True.")
    else:
        print(f"Test plot generation failed for path: {specific_path}")
