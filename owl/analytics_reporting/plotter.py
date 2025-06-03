import matplotlib
matplotlib.use('Agg') # Use a non-interactive backend suitable for scripts/servers
import matplotlib.pyplot as plt
import pandas as pd

def plot_equity_curve(portfolio_history_df, output_path="equity_curve.png"):
    """
    Generates and saves an equity curve plot from portfolio history.

    Args:
        portfolio_history_df (pd.DataFrame): DataFrame with 'timestamp', 'total_value', and 'price' columns.
                                             'timestamp' should be datetime-like.
                                             'price' represents the price of the symbol at the given timestamp.
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
    required_columns = ['timestamp', 'total_value', 'price']
    for col in required_columns:
        if col not in portfolio_history_df.columns:
            print(f"Error: portfolio_history_df must contain '{col}' column.")
            return False
    if portfolio_history_df['timestamp'].isnull().any() or \
       portfolio_history_df['total_value'].isnull().any() or \
       portfolio_history_df['price'].isnull().any():
        print("Error: 'timestamp', 'total_value', or 'price' columns contain NaN values.")
        return False

    try:
        # Ensure timestamp is in datetime format for plotting
        # This should ideally be handled before calling, but as a safeguard:
        df_to_plot = portfolio_history_df.copy()
        df_to_plot['timestamp'] = pd.to_datetime(df_to_plot['timestamp'])
        df_to_plot = df_to_plot.sort_values(by='timestamp')

        # Normalize the 'price' column
        if not df_to_plot.empty and 'total_value' in df_to_plot.columns and 'price' in df_to_plot.columns:
            initial_total_value = df_to_plot['total_value'].iloc[0]
            initial_price = df_to_plot['price'].iloc[0]

            if initial_price != 0:
                scaling_factor = initial_total_value / initial_price
            else:
                scaling_factor = 1  # Avoid division by zero, or log a warning
                print("Warning: Initial symbol price is zero. Price normalization may not be accurate.")

            df_to_plot['normalized_price'] = df_to_plot['price'] * scaling_factor
        else:
            # Handle cases where df_to_plot is empty or columns are missing after sort
            # This should ideally not happen if input validation passed, but as a safeguard.
            df_to_plot['normalized_price'] = df_to_plot['price'] # Fallback or raise error

        fig, ax1 = plt.subplots(figsize=(12, 6))

        # Plot Portfolio Value on primary y-axis
        color = 'tab:blue'
        ax1.set_xlabel('Time', fontsize=12)
        ax1.set_ylabel('Portfolio Value', color=color, fontsize=12)
        ax1.plot(df_to_plot['timestamp'], df_to_plot['total_value'], label='Portfolio Value', color=color)
        ax1.tick_params(axis='y', labelcolor=color)
        ax1.grid(True, linestyle='--', alpha=0.7) # Apply grid to primary axis

        # Create secondary y-axis for Symbol Price
        ax2 = ax1.twinx()
        color = 'tab:red'
        ax2.set_ylabel('Normalized Symbol Price (Scaled to Initial Equity)', color=color, fontsize=12)  # we already handled the x-label with ax1
        ax2.plot(df_to_plot['timestamp'], df_to_plot['normalized_price'], label='Normalized Symbol Price', color=color)
        ax2.tick_params(axis='y', labelcolor=color)

        # Title
        plt.title('Equity Curve and Symbol Price', fontsize=16)

        # Combined legend
        lines, labels = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax2.legend(lines + lines2, labels + labels2, loc='upper left', fontsize=10)

        # Improve date formatting on x-axis
        if pd.api.types.is_datetime64_any_dtype(df_to_plot['timestamp']):
            ax1.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%Y-%m-%d %H:%M'))
            fig.autofmt_xdate() # Auto formats the x-axis labels (like rotation)
        else:
            plt.xticks(rotation=45) # Fallback rotation if not datetime

        fig.tight_layout()  # Adjust layout to prevent labels from overlapping

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
        {'timestamp': pd.to_datetime('2023-01-01 10:00:00'), 'total_value': 10000, 'price': 100.0},
        {'timestamp': pd.to_datetime('2023-01-02 10:00:00'), 'total_value': 10100, 'price': 101.0},
        {'timestamp': pd.to_datetime('2023-01-03 10:00:00'), 'total_value': 10050, 'price': 100.5},
        {'timestamp': pd.to_datetime('2023-01-04 10:00:00'), 'total_value': 10200, 'price': 102.0},
        {'timestamp': pd.to_datetime('2023-01-05 10:00:00'), 'total_value': 10150, 'price': 101.5},
    ]
    dummy_df = pd.DataFrame(dummy_history)
    # Ensure timestamp is datetime (already done in dummy_history but good practice for real data)
    dummy_df['timestamp'] = pd.to_datetime(dummy_df['timestamp'])

    print("Testing plot_equity_curve with valid data...")
    success = plot_equity_curve(dummy_df, output_path="test_equity_curve_with_price.png")
    if success:
        print("Test plot generated successfully: test_equity_curve_with_price.png")
    else:
        print("Test plot generation failed for valid data.")

    # Test with empty DataFrame
    print("\nTesting with empty DataFrame...")
    empty_df = pd.DataFrame(columns=['timestamp', 'total_value', 'price'])
    success_empty = plot_equity_curve(empty_df, output_path="test_empty_equity_curve_with_price.png")
    if not success_empty:
        print("Correctly handled empty DataFrame scenario (no plot generated).")
    else:
        print("Failed to handle empty DataFrame scenario correctly (plot might have been attempted or saved).")

    # Test with missing columns
    print("\nTesting with DataFrame missing 'price' column...")
    missing_col_df = pd.DataFrame({'timestamp': [pd.to_datetime('2023-01-01')], 'total_value': [1000]})
    success_missing = plot_equity_curve(missing_col_df, output_path="test_missing_price_col_equity_curve.png")
    if not success_missing:
        print("Correctly handled missing column scenario (no plot generated).")
    else:
        print("Failed to handle missing column scenario correctly.")

    # Test with NaN values
    print("\nTesting with DataFrame containing NaN values in 'price' column...")
    nan_history = [
        {'timestamp': pd.to_datetime('2023-01-01'), 'total_value': 10000, 'price': 100.0},
        {'timestamp': pd.to_datetime('2023-01-02'), 'total_value': 10100, 'price': None}, # NaN price
        {'timestamp': pd.to_datetime('2023-01-03'), 'total_value': 10050, 'price': 100.5},
    ]
    nan_df = pd.DataFrame(nan_history)
    success_nan = plot_equity_curve(nan_df, output_path="test_nan_price_equity_curve.png")
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
    # Create a new dummy_df for this test, as the previous one might be modified by other tests if not careful
    # (though in this script, functions receive copies or new DataFrames, so it should be fine)
    dummy_df_for_path_test = pd.DataFrame([
        {'timestamp': pd.to_datetime('2023-01-01 10:00:00'), 'total_value': 10000, 'price': 100.0},
        {'timestamp': pd.to_datetime('2023-01-02 10:00:00'), 'total_value': 10100, 'price': 101.0}
    ])
    specific_path = "temp_plot_output_with_price.jpg" # Test with a different extension
    success_path = plot_equity_curve(dummy_df_for_path_test, output_path=specific_path)
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
