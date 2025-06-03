import pandas as pd
import numpy as np

def generate_performance_report(portfolio_history, trades_log, initial_capital, risk_free_rate=0.0):
    """
    Generates a performance report from backtesting results.

    Args:
        portfolio_history (list of dict): List of {'timestamp': ts, 'total_value': val} dictionaries.
        trades_log (list of dict): List of trade dictionaries.
        initial_capital (float): The starting capital of the backtest.
        risk_free_rate (float, optional): Annual risk-free rate for Sharpe Ratio calculation. Defaults to 0.0.

    Returns:
        dict: A dictionary containing key performance indicators.
    """
    if not portfolio_history:
        final_portfolio_value = initial_capital
        portfolio_history_df = pd.DataFrame(columns=['timestamp', 'total_value'])
    else:
        portfolio_history_df = pd.DataFrame(portfolio_history)
        if 'timestamp' not in portfolio_history_df.columns or 'total_value' not in portfolio_history_df.columns:
            print("Error: portfolio_history DataFrame must contain 'timestamp' and 'total_value' columns.")
            # Return a minimal report or raise error
            return {
                "initial_capital": initial_capital,
                "final_portfolio_value": initial_capital,
                "total_return_percentage": 0.0,
                "total_trades": len(trades_log),
                "error": "Invalid portfolio_history format"
            }
        portfolio_history_df['timestamp'] = pd.to_datetime(portfolio_history_df['timestamp'])
        portfolio_history_df = portfolio_history_df.set_index('timestamp').sort_index()
        final_portfolio_value = portfolio_history_df['total_value'].iloc[-1] if not portfolio_history_df.empty else initial_capital

    if initial_capital > 0:
        total_return_percentage = ((final_portfolio_value - initial_capital) / initial_capital) * 100
    else:
        total_return_percentage = 0.0 if final_portfolio_value == 0 else float('inf') # Or handle as error

    total_trades = len(trades_log)
    buy_trades = sum(1 for trade in trades_log if trade['type'] == 'BUY')
    sell_trades = sum(1 for trade in trades_log if trade['type'] == 'SELL')

    # Max Drawdown Calculation
    portfolio_values = portfolio_history_df['total_value']
    if not portfolio_values.empty:
        peak = portfolio_values.expanding(min_periods=1).max()
        drawdown = (portfolio_values - peak) / peak # Drawdown as a decimal
        max_drawdown_value = drawdown.min() # This will be negative or zero
        max_drawdown_percentage = max_drawdown_value * 100 if not pd.isna(max_drawdown_value) else 0.0
        if pd.isna(max_drawdown_percentage) or np.isinf(max_drawdown_percentage): # Handle cases of single data point or other issues
             max_drawdown_percentage = 0.0
    else:
        max_drawdown_percentage = 0.0

    # Sharpe Ratio Calculation (assuming daily data if portfolio_history has daily entries)
    # For Sharpe, ensure there are enough data points for std dev calculation
    sharpe_ratio = 0.0
    if not portfolio_history_df.empty and len(portfolio_history_df) > 1:
        daily_returns = portfolio_history_df['total_value'].pct_change().dropna()
        if not daily_returns.empty and daily_returns.std() != 0:
            # Assuming risk_free_rate is annual, convert to period rate matching returns frequency
            # This simple example assumes returns are daily, and 252 trading days for annualization.
            # More robust: determine frequency from data.
            periods_in_year = 252 # Assuming daily data

            # Convert annual risk_free_rate to the period's rate
            # (1+annual_rfr)^(1/periods_in_year) - 1. If rfr=0, periodic_rfr=0.
            if risk_free_rate == 0:
                periodic_risk_free_rate = 0.0
            else:
                periodic_risk_free_rate = (1 + risk_free_rate)**(1/periods_in_year) - 1

            excess_returns = daily_returns - periodic_risk_free_rate

            # Check if excess_returns standard deviation is zero (e.g. flat returns)
            if excess_returns.std() != 0:
                sharpe_ratio_calc = (excess_returns.mean() / excess_returns.std()) * np.sqrt(periods_in_year)
                sharpe_ratio = sharpe_ratio_calc if not pd.isna(sharpe_ratio_calc) else 0.0
            else: # No volatility in excess returns
                sharpe_ratio = float('inf') if excess_returns.mean() > 0 else 0.0 # Or handle as undefined

    report = {
        "initial_capital": initial_capital,
        "final_portfolio_value": final_portfolio_value,
        "total_return_percentage": round(total_return_percentage, 2),
        "total_trades": total_trades,
        "buy_trades_count": buy_trades,
        "sell_trades_count": sell_trades,
        "max_drawdown_percentage": round(max_drawdown_percentage, 2),
        "sharpe_ratio": round(sharpe_ratio, 3) if not (pd.isna(sharpe_ratio) or np.isinf(sharpe_ratio)) else "N/A"
    }
    return report

if __name__ == '__main__':
    # Example Usage (for testing the reporter directly)
    print("Testing reporter module...")
    mock_portfolio_history = [
        {'timestamp': pd.Timestamp('2023-01-01'), 'total_value': 10000},
        {'timestamp': pd.Timestamp('2023-01-02'), 'total_value': 10050},
        {'timestamp': pd.Timestamp('2023-01-03'), 'total_value': 10020}, # Drawdown starts
        {'timestamp': pd.Timestamp('2023-01-04'), 'total_value': 10100}, # New peak
        {'timestamp': pd.Timestamp('2023-01-05'), 'total_value': 9900},  # Max drawdown here
        {'timestamp': pd.Timestamp('2023-01-06'), 'total_value': 10200},
    ]
    mock_trades = [
        {'type': 'BUY', 'cost': 500, 'commission': 0.5},
        {'type': 'SELL', 'proceeds': 520, 'commission': 0.52},
        {'type': 'BUY', 'cost': 300, 'commission': 0.3},
    ]
    initial_cap = 10000.0

    report_data = generate_performance_report(mock_portfolio_history, mock_trades, initial_cap, risk_free_rate=0.02)
    print("\n--- Mock Performance Report ---")
    if report_data:
        for key, value in report_data.items():
            print(f"{key.replace('_', ' ').title()}: {value}")
    else:
        print("Could not generate mock report.")

    # Test with empty history
    empty_history_report = generate_performance_report([], [], initial_cap)
    print("\n--- Mock Performance Report (Empty History) ---")
    if empty_history_report:
        for key, value in empty_history_report.items():
            print(f"{key.replace('_', ' ').title()}: {value}")

    # Test with single entry history
    single_entry_history = [{'timestamp': pd.Timestamp('2023-01-01'), 'total_value': 10000}]
    single_entry_report = generate_performance_report(single_entry_history, [], initial_cap)
    print("\n--- Mock Performance Report (Single Entry History) ---")
    if single_entry_report:
        for key, value in single_entry_report.items():
            print(f"{key.replace('_', ' ').title()}: {value}")

    # Test with history that would lead to NaN/inf sharpe
    flat_history = [
        {'timestamp': pd.Timestamp('2023-01-01'), 'total_value': 10000},
        {'timestamp': pd.Timestamp('2023-01-02'), 'total_value': 10000},
        {'timestamp': pd.Timestamp('2023-01-03'), 'total_value': 10000},
    ]
    flat_report = generate_performance_report(flat_history, [], initial_cap)
    print("\n--- Mock Performance Report (Flat History) ---")
    if flat_report:
        for key, value in flat_report.items():
            print(f"{key.replace('_', ' ').title()}: {value}")

    # Test with history that would lead to positive mean but zero std dev (sharpe inf)
    positive_flat_returns_history = [
        {'timestamp': pd.Timestamp('2023-01-01'), 'total_value': 10000},
        {'timestamp': pd.Timestamp('2023-01-02'), 'total_value': 10100}, # Positive return
        {'timestamp': pd.Timestamp('2023-01-03'), 'total_value': 10100}, # Zero return, std dev of returns is non-zero
    ] # Correction: std of daily_returns will be non-zero. Let's try with actual zero std for excess_returns
      # If risk_free_rate makes excess_returns non-zero mean but zero std (not typical)
      # More simply, if daily returns are constant and non-zero, this would cause issues if not handled.
      # The current code handles daily_returns.std() == 0, but excess_returns.std() == 0 is the one for Sharpe.
      # If daily_returns.std() is non-zero, but excess_returns.std() is zero (e.g. returns perfectly track risk_free_rate),
      # then sharpe could be inf if mean(excess_returns) > 0.

    # Test with 0 initial capital
    zero_capital_report = generate_performance_report(mock_portfolio_history, mock_trades, 0.0)
    print("\n--- Mock Performance Report (Zero Initial Capital) ---")
    if zero_capital_report:
        for key, value in zero_capital_report.items():
            print(f"{key.replace('_', ' ').title()}: {value}")
