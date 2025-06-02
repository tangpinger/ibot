# Owl Trading Bot

Automated trading bot for OKX based on a specified N-day breakout strategy.

## Disclaimer

Trading cryptocurrencies involves significant risk. This bot is provided for educational and experimental purposes only. Use at your own risk. The authors are not responsible for any financial losses.

## Features (Planned)

- Automated trading on OKX.
- N-day high breakout strategy.
- Specific trading windows (Monday, Tuesday, Friday close for entry).
- Dry-run mode for testing without real funds.
- Backtesting engine to evaluate strategy performance on historical data.
- Modular design.

## Setup

1.  **Clone the repository:**
    \`\`\`bash
    git clone <repository_url>
    cd owl-trading-bot
    \`\`\`

2.  **Install Poetry:**
    Follow the instructions on the [official Poetry website](https://python-poetry.org/docs/#installation).

3.  **Install dependencies:**
    \`\`\`bash
    poetry install
    \`\`\`

4.  **Configure the bot:**
    Copy \`config.example.toml\` to \`config.toml\`:
    \`\`\`bash
    cp config.example.toml config.toml
    \`\`\`
    Edit \`config.toml\` with your OKX API keys (if not in dry-run), trading parameters, and other settings.
    **IMPORTANT: Ensure your API keys have the necessary permissions for trading if you intend to run in live mode.**

    **Proxy Configuration:**
    The `DataFetcher` module supports connecting to exchanges via a proxy. This is useful for users in restricted network environments or those who wish to route their traffic through specific IP addresses.
    - Supported proxy types include SOCKS5, SOCKS5h, HTTP, and others supported by the underlying `requests` and `aiohttp` libraries via the URL scheme.
    - To configure a proxy, edit the `[proxy]` section in your `config.toml` file. Refer to `config.example.toml` for examples.
    - The `proxy_url` should be in a format like `socks5h://user:pass@your_proxy_host:port` (for SOCKS5h with authentication) or `http://your_proxy_host:port`.
    - The `proxy_type` can also be specified, but often the URL scheme (e.g., `socks5h://`) is sufficient.

## Usage (Planned)

**Dry-Run Mode:**
\`\`\`bash
poetry run python owl/main.py
\`\`\`

**Backtesting:**
(Command to be defined)

## Modules

-   **Data Fetcher:** Fetches market data from OKX.
-   **Signal Generator:** Generates trading signals based on the strategy.
-   **Order Executor:** Executes trades on OKX (or simulates in dry-run).
-   **Position Manager:** Manages current trading positions.
-   **Scheduler:** Schedules tasks like data fetching and trade execution.
-   **Config Manager:** Manages bot configurations.
-   **Logging:** Logs bot activities and errors.
-   **Backtesting Engine:** Simulates strategy on historical data.
-   **Analytics & Reporting:** Analyzes and reports backtesting results.

## Strategy

The bot implements the following strategy:
"N-day high point breakout after, buy at the close, hold for one trading day and sell before the next day's open."
- Buy signals are only considered on Fridays, Mondays, and Tuesdays near market close (4 PM Beijing Time).
- Sell orders are placed before market open (10 AM Beijing Time) on the day after the holding period.

## Development

(Details about contributing, running tests, etc.)
