# Data Updater

Data Updater is a tool for fetching user data from exchanges and updating a database. This project uses the APScheduler library to periodically update user coin balances and position information.

## Project Overview

- Supports multiple exchanges: Bybit and Binance
- Uses SQLite database for data storage
- Automatically updates user data periodically
- Supports multiple user accounts

## Dependencies

- Python 3.x
- `ccxt` library
- `apscheduler` library
- SQLite database

## Quick Start

1. Clone the project:

```bash
git clone <repository_url>
cd <project_directory>
```

2. Install required dependencies:

```bash
pip install -r requirements.txt
```

3. Configure API keys:

Set up your API keys in the `.keys/config.cfg` file.

4. Run the scripts:

For Bybit users:

```bash
python user1.py
```

For Binance users:

```bash
python user3.py
```

These scripts will start a scheduler that periodically updates the user's coin balances and position information.

## API

The main functionalities are encapsulated in the `utils/bybit.py` and `utils/binance.py` files. Here are the key functions:

### Bybit

- `init_exchange(config)`: Initializes the exchange connection.
- `fetch_total_equity(exchange)`: Retrieves the total equity.
- `fetch_coin_balance(exchange)`: Fetches coin balances.
- `fetch_positions(exchange)`: Gets position information.

### Binance

- `fetch_account_balance(exchange)`: Retrieves account balance information.
- `fetch_total_equity(coins)`: Calculates the total equity.
- `fetch_cm_position(exchange)`: Fetches contract market positions.
- `fetch_um_position(exchange)`: Retrieves spot market positions.

## Data Update

The data update process is encapsulated in the `update_data` function for both Bybit and Binance. This function updates total equity, coin balances, and position information.

## Contributing

Issues and pull requests are welcome.
