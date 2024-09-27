# Data Updater

## Project Overview

Data Updater is a tool for fetching user data from the exchange and updating a database. This project utilizes the APScheduler library for scheduling tasks to periodically update users' coin balances and position information.

## Dependencies

- Python 3.x
- `ccxt` library
- `apscheduler` library
- Database support (e.g., SQLite)

## Installation

1. Clone the project to your local machine:

   ```bash
   git clone <repository_url>
   cd <project_directory>
   ```

2. Install the required dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Configure your API keys and other settings. Please set the `CONFIG` dictionary in the `utils/constants.py` file.

## Usage

1. Configure your Bybit API keys in the `user1.py` file:

   ```python
   BYBIT_API_KEY_1 = CONFIG['bybit']['API_KEY']
   BYBIT_SECRET_1 = CONFIG['bybit']['SECRET']
   ```

2. Run the script:

   ```bash
   python user1.py
   ```

   This script will start a scheduler that updates the user's coin balances and position information every 60 seconds.

## Features

- Fetch user coin balances from the Bybit exchange.
- Retrieve user position information from the Bybit exchange.
- Store the fetched data in a database.
- Periodically update data to ensure real-time information.

## Contributing

Contributions are welcome! Please submit issues or pull requests.

