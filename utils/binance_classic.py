import ccxt
from utils.binance import init_exchange
from utils.constants import CONFIG
from dataclasses import dataclass
import sqlite3
from typing import Dict, List
import time


def get_db_connection():
    return sqlite3.connect("trading_data.db")


@dataclass
class SpotCoin:
    asset: str
    free: float
    locked: float
    price_in_usdt: float

    @property
    def total(self):
        return self.free + self.locked


@dataclass
class UmCoin:
    asset: str
    unrealizedProfit: float
    walletBalance: float
    price_in_usdt: float

    @property
    def total(self):
        return self.walletBalance + self.unrealizedProfit


@dataclass
class CmCoin:
    asset: str
    unrealizedProfit: float
    walletBalance: float
    price_in_usdt: float

    @property
    def total(self):
        return self.walletBalance + self.unrealizedProfit


@dataclass
class UmPosition:
    symbol: str
    position_amt: float
    unrealized_profit: float
    notional: float


@dataclass
class CmPosition:
    symbol: str
    position_amt: float
    unrealized_profit: float


def get_valid_usdt_symbols(exchange: ccxt.binance):
    valid_symbols = []
    market = exchange.load_markets()
    for symbol, mkt in market.items():
        if mkt["spot"] and mkt["active"] and mkt["quote"] == "USDT":
            valid_symbols.append(symbol)
    return valid_symbols


def init_db(user):
    conn = sqlite3.connect("trading_data.db")
    c = conn.cursor()
    c.execute(f"""CREATE TABLE IF NOT EXISTS {user}_total_equity
                 (timestamp INTEGER, spot_equity REAL, um_equity REAL, cm_equity REAL)""")
    c.execute(f"""CREATE TABLE IF NOT EXISTS {user}_coin_balance
                 (asset TEXT PRIMARY KEY, total_balance REAL)""")
    c.execute(f"""CREATE TABLE IF NOT EXISTS {user}_cm_coin_balance
                 (asset TEXT PRIMARY KEY, wallet_balance REAL, unrealized_profit REAL, total_balance REAL)""")
    c.execute(f"""CREATE TABLE IF NOT EXISTS {user}_um_coin_balance
                 (asset TEXT PRIMARY KEY, wallet_balance REAL, unrealized_profit REAL, total_balance REAL)""")

    c.execute(f"""CREATE TABLE IF NOT EXISTS {user}_cm_positions
                 (symbol TEXT PRIMARY KEY, contracts REAL, unrealized_pnl REAL)""")
    c.execute(f"""CREATE TABLE IF NOT EXISTS {user}_um_positions
                 (symbol TEXT PRIMARY KEY, contracts REAL, unrealized_pnl REAL)""")
    conn.commit()
    conn.close()


def query_cm_account_info(exchange: ccxt.binance, valid_usdt_symbols: List[str]):
    res = exchange.dapiprivate_get_account()
    positions = {}
    for pos in res["positions"]:
        if float(pos["positionAmt"]) != 0:
            positions[pos["symbol"]] = CmPosition(
                symbol=pos["symbol"],
                position_amt=float(pos["positionAmt"]),
                unrealized_profit=float(pos["unrealizedProfit"]),
            )

    cm_coins = {}
    symbols = [
        f"{coin['asset']}/USDT"
        for coin in res["assets"]
        if coin["asset"] != "USDT" and float(coin["walletBalance"]) != 0
    ]
    symbols = [s for s in symbols if s in valid_usdt_symbols]
    if symbols:
        tickers = exchange.fetch_tickers(symbols)
    else:
        tickers = {}

    for coin in res["assets"]:
        if float(coin["walletBalance"]) != 0:
            if f"{coin['asset']}/USDT" in tickers:
                price = tickers[f"{coin['asset']}/USDT"]["last"]
            elif coin["asset"] == "USDT":
                price = 1
            else:
                price = 0

            cm_coins[coin["asset"]] = CmCoin(
                asset=coin["asset"],
                unrealizedProfit=float(coin["unrealizedProfit"]),
                walletBalance=float(coin["walletBalance"]),
                price_in_usdt=price,
            )

    return {"position": positions, "coins": cm_coins}


def query_spot_account_info(exchange: ccxt.binance, valid_usdt_symbols: List[str]):
    """
        {'accountType': 'SPOT',
    'balances': [{'asset': 'BTC', 'free': '0.00000000', 'locked': '0.00000000'},
                {'asset': 'LTC', 'free': '0.00000000', 'locked': '0.00000000'},
                {'asset': 'ETH', 'free': '0.00000000', 'locked': '0.00000000'},
                {'asset': 'NEO', 'free': '0.00000000', 'locked': '0.00000000'},
                {'asset': 'BNB', 'free': '0.00000000', 'locked': '0.00000000'},
                {'asset': 'ACX', 'free': '0.00000000', 'locked': '0.00000000'},
                {'asset': 'ORCA', 'free': '0.00000000', 'locked': '0.00000000'},
                {'asset': 'MOVE', 'free': '0.00000000', 'locked': '0.00000000'},
                {'asset': 'ME', 'free': '0.00000000', 'locked': '0.00000000'}],
    'brokered': False,
    'buyerCommission': '0',
    'canDeposit': True,
    'canTrade': True,
    'canWithdraw': True,
    'commissionRates': {'buyer': '0.00000000',
                        'maker': '0.00090000',
                        'seller': '0.00000000',
                        'taker': '0.00100000'},
    'makerCommission': '9',
    'permissions': ['SPOT'],
    'preventSor': False,
    'requireSelfTradePrevention': False,
    'sellerCommission': '0',
    'takerCommission': '10',
    'uid': '1041165650',
    'updateTime': '1733845763670'}
    """
    coins = {}
    res = exchange.private_get_account()
    balances = res["balances"]
    symbols = [
        f"{coin['asset']}/USDT"
        for coin in balances
        if (float(coin["free"]) != 0 or float(coin["locked"]) != 0)
        and coin["asset"] != "USDT"
    ]
    # symbols = [f"{coin['asset']}/USDT" for coin in balances if coin["asset"] != "USDT"]
    symbols = [s for s in symbols if s in valid_usdt_symbols]
    if symbols:
        tickers = exchange.fetch_tickers(symbols)
    else:
        tickers = {}

    for coin in balances:
        if float(coin["free"]) != 0 or float(coin["locked"]) != 0:
            usdt_symbol = f"{coin['asset']}/USDT"

            if usdt_symbol in tickers:
                price = tickers[usdt_symbol]["last"]
            elif coin["asset"] == "USDT":
                price = 1
            else:
                price = 0

            coins[coin["asset"]] = SpotCoin(
                asset=coin["asset"],
                free=float(coin["free"]),
                locked=float(coin["locked"]),
                price_in_usdt=price,
            )
    return coins


def query_um_account_info(exchange: ccxt.binance, valid_usdt_symbols: List[str]):
    """
       {'assets': [{'asset': 'FDUSD',
                'availableBalance': '181130.03317916',
                'crossUnPnl': '0.00000000',
                'crossWalletBalance': '0.00000000',
                'initialMargin': '0.00000000',
                'maintMargin': '0.00000000',
                'marginBalance': '0.00000000',
                'maxWithdrawAmount': '0.00000000',
                'openOrderInitialMargin': '0.00000000',
                'positionInitialMargin': '0.00000000',
                'unrealizedProfit': '0.00000000',
                'updateTime': '0',
                'walletBalance': '0.00000000'},
               {'asset': 'BFUSD',
                'availableBalance': '182701.70208554',
                'crossUnPnl': '0.00000000',
                'crossWalletBalance': '0.00000000',
                'initialMargin': '0.00000000',
                'maintMargin': '0.00000000',
                'marginBalance': '0.00000000',
                'maxWithdrawAmount': '0.00000000',
                'openOrderInitialMargin': '0.00000000',
                'positionInitialMargin': '0.00000000',
                'unrealizedProfit': '0.00000000',
                'updateTime': '0',
                'walletBalance': '0.00000000'},
               {'asset': 'BNB',
                'availableBalance': '256.13216080',
                'crossUnPnl': '0.00000000',
                'crossWalletBalance': '0.00000000',
                'initialMargin': '0.00000000',
                'maintMargin': '0.00000000',
                'marginBalance': '0.00000000',
                'maxWithdrawAmount': '0.00000000',
                'openOrderInitialMargin': '0.00000000',
                'positionInitialMargin': '0.00000000',
                'unrealizedProfit': '0.00000000',
                'updateTime': '0',
                'walletBalance': '0.00000000'},
               {'asset': 'ETH',
                'availableBalance': '47.50363130',
                'crossUnPnl': '0.00000000',
                'crossWalletBalance': '0.00000000',
                'initialMargin': '0.00000000',
                'maintMargin': '0.00000000',
                'marginBalance': '0.00000000',
                'maxWithdrawAmount': '0.00000000',
                'openOrderInitialMargin': '0.00000000',
                'positionInitialMargin': '0.00000000',
                'unrealizedProfit': '0.00000000',
                'updateTime': '0',
                'walletBalance': '0.00000000'},
               {'asset': 'BTC',
                'availableBalance': '1.78893278',
                'crossUnPnl': '0.00000000',
                'crossWalletBalance': '0.00000000',
                'initialMargin': '0.00000000',
                'maintMargin': '0.00000000',
                'marginBalance': '0.00000000',
                'maxWithdrawAmount': '0.00000000',
                'openOrderInitialMargin': '0.00000000',
                'positionInitialMargin': '0.00000000',
                'unrealizedProfit': '0.00000000',
                'updateTime': '0',
                'walletBalance': '0.00000000'},
               {'asset': 'USDT',
                'availableBalance': '182866.11769824',
                'crossUnPnl': '58.90890002',
                'crossWalletBalance': '199872.30969187',
                'initialMargin': '17021.44801107',
                'maintMargin': '1226.60853700',
                'marginBalance': '199931.21859189',
                'maxWithdrawAmount': '182866.11769824',
                'openOrderInitialMargin': '0.00000000',
                'positionInitialMargin': '17021.44801107',
                'unrealizedProfit': '58.90890002',
                'updateTime': '1733890825855',
                'walletBalance': '199872.30969187'},
               {'asset': 'USDC',
                'availableBalance': '182918.96800826',
                'crossUnPnl': '0.00000000',
                'crossWalletBalance': '0.00000000',
                'initialMargin': '0.00000000',
                'maintMargin': '0.00000000',
                'marginBalance': '0.00000000',
                'maxWithdrawAmount': '0.00000000',
                'openOrderInitialMargin': '0.00000000',
                'positionInitialMargin': '0.00000000',
                'unrealizedProfit': '0.00000000',
                'updateTime': '0',
                'walletBalance': '0.00000000'}],
    'availableBalance': '182918.36803405',
    'maxWithdrawAmount': '182918.36803405',
    'positions': [{'initialMargin': '17021.44801107',
                   'isolatedMargin': '0',
                   'isolatedWallet': '0',
                   'maintMargin': '1226.60853700',
                   'notional': '-255321.70740000',
                   'positionAmt': '-2.622',
                   'positionSide': 'BOTH',
                   'symbol': 'BTCUSDT',
                   'unrealizedProfit': '58.90890002',
                   'updateTime': '1733890825855'}],
    'totalCrossUnPnl': '58.91394793',
    'totalCrossWalletBalance': '199889.43675008',
    'totalInitialMargin': '17026.31154941',
    'totalMaintMargin': '1226.95901585',
    'totalMarginBalance': '199948.35069801',
    'totalOpenOrderInitialMargin': '0.00000000',
    'totalPositionInitialMargin': '17026.31154941',
    'totalUnrealizedProfit': '58.91394793',
    'totalWalletBalance': '199889.43675008'}
    """
    res = exchange.fapiPrivateV3GetAccount()

    position = {}
    for pos in res["positions"]:
        if float(pos["positionAmt"]) != 0:
            position[pos["symbol"]] = UmPosition(
                symbol=pos["symbol"],
                position_amt=float(pos["positionAmt"]),
                unrealized_profit=float(pos["unrealizedProfit"]),
                notional=float(pos["notional"]),
            )

    total_wallet_balance = res["totalWalletBalance"]
    total_unrealized_profit = res["totalUnrealizedProfit"]

    um_coins = {}

    symbols = [
        f"{coin['asset']}/USDT"
        for coin in res["assets"]
        if coin["asset"] != "USDT" and float(coin["walletBalance"]) != 0
    ]
    symbols = [s for s in symbols if s in valid_usdt_symbols]
    if symbols:
        tickers = exchange.fetch_tickers(symbols)
    else:
        tickers = {}

    for coin in res["assets"]:
        if float(coin["walletBalance"]) != 0:
            if f"{coin['asset']}/USDT" in tickers:
                price = tickers[f"{coin['asset']}/USDT"]["last"]
            elif coin["asset"] == "USDT":
                price = 1
            else:
                price = 0

            um_coins[coin["asset"]] = UmCoin(
                asset=coin["asset"],
                unrealizedProfit=float(coin["unrealizedProfit"]),
                walletBalance=float(coin["walletBalance"]),
                price_in_usdt=price,
            )

    return {
        "total_wallet_balance": float(total_wallet_balance),
        "total_unrealized_profit": float(total_unrealized_profit),
        "position": position,
        "coins": um_coins,
    }


def update_positions(
    user: str, um_positions: Dict[str, UmPosition], cm_positions: Dict[str, CmPosition]
):
    conn = get_db_connection()
    c = conn.cursor()

    # Get existing CM positions in the database
    c.execute(f"SELECT symbol FROM {user}_cm_positions")
    existing_cm_symbols = set(row[0] for row in c.fetchall())

    # Update or insert CM positions
    current_cm_symbols = set()
    for pos in cm_positions.values():
        symbol = pos.symbol
        contracts = pos.position_amt
        unrealized_pnl = pos.unrealized_profit

        c.execute(
            f"INSERT OR REPLACE INTO {user}_cm_positions VALUES (?, ?, ?)",
            (symbol, contracts, unrealized_pnl),
        )
        current_cm_symbols.add(symbol)

    # Delete CM positions that no longer exist
    cm_symbols_to_delete = existing_cm_symbols - current_cm_symbols
    for symbol in cm_symbols_to_delete:
        c.execute(f"DELETE FROM {user}_cm_positions WHERE symbol = ?", (symbol,))
        print(f"Deleted CM position for {symbol}")

    # Get existing UM positions in the database
    c.execute(f"SELECT symbol FROM {user}_um_positions")
    existing_um_symbols = set(row[0] for row in c.fetchall())
    current_um_symbols = set()

    for pos in um_positions.values():
        symbol = pos.symbol
        contracts = pos.position_amt
        unrealized_pnl = pos.unrealized_profit
        c.execute(
            f"INSERT OR REPLACE INTO {user}_um_positions VALUES (?, ?, ?)",
            (symbol, contracts, unrealized_pnl),
        )
        current_um_symbols.add(symbol)

    # Delete UM positions that no longer exist
    um_symbols_to_delete = existing_um_symbols - current_um_symbols
    for symbol in um_symbols_to_delete:
        c.execute(f"DELETE FROM {user}_um_positions WHERE symbol = ?", (symbol,))
        print(f"Deleted UM position for {symbol}")

    conn.commit()
    conn.close()


def fetch_spot_equity(coins: Dict[str, SpotCoin]):
    if not coins:
        return 0
    return sum([coin.total * coin.price_in_usdt for coin in coins.values()])


def fetch_cm_equity(coins: Dict[str, CmCoin]):
    if not coins:
        return 0
    return sum(
        [
            (coin.walletBalance + coin.unrealizedProfit) * coin.price_in_usdt
            for coin in coins.values()
        ]
    )


def update_total_equity_and_balance(
    user,
    coins: Dict[str, SpotCoin],
    cm_coins: Dict[str, CmCoin],
    um_coins: Dict[str, UmCoin],
    spot_equity: float,
    um_equity: float,
    cm_equity: float,
):
    conn = get_db_connection()
    c = conn.cursor()

    # Insert total equity
    c.execute(
        f"INSERT INTO {user}_total_equity VALUES (?, ?, ?, ?)",
        (int(time.time()), spot_equity, um_equity, cm_equity),
    )

    # Get existing coins in the database
    c.execute(f"SELECT asset FROM {user}_coin_balance")
    existing_coins = set(row[0] for row in c.fetchall())

    # Update or insert coin balances
    current_coins = set()
    for coin in coins.values():
        c.execute(
            f"INSERT OR REPLACE INTO {user}_coin_balance VALUES (?, ?)",
            (coin.asset, coin.total),
        )
        current_coins.add(coin.asset)

    # Delete coins that no longer exist
    coins_to_delete = existing_coins - current_coins
    for coin in coins_to_delete:
        c.execute(f"DELETE FROM {user}_coin_balance WHERE asset = ?", (coin,))
        print(f"Deleted balance for {coin}")

    c.execute(f"SELECT asset FROM {user}_cm_coin_balance")
    existing_cm_coins = set(row[0] for row in c.fetchall())

    current_cm_coins = set()
    for coin in cm_coins.values():
        c.execute(
            f"INSERT OR REPLACE INTO {user}_cm_coin_balance VALUES (?, ?, ?, ?)",
            (coin.asset, coin.walletBalance, coin.unrealizedProfit, coin.total),
        )
        current_cm_coins.add(coin.asset)

    cm_coins_to_delete = existing_cm_coins - current_cm_coins
    for coin in cm_coins_to_delete:
        c.execute(f"DELETE FROM {user}_cm_coin_balance WHERE asset = ?", (coin,))
        print(f"Deleted balance for {coin}")

    c.execute(f"SELECT asset FROM {user}_um_coin_balance")
    existing_um_coins = set(row[0] for row in c.fetchall())

    current_um_coins = set()
    for coin in um_coins.values():
        c.execute(
            f"INSERT OR REPLACE INTO {user}_um_coin_balance VALUES (?, ?, ?, ?)",
            (coin.asset, coin.walletBalance, coin.unrealizedProfit, coin.total),
        )
        current_um_coins.add(coin.asset)

    um_coins_to_delete = existing_um_coins - current_um_coins
    for coin in um_coins_to_delete:
        c.execute(f"DELETE FROM {user}_um_coin_balance WHERE asset = ?", (coin,))
        print(f"Deleted balance for {coin}")

    conn.commit()
    conn.close()


def update_data(user, exchange: ccxt.binance, valid_usdt_symbols: List[str]):
    coins = query_spot_account_info(exchange, valid_usdt_symbols)
    spot_equity = fetch_spot_equity(coins)

    res = query_um_account_info(exchange, valid_usdt_symbols)
    um_equity = res["total_unrealized_profit"] + res["total_wallet_balance"]
    um_coins = res["coins"]

    um_positions = res["position"]
    res = query_cm_account_info(exchange, valid_usdt_symbols)
    cm_positions = res["position"]
    cm_coins = res["coins"]
    cm_equity = fetch_cm_equity(cm_coins)

    update_total_equity_and_balance(
        user, coins, cm_coins, um_coins, spot_equity, um_equity, cm_equity
    )
    update_positions(user, um_positions, cm_positions)
    print("Data updated")


def main():
    account = "binance_strategy_9"

    BINANCE_UNI_API_KEY = CONFIG[account]["API_KEY"]
    BINANCE_UNI_SECRET = CONFIG[account]["SECRET"]

    config = {
        "exchange_id": "binance",
        "sandbox": False,
        "apiKey": BINANCE_UNI_API_KEY,
        "secret": BINANCE_UNI_SECRET,
        "enableRateLimit": False,
    }
    exchange = init_exchange(config)

    init_db(account)
    valid_usdt_symbols = get_valid_usdt_symbols(exchange)
    update_data(account, exchange, valid_usdt_symbols)


if __name__ == "__main__":
    main()
