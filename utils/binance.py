import time

from pprint import pprint
from dataclasses import dataclass
from typing import Any, Dict

import ccxt
import sqlite3

from utils.constants import BINANCE_UNI_API_KEY, BINANCE_UNI_SECRET





@dataclass
class Coin:
    asset: str
    total_wallet_balance: float
    cross_margin_asset: float
    cross_margin_borrowed: float
    cross_margin_free: float
    cross_margin_interest: float
    cross_margin_locked: float
    um_wallet_balance: float
    um_unrealized_pnl: float
    cm_wallet_balance: float
    cm_unrealized_pnl: float
    price_in_usdt: float

@dataclass
class CmPosition:
    symbol: str
    position_amt: float
    entry_price: float
    mark_price: float
    un_realized_profit: float
    liquidation_price: float
    leverage: float
    position_side: str
    max_qty: float
    notional_value: float
    break_even_price: float
    contract_size: float

@dataclass
class UmPosition:
    symbol: str
    position_amt: float
    entry_price: float
    mark_price: float
    un_realized_profit: float
    liquidation_price: float
    leverage: float
    position_side: str
    max_notional_value: float
    notional: float
    break_even_price: float

def init_exchange(config: Dict[str, Any]) -> ccxt.Exchange:
    exchange_class = getattr(ccxt, config['exchange_id'])
    exchange = exchange_class(config)
    exchange.set_sandbox_mode(config.get('sandbox', False))
    return exchange

def init_db(user):
    conn = sqlite3.connect('trading_data.db')
    c = conn.cursor()
    c.execute(f'''CREATE TABLE IF NOT EXISTS {user}_total_equity
                 (timestamp INTEGER, equity REAL)''')
    c.execute(f'''CREATE TABLE IF NOT EXISTS {user}_coin_balance
                 (asset TEXT PRIMARY KEY, total_balance REAL, borrowed REAL ,um_balance REAL, cm_balance REAL)''')
    c.execute(f'''CREATE TABLE IF NOT EXISTS {user}_cm_positions
                 (symbol TEXT PRIMARY KEY, contracts REAL, unrealized_pnl REAL, notional REAL)''')
    c.execute(f'''CREATE TABLE IF NOT EXISTS {user}_um_positions
                 (symbol TEXT PRIMARY KEY, contracts REAL, unrealized_pnl REAL, notional REAL)''')   
    conn.commit()
    conn.close()

def get_db_connection():
    return sqlite3.connect('trading_data.db')

def parse_future_symbol(symbol: str) -> str:
    part = symbol.split('_')
    expiry_date = part[-1]
    base = part[0][:-3]
    quote = part[0][-3:]
    return f"{base}/{quote}:{base}-{expiry_date}"

def fetch_account_balance(exchange: ccxt.binance) -> Dict[str, Coin]:
    
    # [
    #     {
    #         "asset": "USDT",    // asset name
    #         "totalWalletBalance": "122607.35137903", // wallet balance =  cross margin free + cross margin locked + UM wallet balance + CM wallet balance
    #         "crossMarginAsset": "92.27530794", // crossMarginAsset = crossMarginFree + crossMarginLocked
    #         "crossMarginBorrowed": "10.00000000", // principal of cross margin
    #         "crossMarginFree": "100.00000000", // free asset of cross margin
    #         "crossMarginInterest": "0.72469206", // interest of cross margin
    #         "crossMarginLocked": "3.00000000", //lock asset of cross margin
    #         "umWalletBalance": "0.00000000",  // wallet balance of um
    #         "umUnrealizedPNL": "23.72469206",     // unrealized profit of um 
    #         "cmWalletBalance": "23.72469206",       // wallet balance of cm
    #         "cmUnrealizedPNL": "",    // unrealized profit of cm
    #         "updateTime": 1617939110373
    #     }
    # ]
    coins = {}
    res = exchange.papi_get_balance()
    symbols = [f"{coin['asset']}/USDT" for coin in res if float(coin["totalWalletBalance"]) != 0 and coin["asset"] != "USDT"]
    tickers = exchange.fetch_tickers(symbols)

    for coin in res:
        if float(coin["totalWalletBalance"]) != 0:
            coins[coin["asset"]] = Coin(
                asset=coin["asset"],
                total_wallet_balance=float(coin["totalWalletBalance"]),
                cross_margin_asset=float(coin["crossMarginAsset"]),
                cross_margin_borrowed=float(coin["crossMarginBorrowed"]),
                cross_margin_free=float(coin["crossMarginFree"]),
                cross_margin_interest=float(coin["crossMarginInterest"]),
                cross_margin_locked=float(coin["crossMarginLocked"]),
                um_wallet_balance=float(coin["umWalletBalance"]),
                um_unrealized_pnl=float(coin["umUnrealizedPNL"]),
                cm_wallet_balance=float(coin["cmWalletBalance"]),
                cm_unrealized_pnl=float(coin["cmUnrealizedPNL"]),
                price_in_usdt=tickers[f"{coin['asset']}/USDT"]['last'] if coin['asset'] != 'USDT' else 1
            )
    return coins

def fetch_total_equity(coins: Dict[str, Coin]) -> float:
    return sum([(coin.total_wallet_balance+coin.cm_unrealized_pnl+coin.um_unrealized_pnl) * coin.price_in_usdt for coin in coins.values()])

def fetch_cm_position(exchange: ccxt.binance) -> Dict[str, CmPosition]:
    position = {}
    res = exchange.papi_get_cm_positionrisk()
    exchange.load_markets()
    for pos in res:
        position[pos['symbol']] = CmPosition(
            symbol=pos['symbol'],
            position_amt=float(pos['positionAmt']),
            entry_price=float(pos['entryPrice']),
            mark_price=float(pos['markPrice']),
            un_realized_profit=float(pos['unRealizedProfit']),
            liquidation_price=float(pos['liquidationPrice']),
            leverage=float(pos['leverage']),
            position_side=pos['positionSide'],
            max_qty=float(pos['maxQty']),
            notional_value=float(pos['notionalValue']),
            break_even_price=float(pos['breakEvenPrice']),
            contract_size=float(exchange.markets[parse_future_symbol(pos['symbol'])]['info']['contractSize'])
        )
    return position

def fetch_um_position(exchange: ccxt.binance) -> Dict[str, UmPosition]:
    position = {}
    res = exchange.papi_get_um_positionrisk()
    for pos in res:
        position[pos['symbol']] = UmPosition(
            symbol=pos['symbol'],
            position_amt=float(pos['positionAmt']),
            entry_price=float(pos['entryPrice']),
            mark_price=float(pos['markPrice']),
            un_realized_profit=float(pos['unRealizedProfit']),
            liquidation_price=float(pos['liquidationPrice']),
            leverage=float(pos['leverage']),
            position_side=pos['positionSide'],
            max_notional_value=float(pos['maxNotionalValue']),
            notional=float(pos['notional']),
            break_even_price=float(pos['breakEvenPrice'])
        )
    return position

def update_total_equity_and_balance(exchange: ccxt.binance, user):
    coins = fetch_account_balance(exchange)
    total_equity = fetch_total_equity(coins)
    conn = get_db_connection()
    c = conn.cursor()
    
    # Insert total equity
    c.execute(f"INSERT INTO {user}_total_equity VALUES (?, ?)", (int(time.time()), total_equity))
    
    # Get existing coins in the database
    c.execute(f"SELECT asset FROM {user}_coin_balance")
    existing_coins = set(row[0] for row in c.fetchall())
    
    # Update or insert coin balances
    current_coins = set()
    for coin in coins.values():
        c.execute(f"INSERT OR REPLACE INTO {user}_coin_balance VALUES (?, ?, ?, ?, ?)", 
                  (coin.asset, coin.total_wallet_balance, coin.cross_margin_borrowed, 
                   coin.um_wallet_balance, coin.cm_wallet_balance))
        current_coins.add(coin.asset)
    
    # Delete coins that no longer exist
    coins_to_delete = existing_coins - current_coins
    for coin in coins_to_delete:
        c.execute(f"DELETE FROM {user}_coin_balance WHERE asset = ?", (coin,))
        print(f"Deleted balance for {coin}")
    
    conn.commit()
    conn.close()

def update_positions(exchange: ccxt.binance, user):
    cm_positions = fetch_cm_position(exchange)
    um_positions = fetch_um_position(exchange)
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
        unrealized_pnl = pos.un_realized_profit
        notional = abs(contracts * pos.contract_size)
        c.execute(f"INSERT OR REPLACE INTO {user}_cm_positions VALUES (?, ?, ?, ?)", 
                  (symbol, contracts, unrealized_pnl, notional))
        current_cm_symbols.add(symbol)
    
    # Delete CM positions that no longer exist
    cm_symbols_to_delete = existing_cm_symbols - current_cm_symbols
    for symbol in cm_symbols_to_delete:
        c.execute(f"DELETE FROM {user}_cm_positions WHERE symbol = ?", (symbol,))
        print(f"Deleted CM position for {symbol}")
    
    # Get existing UM positions in the database
    c.execute(f"SELECT symbol FROM {user}_um_positions")
    existing_um_symbols = set(row[0] for row in c.fetchall())
    
    # Update or insert UM positions
    current_um_symbols = set()
    for pos in um_positions.values():
        symbol = pos.symbol
        contracts = pos.position_amt
        unrealized_pnl = pos.un_realized_profit
        notional = pos.notional
        c.execute(f"INSERT OR REPLACE INTO {user}_um_positions VALUES (?, ?, ?, ?)", 
                  (symbol, contracts, unrealized_pnl, notional))
        current_um_symbols.add(symbol)
    
    # Delete UM positions that no longer exist
    um_symbols_to_delete = existing_um_symbols - current_um_symbols
    for symbol in um_symbols_to_delete:
        c.execute(f"DELETE FROM {user}_um_positions WHERE symbol = ?", (symbol,))
        print(f"Deleted UM position for {symbol}")
    
    conn.commit()
    conn.close()

def update_data(exchange: ccxt.Exchange, user):
    update_total_equity_and_balance(exchange, user)
    update_positions(exchange, user)
    print("Data updated")
