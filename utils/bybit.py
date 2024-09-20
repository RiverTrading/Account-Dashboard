import ccxt
import sqlite3
import time
from typing import Dict, Any, List, Tuple
from dataclasses import dataclass
from apscheduler.schedulers.blocking import BlockingScheduler

@dataclass
class Position:
    symbol: str
    contracts: float
    unrealized_pnl: float
    notional: float

def init_exchange(config: Dict[str, Any]) -> ccxt.Exchange:
    exchange_class = getattr(ccxt, config['exchange_id'])
    exchange = exchange_class(config)
    exchange.set_sandbox_mode(config.get('sandbox', False))
    return exchange

def fetch_total_equity(exchange: ccxt.Exchange) -> float:
    balance = exchange.fetch_balance()
    total_equity = balance['info']['result']['list'][0]['totalEquity']
    return float(total_equity)

def fetch_coin_balance(exchange: ccxt.Exchange) -> Dict[str, float]:
    balance = exchange.fetch_balance()
    coin_balance = balance['free']
    return coin_balance

def fetch_positions(exchange: ccxt.Exchange) -> Dict[str, Position]:
    positions = {}
    res = exchange.fetch_positions()
    for pos in res:
        symbol = pos['symbol']
        side = pos['side']
        notional = pos['notional']
        if side == 'long':
            contracts = pos['contracts']
        else:
            contracts = -pos['contracts']
        unrealized_pnl = pos['unrealizedPnl']
        position = Position(symbol, contracts, unrealized_pnl, notional)
        positions[symbol] = position
    return positions

def init_db(user):
    conn = sqlite3.connect('trading_data.db')
    c = conn.cursor()
    c.execute(f'''CREATE TABLE IF NOT EXISTS {user}_total_equity
                 (timestamp INTEGER, equity REAL)''')
    c.execute(f'''CREATE TABLE IF NOT EXISTS {user}_coin_balance
                 (coin TEXT PRIMARY KEY, balance REAL)''')
    c.execute(f'''CREATE TABLE IF NOT EXISTS {user}_positions
                 (symbol TEXT PRIMARY KEY, contracts REAL, unrealized_pnl REAL, notional REAL)''')
    conn.commit()
    conn.close()

def get_db_connection():
    return sqlite3.connect('trading_data.db')

def update_total_equity(exchange: ccxt.Exchange, user):
    equity = fetch_total_equity(exchange)
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(f"INSERT INTO {user}_total_equity VALUES (?, ?)", (int(time.time()), equity))
    conn.commit()
    conn.close()

def rename_tables(conn: sqlite3.Connection, table_names: List[Tuple[str, str]]):
    cursor = conn.cursor()
    
    for old_name, new_name in table_names:
        # 检查旧表是否存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (old_name,))
        if cursor.fetchone() is None:
            raise ValueError(f"Table '{old_name}' not exists")
        
        # 重命名表
        cursor.execute(f"ALTER TABLE {old_name} RENAME TO {new_name}")
    
    conn.commit()
    cursor.close()

def update_coin_balance(exchange: ccxt.Exchange, user):
    balances = fetch_coin_balance(exchange)
    conn = get_db_connection()
    c = conn.cursor()

    c.execute(f"SELECT coin FROM {user}_coin_balance")
    existing_coins = set(row[0] for row in c.fetchall())

    current_coins = set(balances.keys())

    for coin, balance in balances.items():
        c.execute(f"INSERT OR REPLACE INTO {user}_coin_balance VALUES (?, ?)", (coin, balance))

    coins_to_delete = existing_coins - current_coins
    for coin in coins_to_delete:
        c.execute(f"DELETE FROM {user}_coin_balance WHERE coin = ?", (coin,))
        print(f"Deleted balance for {coin}")

    conn.commit()
    conn.close()

def update_positions(exchange: ccxt.Exchange, user):
    positions = fetch_positions(exchange)
    conn = get_db_connection()
    c = conn.cursor()
    
    c.execute(f"SELECT symbol FROM {user}_positions")
    existing_symbols = set(row[0] for row in c.fetchall())
    
    current_symbols = set(positions.keys())
    
    for symbol, position in positions.items():
        c.execute(f"INSERT OR REPLACE INTO {user}_positions VALUES (?, ?, ?, ?)",
                  (symbol, position.contracts, position.unrealized_pnl, position.notional))
    
    symbols_to_delete = existing_symbols - current_symbols
    for symbol in symbols_to_delete:
        c.execute(f"DELETE FROM {user}_positions WHERE symbol = ?", (symbol,))
        print(f"Deleted position for {symbol}")
    
    conn.commit()
    conn.close()

def update_data(exchange: ccxt.Exchange, user):
    update_total_equity(exchange, user)
    update_coin_balance(exchange, user)
    update_positions(exchange, user)
    print("Data updated")

