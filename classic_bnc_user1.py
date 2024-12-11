from apscheduler.schedulers.blocking import BlockingScheduler

from utils.binance_classic import init_exchange, init_db, update_data, get_valid_usdt_symbols
from utils.constants import CONFIG

account = 'binance_strategy_9'

BINANCE_UNI_API_KEY = CONFIG[account]['API_KEY']
BINANCE_UNI_SECRET = CONFIG[account]['SECRET']

config = {
    'exchange_id': 'binance',
    'sandbox': False,
    'apiKey': BINANCE_UNI_API_KEY,
    'secret': BINANCE_UNI_SECRET, 
    'enableRateLimit': False,
}

USER = account

if __name__ == '__main__':
    binance = init_exchange(config)
    init_db(USER)

    valid_usdt_symbols = get_valid_usdt_symbols(binance)
    scheduler = BlockingScheduler()
    scheduler.add_job(update_data, 'interval', seconds=60, args=[USER, binance, valid_usdt_symbols])

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
