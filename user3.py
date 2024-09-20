from apscheduler.schedulers.blocking import BlockingScheduler

from utils.binance import init_exchange, init_db, update_data
from utils.constants import BINANCE_UNI_API_KEY, BINANCE_UNI_SECRET

config = {
    'exchange_id': 'binance',
    'sandbox': False,
    'apiKey': BINANCE_UNI_API_KEY,
    'secret': BINANCE_UNI_SECRET, 
    'enableRateLimit': False,
    'options': {
        'portfolioMargin': True
    }
}

USER = 'binance1'

if __name__ == '__main__':
    binance = init_exchange(config)
    init_db(USER)

    scheduler = BlockingScheduler()
    scheduler.add_job(update_data, 'interval', seconds=15, args=[binance, USER])

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
