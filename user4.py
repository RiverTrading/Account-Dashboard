from apscheduler.schedulers.blocking import BlockingScheduler

from utils.binance import init_exchange, init_db, update_data
from utils.constants import CONFIG

BINANCE_UNI_API_KEY_2 = CONFIG['binance_uni2']['API_KEY']
BINANCE_UNI_SECRET_2 = CONFIG['binance_uni2']['SECRET']

config = {
    'exchange_id': 'binance',
    'sandbox': False,
    'apiKey': BINANCE_UNI_API_KEY_2,
    'secret': BINANCE_UNI_SECRET_2, 
    'enableRateLimit': False,
    'options': {
        'portfolioMargin': True
    }
}

USER = 'binance2'

if __name__ == '__main__':
    binance = init_exchange(config)
    init_db(USER)

    scheduler = BlockingScheduler()
    scheduler.add_job(update_data, 'interval', seconds=60, args=[binance, USER])

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
