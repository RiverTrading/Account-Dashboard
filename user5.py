from apscheduler.schedulers.blocking import BlockingScheduler

from utils.binance import init_exchange, init_db, update_data
from utils.constants import CONFIG

BINANCE_VIP = CONFIG['binance_vip']['API_KEY']
BINANCE_VIP_SECRET = CONFIG['binance_vip']['SECRET']

config = {
    'exchange_id': 'binance',
    'sandbox': False,
    'apiKey': BINANCE_VIP,
    'secret': BINANCE_VIP_SECRET, 
    'enableRateLimit': False,
    'options': {
        'portfolioMargin': True
    }
}

USER = 'binance3'

if __name__ == '__main__':
    binance = init_exchange(config)
    init_db(USER)

    scheduler = BlockingScheduler()
    scheduler.add_job(update_data, 'interval', seconds=5 * 60, args=[binance, USER])

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
