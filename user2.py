from apscheduler.schedulers.blocking import BlockingScheduler

from utils.bybit import init_exchange, init_db, update_data
from constants import BYBIT_API_KEY_2, BYBIT_SECRET_2

config = {
    'exchange_id': 'bybit',
    'sandbox': False,
    'apiKey': BYBIT_API_KEY_2,
    'secret': BYBIT_SECRET_2, 
    'enableRateLimit': False,
}

USER = 'bybit2'

if __name__ == '__main__':
    bybit = init_exchange(config)
    init_db(USER)

    scheduler = BlockingScheduler()
    scheduler.add_job(update_data, 'interval', seconds=60, args=[bybit, USER])

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass