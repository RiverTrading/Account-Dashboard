import pytest
import utils.binance as binance_utils
import utils.bybit as bybit_utils

from utils.constants import BYBIT_API_KEY_1, BYBIT_SECRET_1, BINANCE_UNI_API_KEY, BINANCE_UNI_SECRET





@pytest.fixture
def bybit():
    config = {
        'exchange_id': 'bybit',
        'sandbox': False,
        'apiKey': BYBIT_API_KEY_1,
        'secret': BYBIT_SECRET_1, 
        'enableRateLimit': False,
    }
    return bybit_utils.init_exchange(config)

@pytest.fixture
def binance():
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
    return binance_utils.init_exchange(config)

def test_bybit_fetch_positions(bybit):
    bybit_utils.fetch_positions(bybit) 

def test_binance_fetch_account_balance(binance):
    binance_utils.fetch_account_balance(binance)
