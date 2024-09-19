import pytest

from utils.constants import BYBIT_API_KEY_1, BYBIT_SECRET_1
from utils.bybit import init_exchange, fetch_positions




@pytest.fixture
def bybit():
    config = {
        'exchange_id': 'bybit',
        'sandbox': False,
        'apiKey': BYBIT_API_KEY_1,
        'secret': BYBIT_SECRET_1, 
        'enableRateLimit': False,
    }
    return init_exchange(config)

def test_bybit_fetch_positions(bybit):
    fetch_positions(bybit) 
