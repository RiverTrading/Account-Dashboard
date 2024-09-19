from configparser import ConfigParser
from collections import defaultdict


config = ConfigParser()

config.read('.keys/config.cfg')

BYBIT_API_KEY_1 = config['bybit']['API_KEY']
BYBIT_SECRET_1 = config['bybit']['SECRET']

BYBIT_API_KEY_2 = config['bybit2']['API_KEY']
BYBIT_SECRET_2 = config['bybit2']['SECRET']
