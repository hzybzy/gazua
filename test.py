from upbitpy import Upbitpy
from coinone.core import Coinone, CoinoneV1, CoinoneV2
import logging
import threading
import upbitwspy
import time
import requests #for real currency rate
import json #for real currency rate
import logging
import platform
import sqlite3
import datetime

db_filename = 'gazua.db'

class Orderbook(object):
    def __init__(self, code):
        self.code = code
        self.timestamp = 0
        self.ask = 0.0
        self.bid = 0.0

if __name__ == '__main__':
    orderbook = Orderbook('hello')
    orderbook2 = Orderbook('world')
    print(orderbook.code)
    print(orderbook2.code)

    test = {}
    test['hello'] = Orderbook('hello')
    test['world'] = Orderbook('world')
    
    print(test['hello'].code)
