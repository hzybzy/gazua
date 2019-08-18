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


class Gazuabot():

    balance = {}
    balance['KRW'] = 0.0
    balance['USDT'] = 0.0
    balance['BTC'] = 0.0
    balance['ETH'] = 0.0
    coinone_api = Coinone()
    coinone_btc_ask = 0.0
    coinone_btc_bid = 0.0

    coinone_data = {}
    coinone_data['btc'] = Orderbook('btc')
    coinone_data['xrp'] = Orderbook('xrp')
    coinone_data['eth'] = Orderbook('eth')

    upbit_data = {}
    upbit_data['btc'] = Orderbook('btc')
    upbit_data['xrp'] = Orderbook('xrp')
    upbit_data['eth'] = Orderbook('eth')
    
    upbit_btc_ask = 0.0
    upbit_btc_bid = 0.0

    U2C = {}
    C2U = {}
    U2C['btc'] = 0.0
    C2U['btc'] = 0.0
    U2C['eth'] = 0.0
    C2U['eth'] = 0.0
    U2C['xrp'] = 0.0
    C2U['xrp'] = 0.0

    cross_order_unit = 0.005

    def __init__(self):
        test = 0
         
    def worker_cooldown(self):
        time.sleep(1)

    def get_coinone(self):
        time.sleep(1)

    def worker_get_coinone(self, symbol):
        try:
            data = self.coinone_api.orderbook(currency=symbol)
            self.coinone_data[symbol].ask = float(data['ask'][0]['price'])
            self.coinone_data[symbol].bid = float(data['bid'][0]['price'])
        except:
            self.coinone_data[symbol].ask = 0.0
            self.coinone_data[symbol].bid = 0.0
            

    def cooldown_order(self):
        t = threading.Thread(target=self.worker_cooldown)
        t.start()

    def worker_logger(self):
        
        db = sqlite3.connect(db_filename, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
        cursor = db.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS 
            coin_premium(date timestamp, 
            U2C_BTC FLOAT, C2U_BTC FLOAT, U2C_ETH FLOAT, C2U_ETH FLOAT, U2C_XRP FLOAT, C2U_XRP FLOAT, 
            UPBIT_BTC_ASK FLOAT, UPBIT_BTC_BID FLOAT, 
            UPBIT_ETH_ASK FLOAT, UPBIT_ETH_BID FLOAT, 
            UPBIT_XRP_ASK FLOAT, UPBIT_XRP_BID FLOAT, 
            COINONE_BTC_ASK FLOAT, COINONE_BTC_BID FLOAT, 
            COINONE_ETH_ASK FLOAT, COINONE_ETH_BID FLOAT, 
            COINONE_XRP_ASK FLOAT, COINONE_XRP_BID FLOAT)''')
        
        db_patch = []
        # db_patch.append("ALTER TABLE coin_premium ADD column KRW2USD_weight FLOAT")
        # db_patch.append("ALTER TABLE coin_premium ADD column USD2KRW_weight FLOAT")
        # db_patch.append("ALTER TABLE coin_premium ADD column balance_krw FLOAT")
        # db_patch.append("ALTER TABLE coin_premium ADD column balance_usd FLOAT")
        for q in db_patch:
            try:
                cursor.execute(q)
            except:
                print('Failed to add a column')
        db.commit()

        while True:       
            #text = '%.3f, %.3f, %d, %d, %f, %f, %.2f' % (self.KRW2USD, self.USD2KRW, self.krw_ask, self.krw_bid, self.usd_ask, self.usd_bid, self.exchange_rate)
            #logging.info(text)       
            text = 'U2C, C2U[BTC,ETH,XRP] : %.4f, %.4f, %.4f, %.4f, %.4f, %.4f' %\
                (self.U2C['btc'], self.C2U['btc'], 
                self.U2C['eth'], self.C2U['eth'], 
                self.U2C['xrp'], self.C2U['xrp'],)
            logging.info(text)                  
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            #if self.coinone_data['xrp'].ask > 0.0 and self.coinone_data['btc'].ask > 0.0 and self.coinone_data['eth'].ask > 0.0 and self.upbit_data['xrp'].ask > 0.0 and self.upbit_data['btc'].ask > 0.0 and self.upbit_data['eth'].ask > 0.0:
            #cursor.execute('''INSERT INTO coin_premium(date, KRW2USD, USD2KRW, KRW_ASK, KRW_BID, USD_ASK, USD_BID, EXCHANGE_RATE, KRW2USD_weight, USD2KRW_weight, balance_krw, balance_usd, balance_btc) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)''', (now,self.KRW2USD, self.USD2KRW, self.krw_ask, self.krw_bid, self.usd_ask, self.usd_bid, self.exchange_rate,self.KRW2USD_weighted, self.USD2KRW_weighted, self.balance['KRW'], self.balance['USDT'],self.balance['BTC']))
            cursor.execute('''INSERT INTO 
                coin_premium(date, U2C_BTC, C2U_BTC, U2C_ETH, C2U_ETH, U2C_XRP, C2U_XRP, 
                UPBIT_BTC_ASK, UPBIT_BTC_BID, UPBIT_ETH_ASK, UPBIT_ETH_BID, UPBIT_XRP_ASK, UPBIT_XRP_BID, 
                COINONE_BTC_ASK, COINONE_BTC_BID, COINONE_ETH_ASK, COINONE_ETH_BID, COINONE_XRP_ASK, COINONE_XRP_BID) 
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', 
                (now,self.U2C['btc'], self.C2U['btc'], self.U2C['eth'], self.C2U['eth'],self.U2C['xrp'], self.C2U['xrp'],
                self.upbit_data['btc'].ask, self.upbit_data['btc'].bid,self.upbit_data['eth'].ask, self.upbit_data['eth'].bid,self.upbit_data['xrp'].ask, self.upbit_data['xrp'].bid,
                self.coinone_data['btc'].ask, self.coinone_data['btc'].bid,self.coinone_data['eth'].ask, self.coinone_data['eth'].bid,self.coinone_data['xrp'].ask, self.coinone_data['xrp'].bid))


            db.commit()
            time.sleep(1)
            
            #time.sleep(1)
    
    def loop(self, upbitws):
        while True:
            if upbitws.codeindex:
                update_flag = False                
                upbitws.lock_a.acquire()
                
                upbit_ask_qty = 0.0
                upbit_bid_qty = 0.0
                #get data from upbit websocket
                if upbitws.data_flag and upbitws.orderbook[upbitws.codeindex['KRW-BTC']].units and upbitws.orderbook[upbitws.codeindex['KRW-ETH']].units and upbitws.orderbook[upbitws.codeindex['KRW-XRP']].units:
                    mycode = ''
                    if upbitws.last_code == 'KRW-BTC':
                        mycode = 'btc'
                    elif upbitws.last_code == 'KRW-XRP':
                        mycode = 'xrp'
                    elif upbitws.last_code == 'KRW-ETH':
                        mycode = 'eth'
                    
                    t = threading.Thread(target=self.worker_get_coinone, args=(mycode,))
                    t.start()

                    self.upbit_data[mycode].timestamp = upbitws.orderbook[upbitws.codeindex[upbitws.last_code]].timestamp                    
                    self.upbit_data[mycode].ask = upbitws.orderbook[upbitws.codeindex[upbitws.last_code]].units[0].ask_price
                    self.upbit_data[mycode].bid = upbitws.orderbook[upbitws.codeindex[upbitws.last_code]].units[0].bid_price
                        
                    upbitws.data_flag = False

                    t.join()                    
                    #비율 계산
                    self.U2C[mycode] = 100*(self.upbit_data[mycode].bid - self.coinone_data[mycode].ask) / (self.upbit_data[mycode].bid + self.coinone_data[mycode].ask)
                    self.C2U[mycode] = 100*(self.coinone_data[mycode].bid - self.upbit_data[mycode].ask) / (self.upbit_data[mycode].ask + self.coinone_data[mycode].bid)
                    
                    update_flag = True
                
                upbitws.lock_a.release()
                
                
                if update_flag:
                    #need to check timestamp diff  
                    #order and order_cooldown
                    time.sleep(0.1)
                    

#For thread

def worker_get_orderbook(upbit):
    #start worker
    upbit.set_type("orderbook",["KRW-BTC","KRW-ETH", "KRW-XRP"])
    upbit.run()

if __name__ == '__main__':
    if platform.system() == 'Linux':
        logging.basicConfig(filename='gazua.log', format='%(asctime)s, %(message)s', level=logging.INFO, datefmt='20%y-%m-%d %H:%M:%S')
    else:       # equal 'Windows'
        logging.basicConfig(format='%(asctime)s, %(message)s', level=logging.INFO, datefmt='20%y-%m-%d %H:%M:%S')
    
    
    gbot = Gazuabot()
  

    upbitws = upbitwspy.UpbitWebsocket()

    t1 = threading.Thread(target=worker_get_orderbook, args=(upbitws,))
    t1.start()

    t2 = threading.Thread(target=gbot.worker_logger)
    t2.start()

    main_thread = threading.currentThread()

    gbot.order_flag = True
    gbot.loop(upbitws)

    for t in threading.enumerate():
        if t is not main_thread:
            t.join()

