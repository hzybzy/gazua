from upbitpy import Upbitpy
import logging
import threading
import upbitwspy
import requests #for real currency rate
import json #for real currency rate
import logging
import platform
import sqlite3
import datetime
import time
from binance.client import Client # Import the Binance Client
from binance.websockets import BinanceSocketManager # Import the Binance Socket Manager

db_filename = 'gazua.db'

class Orderbook(object):
    def __init__(self, code):
        self.code = code
        self.timestamp = 0
        self.ask = 0.0
        self.bid = 0.0

class Binance_data():
    def __init__(self):
        self.lock_a = threading.Lock()    
        self.orderbook = {}#Orderbook()
        self.orderbook['XRPETH'] = Orderbook('XRPETH')
        self.codeindex = {}
        self.data_flag = False
        self.last_code = ''
    def callback(self,msg):
        try:
            if 'e' in msg:
                print(msg['e'])
            else:
                self.lock_a.acquire()
                # {'lastUpdateId': 211913589, 'bids': [['0.00143597', '41.00000000'], ['0.00143571', '1490.00000000'], ['0.00143455', '1724.00000000'], ['0.00143453', '7453.00000000'], ['0.00143446', '3022.00000000']], 'asks': [['0.00143712', '13.00000000'], ['0.00143724', '15.00000000'], ['0.00143738', '8.00000000'], ['0.00143754', '95.00000000'], ['0.00143821', '923.00000000']]}
                self.orderbook['XRPETH'].ask = float(msg['asks'][0][0])
                self.orderbook['XRPETH'].bid = float(msg['bids'][0][0])
                self.data_flag = True 
                self.lock_a.release()
            # print(msg)
                                    
            # if (ret['type'] == 'orderbook'): 
            #     self.last_code = ret['code']
            #     self.data_flag = True            
                    #     self.orderbook[self.codeindex[ret['code']]].timestamp = time.time()
                    #     self.orderbook[self.codeindex[ret['code']]].units.clear()
                    #     for i in range(10):
                    #         self.orderbook[self.codeindex[ret['code']]].units.append(Orderbook_Unit(ret['orderbook_units'][i]['ask_price'], ret['orderbook_units'][i]['bid_price'], ret['orderbook_units'][i]['ask_size'], ret['orderbook_units'][i]['bid_size']))
                    
            
        except Exception as e:
            logging.info(e)
            


class Gazuabot():

    balance = {}
    balance['KRW'] = 0.0
    balance['USDT'] = 0.0
    balance['BTC'] = 0.0
    balance['ETH'] = 0.0
    
    upbit_data = {}
    
    upbit_data['xrp'] = Orderbook('xrp')
    upbit_data['eth'] = Orderbook('eth')
    upbit_data['usdt'] = Orderbook('usdt')
    upbit_data['XRPETH'] = Orderbook('XRPETH')
    upbit_data['XRPETH2'] = Orderbook('XRPETH2')
    upbit_data['XRPKRW'] = Orderbook('XRPKRW')
    upbit_data['ETHKRW'] = Orderbook('ETHKRW')

    binance_data = {}
    binance_data['ETHBTC'] = Orderbook('ETHBTC')
    binance_data['XRPETH'] = Orderbook('XRPETH')
    # binance_data['ETHBTC'] = Orderbook('ETHBTC')
    
    upbit_btc_ask = 0.0
    upbit_btc_bid = 0.0

    U2B = {}
    B2U = {}
    U2B['XRPETH'] = 0.0    
    B2U['XRPETH'] = 0.0
    U2B['XRPETH2'] = 0.0
    B2U['XRPETH2'] = 0.0
    U2B['eth'] = 0.0
    B2U['eth'] = 0.0
    U2B['xrp'] = 0.0
    B2U['xrp'] = 0.0

    cross_order_unit = 0.005

    def __init__(self):
        test = 0
         
    def add_upbit_orderbook(self, symbol):
        self.upbit_data[symbol] = Orderbook(symbol)


    def worker_cooldown(self):
        time.sleep(1)

    def get_coinone(self):
        time.sleep(1)

    def worker_get_binance(self, symbol):
        print('hello')

    def cooldown_order(self):
        t = threading.Thread(target=self.worker_cooldown)
        t.start()

    def worker_logger(self):
        
        db = sqlite3.connect(db_filename, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
        cursor = db.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS 
            coin_xrpeth(date timestamp, 
            U2B_XRPETH FLOAT, B2U_XRPETH FLOAT, 
            UPBIT_XRPETH_ASK FLOAT, UPBIT_XRPETH_BID FLOAT, 
            BINANCE_XRPETH_ASK FLOAT, BINANCE_XRPETH_BID FLOAT)''')
 
        # db_patch = []
        # # db_patch.append("ALTER TABLE coin_premium ADD column KRW2USD_weight FLOAT")
        # # db_patch.append("ALTER TABLE coin_premium ADD column USD2KRW_weight FLOAT")
        # # db_patch.append("ALTER TABLE coin_premium ADD column balance_krw FLOAT")
        # # db_patch.append("ALTER TABLE coin_premium ADD column balance_usd FLOAT")
        # for q in db_patch:
        #     try:
        #         cursor.execute(q)
        #     except:
        #         print('Failed to add a column')
        db.commit()

        while True:  
            text = 'U2B, B2U(XRPETH) : %f %f %f %f %.8f %.8f %.8f %.8f' % (self.U2B['XRPETH'], self.B2U['XRPETH'], self.U2B['XRPETH2'], self.B2U['XRPETH2'], self.upbit_data['XRPETH'].ask, self.upbit_data['XRPETH'].bid, self.binance_data['XRPETH'].ask, self.binance_data['XRPETH'].bid )
            # text = 'U2B, B2U[BTC,ETH,XRP] : %.4f, %.4f, %.4f, %.4f, %.4f, %.4f' %\
            #     (self.U2B['btc'], self.B2U['btc'], 
            #     self.U2B['eth'], self.B2U['eth'], 
            #     self.U2B['xrp'], self.B2U['xrp'],)
            logging.info(text)                  
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if self.binance_data['XRPETH'].ask > 0.0 and self.binance_data['XRPETH'].ask > 0.0 and self.upbit_data['XRPETH'].ask > 0.0 and self.upbit_data['XRPETH'].ask > 0.0:            
                cursor.execute('''INSERT INTO 
                    coin_xrpeth(date, U2B_XRPETH, B2U_XRPETH,
                    UPBIT_XRPETH_ASK, UPBIT_XRPETH_BID,
                    BINANCE_XRPETH_ASK, BINANCE_XRPETH_BID)
                    VALUES(?,?,?,?,?,?,?)''', 
                    (now,self.U2B['XRPETH'], self.B2U['XRPETH'],
                    self.upbit_data['XRPETH'].ask, self.upbit_data['XRPETH'].bid,
                    self.binance_data['XRPETH'].ask, self.binance_data['XRPETH'].bid))


                db.commit()
            time.sleep(1)
            
            #time.sleep(1)
    
    def loop(self, upbitws, bd):
        while True:
            if upbitws.codeindex:
                update_flag = False                
                upbitws.lock_a.acquire()
                
                upbit_ask_qty = 0.0
                upbit_bid_qty = 0.0
                #get data from upbit websocket
                if upbitws.data_flag and upbitws.orderbook[upbitws.codeindex['ETH-XRP']].units:
                    mycode = ''
                    if upbitws.last_code == 'ETH-XRP':
                        mycode = 'XRPETH'
                        self.upbit_data[mycode].timestamp = upbitws.orderbook[upbitws.codeindex[upbitws.last_code]].timestamp                    
                        self.upbit_data[mycode].ask = upbitws.orderbook[upbitws.codeindex[upbitws.last_code]].units[0].ask_price
                        self.upbit_data[mycode].bid = upbitws.orderbook[upbitws.codeindex[upbitws.last_code]].units[0].bid_price
                        self.binance_data[mycode].ask = bd.orderbook[mycode].ask
                        self.binance_data[mycode].bid = bd.orderbook[mycode].bid
                        self.U2B[mycode] = 100*(self.upbit_data[mycode].bid - self.binance_data[mycode].ask) / (self.upbit_data[mycode].bid + self.binance_data[mycode].ask)
                        self.B2U[mycode] = 100*(self.binance_data[mycode].bid - self.upbit_data[mycode].ask) / (self.upbit_data[mycode].ask + self.binance_data[mycode].bid)
                    elif upbitws.last_code == 'KRW-XRP':
                        mycode = 'XRPKRW'
                        self.upbit_data[mycode].timestamp = upbitws.orderbook[upbitws.codeindex[upbitws.last_code]].timestamp                    
                        self.upbit_data[mycode].ask = upbitws.orderbook[upbitws.codeindex[upbitws.last_code]].units[0].ask_price
                        self.upbit_data[mycode].bid = upbitws.orderbook[upbitws.codeindex[upbitws.last_code]].units[0].bid_price

                    elif upbitws.last_code == 'KRW-ETH':
                        mycode = 'ETHKRW' 
                        self.upbit_data[mycode].timestamp = upbitws.orderbook[upbitws.codeindex[upbitws.last_code]].timestamp                    
                        self.upbit_data[mycode].ask = upbitws.orderbook[upbitws.codeindex[upbitws.last_code]].units[0].ask_price
                        self.upbit_data[mycode].bid = upbitws.orderbook[upbitws.codeindex[upbitws.last_code]].units[0].bid_price
                     
                
                    # t = threading.Thread(target=self.worker_get_binance, args=(mycode,))
                    # t.start()

                    if self.upbit_data['ETHKRW'].bid > 0.0 and self.upbit_data['ETHKRW'].ask > 0.0 and self.binance_data['XRPETH'].ask:
                        self.binance_data['XRPETH'].ask = bd.orderbook['XRPETH'].ask
                        self.binance_data['XRPETH'].bid = bd.orderbook['XRPETH'].bid
                        self.upbit_data['XRPETH2'].ask = self.upbit_data['XRPKRW'].ask / self.upbit_data['ETHKRW'].bid  #ETH2XRP
                        self.upbit_data['XRPETH2'].bid = self.upbit_data['XRPKRW'].bid / self.upbit_data['ETHKRW'].ask  #XRP2ETH

                        self.U2B['XRPETH2'] = 100*(self.upbit_data['XRPETH2'].bid - self.binance_data['XRPETH'].ask) / (self.upbit_data['XRPETH2'].bid + self.binance_data['XRPETH'].ask)
                        self.B2U['XRPETH2'] = 100*(self.binance_data['XRPETH'].bid - self.upbit_data['XRPETH2'].ask) / (self.upbit_data['XRPETH2'].ask + self.binance_data['XRPETH'].bid)
                    upbitws.data_flag = False
                            
                    #비율 계산

                    
                    update_flag = True
                
                upbitws.lock_a.release()
                
                
                # if update_flag:
                    #need to check timestamp diff  
                    #order and order_cooldown
                time.sleep(0.1)
                    

#For thread
def handle_message(msg):
    print(msg)

def worker_get_orderbook(upbit):
    #start worker
    upbit.set_type("orderbook",["ETH-XRP","KRW-XRP","KRW-ETH"])
    upbit.run()

if __name__ == '__main__':
    if platform.system() == 'Linux':
        logging.basicConfig(filename='gazua.log', format='%(asctime)s, %(message)s', level=logging.INFO, datefmt='20%y-%m-%d %H:%M:%S')
    else:       # equal 'Windows'
        logging.basicConfig(format='%(asctime)s, %(message)s', level=logging.INFO, datefmt='20%y-%m-%d %H:%M:%S')
    
    
    gbot = Gazuabot()
    gbot.add_upbit_orderbook('ETH-XRP')
  
    upbitws = upbitwspy.UpbitWebsocket()

    t1 = threading.Thread(target=worker_get_orderbook, args=(upbitws,))
    t1.start()

    t2 = threading.Thread(target=gbot.worker_logger)
    t2.start()

    # Binance setting
    PUBLIC = '<YOUR-PUBLIC-KEY>'
    SECRET = '<YOUR-SECRET-KEY>'

    bd = Binance_data()
    binance_client = Client(api_key=PUBLIC, api_secret=SECRET)
    bm = BinanceSocketManager(binance_client)
    # conn_key = bm.start_trade_socket('XRPETH', bd.callback)
    partial_key = bm.start_depth_socket('XRPETH', bd.callback, depth=BinanceSocketManager.WEBSOCKET_DEPTH_5)
    bm.start()

    main_thread = threading.currentThread()

    gbot.order_flag = True
    gbot.loop(upbitws,bd)

    for t in threading.enumerate():
        if t is not main_thread:
            t.join()

    bm.stop_socket(partial_key)

    

    

