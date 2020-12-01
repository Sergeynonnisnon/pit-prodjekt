import hashlib
import hmac
import json
import time, datetime
import urllib
import requests
from kuna_API.kunaAPI import *
import sqlite3
import threading
from pandas.io.sql import read_sql
# программа выполняет функции моста между МТ 4 и API куны

#TODO сделать посекундную загрузку маркетдаты в сет
#TODO обработать переменную at для математических исчислений
#TODO настроить поток
#TODO создать загрузку пакетов в файл поминутно для отправки
#TODO создать возможность отправить по запросу *
kuna=KunaAPI()
#print(kuna.get_recent_market_data('btcuah'))
VALID_MARKET_DATA_PAIRS= VALID_MARKET_DATA_PAIRS[8]

class market_data(KunaAPI):
    #currency ==VALID_MARKET_DATA_PAIRS[]
    def __init__(self,currency):
        self.kuna = KunaAPI()
        self.currency = currency

    def market_data_pars(self):
        a = self.kuna.get_recent_market_data(self.currency)
        at = a.get('at')
        buy = a.get('ticker').get('buy')
        sell = a.get('ticker').get('sell')
        low = a.get('ticker').get('low')
        high = a.get('ticker').get('high')
        last = a.get('ticker').get('last')
        vol = a.get('ticker').get('vol')
        price = a.get('ticker').get('price')
        result = (at, buy, sell, low, high, last, vol, price)
        return result
#print (market_data(VALID_MARKET_DATA_PAIRS[8]).market_data_pars())

class DB (market_data):
    # currency ==VALID_MARKET_DATA_PAIRS[]
    def __init__(self,market_data):
        self.market_data = market_data
        self.currency = market_data.currency

    def create_db(self):
        con = sqlite3.connect(self.currency+'.db')
        cur = con.cursor()
        cur.execute(
            'CREATE TABLE IF NOT EXISTS ' +str(self.currency)+ ' (at PRIMARY KEY, buy TEXT,sell TEXT,low  TEXT,high TEXT,last TEXT,vol TEXT,price TEXT)')
        con.commit()
        cur.close()
        con.close()

    def writhing(self,market_data_pars):
        con = sqlite3.connect(self.currency+'.db')
        cur = con.cursor()
        cur.execute('INSERT INTO ' +str(self.currency)+ ' VALUES (?,?,?,?,?,?,?,?)', market_data_pars)
        con.commit()

        cur.close()
        con.close()

    def reading(self):
        con = sqlite3.connect(self.currency+'.db')
        cur = con.cursor()
        cur.execute('SELECT at,buy,sell,low,high,last,vol,price FROM '+str(self.currency)+' ORDER BY at')
        data = cur.fetchall()

        cur.close()
        con.close()
        return (print(data))


class main(KunaAPI):
    def __init__(self):
        kuna=KunaAPI()
        market_data_main = market_data(VALID_MARKET_DATA_PAIRS)

        DB_main= DB(market_data_main)
        DB_main.create_db()

        threading.Timer(2.0, main).start()
        market_data_main.market_data_pars()
        DB_main.writhing(market_data_main.market_data_pars())
        print (time.thread_time())



if __name__ == '__main__':
    main()
