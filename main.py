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


"""class Tread(threading.Thread,market_data,DB):

    def __init__(self,  name, instruction,timer):
        #Инициализация потока
        Thread.__init__(self)
        self.name = name
        #self.instruction = instruction
        self.timer = timer

    def run(self):
        threading.Timer(self.timer,main).start()
        self.instruction
        msg = "% добавлено %s!" % (self.name)
        print(msg)"""



class main(KunaAPI):
    def __init__(self):
        kuna=KunaAPI()
        #self.tread= Tread()
        self.market_data_main = market_data(VALID_MARKET_DATA_PAIRS)

        self.DB_main= DB(self.market_data_main)
        self.DB_main.create_db()
        #инициируем потоки

        self.bd_writing()
        self.minutes_given()

    def bd_writing(self):
        threading.Timer(2.0, self.bd_writing).start()
        self.market_parse=self.market_data_main.market_data_pars()
        self.DB_main.writhing(self.market_data_main.market_data_pars())
        print ("время выполнения потока 1", time.thread_time())
    def minutes_given(self):
        threading.Timer(60.0, self.testinger).start()
        print("время выполнения потока 2 ", time.thread_time())







if __name__ == '__main__':
    main()
