import hashlib
import hmac
import json
import time, datetime
import urllib
import requests
from kuna_API.kunaAPI import *
import sqlite3
from pandas.io.sql import read_sql
# программа выполняет функции моста между МТ 4 и API куны

#TODO сделать посекундную загрузку маркетдаты в сет
#TODO обработать переменную at для математических исчислений
#TODO настроить поток
#TODO создать загрузку пакетов в файл поминутно для отправки
#TODO создать возможность отправить по запросу *

class main(KunaAPI()):
    def __init__(self):
        kuna=KunaAPI()




def market_data_pars(currency):
    a = kuna.get_recent_market_data(currency)
    at = a.get('at')
    buy = a.get('ticker').get('buy')
    sell = a.get('ticker').get('sell')
    low = a.get('ticker').get('low')
    high = a.get('ticker').get('high')
    last = a.get('ticker').get('last')
    vol = a.get('ticker').get('vol')
    price = a.get('ticker').get('price')
    return (at, buy, sell, low, high, last, vol, price)


# create db
def create_db():
    con = sqlite3.connect('test.db')
    cur = con.cursor()
    cur.execute(
        'CREATE TABLE IF NOT EXISTS test (at PRIMARY KEY, buy TEXT,sell TEXT,low  TEXT,high TEXT,last TEXT,vol TEXT,price TEXT)')
    con.commit()
    cur.close()
    con.close()


create_db()


def writhing():
    con = sqlite3.connect('test.db')
    cur = con.cursor()
    cur.execute('INSERT INTO test VALUES (?,?,?,?,?,?,?,?)', market_data_pars('btcuah'))
    con.commit()

    cur.close()
    con.close()


def reading():
    con = sqlite3.connect('test.db')
    cur = con.cursor()
    cur.execute('SELECT at,buy,sell,low,high,last,vol,price FROM test ORDER BY at')
    data = cur.fetchall()

    cur.close()
    con.close()
    return (print(data))


#def main():
#    threading.Timer(1.0, main).start()
#    print(kuna.get_recent_market_data('btcuah').get('at'))
#    # reading()
#    writhing()
if __name__ == '__main__':
    main()
