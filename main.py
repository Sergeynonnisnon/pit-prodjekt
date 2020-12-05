from kuna_API.kunaAPI import *
import sqlite3
import threading
from traceback import format_exception_only

#
# this is the bridge between ws kuna.io and MT4
# implemented by collecting 1 minute data with public API kuna.io
# and pack him from bd based on mysql3


kuna = KunaAPI()


class market_data(KunaAPI):
    """ get market_data from kuna.io
    currency ==VALID_MARKET_DATA_PAIRS[]
    """

    def __init__(self, currency):
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


class DB(market_data):
    # currency ==VALID_MARKET_DATA_PAIRS[]
    def __init__(self, market_data):
        self.market_data = market_data
        self.currency = market_data.currency

    def create_db(self):
        con = sqlite3.connect(self.currency + '.db')
        cur = con.cursor()
        cur.execute(
            'CREATE TABLE IF NOT EXISTS '
            + str(self.currency) +
            ' (at PRIMARY KEY, buy TEXT,'
            'sell TEXT,low  TEXT,high TEXT,'
            'last TEXT,vol TEXT,price TEXT)')
        con.commit()
        cur.close()
        con.close()

    def writhing(self, market_data_pars):
        con = sqlite3.connect(self.currency + '.db')
        cur = con.cursor()
        cur.execute('INSERT INTO ' + str(self.currency) + ' VALUES (?,?,?,?,?,?,?,?)', market_data_pars)
        con.commit()

        cur.close()
        con.close()

    def reading(self):
        con = sqlite3.connect(self.currency + '.db')
        cur = con.cursor()
        cur.execute('SELECT at,buy,sell,low,high,last,vol,price FROM ' + str(self.currency) + ' ORDER BY at')
        data = cur.fetchall()

        cur.close()
        con.close()
        return data


class tread_start(KunaAPI):

    def __init__(self, currency=VALID_MARKET_DATA_PAIRS):
        self.pairs_currency = currency

    def start_parsing(self):
        self.market_data_main = market_data(self.pairs_currency).market_data_pars()

        return self.market_data_main


class main(KunaAPI):
    def __init__(self):
        self.servertick = kuna.get_server_time()

        for i in MARKET_PAIRS_TO_GRYVNA:
            self.market_data_main = market_data(i)
            self.DB_main = DB(self.market_data_main)
            self.DB_main.create_db()

        threading.Timer(1.0, main).start()

        for i in MARKET_PAIRS_TO_GRYVNA:
            self.market_data_main = market_data(i)
            self.DB_main = DB(self.market_data_main)
            self.tread_start = tread_start(currency=i).start_parsing()
            try:
                self.DB_main.writhing(self.tread_start)
                print (time.thread_time())
            except Exception:
                log_writing = open('log.txt', 'a')
                log_writing.write(f'{i} {self.servertick} \n {format_exc()}')
                print(f'{i} ошибка записи  в бд')


if __name__ == '__main__':
    main()
