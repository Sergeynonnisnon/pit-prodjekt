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

    :return : result
    """

    def __init__(self, currency):
        """

        :param currency: pair currency from list
        """
        self.kuna = KunaAPI()
        self.currency = currency

    def market_data_pars(self):
        """
        get recent_market_data from API
        :at: 'at'- ISO format time
        :buy: demand buy
        :sell: supply sell
        :low: low price in 24 hour*
        :high:high price in 24 hour*
        :last: last deal price
        :return: (str,str,str,str,str,str,str,str)
        """
        _get_data = self.kuna.get_recent_market_data(self.currency)
        at = _get_data.get('at')
        buy = _get_data.get('ticker').get('buy')
        sell = _get_data.get('ticker').get('sell')
        low = _get_data.get('ticker').get('low')
        high = _get_data.get('ticker').get('high')
        last = _get_data.get('ticker').get('last')
        vol = _get_data.get('ticker').get('vol')
        price = _get_data.get('ticker').get('price')
        result = (at, buy, sell, low, high, last, vol, price)
        return result


class DB(market_data):
    """
    Use sql3 base
    """
    def __init__(self, market_data):
        self.market_data = market_data
        self.currency = market_data.currency

    def create_db(self):
        """
        create sql3 DB
        :currency: pairs currency
        :return: None
        """
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
        """
        writing result market_data_pars in DB
        :param market_data_pars: (str,str,str..)
        :return: None
        """
        con = sqlite3.connect(self.currency + '.db')
        cur = con.cursor()
        cur.execute('INSERT INTO ' + str(self.currency) + ' VALUES (?,?,?,?,?,?,?,?)', market_data_pars)
        con.commit()

        cur.close()
        con.close()

    def reading(self):
        """
        non use now but mb be needed
        :return: data from BD
        """
        con = sqlite3.connect(self.currency + '.db')
        cur = con.cursor()
        cur.execute('SELECT at,buy,sell,low,high,last,vol,price FROM ' + str(self.currency) + ' ORDER BY at')
        data = cur.fetchall()

        cur.close()
        con.close()
        return data


class thread_start(KunaAPI):
    """
    create structure a new thread
    """

    def __init__(self, currency=VALID_MARKET_DATA_PAIRS):
        self.pairs_currency = currency

    def start_parsing(self):
        self.market_data_main = market_data(self.pairs_currency).market_data_pars()

        return self.market_data_main


class main(KunaAPI):
    def __init__(self):
        """
        starting script
        :server_tick: get time on kuna.io server to use on exception
        Market_pairs_to_gryvna - list on KunaApi.py
        """
        self.server_tick = kuna.get_server_time()

        for i in MARKET_PAIRS_TO_GRYVNA:
            self.market_data_main = market_data(i)
            self.DB_main = DB(self.market_data_main)
            self.DB_main.create_db()

        threading.Timer(1.0, main).start()

        for i in MARKET_PAIRS_TO_GRYVNA:
            self.market_data_main = market_data(i)
            self.DB_main = DB(self.market_data_main)
            self.tread_start = thread_start(currency=i).start_parsing()
            try:
                self.DB_main.writhing(self.tread_start)

            except Exception:
                log_writing = open('log.txt', 'a')
                log_writing.write(f'{i} {self.server_tick} \n {format_exc()}')
                print(f'{i} error writing')


if __name__ == '__main__':
    main()
