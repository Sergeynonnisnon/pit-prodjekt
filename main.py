import hashlib
import hmac
import json
import time, datetime
import urllib
import requests
from kuna_API.kunaAPI import *

class db:
    pass


class main:
    def __init__(self):
        kuna=KunaAPI()
        print(kuna.get_trades_history('btcuah'))

if __name__ == '__main__':
    main()
