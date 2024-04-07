from finhack.library.config import Config
from runtime.constant import *
from finhack.market.astock.astock import AStock
import json
import pickle
import pandas as pd
import os
import time
from datetime import datetime
import trader.qmt.calendar
import finhack.library.log as Log
from finhack.core.classes.dictobj import DictObj
from trader.qmt.qmtClient import qclient


class Data: 
    def get_daily_info(code,context,date=None):
        return qclient.getInfo(code)

    #根据当前时间获取价格
    def get_price(code,context=None):
        return qclient.getPrice(code)
  

