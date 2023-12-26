import sys
import time
import datetime
import traceback
import pandas as pd

from finhack.library.mydb import mydb
from finhack.library.alert import alert
from finhack.library.monitor import tsMonitor
from finhack.collector.tushare.helper import tsSHelper
import finhack.library.log as Log

class tsHStock:
    @tsMonitor
    def hk_basic(pro,db):
        tsSHelper.getDataAndReplace(pro,'hk_basic','hstock_basic',db)
        
    @tsMonitor
    def hk_tradecal(pro,db):
        tsSHelper.getDataWithLastDate(pro,'hk_tradecal','hstock_tradecal',db,'cal_date')
        
    @tsMonitor
    def hk_daily(pro,db):
        tsSHelper.getDataWithLastDate(pro,'hk_daily','hk_daily',db)