import sys
import datetime
from library.config import config
from library.mydb import mydb
from collect.ts.helper import tsSHelper
from library.monitor import tsMonitor
import time
import pandas as pd
import traceback
from library.alert import alert

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