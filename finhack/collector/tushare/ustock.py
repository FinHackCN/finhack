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

class tsUStock:
    @tsMonitor
    def us_basic(pro,db):
        tsSHelper.getDataAndReplace(pro,'us_basic','ustock_basic',db)

    @tsMonitor
    def us_tradecal(pro,db):
        tsSHelper.getDataWithLastDate(pro,'us_tradecal','ustock_tradecal',db,'cal_date')
        
    @tsMonitor
    def us_daily(pro,db):
        tsSHelper.getDataWithLastDate(pro,'us_daily','us_daily',db)