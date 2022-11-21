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