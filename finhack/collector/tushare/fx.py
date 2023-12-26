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

class tsFX:
    @tsMonitor
    def fx_basic(pro,db):
        tsSHelper.getDataAndReplace(pro,'fx_obasic','fx_basic',db)

    @tsMonitor
    def fx_daily(pro,db):
        tsSHelper.getDataWithLastDate(pro,'fx_daily','fx_daily',db)