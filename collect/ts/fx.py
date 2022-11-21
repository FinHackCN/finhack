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

class tsFX:
    @tsMonitor
    def fx_basic(pro,db):
        tsSHelper.getDataAndReplace(pro,'fx_obasic','fx_basic',db)

    @tsMonitor
    def fx_daily(pro,db):
        tsSHelper.getDataWithLastDate(pro,'fx_daily','fx_daily',db)