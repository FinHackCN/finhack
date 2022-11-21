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

class tsOther:
    @tsMonitor
    def news(pro,db):
        pass
        # lastdate=tsSHelper.getLastDateAndDelete(table='news',filed='',ts_code=ts_code,db=db)
        # tsSHelper.getDataWithLastDate(pro,'news','cctv_news',db,'date')
        # tsSHelper.getDataWithLastDate(pro,'cctv_news','cctv_news',db,'date')
        # tsSHelper.getDataWithLastDate(pro,'cctv_news','cctv_news',db,'date')
        # tsSHelper.getDataWithLastDate(pro,'cctv_news','cctv_news',db,'date')
        # tsSHelper.getDataWithLastDate(pro,'cctv_news','cctv_news',db,'date')

    @tsMonitor
    def fund_basic(pro,db):
        pass
    
    @tsMonitor
    def fund_basic(pro,db):
        pass
    
    @tsMonitor
    def fund_basic(pro,db):
        pass
    
    @tsMonitor
    def fund_basic(pro,db):
        pass
    
    @tsMonitor
    def fund_basic(pro,db):
        pass
    
    @tsMonitor
    def cctv_news(pro,db):
        tsSHelper.getDataWithLastDate(pro,'cctv_news','other_cctv_news',db,'date')