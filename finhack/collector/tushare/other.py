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