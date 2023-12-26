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

class tsEcono:
    @tsMonitor
    def shibor(pro,db):
        tsSHelper.getDataWithLastDate(pro,'shibor','econo_shibor',db,'date')
    
    @tsMonitor
    def shibor_quote(pro,db):
        tsSHelper.getDataWithLastDate(pro,'shibor_quote','econo_shibor_quote',db,'date')
    
    @tsMonitor
    def shibor_lpr(pro,db):
        tsSHelper.getDataWithLastDate(pro,'shibor_lpr','econo_shibor_lpr',db,'date')
    
    @tsMonitor
    def libor(pro,db):
        tsSHelper.getDataWithLastDate(pro,'libor','econo_libor',db,'date')
    
    @tsMonitor
    def hibor(pro,db):
        tsSHelper.getDataWithLastDate(pro,'hibor','econo_hibor',db,'date')
    
    @tsMonitor
    def wz_index(pro,db):
        tsSHelper.getDataAndReplace(pro,'wz_index','econo_wz_index',db)
    
    @tsMonitor
    def gz_index(pro,db):
        tsSHelper.getDataAndReplace(pro,'gz_index','econo_gz_index',db)
    
    @tsMonitor
    def cn_gdp(pro,db):
        tsSHelper.getDataAndReplace(pro,'cn_gdp','econo_gdp',db)
    
    @tsMonitor
    def cn_cpi(pro,db):
        tsSHelper.getDataAndReplace(pro,'cn_cpi','econo_cn_cpi',db)
    
    @tsMonitor
    def cn_ppi(pro,db):
        tsSHelper.getDataAndReplace(pro,'cn_ppi','econo_cn_ppi',db)
    
    @tsMonitor
    def cn_m(pro,db):
        tsSHelper.getDataAndReplace(pro,'cn_m','econo_cn_m',db)
    
    @tsMonitor
    def us_tycr(pro,db):
        tsSHelper.getDataWithLastDate(pro,'us_tycr','econo_us_tycr',db,'date')
    
    @tsMonitor
    def us_trycr(pro,db):
        tsSHelper.getDataWithLastDate(pro,'us_trycr','econo_us_trycr',db,'date')
    
    @tsMonitor
    def us_tbr(pro,db):
        tsSHelper.getDataWithLastDate(pro,'us_tbr','econo_us_tbr',db,'date')
    
    @tsMonitor
    def us_tltr(pro,db):
        tsSHelper.getDataWithLastDate(pro,'us_tltr','econo_us_tltr',db,'date')
    
    @tsMonitor
    def us_trltr(pro,db):
        tsSHelper.getDataWithLastDate(pro,'us_trltr','econo_us_trltr',db,'date')
        
    @tsMonitor
    def eco_cal(pro,db):
        tsSHelper.getDataWithLastDate(pro,'eco_cal','econo_cal',db,'date')       
        
    
