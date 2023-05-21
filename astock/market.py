import numpy as np
from astock.astock import AStock
import pandas as pd
import importlib
import os
import quantstats as qs
import time
import math
import datetime
import empyrical as ey
import hashlib
from library.mydb import mydb
from library.config import config
from library.globalvar import *
import multiprocessing
import redis
import json
import pickle
import mmap
import calendar
from dateutil.rrule import rrule, DAILY

class market:
    #加载分红送股数据
    def load_dividend():
        cfg=config.getConfig('db','redis')
        redisPool = redis.ConnectionPool(host=cfg['host'],port=int(cfg['port']),password=cfg['password'],db=int(cfg['db']))
        client = redis.Redis(connection_pool=redisPool)           
        df=mydb.selectToDf('SELECT ts_code,record_date,ex_date,pay_date,stk_div,cash_div_tax FROM `tushare`.`astock_finance_dividend` where  div_proc="实施"','tushare')
        grouped=df.groupby("record_date")
        for date,group in grouped:
            group=group.drop_duplicates('ts_code',keep='last')
            group=group.set_index('ts_code')
            df_json = group.to_json()
            key="dividend_"+date
            client.set(key,df_json)
            # x=json.loads(client.get(key))
            # x=pd.DataFrame(x)
        client.set('dividend_state',time.time())
        return True

    
    def load_price(cache=True):
        cfg=config.getConfig('db','redis')
        redisPool = redis.ConnectionPool(host=cfg['host'],port=int(cfg['port']),password=cfg['password'],db=int(cfg['db']))
        client = redis.Redis(connection_pool=redisPool)        
        cache_path=PRICE_CACHE_DIR+"/bt_price"
        df_price=pd.DataFrame()
        if os.path.isfile(cache_path):
            #print('read cache---'+code)
            t = time.time()-os.path.getmtime(cache_path)
            if t<60*60*24 and cache: #缓存时间为12小时
                df_price=pd.read_pickle(cache_path)
            else:
                df_price=pd.DataFrame()
        
        if df_price.empty:
            df_price=AStock.getStockDailyPrice(fq='no')
            df_price=df_price.reset_index(drop=True)
            df_price.to_pickle(cache_path)
        
 
        for row in df_price.itertuples():
            key='market_no_'+row[2]+'_'+row[1]
            value=json.dumps(row)
            client.set(key,value)
        client.set('price_state',time.time())
        return df_price
     
     
    
    # def get_file_no(start_date="20170101",end_date="20230501",cache=True):
    #     market_path=PRICE_CACHE_DIR+"/bt_market_data_"+start_date+"_"+end_date
    #     market.load_data(start_date=start_date,end_date=end_date,cache=cache)
    #     with open(market_path, 'rb') as f:
    #         mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
    #         return mm.fileno()
        
    def load_mm_data(fileno):
        mm = mmap.mmap(fileno, 0, access=mmap.ACCESS_READ)
        data = pickle.loads(mm.read())
        return data
     
     
    def slice_date(start_date="20170101",end_date="20230501",slice_type='n'):
        slice_list=[]
        start_dt = datetime.datetime.strptime(start_date, '%Y%m%d')
        end_dt = datetime.datetime.strptime(end_date, '%Y%m%d')
            
        if slice_type=='n':
            slice_list=[(start_date,end_date)]
            pass
        
        elif slice_type=='y':
            current_year = start_dt.year
            while current_year <= end_dt.year:
                slice_list.append((str(current_year)+"0101",str(current_year)+"1231"))
                current_year += 1
                
        elif slice_type=='m':
            current_year = start_dt.year
            current_month = start_dt.month
            
            while current_year < end_dt.year or (current_year == end_dt.year and current_month <= end_dt.month):
                last_day = calendar.monthrange(current_year, current_month)[1]
                last_day_dt = datetime.datetime(year=current_year, month=current_month, day=last_day)
                slice_list.append((last_day_dt.strftime("%Y%m")+"01",last_day_dt.strftime("%Y%m%d")))
                current_month += 1
                if current_month > 12:
                    current_month = 1
                    current_year += 1
        
        elif slice_type=='d':
            for dt in rrule(DAILY, dtstart=start_dt, until=end_dt):
                slice_list.append((dt.strftime("%Y%m%d"),dt.strftime("%Y%m%d")))
        else:
            pass
        return slice_list
        
        
    
    def get_data(start_date="20170101",end_date="20230501",slice_type='n',now_date='20170101',cache=True):
        
        #这块实现的不对，太麻烦了，直接now_data[0:4]+"0101"啥的就行，但是懒得改了
        slice_list=market.slice_date(start_date=start_date,end_date=end_date,slice_type=slice_type)
        for slice_item in reversed(slice_list):
            start_date=slice_item[0]
            end_date=slice_item[1]
            if now_date>=start_date:
                break
            
            
        
        market_data={}
        market_path=PRICE_CACHE_DIR+"/bt_market_data_"+start_date+"_"+end_date
        if os.path.isfile(market_path):
            t = time.time()-os.path.getmtime(market_path)
            if cache:
                df_price=pd.read_pickle(market_path)
                f= open(market_path, 'rb')
                market_data = pickle.load(f)
                f.close()
        return market_data        
        
     
    #文件形式加载数据 
    def load_data(start_date="20170101",end_date="20230501",slice_type='n',cache=True): 
        slice_list=market.slice_date(start_date=start_date,end_date=end_date,slice_type=slice_type)
        df_div=mydb.selectToDf('SELECT ts_code,record_date,ex_date,pay_date,stk_div,cash_div_tax FROM `tushare`.`astock_finance_dividend` where  div_proc="实施"','tushare')
        price_cache_path=PRICE_CACHE_DIR+"/bt_price"
        df_price=pd.DataFrame()
        if os.path.isfile(price_cache_path):
                #print('read cache---'+code)
            t = time.time()-os.path.getmtime(price_cache_path)
            if t<60*60*24 and cache: #缓存时间为12小时
                df_price=pd.read_pickle(price_cache_path)
            else:
                df_price=pd.DataFrame()
            
        if df_price.empty:
            df_price=AStock.getStockDailyPrice(fq='no')
            df_price=df_price.reset_index(drop=True)
            df_price.to_pickle(price_cache_path)


        for slice_item in slice_list:
            start_date=slice_item[0]
            end_date=slice_item[1]
            market_data={}
            market_path=PRICE_CACHE_DIR+"/bt_market_data_"+start_date+"_"+end_date
            if os.path.isfile(market_path):
                t = time.time()-os.path.getmtime(market_path)
                if cache:
                    continue
                    # df_price=pd.read_pickle(market_path)
                    # f= open(market_path, 'rb')
                    # market_data = pickle.load(f)
                    # f.close()
                    # return market_data
    
            
            df_div_tmp=df_div[df_div.record_date>=start_date]
            df_div_tmp=df_div_tmp[df_div_tmp.pay_date<=end_date]
    
            grouped=df_div_tmp.groupby("record_date")
            for date,group in grouped:
                group=group.drop_duplicates('ts_code',keep='last')
                group=group.set_index('ts_code')
                df_json = group.to_json()
                key="dividend_"+date
                market_data[key]=df_json

    
            df_price_tmp=df_price[df_price.trade_date>=start_date]
            df_price_tmp=df_price_tmp[df_price_tmp.trade_date<=end_date]        
            
            for row in df_price_tmp.itertuples():
                key='market_no_'+row[2]+'_'+row[1]
                value=json.dumps(row)
                value=json.loads(value)
                price={
                    'ts_code':value[1],
                    'open':value[3],
                    'high':value[4],
                    'low':value[5],
                    'close':value[6],
                    'volume':value[10],
                    'amount':value[11],
                    'name':value[12],
                    'vwap':value[13],
                    'stop':value[14],
                    'upLimit':float(value[15]),
                    'downLimit':float(value[16]),
                }
                
                market_data[key]=price
    
    
            market_data['start_date']=start_date
            market_data['end_date']=end_date    
    
    
            f = open(market_path, 'wb')
            pickle.dump(market_data, f)
            print("writting "+market_path)
            f.close()
        
        return True

        
        
    def get_price(ts_code,trade_date,client=None):
        if client==None:
            cfg=config.getConfig('db','redis')
            redisPool = redis.ConnectionPool(host=cfg['host'],port=int(cfg['port']),password=cfg['password'],db=int(cfg['db']))
            client = redis.Redis(connection_pool=redisPool)   

        # cfg=config.getConfig('db','memcached')
        # client=memcache.Client([cfg['host']+':'+cfg['port']], debug=0)  

        key='market_no_'+trade_date+'_'+ts_code
        value=client.get(key)
        
        if value==None:
            return None
        
        
        value=json.loads(value)
        
 
        
        price={
            'ts_code':value[1],
            'open':value[3],
            'high':value[4],
            'low':value[5],
            'close':value[6],
            'volume':value[10],
            'amount':value[11],
            'name':value[12],
            'vwap':value[13],
            'stop':value[14],
            'upLimit':float(value[15]),
            'downLimit':float(value[16]),
            
            }
            
 
 
        # print(value)
        # print(price)
        # #exit()
        
        #df_price[['high','low','open','close','volume','stop','upLimit','downLimit','name']]
        return price
        
    
    #获取行情数据状态，即判断是否需要重新加载
    def get_state():
        cfg=config.getConfig('db','redis')
        redisPool = redis.ConnectionPool(host=cfg['host'],port=int(cfg['port']),password=cfg['password'],db=int(cfg['db']))
        client = redis.Redis(connection_pool=redisPool)       
        price_state=client.get('price_state')
        dividend_state=client.get('dividend_state')
        if price_state!=None:
            price_state=float(price_state.decode())
        if dividend_state!=None:
            dividend_state=float(dividend_state.decode())
        return price_state,dividend_state
 