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
import memcache

class market:
    
    #加载分红送股数据
    def load_dividend():
        cfg=config.getConfig('db','redis')
        redisPool = redis.ConnectionPool(host=cfg['host'],port=int(cfg['port']),password=cfg['password'],db=int(cfg['db']))
        client = redis.Redis(connection_pool=redisPool)           

        # cfg=config.getConfig('db','memcached')
        # client=memcache.Client([cfg['host']+':'+cfg['port']], debug=0)  

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
        
        # cfg=config.getConfig('db','memcached')
        # client=memcache.Client([cfg['host']+':'+cfg['port']], debug=0)        
        
        
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

    
            # a=client.get(key)
            # print(a)
    
    
            # print(json.loads(a)[1])
            # exit()
        client.set('price_state',time.time())
        return df_price
        
        
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
 