import numpy as np
from library.astock import AStock
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

class market:
    def load_price(cache=True):
        cfg=config.getConfig('db','redis')
        redisPool = redis.ConnectionPool(host=cfg['host'],port=int(cfg['port']),password=cfg['password'],db=int(cfg['db']))
        client = redis.Redis(connection_pool=redisPool)        
        
        cache_path=PRICE_CACHE_DIR+"/bt_price"
        df_price=None
        if os.path.isfile(cache_path):
            #print('read cache---'+code)
            t = time.time()-os.path.getmtime(cache_path)
            if t<60*60*24*100 and cache: #缓存时间为12小时
                df_price=pd.read_pickle(cache_path)
        else:
            df_price=AStock.getStockDailyPrice(fq='qfq')
            df_price=df_price.reset_index()
            df_price.to_pickle(cache_path)
        
        df_price=df_price.reset_index()
        
        for row in df_price.itertuples():
            key='market_'+row[2]+'_'+row[1]
            value=json.dumps(row)
            client.set(key,value)

    
            # a=client.get(key)
            # print(a)
    
    
            # print(json.loads(a)[1])
            # exit()
        return df_price
        
        
    def get_price(ts_code,trade_date,client=None):
        if client==None:
            cfg=config.getConfig('db','redis')
            redisPool = redis.ConnectionPool(host=cfg['host'],port=int(cfg['port']),password=cfg['password'],db=int(cfg['db']))
            client = redis.Redis(connection_pool=redisPool)   
        key='market_'+ts_code+'_'+trade_date
        value=client.get(key)
        
        if value==None:
            return None
        
        
        value=json.loads(value)
        
        
        price={
            'high':value[3],
            'low':value[4],
            'open':value[5],
            'close':value[6],
            'volume':value[7],
            'stop':value[8],
            'upLimit':value[9],
            'downLimit':value[10],
            'name':value[11],
        }
        #df_price[['high','low','open','close','volume','stop','upLimit','downLimit','name']]
        return price
        
 