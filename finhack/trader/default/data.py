from finhack.library.config import Config
from runtime.constant import *
from finhack.market.astock.astock import AStock
import redis
import json
import pickle
import pandas as pd
import os
import time
from datetime import datetime
import calendar
import finhack.library.log as Log
from finhack.library.mydb import mydb
from finhack.library.mycache import mycache
from finhack.core.classes.dictobj import DictObj


class Data:
    def init_data(cache=True):
        data_updated_time=mycache.get('data_updated_time')
        #每60分钟重新加载一次
        #if data_updated_time==None or (time.time()-data_updated_time)>60*60*1:
        Data.init_astock_info(cache=True)
        #Data.init_astock_event(cache=True)
        mycache.set('data_updated_time',time.time())
    
    def init_astock_info(cache=True):
        df_price=Data.load_astock_daily_info(cache=True)
        Data.init_astock_daily_info_to_file(df_price)
        Data.init_astock_daily_info_to_redis(df_price)
        Data.init_astock_dividend_to_redis(client=None)
    

    #加载每日基础数据
    def load_astock_daily_info(cache=True):
        cache_path=PRICE_CACHE_DIR+"/astock_daily_price"
        df_price=pd.DataFrame()
        if os.path.isfile(cache_path):
            t = time.time()-os.path.getmtime(cache_path)
            if t<60*60*12 and cache: #缓存时间为12小时
                df_price=pd.read_pickle(cache_path)
            else:
                df_price=pd.DataFrame()
            
        if df_price.empty:
            df_price=AStock.getStockDailyPrice(fq='no')
            df_price=df_price.reset_index(drop=True)
            df_price.to_pickle(cache_path)   
        return df_price
        
        
        
    #对日期进行切片
    #n不切片，y按年切片，m按月切片，d按日切片
    def slice_date(start_date="20000101",end_date="20991231",slice_type='n'):
        slice_list=[]
        start_dt = datetime.strptime(start_date, '%Y%m%d')
        end_dt = datetime.strptime(end_date, '%Y%m%d')
            
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
                last_day_dt = datetime(year=current_year, month=current_month, day=last_day)
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
        
    
    #把每日基本信息写入文件缓存
    #文件格式 astock_daily_info_20000101_20000131
    #daily_info[astock_daily_info_20000101_002624.SZ]={
                #     'ts_code':value[1],
                #     'open':value[3],
                #     'high':value[4],
                #     'low':value[5],
                #     'close':value[6],
                #     'volume':value[10],
                #     'amount':value[11],
                #     'name':value[12],
                #     'vwap':value[13],
                #     'stop':value[14],
                #     'upLimit':float(value[15]),
                #     'downLimit':float(value[16]),
                # }
    #daily_info['dividend_20000101']={
                #   "record_date":{"000420.SZ":"20000418","600177.SH":"20000418"},
                #   "ex_date":{"000420.SZ":"20000419","600177.SH":"20000419"},
                #   "pay_date":{"000420.SZ":"20000420","600177.SH":"20000420"},
                #   "stk_div":{"000420.SZ":0.0,"600177.SH":0.0},
                #   "cash_div_tax":{"000420.SZ":0.1,"600177.SH":0.3}}
    def init_astock_daily_info_to_file(df_price,cache=True):
        slice_list=Data.slice_date(start_date='20000101',end_date=datetime.now().strftime("%Y%m%d"),slice_type='m')
        df_div=mydb.selectToDf('SELECT ts_code,record_date,ex_date,pay_date,stk_div,cash_div_tax FROM `tushare`.`astock_finance_dividend` where  div_proc="实施"','tushare')
        
        

        
        for slice_item in slice_list:
            daily_info={}
            start_date=slice_item[0]
            end_date=slice_item[1]
            daily_info_path=PRICE_CACHE_DIR+"/astock_daily_info_"+start_date+"_"+end_date

            if slice_item == slice_list[-1]:
                if os.path.exists(daily_info_path):
                    t = time.time()-os.path.getmtime(daily_info_path)
                    if t<60*60*12 and cache: #缓存时间为12小时
                        continue
                else:
                    pass
            elif os.path.exists(daily_info_path):
                continue
            
            
            df_price_tmp=df_price[df_price.trade_date>=start_date]
            df_price_tmp=df_price_tmp[df_price_tmp.trade_date<=end_date]        
            
            #print(df_price_tmp)
            
            df_div_tmp=df_div[df_div.record_date>=start_date]
            df_div_tmp=df_div_tmp[df_div_tmp.pay_date<=end_date]
    
            grouped=df_div_tmp.groupby("record_date")
            for date,group in grouped:
                group=group.drop_duplicates('ts_code',keep='last')
                group=group.set_index('ts_code')
                df_json = group.to_json()
                key="dividend_"+date
                daily_info[key]=df_json      
                #print(df_json)
            
            for row in df_price_tmp.itertuples():
                key='astock_daily_info_'+row[2]+'_'+row[1]
                value=json.dumps(row)
                value=json.loads(value)
                info={
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
                daily_info[key]=info
            daily_info['start_date']=start_date
            daily_info['end_date']=end_date  
            Log.logger.info("writting "+daily_info_path)
            f = open(daily_info_path, 'wb')
            pickle.dump(daily_info, f)
            f.close()
    
    
    def init_astock_daily_info_to_redis(df_price,client=None):
        if client==None:
            cfg=Config.get_config('db','redis')
            redisPool = redis.ConnectionPool(host=cfg['host'],port=int(cfg['port']),password=cfg['password'],db=int(cfg['db']))
            client = redis.Redis(connection_pool=redisPool)           
        
        enddate=client.get('market_astock_price_enddate')
        if enddate!=None:
            enddate=str(enddate.decode())        
        
        now_date=df_price['trade_date'].max()
        if enddate==None:
            pass
        elif now_date>enddate:
            print(now_date)
            print(enddate)
            df_price=df_price[df_price['trade_date']>enddate]
        else:
            return True
        
        for row in df_price.itertuples():
            #Pandas(Index=0, ts_code='000001.SZ', trade_date='20000104', open=17.5, high=18.55, low=17.2, close=18.29, pre_close=17.45, change=0.84, returns=4.81, volume=82161.0, amount=147325.3568, name='平安银行', vwap=17.931298166855544, stop=0, upLimit=20.12, downLimit=16.46, adj_factor='21.662')
            date_obj = datetime.strptime(row[2], '%Y%m%d')
            event_date = date_obj.strftime('%Y%m%d')
            daily_key='astock_daily_'+event_date+'_'+row[1]
            daily_value=json.dumps(row)
            client.set(daily_key,daily_value)
                
                
            # open_key='astock_daily_'+event_date+'_open'+'_'+row[1]
            # client.set(open_key,row[3])
            
            # close_key='astock_daily_'+event_date+'_close'+'_'+row[1]
            # client.set(close_key,row[6])
                
            #astock_daily_2000-01-04_000001.SZ [0, "000001.SZ", "20000104", 17.5, 18.55, 17.2, 18.29, 17.45, 0.84, 4.81, 82161.0, 147325.3568, "\u5e73\u5b89\u94f6\u884c", 17.931298166855544, 0, 20.12, 16.46, "21.662"]
            #astock_daily_2000-01-04 09:30:00_000001.SZ 17.5
            #astock_daily_2000-01-04 15:00:00_000001.SZ 18.29
                
        client.set('market_astock_price_enddate',now_date) 
    
    
    def init_astock_dividend_to_redis(client=None):
        if client==None:
            cfg=Config.get_config('db','redis')
            redisPool = redis.ConnectionPool(host=cfg['host'],port=int(cfg['port']),password=cfg['password'],db=int(cfg['db']))
            client = redis.Redis(connection_pool=redisPool)    
        current_time = time.time()
        dividend_state = client.get('dividend_state')

        # 如果dividend_state不存在或者时间间隔大于12小时，则进行更新
        #if True:
        if dividend_state is None or (current_time - float(dividend_state)) > 43200:
            df=mydb.selectToDf('SELECT ts_code,record_date,ex_date,pay_date,stk_div,cash_div_tax FROM `tushare`.`astock_finance_dividend` where  div_proc="实施"','tushare')
            grouped=df.groupby("record_date")
            for date,group in grouped:
                group=group.drop_duplicates('ts_code',keep='last')
                group=group.set_index('ts_code')
                df_json = group.to_json()
                key="astock_dividend_"+date
                client.set(key,df_json)
                x=json.loads(client.get(key))
                x=pd.DataFrame(x)
            client.set('dividend_state',time.time())
        return True    
    
    
    
    def get_dividend(context,date=None):
        if date==None:
            date=context.current_dt.strftime('%Y%m%d')
        if context.data.data_source=='file':
            key="dividend_"+date
            #DictObj(ts_code='300067.SZ', open=4.34, high=4.45, low=4.29, close=4.45, volume=302997.06, amount=132608.013, name='安诺其', vwap=4.37654440024783, stop=0, upLimit=4.73, downLimit=3.87)
            if key in context.data.daily_info:
                dividend=context.data.daily_info[key]
                dividend=json.loads(dividend)
            else:
                dividend=None
            return dividend
        else:
            key="astock_dividend_"+date
            client=context.data.client
            value=client.get(key)
            if value!=None:
                value=json.loads(value)
            return value
    
    
    
    def get_data_from_file(start_date="20170101",end_date="20230701",slice_type='n',now_date='20170101',filename='',cache=True):
        #这块实现的不对，太麻烦了，直接now_data[0:4]+"0101"啥的就行，但是懒得改了
        slice_list=Data.slice_date(start_date=start_date,end_date=end_date,slice_type=slice_type)
        for slice_item in reversed(slice_list):
            start_date=slice_item[0]
            end_date=slice_item[1]
            if now_date>=start_date:
                break
        market_data={}
        
        market_path=PRICE_CACHE_DIR+"/"+filename+"_"+start_date+"_"+end_date
        if os.path.isfile(market_path):
            t = time.time()-os.path.getmtime(market_path)
            if cache:
                df_price=pd.read_pickle(market_path)
                f= open(market_path, 'rb')
                market_data = pickle.load(f)
                f.close()
        else:
            Log.logger.error(f"{market_path} not found!")
            
        return market_data           
        
        
        
        

    
    
    def get_daily_info(code,context,date=None):
        if date==None:
            date=context.current_dt.strftime('%Y%m%d')
            
        if context.data.data_source=='file':
            info_key='astock_daily_info_'+date+'_'+code
            #DictObj(ts_code='300067.SZ', open=4.34, high=4.45, low=4.29, close=4.45, volume=302997.06, amount=132608.013, name='安诺其', vwap=4.37654440024783, stop=0, upLimit=4.73, downLimit=3.87)
            if info_key in context.data.daily_info:
                info=context.data.daily_info[info_key]
            else:
                info=None
            return info
        else:
            client=context.data.client
            info_key='astock_daily_'+date+'_'+code
            value=client.get(info_key)
            if value==None:
                return None
            else:
                value=json.loads(value)
                info=DictObj({
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
                    
                })
                return info

    #根据当前时间获取价格
    def get_price(code,context=None):
        if True:
            #时间是9点半以前，取昨天数据
            if context.current_dt.strftime('%H:%M:%S')<'09:30:00':
                if context.previous_date!=None:
                    date=context.previous_date.strftime('%Y%m%d')
                    info=Data.get_daily_info(code=code,context=context,date=date)
                    return None if info==None else info.close
            #否则取今天数据
            else:
                date=context.current_dt.strftime('%Y%m%d')
                info=Data.get_daily_info(code=code,context=context,date=date)
                if info==None:
                    return None
                elif context.current_dt.strftime('%H:%M:%S')<'15:00:00':
                    return info.open
                else:
                    return info.close
        else:
            pass
            #print(context.data.client)
  
    
   
    
    
    
    
    
    
            
        
    # def get_daily_info_from_file():
    #     pass
        
        # #这块实现的不对，太麻烦了，直接now_data[0:4]+"0101"啥的就行，但是懒得改了
        # slice_list=market.slice_date(start_date=start_date,end_date=end_date,slice_type=slice_type)
        # for slice_item in reversed(slice_list):
        #     start_date=slice_item[0]
        #     end_date=slice_item[1]
        #     if now_date>=start_date:
        #         break
            
            
        
        # market_data={}
        # market_path=PRICE_CACHE_DIR+"/bt_market_data_"+start_date+"_"+end_date
        # if os.path.isfile(market_path):
        #     t = time.time()-os.path.getmtime(market_path)
        #     if cache:
        #         df_price=pd.read_pickle(market_path)
        #         f= open(market_path, 'rb')
        #         market_data = pickle.load(f)
        #         f.close()
        # return market_data   
    
 
    
    #这里是想回放订单薄，发现不好搞，先注释掉
    # def replay_astock_quote(start_time,end_time,calendar,load_price=True,market='astock',cache=True):
    #     quote_list=[]
    #     for date in calendar[market]:
    #         quote_list.append(date)
        
    #     return quote_list
    
    
    # def replay_quote(start_time,end_time,calendar,load_price=True,market_list='astock',cache=True):
    #     quote_list={}
    #     if not isinstance(market_list, list):
    #         market_list = [market_list]
    #     for market in market_list:
    #         if load_price:
    #             Quote.load_price(market_list=[market])
    #         method_name = f"replay_{market}_quote"  # 构造方法名称
    #         if hasattr(Quote, method_name):  # 检查实例 q 是否有名为 method_name 的方法
    #             method = getattr(Quote, method_name)  # 获取这个方法的引用
    #             sub_quote_list=method(start_time,end_time,calendar,load_price=True,market='astock',cache=True) 
    #             quote_list[market]=sub_quote_list
    #     return quote_list
    
    

 
    
    
