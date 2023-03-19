# coding=utf-8
import pandas as pd
import sys
import datetime
import os
from pandarallel import pandarallel
sys.path.append("..")
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor, wait, ALL_COMPLETED
import traceback
from library.mydb import mydb
import numpy as np
import hashlib
from library.globalvar import *
import time
import threading
# 股票信息获取模块
from datetime import timedelta

class ProcessPoolExecutor2(ProcessPoolExecutor):
    '''
    重写线程池修改队列数
	'''
    def __init__(self, max_workers=None, thread_name_prefix=''):
        super().__init__(max_workers, thread_name_prefix)
        # 队列大小为最大线程数的两倍
        self._work_queue = queue.Queue(self._max_workers * 2)


class AStock:
    def getStockCodeList(db='tushare'):
        sql = "select ts_code from astock_basic;";
        try:
            df_code=mydb.selectToDf(sql,'tushare')
            return df_code
        except Exception as e:
            print("MySQL getStockCodeList Error:%s" % str(e))  
            return False
            
            
            
    def getIndexMember(index='000300.SH',trade_date='20221031'):
        if trade_date=='':
            sql = "select con_code from astock_index_weight where ts_code='%s'" % (index)
        else:
            sql = "select con_code from astock_index_weight where ts_code='%s' and trade_date='%s' " % (index,trade_date);
        
        
        try:
            df=mydb.selectToDf(sql,'tushare')
            return df['con_code'].tolist()
        except Exception as e:
            print("MySQL getStockCodeList Error:%s" % str(e))  
            return False        
            
    
    def getIndexPrice(ts_code='000300.SH',start_date=None,end_date=None):
        c1=""
        c2=""
        if not start_date==None:
            c1=" and trade_date>='%s' " % (start_date)
        if not end_date==None:
            c2=" and trade_date<='%s' " % (end_date)
        
        sql = "select trade_date,close from astock_index_daily where ts_code='%s' %s %s order by trade_date asc" % (ts_code,c1,c2)
        
        #print(sql)
        
        try:
            df=mydb.selectToDf(sql,'tushare')
            return df
        except Exception as e:
            print("MySQL getStockCodeList Error:%s" % str(e))  
            return False   
        return df    
    
    
    
    
    def getTableDataByCode(table,ts_code,where="",db='tushare'):
        sql="select * from "+table+" where ts_code='"+ts_code+"' "+where
        result=mydb.select(sql,'tushare')
        df_date = pd.DataFrame(list(result))
        df_date=df_date.reset_index(drop=True)
        return df_date


    def getTableData(table,where="",db='tushare'):
        sql="select * from "+table+" where 1=1 "+where
        #print(sql)
        result=mydb.select(sql,'tushare')
        df_date = pd.DataFrame(list(result))
        df_date=df_date.reset_index(drop=True)
        return df_date    
    
    
    
    #获取股票日线行情
    def getStockDailyPrice(code_list=[],where="",startdate='',enddate='',fq='hfq',db='tushare',cache=True):
        df=[]
        
        result=[]
        if len(code_list)==0:
            df_code=AStock.getStockCodeList()
            code_list=df_code['ts_code'].tolist()



        hashstr=','.join(code_list)+'-'+where+'-'+startdate+'-'+enddate+'-'+fq+'-'+db
        md5=hashlib.md5(hashstr.encode(encoding='utf-8')).hexdigest()
        cache_path=PRICE_CACHE_DIR+md5
        if os.path.isfile(cache_path):
            #print('read cache---'+cache_path)
            #print(hashstr)
            t = time.time()-os.path.getmtime(cache_path)
            if t<60*60*12 and cache: #缓存时间为12小时
                df=pd.read_pickle(cache_path)
                return df


        with ProcessPoolExecutor(max_workers=5) as pool:
            def get_result(task):
                exception = task.exception()
                if exception:
                    # 如果exception获取到了值，说明有异常.exception就是异常类
                    print(exception)   
                else:
                    result.append(task.result())
            tasklist=[]
            for code in code_list:
                mytask=pool.submit(AStock.getStockDailyPriceByCode,code=code,where=where,startdate=startdate,enddate=enddate,fq=fq)
                mytask.add_done_callback(get_result)
                tasklist.append(mytask)
            wait(tasklist, return_when=ALL_COMPLETED)
            
        print('all completed')
        if len(result)==0:
            return pd.DataFrame()
        df=pd.concat(result)
        
        df=df.sort_values(by=['ts_code','trade_date'], ascending=[True,True])
        
        df.to_pickle(cache_path)
        return df
        
    
    def checkLimit():
        pass

    def getStockDailyPriceByCode(code,where="",startdate='',enddate='',fq='hfq',db='tushare',cache=True):
        try:
            
            datewhere1=''
            datewhere2=''
            if startdate!='':
                datewhere1=' and trade_date>='+startdate+' '
            if enddate!='':
                datewhere2=' and trade_date<='+enddate+' '
            datewhere=datewhere1+datewhere2
            
            where=where+datewhere
            
            #print('getStockDailyPriceByCode---'+code)
            hashstr=code+'-'+where+'-'+startdate+'-'+enddate+'-'+fq
            md5 = hashlib.md5(hashstr.encode(encoding='utf-8')).hexdigest()
            cache_path=PRICE_CACHE_DIR+code+'_'+md5
     
  
            try:
                if os.path.isfile(cache_path):
                    #print('read cache---'+code+','+cache_path)
                    t = time.time()-os.path.getmtime(cache_path)
                    if t<60*60*12 and cache: #缓存时间为12小时
                        df=pd.read_pickle(cache_path)
                        return df
            except Exception as e:
                print(str(e))
                print(cache_path)

          
            if where.strip()!="" and where.strip().lower()[0:3] !='and':
                where=' and '+where
                
            
            df_price=AStock.getTableDataByCode('astock_price_daily',code,where)
            df_price.drop_duplicates(subset='trade_date',keep='first',inplace=True)
    
            if(df_price.empty):
                return df_price

            calendar=AStock.getTableData('astock_trade_cal',datewhere.replace('trade_date','cal_date')+' and is_open=1')
            calendar.rename(columns={'cal_date':'trade_date'}, inplace = True)
            calendar=calendar[['trade_date']]
            last_date=max(df_price['trade_date'])
            calendar=calendar[calendar.trade_date<=last_date]  
        
            df_adj = AStock.getTableDataByCode('astock_price_adj_factor',code,'')
            df_adj = pd.merge(calendar,df_adj, on='trade_date',how='left')
            df_adj = df_adj.fillna(method='ffill')
            df_adj=df_adj.drop('ts_code',axis=1)
            
            
            df_name=AStock.getTableDataByCode('astock_namechange',code,datewhere.replace('trade_date','ann_date'))
            
 
            
            if df_name.empty:
                name=AStock.getTableDataByCode('astock_basic',code,'')
                df_price['name']=name['name'].values[0]
            else:
                df_name.rename(columns={'start_date':'trade_date'}, inplace = True)
                df_name=df_name[['trade_date','name']]
                df_price = pd.merge(df_price,df_name,how = 'outer',on=['trade_date'])
                df_price=df_price.sort_values('trade_date')
                df_price['name']=df_price['name'].fillna(method='ffill')
                df_price=df_price.dropna(subset=['ts_code'])

            
            
            if(df_adj.empty): 
                    adj_factor=1
                    df=df_price
                    df["adj_factor"]=1
            else:
                if fq=='qfq':
                    adj_factor=float(df_adj.tail(1).adj_factor)
                else:
                    adj_factor=1
                    
                df = pd.merge(df_price,df_adj,how = 'right',on=['trade_date'])
                df["open"]=df["open"].astype(float)*df["adj_factor"].astype(float)/adj_factor
                df["high"]=df["high"].astype(float)*df["adj_factor"].astype(float)/adj_factor
                df["low"]=df["low"].astype(float)*df["adj_factor"].astype(float)/adj_factor
                df["close"]=df["close"].astype(float)*df["adj_factor"].astype(float)/adj_factor
                df["pre_close"]=df["pre_close"].astype(float)*df["adj_factor"].astype(float)/adj_factor
                df["change"]=df["change"].astype(float)*df["adj_factor"].astype(float)/adj_factor
                df["pct_chg"]=df["pct_chg"].astype(float)
                df["vol"]=df["vol"].astype(float)
                df['amount']=df['amount'].astype(float)
                df["vwap"]=(df['amount'].astype(float)*1000)/(df['vol'].astype(float)*100+1) 
                df["stop"]=pd.isna(df['close']).astype(int)
                df["lh_limit"]=pd.isna(df['high']==df['low']).astype(int)
                df.rename(columns={'vol':'volume','pct_chg':'returns'}, inplace = True)





            df['ts_code']=code
            df.drop_duplicates('trade_date',inplace = True)
            df=df.sort_values(by='trade_date', ascending=True)
            df=df.fillna(method='ffill')
            df=df.dropna(subset=['adj_factor'])
    
    
            df['name']=df['name'].fillna(method='bfill')
            df['name']=df['name'].fillna("")
            
            if code[0:3]=='300' or code[0:3]=='688':
                limit=0.20
                df["upLimit"]=limit       
                df["downLimit"]=limit                
            elif code[0:1]=='7' or code[0:1]=='8':
                limit=0.30
                df["upLimit"]=limit       
                df["downLimit"]=limit                
            else:
                limit=0.10
                df["upLimit"]=limit       
                df["downLimit"]=limit
                df["upLimit"] =np.where(df.name.str.contains('ST'),0.05,limit)
                df["downLimit"] =np.where(df.name.str.contains('ST'),0.05,limit)
                
            df["upLimit"]=round(df['close'].shift(1)*(1+df["upLimit"]),2)
            df["downLimit"]=round(df['close'].shift(1)*(1-df["downLimit"]),2)
            
            del df_adj
            del calendar
         
            df.to_pickle(cache_path )
            return df
        except Exception as e:
            print("error")
            print("err exception is %s" % traceback.format_exc())
            traceback.print_exc()
            return pd.DataFrame()
        
        
        
        
    def alignStockFactors(df,table,date,filed,conv=0,db='tushare'):
        df=df.copy()
        df=df.reset_index()
        ts_code=df['ts_code'].tolist()[0]
        df.drop_duplicates('trade_date',inplace = True)
 
        
        if(filed=='*'):
            df_factor=mydb.selectToDf("select * from "+table+" where ts_code='"+ts_code+"'",'tushare')
            filed=mydb.selectToDf("select COLUMN_NAME from information_schema.COLUMNS where table_name = '"+table+"'",'tushare')
            filed=filed['COLUMN_NAME'].tolist()
            filed=",".join(filed)
        else:
            df_factor=mydb.selectToDf("select "+date+","+filed+" from "+table+" where ts_code='"+ts_code+"'",'tushare')
        
        
        if isinstance(df_factor, bool) or df_factor.empty:
            return pd.DataFrame()
        
        #去重
        try:
            df_factor = df_factor[~df_factor[date].duplicated()]
        except Exception as e:
            print(df_factor)
        
        #财务报表中的时间，需要+1处理
        if conv==3:
            df_factor[date]=df_factor[date].astype(str)
            df_factor[date]=pd.to_datetime(df_factor[date],format='%Y%m%d',errors='coerce')
            df_factor[date]=df_factor[date]+timedelta(days=1)
            df_factor[date]=df_factor[date].astype(str)
            df_factor[date]=df_factor[date].map(lambda x: x.replace('-',''))  
            df_factor['trade_date']=df_factor[date].map(lambda x: x.replace('-',''))

        
        if not 'pandas' in str(type(df_factor)) or df_factor.empty:
            df_res=df
            for f in filed.split(','):
                df[f]=0
            return df_res

        #转换时间,将yyyy-mm-dd转为yyyymmdd
        if conv==1:
            df_factor[date]=df_factor[date].astype(str)
            df_factor['trade_date']=df_factor[date].map(lambda x: x.replace('-',''))
            
        df_res=pd.merge(df, df_factor, how='left', on='trade_date',validate="one_to_many", copy=True, indicator=False)
        df_res.drop_duplicates('trade_date',inplace = True)
        
        if conv==2: #不填充
            pass
        else:
            df_res=df_res.fillna(method='ffill') # conv=0向下填充
        
        df_res=df_res.set_index('index')
        
        del df_factor
        return df_res
        

