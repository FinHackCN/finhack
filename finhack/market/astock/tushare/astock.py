# coding=utf-8
import pandas as pd
import sys
import datetime
import os
import traceback
import numpy as np
import hashlib
import time
import threading
# 股票信息获取模块
from datetime import timedelta
from runtime.constant import *
from finhack.library.db import DB  # 替换为统一的DB类
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor, wait, ALL_COMPLETED



def getStockCodeList(strict=True,db='tushare'):
        #
        if strict:
            sql="select ts_code from astock_price_daily GROUP BY ts_code"
        else:
            sql = "select ts_code from astock_basic;"
        try:
            df_code=DB.select_to_df(sql, db)  # 使用DB类替代mydb
            return df_code
        except Exception as e:
            print(f"获取股票代码列表错误: {str(e)}")  
            return False
            
            
            
def getStockIndustry():
    df=getTableData("astock_basic")
    df=df[['ts_code','industry']]
    return df
            
            
def getIndexMember(index='000300.SH',trade_date='20221031'):
        if trade_date=='':
            sql = "select con_code from astock_index_weight where ts_code='%s'" % (index)
        else:
            sql = "select con_code from astock_index_weight where ts_code='%s' and trade_date='%s' " % (index,trade_date);
        
        
        try:
            df=DB.select_to_df(sql, 'tushare')  # 使用DB类替代mydb
            return df['con_code'].tolist()
        except Exception as e:
            print(f"获取指数成分错误: {str(e)}")  
            return False        
            
    
def getIndexPrice(ts_code='000300.SH',start_date=None,end_date=None):
        c1=""
        c2=""
        if not start_date==None:
            c1=" and trade_date>='%s' " % (start_date)
        if not end_date==None:
            c2=" and trade_date<='%s' " % (end_date)
        
        sql = "select * from astock_index_daily where ts_code='%s' %s %s order by trade_date asc" % (ts_code,c1,c2)
        
        try:
            df=DB.select_to_df(sql, 'tushare')  # 使用DB类替代mydb
            df["open"]=df["open"].astype(float)
            df["high"]=df["high"].astype(float)
            df["low"]=df["low"].astype(float)
            df["close"]=df["close"].astype(float)
            df["pre_close"]=df["pre_close"].astype(float)
            df["change"]=df["change"].astype(float)
            df["pct_chg"]=df["pct_chg"].astype(float)
            df["vol"]=df["vol"].astype(float)
            df['amount']=df['amount'].astype(float)
            return df
        except Exception as e:
            print(f"获取指数价格错误: {str(e)}")  
            return False   
        return df    
    
    
    
    
def getTableDataByCode(table,ts_code,where="",db='tushare'):
        sql="select * from "+table+" where ts_code='"+ts_code+"' "+where
        result=DB.select(sql, db)  # 使用DB类替代mydb
        df_date = pd.DataFrame(list(result))
        df_date=df_date.reset_index(drop=True)
        return df_date


def getTableData(table,where="",db='tushare'):
        sql="select * from "+table+" where 1=1 "+where
        #print(sql)
        result=DB.select(sql, db)  # 使用DB类替代mydb
        df_date = pd.DataFrame(list(result))
        df_date=df_date.reset_index(drop=True)
        return df_date    
    
    
    
    #获取股票日线行情
def getStockDailyPrice(code_list=[],where="",startdate='',enddate='',fq='hfq',db='tushare',cache=True):
        df=[]
        
        result=[]
        if len(code_list)==0:
            df_code=getStockCodeList()
            code_list=df_code['ts_code'].tolist()



        hashstr=','.join(code_list)+'-'+where+'-'+startdate+'-'+enddate+'-'+fq+'-'+db
        md5=hashlib.md5(hashstr.encode(encoding='utf-8')).hexdigest()
        cache_path=PRICE_CACHE_DIR+md5
        if os.path.isfile(cache_path):
            print('read cache---'+cache_path)
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
                mytask=pool.submit(getStockDailyPriceByCode,code=code,where=where,startdate=startdate,enddate=enddate,fq=fq,db=db,cache=cache)
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
                
            
            df_price=getTableDataByCode('astock_price_daily',code,where)
            df_price.drop_duplicates(subset='trade_date',keep='first',inplace=True)
            #df_price['ts_code']=code
            if(df_price.empty):
                return df_price






            calendar=getTableData('astock_trade_cal',datewhere.replace('trade_date','cal_date')+' and is_open=1')
            calendar.rename(columns={'cal_date':'trade_date'}, inplace = True)
            calendar=calendar[['trade_date']]
            last_date=max(df_price['trade_date'])
            calendar=calendar[calendar.trade_date<=last_date]  
        
            df_adj = getTableDataByCode('astock_price_adj_factor',code,'')
            
            # print(111)
            # print(df_adj)
                       
          
          
            if not df_adj.empty:
                first_adj=float(df_adj.iloc[0].adj_factor)
                last_adj=float(df_adj.iloc[-1].adj_factor)
                
                df_adj = pd.merge(calendar,df_adj, on='trade_date',how='left')
                
                
                

                df_adj = df_adj.ffill()
                df_adj=df_adj.drop('ts_code',axis=1)
            else:
                 first_adj=1
                 last_adj=1
            # print(df_adj)
            
            
            df_name=getTableDataByCode('astock_namechange',code,datewhere.replace('trade_date','ann_date'))
            
 
            
            if df_name.empty:
                name=getTableDataByCode('astock_basic',code,'')
                if not 'name' in name:
                    print(code+" can't find name")
                    df_price['name']=code
                else:
                    df_price['name']=name['name'].values[0]
            else:
                df_name.rename(columns={'start_date':'trade_date'}, inplace = True)
                df_name=df_name[['trade_date','name']]
                df_price = pd.merge(df_price,df_name,how = 'outer',on=['trade_date'])
                df_price=df_price.sort_values('trade_date')
                df_price['name']=df_price['name'].ffill()
                df_price=df_price.dropna(subset=['ts_code'])

            
            
               
            df_updown=getTableDataByCode('astock_price_stk_limit',code,'')
            
            
            if df_updown.empty or df_price.empty:
                return pd.DataFrame()
            df_price = pd.merge(df_price,df_updown, on='trade_date',how='left')
            
            df_price.rename(columns={'ts_code_x':'ts_code'}, inplace = True)


            df_price['name']=df_price['name'].bfill()
            df_price['name']=df_price['name'].fillna("")


            df=df_price
            df["adj_factor"]=1
            df["open"]=df["open"].astype(float)
            df["high"]=df["high"].astype(float)
            df["low"]=df["low"].astype(float)
            df["close"]=df["close"].astype(float)
            df["pre_close"]=df["pre_close"].astype(float)
            df["change"]=df["change"].astype(float)
            df["pct_chg"]=df["pct_chg"].astype(float)
            df["vol"]=df["vol"].astype(float)
            df['amount']=df['amount'].astype(float)
            df["vwap"]=(df['amount'].astype(float)*1000)/(df['vol'].astype(float)*100+1) 
            df["stop"]=pd.isna(df['close']).astype(int)
            #df["lh_limit"]=pd.isna(df['high']==df['low']).astype(int)
            df.rename(columns={'vol':'volume','pct_chg':'returns'}, inplace = True)


            df["upLimit"]=df['close'].shift(1)
            df["downLimit"]=df['close'].shift(1)
            
            def updown(x,t="up"):
                
                if x.ts_code[0:3]=='300':
                    limit=0.20
                    if x.trade_date<"2020824":
                        limit=0.10
                elif x.ts_code[0:3]=='688':
                    limit=0.20
                elif x.ts_code[0:1]=='7' or x.ts_code[0:1]=='8':
                    limit=0.30
                else:
                    limit=0.10
                    if "ST" in x['name'] or "st" in x['name']:
                        limit=0.05
                     
                     
                if t=="up":
                    if pd.isnull(x.up_limit)  and not pd.isnull(x.upLimit):
                        return round(x.upLimit*(1+limit),2)
                    else:
                        return x.up_limit
                else:
                    if pd.isnull(x.down_limit) and not pd.isnull(x.downLimit) :
                        return round(x.downLimit*(1-limit),2)
                    else:
                        return x.down_limit
            
            
            # df_price.at[0,'upLimit']=9999
            # df_price.at[0,'downLimit']=0
 
 

 
            df["upLimit"]= df.apply(lambda x:updown(x,"up") , axis=1)
            df["downLimit"]= df.apply(lambda x:updown(x,"down") , axis=1)
            
                    


             
            if(df_adj.empty): 
                    df=df_price
                    df["adj_factor"]=1
            else:
                    df=df.drop("adj_factor",axis=1)
                    df = pd.merge(df,df_adj,how = 'right',on=['trade_date'])   
                    df=df.dropna(subset=['ts_code'])
                    
                    if fq=="no":
                        df["open"]=df["open"].astype(float)
                        df["high"]=df["high"].astype(float)
                        df["low"]=df["low"].astype(float)
                        df["close"]=df["close"].astype(float)
                        df["pre_close"]=df["pre_close"].astype(float)
                        
                        df["upLimit"]=df["upLimit"].astype(float)
                        df["downLimit"]=df["downLimit"].astype(float)
                        
                    elif fq=="qfq":
                        #前复权价格 = close * adj_factor / last_adj

                        df["open"]=df["open"].astype(float)*df["adj_factor"].astype(float)/last_adj
                        df["high"]=df["high"].astype(float)*df["adj_factor"].astype(float)/last_adj
                        df["low"]=df["low"].astype(float)*df["adj_factor"].astype(float)/last_adj
                        df["close"]=df["close"].astype(float)*df["adj_factor"].astype(float)/last_adj
                        df["pre_close"]=df["pre_close"].astype(float)*df["adj_factor"].astype(float)/last_adj
                        
                        df["upLimit"]=df["upLimit"].astype(float)*df["adj_factor"].astype(float)/last_adj
                        df["downLimit"]=df["downLimit"].astype(float)*df["adj_factor"].astype(float)/last_adj
                        
    
                    else:
                        #后复权价格 = close × adj_factor / first_adj
                        #有些算法是直接close × adj_factor，这里除去了首次的复权因子，整体无影响，但跟行情软件会差异较大
                        df["open"]=df["open"].astype(float)*df["adj_factor"].astype(float)/first_adj
                        df["high"]=df["high"].astype(float)*df["adj_factor"].astype(float)/first_adj
                        df["low"]=df["low"].astype(float)*df["adj_factor"].astype(float)/first_adj
                        df["close"]=df["close"].astype(float)*df["adj_factor"].astype(float)/first_adj
                        df["pre_close"]=df["pre_close"].astype(float)*df["adj_factor"].astype(float)/first_adj
                        
                        df["upLimit"]=df["upLimit"].astype(float)*df["adj_factor"].astype(float)/first_adj
                        df["downLimit"]=df["downLimit"].astype(float)*df["adj_factor"].astype(float)/first_adj
                    
                    
            df["change"]=df["change"].astype(float)
            df["vwap"]=(df['amount'].astype(float)*1000)/(df['volume'].astype(float)*100+1) 
            df["stop"]=pd.isna(df['close']).astype(int)

                    
            df=df.drop('ts_code_y', axis=1)
            df=df.drop(labels='up_limit', axis=1)
            df=df.drop(labels='down_limit', axis=1)


            df=df.ffill()
            df=df.dropna(subset=['adj_factor','ts_code'])
            df.drop_duplicates('trade_date',inplace = True)
            df=df.sort_values(by='trade_date', ascending=True)
            df=df.reset_index(drop=True)
            del df_adj
            del calendar
         
         
            #print(df.columns)
         
            df.to_pickle(cache_path )
            return df
        except Exception as e:
            print("error")
            print("err exception is %s" % traceback.format_exc())
            print(df_price)
            print(df_adj)
            traceback.print_exc()
            return pd.DataFrame()
        
        
       
        
def alignStockFactors(df, table, date, filed, conv=0, max_workers=10,db=None):
    # 保存原始索引结构以便后续恢复
    has_multi_index = isinstance(df.index, pd.MultiIndex)
    original_index = df.index.copy()
    
    # 复制一份 DataFrame 防止修改原始数据
    df = df.copy()
    
    # 处理多级索引，将索引重置为列
    df = df.reset_index()
    
    # 将 code 重命名为 ts_code (如果存在)
    if 'code' in df.columns and 'ts_code' not in df.columns:
        df.rename(columns={'code': 'ts_code'}, inplace=True)
    
    # 将 time 转换为 trade_date
    if 'time' in df.columns:
        df['trade_date'] = df['time'].dt.strftime('%Y%m%d')
    
    # 检查可能的 level_0 列 (通常由 reset_index 引入)
    if 'level_0' in df.columns:
        df = df.drop(columns=['level_0'])
    
    # 获取唯一的 ts_code 列表
    ts_codes = df['ts_code'].unique().tolist()
    
    # 从CSV文件加载数据
    csv_data = None
    try:
        csv_path = f"{DATA_DIR}/market/reference/cn_stock/{table}.csv"
        csv_data = pd.read_csv(csv_path)
        # 确保所有字符串类型的列没有前后空格
        for col in csv_data.select_dtypes(include=['object']).columns:
            csv_data[col] = csv_data[col].str.strip()
        print(f"成功加载CSV数据: {csv_path}, 共{len(csv_data)}行")
    except Exception as e:
        print(f"加载CSV数据失败: {str(e)}")
        return df  # 如果无法加载CSV数据，返回原始DataFrame
    
    # 定义处理单个股票的函数
    def process_single_stock(ts_code):
        try:
            # 筛选当前股票的数据
            df_single = df[df['ts_code'] == ts_code].copy()
            df_single.drop_duplicates('trade_date', inplace=True)
            
            # 从CSV数据中筛选
            df_factor = csv_data[csv_data['ts_code'] == ts_code].copy()
            
            # 如果用户指定了特定字段
            if filed != '*':
                fields_list = [f.strip() for f in filed.split(',') if f.strip()]
                if date not in fields_list:
                    fields_list.append(date)
                if 'ts_code' not in fields_list:
                    fields_list.append('ts_code')
                # 只保留需要的列
                available_cols = [col for col in fields_list if col in df_factor.columns]
                df_factor = df_factor[available_cols]
            
            # 检查是否成功获取因子数据
            if df_factor is None or df_factor.empty:
                # 处理未获取到数据的情况
                if filed == '*':
                    # 如果是读取所有字段，为每个字段创建空值列
                    for col in csv_data.columns:
                        if col not in df_single.columns:
                            df_single[col] = None
                elif filed != '*':
                    # 为指定的字段创建空值列
                    fields_list = [f.strip() for f in filed.split(',') if f.strip()]
                    for f in fields_list:
                        if f != date and f != 'ts_code' and f not in df_single.columns:
                            df_single[f] = None
                return df_single
            
            # 数据去重
            try:
                df_factor = df_factor[~df_factor[date].duplicated()]
            except Exception as e:
                print(f"处理去重时出错: {str(e)}")
                print(df_factor)
            
            # 财务报表时间处理 (conv=3)
            if conv == 3:
                df_factor[date] = df_factor[date].astype(str)
                df_factor[date] = pd.to_datetime(df_factor[date], format='%Y%m%d', errors='coerce')
                df_factor[date] = df_factor[date] + timedelta(days=1)
                df_factor[date] = df_factor[date].astype(str)
                df_factor[date] = df_factor[date].map(lambda x: x.replace('-', ''))
                df_factor['trade_date'] = df_factor[date]
            
            # 时间格式转换 (conv=1)
            elif conv == 1:
                df_factor[date] = df_factor[date].astype(str)
                df_factor['trade_date'] = df_factor[date].map(lambda x: x.replace('-', ''))
            # 其他情况保持原样，但确保列名一致
            elif 'trade_date' not in df_factor.columns:
                df_factor.rename(columns={date: 'trade_date'}, inplace=True)
            
            # 处理列名重复
            overlap_cols = [col for col in df_single.columns if col in df_factor.columns 
                          and col != 'trade_date' and col != 'ts_code']
            df_single = df_single.drop(columns=overlap_cols)
            
            # 合并数据
            df_merged = pd.merge(df_single, df_factor, how='left', on='trade_date', validate="one_to_one", copy=True)
            df_merged.drop_duplicates('trade_date', inplace=True)
            
            # 根据 conv 参数确定是否向下填充
            if conv != 2:  # conv!=2 表示需要填充
                df_merged = df_merged.fillna(method='ffill')
            
            return df_merged
        except Exception as e:
            print(f"处理股票 {ts_code} 时出错: {str(e)}")
            traceback.print_exc()
            return pd.DataFrame()
    
    result_dfs = []
    
    # 使用线程池并行处理每个股票
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务并获取Future对象列表
        future_to_ts_code = {executor.submit(process_single_stock, ts_code): ts_code for ts_code in ts_codes}
        
        # 获取任务结果
        for future in future_to_ts_code:
            try:
                df_result = future.result()
                if not df_result.empty:
                    result_dfs.append(df_result)
            except Exception as e:
                print(f"处理结果时出错: {str(e)}")
                traceback.print_exc()

    # 合并所有处理过的数据
    if result_dfs:
        final_df = pd.concat(result_dfs)
        final_df['code'] = final_df['ts_code']
        # 恢复原始索引结构
        if has_multi_index:
            # 确保有必要的列用于重建索引
            index_names = original_index.names
            if 'time' in df.columns:
                final_df = final_df.sort_values(['code', 'time'])
                # 设置多级索引
                final_df = final_df.set_index(['code', 'time'])
            else:
                # 如果没有time列，尝试用trade_date代替
                final_df = final_df.sort_values(['code', 'trade_date'])
                # 将trade_date转回datetime格式
                final_df['time'] = pd.to_datetime(final_df['trade_date'], format='%Y%m%d')
                final_df = final_df.set_index(['code', 'time'])
            
            # 重命名索引以匹配原始索引名称
            final_df.index.names = index_names
        
        return final_df
    else:
        # 如果没有数据，返回一个保持原始索引结构的空DataFrame
        return pd.DataFrame(index=original_index)


