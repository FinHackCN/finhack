# region Auxiliary functions
import os
import re

import ast
import time
import datetime
import builtins
import traceback

import pandas as pd
from numpy import abs
from numpy import sign
import numpy as np
import bottleneck as bn
from functools import reduce
from scipy.stats import rankdata

np.seterr(all='ignore',over='ignore',divide='ignore') 
import warnings
warnings.filterwarnings("ignore", category=RuntimeWarning) 
from runtime.constant import *
from finhack.factor.default.factorManager import factorManager
import finhack.library.log as Log

class RewriteNode(ast.NodeTransformer):
    def visit_IfExp(self, node):
        #xprint(ast.dump(node))
        tmp=node.body
        node.body=node.test
        node.test=tmp        
        test=ast.unparse(node.test)
        body=ast.unparse(node.body)
        orelse=ast.unparse(node.orelse)
        newnode=ast.parse("where(%s,%s,%s)" % (test,body,orelse)).body[0]
        return newnode


def ternary_trans(formula):
    if not '?' in formula:
        return formula
    formula=formula.replace('?',' if ')
    formula=formula.replace(':',' else ')
    tree=ast.parse(formula)
    #print(ast.dump(tree))
    for node in ast.walk(tree):
        ast.fix_missing_locations(RewriteNode().visit(node)) 
    formula=ast.unparse(tree)
#    print("\n转义公式:"+formula+"\n")
    return formula


def and_trans(formula):
    tree=ast.parse(formula)
    Log.logger.debug(formula)
    Log.logger.debug(ast.dump(tree))

    for node in ast.walk(tree):
        ast.fix_missing_locations(RewriteNode().visit(node)) 
    formula=ast.unparse(tree)
    #print("\n转义公式:"+formula+"\n")
    return formula


def coviance(x, y, window=10):
    return covariance(x, y, window)
    
def covariance(x, y, window=10):
    window=int(window)
    if type(x)==type(()):
        x=x[0]
    if type(y)==type(()):
        y=y[0]
 
    grouped=x.groupby('ts_code')
    cov_all=[]
    for name,group in grouped:
        cov=group.rolling(window).cov(y.loc[name])
        cov_all.append(cov)
    df=pd.concat(cov_all)
    return df
    
    
    

def corr(x, y, window=10):
    return correlation(x, y, window)
def correlation(x, y, window=10):
    window=int(window)
    if type(x)==type(()):
        x=x[0]
    if type(y)==type(()):
        y=y[0]
        
    df=pd.DataFrame()
    grouped=x.groupby('ts_code')
    corr_all=[]
    for name,group in grouped:
        corr=group.rolling(window).corr(y.loc[name])
        corr_all.append(corr)
    df=pd.concat(corr_all)
    return df
    
def log(df):
    df=np.log(df)
    return df
 
def min(x,y):
    df=pd.DataFrame()
    df['x']=x
    df['y']=y
    df['min']=df.min(axis=1)
    return df['min']
    
def max(x,y):
    df=pd.DataFrame()
    df['x']=x
    df['y']=y
    df['max']=df.max(axis=1)
    return df['max']
        

def add(x,y):        
    df=pd.DataFrame()
    df['x']=x
    df['y']=y
    df['z']=df['x']+df['y']
    return df['z']        
        
        
def sub(x,y):        
    df=pd.DataFrame()
    df['x']=x
    df['y']=y
    df['z']=df['x']-df['y']
    return df['z']           
        
def mul(x,y):        
    df=pd.DataFrame()
    df['x']=x
    df['y']=y
    df['z']=df['x']*df['y']
    return df['z']   
    
def div(x,y):        
    df=pd.DataFrame()
    df['x']=x
    df['y']=y
    df['z']=df['x']/df['y']
    return df['z']   
 
def sqrt(x):  
    return np.sqrt(x)
    
def abs(x):  
    return np.absolute(x)
    
def sin(x):  
    return np.sign(x)
    
def cos(x):  
    return np.cos(x)
    
def tan(x):  
    return np.tan(x)
    
        

        
        
def where(c,t,f):
    df=pd.DataFrame()
    df['c']=c
    df['t']=t
    df['f']=f
    df['r']=df.apply(lambda x:x.t if x.c else x.f, axis=1)
    return df['r']
        

def sum(x,y=None):
    if y==None:
        return builtins.sum(x)
    return ts_sum(x,y)




def ts_sum(df, window=10):
    window=int(window)
    grouped=df.groupby('ts_code')
    ts_all=[]
    for name,group in grouped:
        if len(group)<window:
            ts_array=bn.move_sum(group.values,len(group))
        else:
            ts_array=bn.move_sum(group.values,window)
    
        ts_series=group
        ts_series.values[:] = ts_array
        ts_all.append(ts_series)
        
    df=pd.concat(ts_all)    
    return df     


def delta(df, period=1):
    if type(df)==type(()):
        df=df[0]
    df=df.groupby('ts_code').diff(period)
    if len(df.index.names)==3:
        df=df.droplevel(1)
    return df




#此函数应该是会存在未来数据
def scale(df, k=1):
    #return df.mul(k).div(np.abs(df).sum())
    return df/1000



def prod(df, window=10):
    return product(df,window)


def np_ts_prod(arr, window=10):
    result = [0] * (window-1)
    n=len(arr) - window + 1
    for i in range(n):
        result.append(np.prod(arr[i:i+window]))
    return result
 


def product(df, window=10):
    window=int(window)
    grouped=df.groupby('ts_code')
    ts_all=[]
    for name,group in grouped:
        if len(group)<window:
            ts_array=np_ts_prod(np.nan_to_num(group.values),len(group))
        else:
            ts_array=np_ts_prod(np.nan_to_num(group.values),window)
    
        ts_series=group
        ts_series.values[:] = ts_array
        ts_all.append(ts_series)
        
    df=pd.concat(ts_all)    
    return df   


    
def mean(df, window=10):
    window=int(window)
    grouped=df.groupby('ts_code')
    ts_all=[]
    for name,group in grouped:
        if len(group)<window:
            ts_array=bn.move_mean(group.values,len(group))
        else:
            ts_array=bn.move_mean(group.values,window)
    
        ts_series=group
        ts_series.values[:] = ts_array
        ts_all.append(ts_series)
        
    df=pd.concat(ts_all)    
    return df         
    
 




def tsmin(df, window=10):
    return ts_min(df,window)
    
    
    
def tsmax(df, window=10):
    return ts_max(df,window)
    

def ts_min(df, window=10):
    window=int(window)
    grouped=df.groupby('ts_code')
    ts_all=[]
    for name,group in grouped:
        if len(group)<window:
            ts_array=bn.move_min(group.values,len(group))
        else:
            ts_array=bn.move_min(group.values,window)
    
        ts_series=group
        ts_series.values[:] = ts_array
        ts_all.append(ts_series)
        
    df=pd.concat(ts_all)    
    return df       



def ts_max(df, window=10):
    window=int(window)
    grouped=df.groupby('ts_code')
    ts_all=[]
    for name,group in grouped:
        if len(group)<window:
            ts_array=bn.move_max(group.values,len(group))
        else:
            ts_array=bn.move_max(group.values,window)
    
        ts_series=group
        ts_series.values[:] = ts_array
        ts_all.append(ts_series)
        
    df=pd.concat(ts_all)    
    return df     


def delay_1(df):
    return delay(df,1)

def delay_3(df):
    return delay(df,3)
    
def delay_5(df):
    return delay(df,5)
    
def delay_7(df):
    return delay(df,7)


def shift(df, period=1):
    return delay(df,period)

    
def delay(df, period=1):
    df=df.groupby('ts_code').shift(period)
    if len(df.index.names)==3:
        df=df.droplevel(1)
    return df

def std(df, window=10):
    return stddev(df,window)




def stddev(df, window=10):
    window=int(window)
    grouped=df.groupby('ts_code')
    ts_all=[]
    for name,group in grouped:
        if len(group)<window:
            ts_array=bn.move_std(group.values,len(group))
        else:
            ts_array=bn.move_std(group.values,window)
    
        ts_series=group
        ts_series.values[:] = ts_array
        ts_all.append(ts_series)
        
    df=pd.concat(ts_all)    
    return df    



def rolling_rank(na):
    return rankdata(na)[-1]

def tsrank(df, window=10):
    return ts_rank(df, window)





def ts_rank(df, window=10):
    window=int(window)
    grouped=df.groupby('ts_code')
    ts_rank_all=[]
    for name,group in grouped:
        # rank=group.rolling(window)
        # rank= group.rolling(window).apply(lambda z: np.nan if np.all(np.isnan(z)) else ((rankdata(z[~np.isnan(z)])[-1] -1) * (len(z)-1) / (len(z[~np.isnan(z)]) - 1) + 1), raw = True)
        if len(group)<window:
            ts_rank_array=bn.move_rank(group.values,len(group))
        else:
            ts_rank_array=bn.move_rank(group.values,window)
    
        ts_rank_series=group
        ts_rank_series.values[:] = ts_rank_array
        ts_rank_all.append(ts_rank_series)
        
    df=pd.concat(ts_rank_all)    
    return df
  

def rank(df):
    if type(df)==type(()):
        df=df[0] 
    df=df.groupby('trade_date').rank(pct=True)
    if len(df.index.names)==3:
        df=df.droplevel(1)
    return df  
    

        
        
def ts_argmax(df, window=10):
    window=int(window)
    grouped=df.groupby('ts_code')
    ts_all=[]
    for name,group in grouped:
        if len(group)<window:
            ts_array=bn.move_argmax(group.values,len(group))
        else:
            ts_array=bn.move_argmax(group.values,window)
    
        ts_series=group
        ts_series.values[:] = ts_array
        ts_all.append(ts_series)
        
    df=pd.concat(ts_all)    
    return df        
        
        
    
def ts_argmin(df, window=10):
    window=int(window)
    grouped=df.groupby('ts_code')
    ts_all=[]
    for name,group in grouped:
        if len(group)<window:
            ts_array=bn.move_argmin(group.values,len(group))
        else:
            ts_array=bn.move_argmin(group.values,window)
    
        ts_series=group
        ts_series.values[:] = ts_array
        ts_all.append(ts_series)
        
    df=pd.concat(ts_all)    
    return df       
        
        

    
    
    
def sign(x):
    return np.sign(x)

    
def signedpower(x,t):
    return np.sign(x)*(np.abs(x)**t)    
  
def sequence(n):
    return np.arange(1,n+1)    
    
    
def decay_linear(A,n):
    return decaylinear(A,n)
    
    

    
def np_decaylinear(arr, window=10):
    w = np.arange(window,0,-1) 
    w = w/w.sum()    
    result = [0] * (window-1)
    n=len(arr) - window + 1
    for i in range(n):
        result.append((arr[i:i+window] * w).sum())
    return result
 

#这里decaylinear和wma的实现一样了，待修改
def decaylinear(df,window=10):
    window=int(window)
    grouped=df.groupby('ts_code')
    ts_all=[]
    for name,group in grouped:
        if len(group)<window:
            ts_array=np_decaylinear(np.nan_to_num(group.values),len(group))
        else:
            ts_array=np_decaylinear(np.nan_to_num(group.values),window)
    
        ts_series=group
        ts_series.values[:] = ts_array
        ts_all.append(ts_series)
        
    df=pd.concat(ts_all)    
    return df         
    
    
def np_wma(arr, window=10):
    w = np.arange(window,0,-1) 
    w = w/w.sum()    
    result = [0] * (window-1)
    n=len(arr) - window + 1
    for i in range(n):
        result.append((arr[i:i+window] * w).sum())
    return result
 


def wma(df,window=10):
    window=int(window)
    grouped=df.groupby('ts_code')
    ts_all=[]
    for name,group in grouped:
        if len(group)<window:
            ts_array=np_wma(np.nan_to_num(group.values),len(group))
        else:
            ts_array=np_wma(np.nan_to_num(group.values),window)
    
        ts_series=group
        ts_series.values[:] = ts_array
        ts_all.append(ts_series)
        
    df=pd.concat(ts_all)    
    return df      
    
    


    
    
def np_lowday(arr, window=10):
    result = [0] * (window-1)
    n=len(arr) - window + 1
    for i in range(n):
        x=(window-1) -  np.argsort(arr[i:i+window])[0]
        result.append(x)
    return result
    
    
def lowday(df,window=10):
    window=int(window)
    grouped=df.groupby('ts_code')
    ts_all=[]
    for name,group in grouped:
        if len(group)<window:
            ts_array=np_lowday(np.nan_to_num(group.values),len(group))
        else:
            ts_array=np_lowday(np.nan_to_num(group.values),window)
    
        ts_series=group
        ts_series.values[:] = ts_array
        ts_all.append(ts_series)
        
    df=pd.concat(ts_all)    
    return df         
    
    

    
    
def np_highday(arr, window=10):
    result = [0] * (window-1)
    n=len(arr) - window + 1
    for i in range(n):
        x=(window-1) -  np.argsort(arr[i:i+window])[window-1]
        result.append(x)
    return result
    
    
def highday(df,window=10):
    window=int(window)
    grouped=df.groupby('ts_code')
    ts_all=[]
    for name,group in grouped:
        if len(group)<window:
            ts_array=np_highday(np.nan_to_num(group.values),len(group))
        else:
            ts_array=np_highday(np.nan_to_num(group.values),window)
    
        ts_series=group
        ts_series.values[:] = ts_array
        ts_all.append(ts_series)
        
    df=pd.concat(ts_all)    
    return df         
   

 
def np_sumif(arr,condition,window=10):
    result = [0] * (window-1)
    n=len(arr) - window + 1
    for i in range(n):
        x=np.sum(condition[i:i+window]*arr[i:i+window])
        result.append(x)
    return result

def sumif(df,window,condition):
    window=int(window)
    grouped=df.groupby('ts_code')
    ts_all=[]
    for name,group in grouped:
        if len(group)<window:
            ts_array=np_sumif(np.nan_to_num(group.values),condition.loc[name].values,len(group))
        else:
            ts_array=np_sumif(np.nan_to_num(group.values),condition.loc[name].values,window)
    
        ts_series=group
        ts_series.values[:] = ts_array
        ts_all.append(ts_series)
        
    df=pd.concat(ts_all)    
    return df    

    
    
    
def np_regbeta(arr,B,window=10):
    result = [0] * (window-1)
    n=len(arr) - window + 1
    try:
        for i in range(n):
            if len(arr[i:i+window])<len(B):
                x=np.cov(arr[i:i+window], B[:len(arr[i:i+window])])[0][1]/np.var(B[:len(arr[i:i+window])])
            else:
                x=np.cov(arr[i:i+window], B)[0][1]/np.var(B)
            result.append(x)
    except Exception as e:
        Log.logger.warning(arr)
        Log.logger.warning(B)
        Log.logger.warning(window)
    return result

def regbeta(A,B,window=0):
    window=int(window)
    grouped=A.groupby('ts_code')
    ts_all=[]
    for name,group in grouped:
        if window==0:
            tmp_window=len(B)
        else:
            tmp_window=window
        if len(group)<tmp_window:
            tmp_window=len(group)
        
        ts_array=np_regbeta(np.nan_to_num(group.values),B,tmp_window)
    
        ts_series=group
        ts_series.values[:] = ts_array
        ts_all.append(ts_series)
        
    df=pd.concat(ts_all)    
    return df      
    
    
    
    
    
    
def smean(x,n,m):
    return sma(x,n,m)   
def sma(x,n,m=2):
    x = x.fillna(0)
    sma_all=[]
    grouped=x.groupby('ts_code')
    for name,group in grouped:
        res = group.copy()
        for i in range(1,len(group)):
            res.iloc[i] = (group.iloc[i]*m+group.iloc[i-1]*(n-m))/float(n)
        res=pd.Series(res,index=group.index)
        sma_all.append(res)
    df=pd.concat(sma_all)    
    return df    
    
    
    
    
    
    
def np_sma(arr, n,m):
    result=[arr[0]]
    for i in range(1,len(arr)):
        result.append((arr[i]*m+arr[i-1] * (n-m))/float(n))
    return result
 


def sma(df,n,m=2):
    grouped=df.groupby('ts_code')
    ts_all=[]
    for name,group in grouped:
        ts_array=np_sma(group.values,n,m)
    
        ts_series=group
        ts_series.values[:] = ts_array
        ts_all.append(ts_series)
        
    df=pd.concat(ts_all)    
    return df       
    


def count(condition, n):
    grouped=condition.groupby('ts_code')
    c_all=[]
    for name,group in grouped:
        cache = group.fillna(0)#now condition is the boolean DataFrame/Series/Array
        c=cache.rolling(window = n, center = False, min_periods=minp(n) ).sum()    
        c_all.append(c)
    df=pd.concat(c_all) 
    return df
    
    
def sumac(df, window=10):
    grouped=df.groupby('ts_code')
    s_all=[]
    for name,A in grouped:
        s=A.rolling(window=window,min_periods = minp(window)).sum().fillna(method = 'ffill')
        s_all.append(s)
    df=pd.concat(s_all)      
    return df     

    


def minp(d):
    """
    :param d: Pandas rolling 的 window
    :return: 返回值为 int，对应 window 的 min_periods
    """
    if not isinstance(d, int):
        d = int(d)
    if d <= 10:
        return d - 1
    else:
        return d * 2 // 3
 

 


def save_lastdate(res,name):
    #计算单日指标
    res=res.reset_index()
    lastdate=str(res['trade_date'].max())
    date_factors_path=DATE_FACTORS_DIR+lastdate
    if not os.path.exists(date_factors_path): 
        try:
            os.mkdir(date_factors_path)
        except Exception as e:
            Log.logger.error(str(e))                
                
    res=res[res.trade_date==lastdate]
    res=res.set_index(['ts_code','trade_date']) 
    res.to_csv(date_factors_path+"/"+name+'.csv',header=None)
    return res

class alphaEngine():
    def get_df(formula="",df=pd.DataFrame(),name="alpha",check=False,ignore_notice=False,stock_list=[],diff=True):
        try:
            #根据 $符号匹配列名
            col_list=alphaEngine.get_col_list(formula)
            
            #缓存路径
            data_path=SINGLE_FACTORS_DIR+name+'.csv'   
            diff_date=999
            max_date=''
            
            
            # print(data_path)
            
            #如果只是用来检测，则diff_date保持999
            if os.path.exists(data_path) and check==False and diff:
                df_old=pd.read_csv(data_path, header=None, names=['ts_code','trade_date','alpha'])
                max_date=df_old['trade_date'].max()
                today=time.strftime("%Y%m%d",time.localtime())
                diff_date=int(today)-int(max_date)


            if df.empty:
                df=factorManager.getFactors(factor_list=col_list,cache=True)
            else:
                df=df.sort_index()


            #需要对比差异然后再计算
            if diff:
                if diff_date>0 and diff_date<100:
                    dt=datetime.datetime.strptime(str(max_date),'%Y%m%d')
                    start_date=dt-datetime.timedelta(days=700)
                    start_date=start_date.strftime('%Y%m%d')
                    df=df.reset_index()
                    df=df[df.trade_date>=start_date]
                    df=df.set_index(['ts_code','trade_date'])
                elif diff_date==0:
                    return pd.DataFrame()
    
    
            if stock_list!=[]:
                df=df.reset_index()
                df = df[df['ts_code'].isin(stock_list)]
                df=df.set_index(['ts_code','trade_date'])
                
            df=df.fillna(0)
            return diff_date,max_date,df

        except Exception as e:
            if ignore_notice:
                return pd.DataFrame()
            else:
                Log.logger.error("%s error:%s" % (name,str(e))) 
                Log.logger.error("err exception is %s" % traceback.format_exc())
            return 999,max_date,pd.DataFrame()        
        
    def get_col_list(formula):
            #根据 $符号匹配列名
        col_list = []
        col_list = re.findall(r'(?:\$)[a-zA-Z0-9_]+', formula)
        col_list=list(set(col_list))
        return col_list
            
    def calc(formula='',df=pd.DataFrame(),name="alpha",check=False,save=False,ignore_notice=False,stock_list=[],diff=True):
        # print(formula)
        try:
            if df.empty:
                diff_date,max_date,df=alphaEngine.get_df(formula=formula,df=df,name=name,check=check,ignore_notice=False,stock_list=stock_list,diff=diff)
            if df.empty:
                return df
                
            col_list=alphaEngine.get_col_list(formula)
            
            # #缓存路径
            data_path=SINGLE_FACTORS_DIR+name+'.csv'   
            # diff_date=999
            # max_date=''
            
            
            # # print(data_path)
            
            # #如果只是用来检测，则diff_date保持999
            # if os.path.exists(data_path) and check==False:
            #     df_old=pd.read_csv(data_path, header=None, names=['ts_code','trade_date','alpha'])
            #     max_date=df_old['trade_date'].max()
            #     today=time.strftime("%Y%m%d",time.localtime())
            #     diff_date=int(today)-int(max_date)
    
            # if df.empty:
            #     df=factorManager.getFactors(factor_list=col_list,cache=True)
            # else:
            #     df=df.sort_index()

            # if diff_date>0 and diff_date<100:
            #     dt=datetime.datetime.strptime(str(max_date),'%Y%m%d')
            #     start_date=dt-datetime.timedelta(days=700)
            #     start_date=start_date.strftime('%Y%m%d')
            #     df=df.reset_index()
            #     df=df[df.trade_date>=start_date]
            #     df=df.set_index(['ts_code','trade_date'])
            # elif diff_date==0:
            #     return True
    
            # df=df.fillna(0)
            
            todolist=['indneutralize','cap','filter','self','banchmarkindex']
            for todo in todolist:
                if todo in formula:
                    return pd.DataFrame()     
                
            formula=formula.replace("$dtm"," ($open<=delay($open,1)?0:max(($high-$open),($open-delay($open,1)))) ")
            formula=formula.replace("$dbm"," ($open>=delay($open,1)?0:max(($open-$low),($open-delay($open,1)))) ")
            formula=formula.replace("$tr"," max(max($high-$low,abs($high-delay($close,1))),abs($low-delay($close,1)) ) ")
            formula=formula.replace("$hd"," $high-delay($high,1) ")
            formula=formula.replace("$ld"," delay($low,1)-$low ")    



            pd.options.display.max_rows = 100
            t1=time.time()
            formula=formula.replace("||"," | ")
            formula=formula.replace("&&"," & ")
            formula=formula.replace("^"," ** ")
            
            # fields=['$open','$high','$low','$close','$amount','$volume','$vwap','$returns']
            # for field in fields:
            #     formula=formula.replace(field,"df['%s']" % (field[1:]))
            try:
                for col in col_list:
                    formula=formula.replace(col,"df['%s']" % (col[1:]))
                    df[col[1:]]=df[col[1:]].astype(float)
            except KeyError:
                if check:
                    Log.logger.error("%s error:%s" % (formula,str(e))) 
                    return pd.DataFrame()
                else:
                    Log.logger.error("%s error:%s" % (formula,str(e))) 
                    return pd.DataFrame()

            if '?' in formula:
                formula=ternary_trans(formula)
 
            formula=formula.replace("\n"," ")

            if not check and not ignore_notice:
                Log.logger.info(name+"计算公式:"+formula)
            res=eval(formula)
            
            #这里的res是近700天的数据，可以和old_df拼接起来变成完整数据
            
            # print(res)
            
            #如果是用来检测，或者不保存，则直接返回
            if check or save==False:
                return res
            else:
                if diff_date>0 and diff_date<100:
                    res=res.reset_index()
                    res=res[res.trade_date>str(max_date)]
                    res=res.set_index(['ts_code','trade_date'])        
                    if not res.empty:
                        res.to_csv(data_path,mode='a',header=False)
                elif diff_date>100:
                    res.to_csv(data_path,header=None)
                else:
                    return True
                        

                #计算单日指标
                res=save_lastdate(res,name)

                del df
                del res
                
            return True

        except Exception as e:
            if ignore_notice:
                return pd.DataFrame()
            todolist=[]#['identically-labeled']
            for todo in todolist:
                if todo in str(e):
                    return pd.DataFrame()
                else:
                    if(len(df)>100):
                        Log.logger.error("%s error:%s" % (name,str(e))) 
                        Log.logger.error("err exception is %s" % traceback.format_exc())
            if(len(df)>100):
                Log.logger.error("%s error:%s" % (name,str(e))) 
                Log.logger.error("err exception is %s" % traceback.format_exc())
            return pd.DataFrame()
        pass
