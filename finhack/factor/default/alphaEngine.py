from multiprocessing import cpu_count 
import os
import re
import multiprocessing as mp
from functools import partial

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
 
    df=pd.DataFrame({'x':x,'y':y}).sort_index()
    return df.groupby('code', group_keys=False).apply(lambda g: g['x'].rolling(window).cov(g['y']))
def corr(x, y, window=10):
    return correlation(x, y, window)
def correlation(x, y, window=10):
    window=int(window)
    if type(x)==type(()):
        x=x[0]
    if type(y)==type(()):
        y=y[0]
        
    df=pd.DataFrame({'x':x,'y':y}).sort_index()
    return df.groupby('code', group_keys=False).apply(lambda g: g['x'].rolling(window).corr(g['y']))
    
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
    grouped=df.groupby('code')
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
    df=df.groupby('code').diff(period)
    if len(df.index.names)==3:
        df=df.droplevel(1)
    return df


#横截面标准化：Alpha101 定义 scale(x, a) = x * a / sum(|x|)，sum 在横截面(同一时刻所有股票)上进行
def scale(df, k=1):
    if type(df)==type(()):
        df=df[0]
    # 用 transform 保持原索引结构，避免 groupby.apply 引入额外层级；sum=0 时置 0
    abs_sum = df.groupby('time').transform(lambda x: np.abs(x).sum())
    return df.mul(k).div(abs_sum.replace(0, np.nan)).fillna(0)

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
    grouped=df.groupby('code')
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
    grouped=df.groupby('code')
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
    grouped=df.groupby('code')
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
    grouped=df.groupby('code')
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
    df=df.groupby('code').shift(period)
    if len(df.index.names)==3:
        df=df.droplevel(1)
    return df

def std(df, window=10):
    return stddev(df,window)


def stddev(df, window=10):
    window=int(window)
    grouped=df.groupby('code')
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
    grouped=df.groupby('code')
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
    df=df.groupby('time').rank(pct=True)
    if len(df.index.names)==3:
        df=df.droplevel(1)
    return df  
    

        
        
def ts_argmax(df, window=10):
    window=int(window)
    grouped=df.groupby('code')
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
    grouped=df.groupby('code')
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
    grouped=df.groupby('code')
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
    grouped=df.groupby('code')
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
    grouped=df.groupby('code')
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
    grouped=df.groupby('code')
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
    grouped=df.groupby('code')
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
    grouped=A.groupby('code')
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
    grouped=x.groupby('code')
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
    grouped=df.groupby('code')
    ts_all=[]
    for name,group in grouped:
        ts_array=np_sma(group.values,n,m)
    
        ts_series=group
        ts_series.values[:] = ts_array
        ts_all.append(ts_series)
        
    df=pd.concat(ts_all)    
    return df       
    


def count(condition, n):
    grouped=condition.groupby('code')
    c_all=[]
    for name,group in grouped:
        cache = group.fillna(0)#now condition is the boolean DataFrame/Series/Array
        c=cache.rolling(window = n, center = False, min_periods=minp(n) ).sum()    
        c_all.append(c)
    df=pd.concat(c_all) 
    return df
    
    
def sumac(df, window=10):
    grouped=df.groupby('code')
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


class alphaEngine():   

    @staticmethod
    def get_alpha_list(market,freq,task_list):
        """
        获取所有的alpha列表
        返回包含alpha_name和formula的字典列表
        """
        alpha_list = []
        
        for root, dirs, files in os.walk(CONFIG_DIR + f"/factorlist/alphalist/{market}/{freq}/"):
            for file in files:
                if file in task_list.split(','):
                    with open(os.path.join(root, file), 'r', encoding='utf-8') as f:
                        lines = [line.strip() for line in f.readlines()]
                        file_name = os.path.splitext(file)[0]
                        for i, formula in enumerate(lines, 1):
                            alpha_name = f"{file_name}_{str(i).zfill(3)}"
                            alpha_list.append({"name": alpha_name, "formula": formula})
                            
        for root, dirs, files in os.walk(CONFIG_DIR + f"/factorlist/alphalist/{market}/x{freq}/"):
            for file in files:
                if file in task_list.split(','):
                    with open(os.path.join(root, file), 'r', encoding='utf-8') as f:
                        lines = [line.strip() for line in f.readlines()]
                        file_name = os.path.splitext(file)[0]
                        for i, formula in enumerate(lines, 1):
                            alpha_name = f"{file_name}_{str(i).zfill(3)}"
                            alpha_list.append({"name": alpha_name, "formula": formula})
                            
        return alpha_list


    def get_col_list(formula):
            #根据 $符号匹配列名
        col_list = []
        col_list = re.findall(r'(?:\$)[a-zA-Z0-9_]+', formula)
        col_list=list(set(col_list))
        return col_list

    def computeAlphaBatch(market="cn_stock",freq="1d",alpha_list=[],start_date='',end_date='',code_list=[], process_num="auto"):
            # 确定进程数量
        if process_num == "auto":
            # 获取CPU核心数并减1，至少为1
            cpu_cores = cpu_count()
            process_num = cpu_cores - 1
            if process_num <= 0:
                process_num = 1
        else:
            # 如果指定了进程数，确保它是整数
            process_num = int(process_num)
        
        print(f"使用进程数: {process_num}")

        # 创建进程池
        pool = mp.Pool(processes=process_num)

        # 定义部分函数
        partial_computeAlpha = partial(alphaEngine.computeAlpha, market=market, freq=freq, start_date=start_date, end_date=end_date, code_list=code_list)

        # 使用进程池并行计算
        results = pool.map(partial_computeAlpha, alpha_list)

        # 关闭进程池
        pool.close()
        pool.join()

        return results


    def computeAlpha(alpha_item, market="cn_stock", freq="1d", start_date='', end_date='', code_list=[]):
        try:
            # 计算单个alpha因子
            """
            计算单个alpha因子
            
            参数:
                alpha_item: 单个alpha项，包含name和formula
                market: 市场类型
                freq: 频率
                start_date: 开始日期
                end_date: 结束日期
                code_list: 代码列表
            
            返回:
                计算结果包含alpha_name和alpha_result
            """
            alpha_name = alpha_item["name"]
            formula = alpha_item["formula"]

            Log.logger.info(f"开始计算Alpha因子: {alpha_name}")
            Log.logger.info(f"计算参数 - market: {market}, freq: {freq}, start_date: {start_date}, end_date: {end_date}")
            Log.logger.info(f"原始公式: {formula}")

            pd.options.display.max_rows = 100
            t1=time.time()
            
            # 公式预处理
            original_formula = formula
            formula=formula.replace("||"," | ")
            formula=formula.replace("&&"," & ")
            formula=formula.replace("^"," ** ")
            formula=formula.replace("\n"," ")
            
            Log.logger.debug(f"公式预处理后: {formula}")

            if '?' in formula:
                formula=ternary_trans(formula)
                Log.logger.debug(f"三元运算符转换后: {formula}")

            col_list=alphaEngine.get_col_list(formula)
            Log.logger.info(f"公式依赖的列: {col_list}")

            time_ranges = factorManager.timeSplit(start_date, end_date, freq)
            Log.logger.info(f"时间范围拆分为 {len(time_ranges)} 个区间: {time_ranges}")

            all_results = []  # 收集各时间区间的计算结果，用于跨区间合并

            # 遍历时间区间
            for i, (range_start, range_end) in enumerate(time_ranges):
                Log.logger.info(f"处理Alpha时间区间 [{i+1}/{len(time_ranges)}]: {range_start} - {range_end}")
                    
                # 判断当前是否为最后一个time_ranges元素
                is_last_range = (i == len(time_ranges) - 1)
                    
                # 如果不是最后一个区间，检查因子是否已存在
                if not is_last_range:
                    should_skip = True
                    factor_info = factorManager.inspectFactor(alpha_name, market=market, freq=freq,start_date=range_start, end_date=range_end,only_exists=True)
                    if not factor_info["exists"]:
                        should_skip = False
                        Log.logger.debug(f"Alpha因子 {alpha_name} 在时间区间 {range_start}-{range_end} 不存在，需要计算")
                    else:
                        Log.logger.debug(f"Alpha因子 {alpha_name} 在时间区间 {range_start}-{range_end} 已存在")
                        
                    if should_skip:
                        Log.logger.info(f"时间区间 {range_start} - {range_end} 的Alpha因子已存在，跳过计算")
                        continue
                    
                # 加载该时间区间的依赖字段数据
                # 根据频率调整起始日期，确保有足够的历史数据用于计算
                adjusted_start_date = factorManager.adjustStartDateByFreq(range_start, freq)
                Log.logger.info(f"调整Alpha计算窗口 - 原始: {range_start}, 调整后: {adjusted_start_date}")
                
                Log.logger.info(f"开始加载Alpha依赖数据 - 字段: {[s.replace('$', '', 1) for s in col_list]}")
                # 1m/秒级频率：按 code chunk 加载计算，避免全量进内存（alpha191 含横截面 rank，
                # chunk 下变 chunk 内排名，语义变，符合"先跑通再优化"）
                if ('m' in freq) or ('s' in freq):
                    _prep = alphaEngine._prep_formula(formula)
                    _prep_cols = alphaEngine.get_col_list(_prep)
                    for _chunk_df in factorManager.loadFactorsByCodeChunk(
                                matrix_list=[s.replace('$', '', 1) for s in _prep_cols],
                                code_list=code_list, market=market, freq=freq,
                                start_date=adjusted_start_date, end_date=range_end, chunk_size=50):
                        _r = alphaEngine._process_alpha_df(_chunk_df, _prep, alpha_name,
                                                            range_start, range_end, market, freq)
                        if _r is not None and not _r.empty:
                            all_results.append(_r)
                    continue
                df = factorManager.loadFactors(
                            matrix_list=[s.replace('$', '', 1) for s in col_list],
                            vector_list=[],
                            code_list=code_list,
                            market=market,
                            freq=freq,
                            start_date=adjusted_start_date,  # 使用调整后的起始日期
                            end_date=range_end,
                            cache=True  # 使用缓存加速
                )

                Log.logger.info(f"成功加载Alpha数据 - 数据量: {len(df)}, 列数: {len(df.columns)}")
                Log.logger.debug(f"数据列名: {list(df.columns)}")
                Log.logger.debug(f"数据索引信息: {df.index.names if hasattr(df.index, 'names') else 'Simple Index'}")
                
                if len(df) > 0:
                    Log.logger.debug(f"Alpha数据样本:\n{df.head()}")

                # 防御：去除重复 (time,code) 行（多市场数据质量参差；get_factors 读取时已去重，mock/其他来源未必）
                if isinstance(df.index, pd.MultiIndex) and df.index.duplicated().any():
                    _dup = int(df.index.duplicated().sum())
                    df = df[~df.index.duplicated(keep='last')]
                    Log.logger.debug(f"去除重复索引行 {_dup} 条")

                if df.empty:
                    Log.logger.warning(f"Alpha数据为空，跳过该区间: {alpha_name} [{range_start}-{range_end}]")
                    continue
                
                try:
                    Log.logger.debug("开始处理Alpha公式中的列名替换")
                    processed_formula = formula
                    for col in col_list:
                        clean_col = col[1:]  # 移除$符号
                        if clean_col not in df.columns:
                            Log.logger.error(f"Alpha计算缺少必需的列: {clean_col}")
                            Log.logger.error(f"可用的列: {list(df.columns)}")
                            Log.logger.error(f"公式: {original_formula}")
                            continue
                        
                        processed_formula = processed_formula.replace(col, f"df['{clean_col}']")
                        df[clean_col] = df[clean_col].astype(float)
                        Log.logger.debug(f"列 {col} -> df['{clean_col}']，数据类型转换为float")
                        
                except KeyError as e:
                    Log.logger.error(f"Alpha公式处理错误 - {alpha_name}: {str(e)}")
                    Log.logger.error(f"原始公式: {original_formula}")
                    Log.logger.error(f"处理后公式: {processed_formula}")
                    Log.logger.error(f"可用列: {list(df.columns)}")
                    continue

                Log.logger.info(f"{alpha_name} 最终计算公式: {processed_formula}")
                
                try:
                    Log.logger.debug("开始执行Alpha公式计算")
                    res = eval(processed_formula)
                    Log.logger.info(f"Alpha公式计算完成，结果类型: {type(res)}")
                    
                    if hasattr(res, 'shape'):
                        Log.logger.debug(f"计算结果形状: {res.shape}")
                    elif hasattr(res, '__len__'):
                        Log.logger.debug(f"计算结果长度: {len(res)}")
                        
                except Exception as e:
                    Log.logger.error(f"Alpha公式执行失败 - {alpha_name}: {str(e)}")
                    Log.logger.error(f"公式: {processed_formula}")
                    Log.logger.error(f"数据信息: {len(df)} rows, columns: {list(df.columns)}")
                    traceback.print_exc()
                    continue

                # 创建结果DataFrame
                result_df = pd.DataFrame()
                
                try:
                    # 检查res是否为Series或DataFrame，并相应处理
                    if isinstance(res, pd.Series):
                        Log.logger.debug("将Series结果转换为DataFrame")
                        result_df = pd.DataFrame({alpha_name: res})
                    elif isinstance(res, pd.DataFrame):
                        Log.logger.debug("处理DataFrame结果")
                        if len(res.columns) == 1:
                            result_df = res.rename(columns={res.columns[0]: alpha_name})
                        else:
                            Log.logger.debug(f"DataFrame有多列({len(res.columns)})，选择第一列")
                            result_df = pd.DataFrame({alpha_name: res.iloc[:, 0]})
                    else:
                        Log.logger.warning(f"未知的结果类型: {type(res)}，尝试转换为DataFrame")
                        result_df = pd.DataFrame({alpha_name: res})
                        
                    Log.logger.info(f"结果DataFrame创建成功，数据量: {len(result_df)}")
                    
                except Exception as e:
                    Log.logger.error(f"创建结果DataFrame失败: {str(e)}")
                    Log.logger.error(f"结果类型: {type(res)}")
                    if hasattr(res, 'shape'):
                        Log.logger.error(f"结果形状: {res.shape}")
                    traceback.print_exc()
                
                # 过滤结果，确保只保存在指定日期范围内的数据
                if not result_df.empty:
                    # 使用当前处理的时间范围进行过滤，而不是整个任务的日期范围
                    current_start_date = range_start
                    current_end_date = range_end
                    
                    # 确保end_date处理
                    if current_end_date == 'now':
                        current_end_date = datetime.datetime.now().strftime("%Y%m%d")
                        
                    try:
                        # 获取结果DataFrame中的时间索引
                        if isinstance(result_df.index, pd.MultiIndex):
                            times = result_df.index.get_level_values('time')
                        else:
                            # 如果不是MultiIndex，检查是否有time列
                            if 'time' in result_df.columns:
                                times = result_df['time']
                            else:
                                # 假设整个索引就是时间
                                times = result_df.index
                        
                        # 确保times是datetime类型
                        if not pd.api.types.is_datetime64_any_dtype(times):
                            try:
                                # 尝试将times转换为datetime
                                times = pd.to_datetime(times)
                                Log.logger.debug("成功将Alpha结果时间索引转换为datetime类型")
                            except Exception as e:
                                Log.logger.error(f"无法将Alpha结果时间索引转换为datetime: {str(e)}")
                                Log.logger.error("跳过日期过滤，使用原始数据")
                                continue
                        
                        # 统一时区处理
                        # 1. 先判断times是否有时区信息
                        has_tz = False
                        if hasattr(times, 'tz') and times.tz is not None:
                            has_tz = True
                            tz_info = times.tz
                            Log.logger.debug(f"检测到Alpha结果时区信息: {tz_info}")
                        
                        # 2. 转换输入的日期为datetime对象
                        start_datetime = pd.to_datetime(current_start_date)
                        end_datetime = pd.to_datetime(current_end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
                        
                        # 3. 根据times的时区情况统一处理
                        if has_tz:
                            # 如果times有时区，将start_datetime和end_datetime本地化为相同时区
                            try:
                                start_datetime = start_datetime.tz_localize(tz_info)
                                end_datetime = end_datetime.tz_localize(tz_info)
                            except TypeError:  # 已经有时区信息的情况
                                start_datetime = start_datetime.tz_convert(tz_info)
                                end_datetime = end_datetime.tz_convert(tz_info)
                        else:
                            # 如果times没有时区，移除所有时区信息
                            if hasattr(times, 'dt'):  # Series类型
                                times = times.dt.tz_localize(None)
                            else:  # DatetimeIndex类型
                                times = times.tz_localize(None)
                            
                            # 确保start_datetime和end_datetime也没有时区信息
                            if hasattr(start_datetime, 'tzinfo') and start_datetime.tzinfo is not None:
                                start_datetime = start_datetime.tz_localize(None)
                            if hasattr(end_datetime, 'tzinfo') and end_datetime.tzinfo is not None:
                                end_datetime = end_datetime.tz_localize(None)
                        
                        # 创建日期过滤条件
                        date_mask = (times >= start_datetime) & (times <= end_datetime)
                        
                        # 应用过滤
                        filtered_df = result_df[date_mask]
                        
                        # 如果过滤后结果为空，则记录警告
                        if filtered_df.empty and not result_df.empty:
                            Log.logger.warning(f"Alpha日期过滤后没有符合范围 {current_start_date} - {current_end_date} 的数据")
                            Log.logger.warning(f"时间范围: {start_datetime} - {end_datetime}")
                            Log.logger.warning(f"样本时间: {times.iloc[0] if hasattr(times, 'iloc') else times[0]}")
                        else:
                            Log.logger.info(f"Alpha日期过滤: {len(result_df)} -> {len(filtered_df)} 条记录")
                            result_df = filtered_df
                    except Exception as e:
                        Log.logger.error(f"Alpha日期过滤过程出错: {str(e)}")
                        Log.logger.error(f"当前处理区间: {current_start_date} - {current_end_date}")
                        if 'times' in locals():
                            Log.logger.error(f"时间数据类型: {type(times)}")
                            Log.logger.error(f"时区信息: {getattr(times, 'tz', None)}")
                            if len(times) > 0:
                                Log.logger.error(f"样本时间: {times.iloc[0] if hasattr(times, 'iloc') else times[0]}")
                        Log.logger.error("跳过日期过滤，使用原始数据")
                        traceback.print_exc()
                
                # 整理结果的索引结构
                if 'time' in result_df.columns:
                    result_df = result_df.reset_index(drop=True)
                else:
                    result_df = result_df.reset_index(drop=False)
                
                # 排序和设置索引
                try:
                    result_df = result_df.sort_values(by=['time', 'code'])
                    result_df = result_df.set_index(['time', 'code'])
                    Log.logger.debug("成功设置Alpha结果的时间和代码索引")
                except Exception as e:
                    Log.logger.error(f"设置Alpha结果索引时出错: {str(e)}")
                    Log.logger.error(f"结果DataFrame列名: {list(result_df.columns)}")
                    if 'time' not in result_df.columns:
                        Log.logger.error("结果中缺少time列")
                    if 'code' not in result_df.columns:
                        Log.logger.error("结果中缺少code列")
                    traceback.print_exc()
                
                # 保存因子数据
                try:
                    Log.logger.info(f"开始保存Alpha因子数据: {alpha_name}")
                    factorManager.saveFactors(result_df, [alpha_name], market, freq)
                    Log.logger.info(f"Alpha因子数据保存完成: {alpha_name}")
                except Exception as e:
                    Log.logger.error(f"保存Alpha因子数据失败: {str(e)}")
                    Log.logger.error(f"因子名: {alpha_name}, 数据量: {len(result_df)}")
                    traceback.print_exc()
                
                Log.logger.info(f"计算Alpha因子 {alpha_name} 区间 [{range_start}-{range_end}] 成功，数据量: {len(result_df)}")
                if len(result_df) > 0:
                    Log.logger.debug(f"Alpha结果数据样本:\n{result_df.head()}")

                all_results.append(result_df)

            # 所有时间区间处理完毕，汇总结果
            computation_time = time.time() - t1
            if all_results:
                final_result = pd.concat(all_results)
                Log.logger.info(f"Alpha因子 {alpha_name} 全部区间完成: {len(all_results)} 个区间, 合并后数据量: {len(final_result)}, 总耗时: {computation_time:.2f} 秒")
                return {"name": alpha_name, "result": final_result}
            else:
                Log.logger.warning(f"Alpha因子 {alpha_name} 所有区间均无有效结果, 总耗时: {computation_time:.2f} 秒")
                return {"name": alpha_name, "result": pd.DataFrame()}

        except Exception as e:
            Log.logger.error(f"计算Alpha因子 {alpha_item['name']} 失败: {str(e)}")
            Log.logger.error(f"Alpha项信息: {alpha_item}")
            Log.logger.error(f"计算参数: market={market}, freq={freq}, start_date={start_date}, end_date={end_date}")
            traceback.print_exc()
            return {"name": alpha_item["name"], "result": pd.DataFrame()}

    # ============ 单公式计算工具（供 factorMining 等复用）============
    @staticmethod
    def _prep_formula(formula):
        """公式预处理：语法转换 + alpha191 特殊字段展开 + 三元"""
        formula = formula.replace("||", " | ").replace("&&", " & ").replace("^", " ** ").replace("\n", " ")
        # alpha191 #69 的 dtm/dbm 等（无 indicator 定义时的兜底展开）
        formula = formula.replace("$dtm", " ($open<=delay($open,1)?0:max(($high-$open),($open-delay($open,1)))) ")
        formula = formula.replace("$dbm", " ($open>=delay($open,1)?0:max(($open-$low),($open-delay($open,1)))) ")
        formula = formula.replace("$tr", " max(max($high-$low,abs($high-delay($close,1))),abs($low-delay($close,1))) ")
        formula = formula.replace("$hd", " $high-delay($high,1) ")
        formula = formula.replace("$ld", " delay($low,1)-$low ")
        if '?' in formula:
            formula = ternary_trans(formula)
        return formula

    @staticmethod
    def get_df(formula='', df=pd.DataFrame(), code_list=[], market='cn_stock', freq='1d',
               start_date='', end_date=''):
        """准备公式依赖的数据。df 非空时直接用；否则按公式字段 loadFactors。
        返回 DataFrame（索引 time, code）。"""
        try:
            if df is None or df.empty:
                col_list = alphaEngine.get_col_list(formula)
                df = factorManager.loadFactors(
                    matrix_list=[c[1:] for c in col_list],
                    code_list=code_list, market=market, freq=freq,
                    start_date=start_date, end_date=end_date)
            if df is None or df.empty:
                return pd.DataFrame()
            if isinstance(df.index, pd.MultiIndex):
                df = df.sort_index()
            return df
        except Exception as e:
            Log.logger.error(f"get_df error: {e}")
            return pd.DataFrame()

    @staticmethod
    def calc(formula='', df=pd.DataFrame(), name='alpha', save=False,
             market='cn_stock', freq='1d', code_list=[], start_date='', end_date=''):
        """在给定 df 上计算单个 alpha 公式，返回结果 Series（索引 time,code）。
        df 为空时自动 loadFactors；save=True 时落盘为因子。"""
        try:
            # 跳过当前引擎不支持的公式（依赖行业/市值/基准等外部数据）
            for todo in ['indneutralize', 'cap', 'filter', 'self', 'banchmarkindex']:
                if todo in formula:
                    return pd.Series()
            formula = alphaEngine._prep_formula(formula)
            col_list = alphaEngine.get_col_list(formula)
            if df is None or df.empty:
                df = alphaEngine.get_df(formula=formula, code_list=code_list, market=market, freq=freq,
                                        start_date=start_date, end_date=end_date)
            if df is None or df.empty:
                return pd.Series()
            for col in col_list:
                clean = col[1:]
                if clean not in df.columns:
                    return pd.Series()
                formula = formula.replace(col, f"df['{clean}']")
                df[clean] = df[clean].astype(float)
            res = eval(formula)
            if isinstance(res, pd.DataFrame):
                res = res.iloc[:, 0]
            if not isinstance(res, pd.Series):
                res = pd.Series(res, index=df.index)
            if save and not res.empty:
                res.name = name
                factorManager.saveFactors(res.to_frame(name=name), [name], market, freq)
            return res
        except Exception as e:
            Log.logger.debug(f"calc error: {formula} -> {e}")
            return pd.Series()

    @staticmethod
    def _process_alpha_df(df, formula, alpha_name, range_start, range_end,
                          market='cn_stock', freq='1d'):
        """在给定 df（全量或 chunk）上算 alpha 并存盘。供 computeAlpha 的 1d/1m 两种模式复用。
        formula 应已 _prep_formula 预处理（含 dtm/dbm 展开）。返回 result_df 或 None。"""
        import datetime as _dt
        if df is None or df.empty:
            return None
        if isinstance(df.index, pd.MultiIndex) and df.index.duplicated().any():
            df = df[~df.index.duplicated(keep='last')]
        if df.empty:
            return None
        try:
            cols = alphaEngine.get_col_list(formula)
            for col in cols:
                clean = col[1:]
                if clean not in df.columns:
                    return None
                formula = formula.replace(col, f"df['{clean}']")
                df[clean] = df[clean].astype(float)
            res = eval(formula)
            if isinstance(res, pd.DataFrame):
                res = res.iloc[:, 0]
            if not isinstance(res, pd.Series):
                res = pd.Series(res, index=df.index)
            result_df = pd.DataFrame({alpha_name: res})
            if result_df.empty:
                return None
            # 日期过滤（只保留 range_start~range_end）
            rs = range_start if range_start != 'now' else _dt.datetime.now().strftime('%Y%m%d')
            re_ = range_end if range_end != 'now' else _dt.datetime.now().strftime('%Y%m%d')
            try:
                if isinstance(result_df.index, pd.MultiIndex):
                    times = result_df.index.get_level_values('time')
                else:
                    times = result_df.index
                if not pd.api.types.is_datetime64_any_dtype(times):
                    times = pd.to_datetime(times)
                s_dt = pd.to_datetime(rs)
                e_dt = pd.to_datetime(re_) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
                result_df = result_df[(times >= s_dt) & (times <= e_dt)]
            except Exception as e:
                Log.logger.debug(f"_process_alpha_df 日期过滤跳过: {e}")
            if result_df.empty:
                return None
            if 'time' in result_df.columns:
                result_df = result_df.reset_index(drop=True)
            else:
                result_df = result_df.reset_index(drop=False)
            try:
                result_df = result_df.sort_values(by=['time', 'code']).set_index(['time', 'code'])
            except Exception:
                pass
            try:
                factorManager.saveFactors(result_df, [alpha_name], market, freq)
            except Exception as e:
                Log.logger.error(f"_process_alpha_df save 失败: {e}")
            return result_df
        except Exception as e:
            Log.logger.debug(f"_process_alpha_df error: {formula} -> {e}")
            return None
        