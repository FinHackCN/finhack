import sys
import os
import time
import random
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
import numpy as np

from sklearn.datasets import load_boston
from gplearn.genetic import SymbolicTransformer
import gplearn as gp

from library.config import config
from library.mydb import mydb
from library.astock import AStock
from factors.indicatorCompute import indicatorCompute
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor, wait, ALL_COMPLETED
from factors.factorAnalyzer import factorAnalyzer
from factors.factorManager import factorManager
from factors.alphaEngine import alphaEngine
import factors.alphaEngine as alphaFunc





init_function = ['add', 'sub', 'mul', 'div', 'sqrt', 'abs', 'sin', 'cos', 'tan']



def trans_xy(xy,key='x'):
    status=True
    
    if(len(xy)<100):
        status=False
        return status,xy
    if not 'numpy' in str(type(xy)):
        xy=np.zeros(len(xy))
        status=False
        return status,xy
    if 'numpy.memmap' in str(type(xy)):
        xy=np.array(xy)   
        
    if xy.max()==xy.min():
        xy=np.zeros(len(xy))
        status=False
        return status,xy
        
    if(type(xy)==type(np.ndarray([]))):
        df_xy=df_tmp.copy()
        df_xy[key]=xy
        xy=df_xy
        xy=xy.set_index(['ts_code','trade_date'])
        xy=xy[key]   
    return status,xy
    
def check_window(window):
    status=True
    if window.max()==window.min() and window.mean()>0:
        window = int(window[0])
    else:
        status=False
        window=np.zeros(len(window))
    return status,window


def _correlation(x,y):
    # status,window=check_window(window)
    # if not status:
    #     return window
    status,x=trans_xy(x,'x')
    if not status:
        return x
    status,y=trans_xy(y,'y')
    if not status:
        return y
    df=alphaFunc.correlation(x,y)
    return np.nan_to_num(df.values)



def _covariance(x,y):
    # status,window=check_window(window)
    # if not status:
    #     return window
    status,x=trans_xy(x,'x')
    if not status:
        return x
    status,y=trans_xy(y,'y')
    if not status:
        return y
    df=alphaFunc.covariance(x,y)
    return np.nan_to_num(df.values)
    

def _rank(x):
    status,x=trans_xy(x,'x')
    if not status:
        return x
    df=alphaFunc.rank(x)
    return np.nan_to_num(df.values)    
 
def _rank(x):
    status,x=trans_xy(x,'x')
    if not status:
        return x
    df=alphaFunc.rank(x)
    return np.nan_to_num(df.values)       
    
def _log(x):
    if x.max()==x.min() and x.mean()==0:
        return np.zeros(len(x))
    status,x=trans_xy(x,'x')
    if not status:
        return x
    df=alphaFunc.log(x)
    return np.nan_to_num(df.values)   

def _ts_sum(x):
    # status,window=check_window(window)
    # if not status:
    #     return window
    status,x=trans_xy(x,'x')
    if not status:
        return x
    df=alphaFunc.ts_sum(x)
    return np.nan_to_num(df.values)
    
def _delta(x):
    status,x=trans_xy(x,'x')
    if not status:
        return x
    df=alphaFunc.delta(x)
    return np.nan_to_num(df.values)   
    
def _product(x):
    status,x=trans_xy(x,'x')
    if not status:
        return x
    df=alphaFunc.product(x)
    return np.nan_to_num(df.values)  
    
def _ts_min(x):
    status,x=trans_xy(x,'x')
    if not status:
        return x
    df=alphaFunc.ts_min(x)
    return np.nan_to_num(df.values)  
    
    
def _ts_max(x):
    status,x=trans_xy(x,'x')
    if not status:
        return x
    df=alphaFunc.ts_max(x)
    return np.nan_to_num(df.values)  
    
def _delay_1(x):
    status,x=trans_xy(x,'x')
    if not status:
        return x
    df=alphaFunc.delay(x,1)
    return np.nan_to_num(df.values)      
 
def _delay_3(x):
    status,x=trans_xy(x,'x')
    if not status:
        return x
    df=alphaFunc.delay(x,3)
    return np.nan_to_num(df.values)  

def _delay_5(x):
    status,x=trans_xy(x,'x')
    if not status:
        return x
    df=alphaFunc.delay(x,5)
    return np.nan_to_num(df.values)  

def _delay_7(x):
    status,x=trans_xy(x,'x')
    if not status:
        return x
    df=alphaFunc.delay(x,7)
    return np.nan_to_num(df.values)  
    
def _stddev(x):
    status,x=trans_xy(x,'x')
    if not status:
        return x
    df=alphaFunc.stddev(x)
    return np.nan_to_num(df.values)     
  
def _ts_rank(x):
    status,x=trans_xy(x,'x')
    if not status:
        return x
    df=alphaFunc.ts_rank(x)
    return np.nan_to_num(df.values)    
    
def _ts_argmax(x):
    status,x=trans_xy(x,'x')
    if not status:
        return x
    df=alphaFunc.ts_argmax(x)
    return np.nan_to_num(df.values)  
    
def _ts_argmin(x):
    status,x=trans_xy(x,'x')
    if not status:
        return x
    df=alphaFunc.ts_argmin(x)
    return np.nan_to_num(df.values)  
    
def _lowday(x):
    status,x=trans_xy(x,'x')
    if not status:
        return x
    df=alphaFunc.lowday(x)
    return np.nan_to_num(df.values)  
    
def _highday(x):
    status,x=trans_xy(x,'x')
    if not status:
        return x
    df=alphaFunc.highday(x)
    return np.nan_to_num(df.values) 
   
def _sumac(x):
    status,x=trans_xy(x,'x')
    if not status:
        return x
    df=alphaFunc.sumac(x)
    return np.nan_to_num(df.values)    
   
function_set = [
    gp.functions.make_function(function = _covariance,name = 'covariance',arity = 2),
    gp.functions.make_function(function = _correlation,name = 'correlation',arity = 2),
    gp.functions.make_function(function = _rank,name = 'rank',arity = 1),
    gp.functions.make_function(function = _log,name = 'log',arity = 1),
    gp.functions.make_function(function = alphaFunc.min,name = 'min',arity = 2),
    gp.functions.make_function(function = alphaFunc.max,name = 'max',arity = 2),
    gp.functions.make_function(function = _ts_sum,name = 'ts_sum',arity = 1),
    gp.functions.make_function(function = _delta,name = 'delta',arity = 1),
    gp.functions.make_function(function = _product,name = 'product',arity = 1),
    gp.functions.make_function(function = _ts_min,name = 'ts_min',arity = 1),
    gp.functions.make_function(function = _ts_max,name = 'ts_max',arity = 1),
    gp.functions.make_function(function = _delay_1,name = 'delay_1',arity = 1),
    gp.functions.make_function(function = _delay_3,name = 'delay_3',arity = 1),
    gp.functions.make_function(function = _delay_5,name = 'delay_5',arity = 1),
    gp.functions.make_function(function = _delay_7,name = 'delay_7',arity = 1),
    gp.functions.make_function(function = _ts_rank,name = 'ts_rank',arity = 1),
    gp.functions.make_function(function = _stddev,name = 'stddev',arity = 1),
    gp.functions.make_function(function = _ts_argmax,name = 'ts_argmax',arity = 1),
    gp.functions.make_function(function = _ts_argmin,name = 'ts_argmin',arity = 1),
    gp.functions.make_function(function = _lowday,name = 'lowday',arity = 1),
    gp.functions.make_function(function = _highday,name = 'highday',arity = 1),
    gp.functions.make_function(function = alphaFunc.sign,name = 'sign',arity = 1)
    
]



    
startdate='20160619'
enddate='20160619'
    
hs300=AStock.getIndexMember(index='000300.SH',trade_date='')
    
hs300=list(set(hs300))


while True:
    t1=time.time()
    
    flist=factorManager.getAnalysedIndicatorsList()
    random.shuffle(flist)
    n=random.randint(3,7)
    factor_list=[]
    
    for i in range(0,n):
        factor_list.append(flist.pop())
    
    factor_list=factor_list+['open','high','low','close','pre_close','change','returns','volume','amount','vwap'] 
    
    #factor_list=['open','high','low','close']
    
    df_all_25=factorManager.getFactors(factor_list=factor_list,stock_list=hs300[0:25],start_date='20160823',end_date='20200823')
    df_all_25['Y']=df_all_25.groupby('ts_code',group_keys=False).apply(lambda x: x['close'].shift(-10)/x['open'].shift(-1))
    df_all_25=df_all_25.dropna()
    df_all_25=df_all_25.reset_index() 
    
    df_all_300=factorManager.getFactors(factor_list=factor_list,stock_list=hs300,start_date='20160823',end_date='20200823')
    df_all_300['Y']=df_all_300.groupby('ts_code',group_keys=False).apply(lambda x: x['close'].shift(-10)/x['open'].shift(-1))
    df_all_300=df_all_300.dropna()  
    
    
    df_tmp=df_all_25[['ts_code','trade_date']]
    df_tmp=df_tmp.reset_index(drop=True)
    
    
    label = df_all_25['Y']
    train = df_all_25.drop(columns=['ts_code','trade_date','Y'])
    
    
    gp1 = SymbolicTransformer(  
                                generations=random.randint(2,6), #整数，可选(默认值=20)要进化的代数
                                population_size=1000,# 整数，可选(默认值=1000)，每一代群体中的公式数量
                                hall_of_fame=200, # 备选因子的数量
                                n_components=200,#最终筛选出的最优因子的数量
                                function_set=function_set+init_function , # 函数集
                                parsimony_coefficient=0.001, # 节俭系数
                                tournament_size=20,  # 作为父代的数量
                                init_depth=(2, 6),  # 公式树的初始化深度
                                max_samples=1, 
                                verbose=1,
                                #const_range = (0,0),
                                p_crossover=0.9,  # 交叉变异概率
                                p_subtree_mutation=0.01,  # 子树变异概率
                                p_hoist_mutation=0.01,  # Hoist 变异概率
                                p_point_mutation=0.01,  # 点变异概率
                                p_point_replace=0.05,  # 点替代概率                             
                                feature_names=list('$'+n for n in train.columns),
                                random_state=int(time.time()),  # 随机数种子
                                n_jobs=12
                        )
    
     
     

    
    gp1.fit(train,label)
    new_df2 = gp1.transform(train)
    
    print(gp1)
    
    
    alphas=[]
    
    for formula in gp1:
        alphas.append(str(formula))
    
    alphas=list(set(alphas))
     
    
    
    for alpha in alphas:
        print(alpha)
        df_alpha=alphaEngine.calc(alpha,df_all_300.copy(),'alpha',True)
        
        if df_alpha.empty:
            print('err')
            continue
        
        
        df_analys=df_all_300.copy()
        df_analys['alpha']=df_alpha
        df_analys=df_analys[['close','open','alpha']]
        
        factorAnalyzer.analys('alpha',df=df_analys,start_date='',end_date='',formula=alpha,pool='hs300',table='factors_mining')
        print("\n")
    
    
    print(time.time()-t1)
    
    
