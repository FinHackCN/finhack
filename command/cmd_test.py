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



t1=time.time()


hs300=AStock.getIndexMember(index='000300.SH',trade_date='20220104')
df_25=AStock.getStockDailyPrice(hs300[0:25],where='',startdate='20160619',enddate='20200805',fq='hfq')
df_300=AStock.getStockDailyPrice(hs300,where='',fq='hfq')
mypath=os.path.dirname(os.path.dirname(__file__))
cache_path_25=mypath+"/cache/factors/mining_factor_df_25"
cache_path_300=mypath+"/cache/factors/mining_factor_df_300"
if os.path.isfile(cache_path_25):
    df_all_25=pd.read_pickle(cache_path_25)
else:

    factor_list=['buySmVol_0','WILLR_0']
    df_all_25=indicatorCompute.computeListByStock(ts_code='test',list_name='test',where='',factor_list=factor_list,c_list=[],pure=True,check=True,df_price=df_25,db='tushare')
    df_all_25=df_all_25.dropna()
    df_all_25['Y']=df_all_25.groupby('ts_code')['close'].shift(-10)/df_all_25['close']
    df_all_25.to_pickle(cache_path_25)          
df_all_25=df_all_25.dropna()
df_all_25=df_all_25.reset_index(drop=True)

      
if os.path.isfile(cache_path_300):
    df_all_300=pd.read_pickle(cache_path_300)
else:

    factor_list=['buySmVol_0','WILLR_0']
    df_all_300=indicatorCompute.computeListByStock(ts_code='test',list_name='test',where='',factor_list=factor_list,c_list=[],pure=True,check=True,df_price=df_25,db='tushare')
    df_all_300=df_all_300.dropna()
    df_all_300['Y']=df_all_300.groupby('ts_code')['close'].shift(-10)/df_all_300['close']
    df_all_300.to_pickle(cache_path_300)   




init_function = ['add', 'sub', 'mul', 'div', 'sqrt', 'abs', 'sin', 'cos', 'tan']

df_tmp=df_all_25[['ts_code','trade_date']]
df_tmp=df_tmp.reset_index(drop=True)


 

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
    
def _delay(x):
    status,x=trans_xy(x,'x')
    if not status:
        return x
    df=alphaFunc.delay(x)
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
    gp.functions.make_function(function = _delay,name = 'delay',arity = 1),
    gp.functions.make_function(function = _ts_rank,name = 'ts_rank',arity = 1),
    gp.functions.make_function(function = _stddev,name = 'stddev',arity = 1),
    gp.functions.make_function(function = _ts_argmax,name = 'ts_argmax',arity = 1),
    gp.functions.make_function(function = _ts_argmin,name = 'ts_argmin',arity = 1),
    gp.functions.make_function(function = _lowday,name = 'lowday',arity = 1),
    gp.functions.make_function(function = _highday,name = 'highday',arity = 1),
    gp.functions.make_function(function = alphaFunc.sign,name = 'sign',arity = 1)
    
]






label = df_all_25['Y']
train = df_all_25.drop(columns=['ts_code','trade_date','change','pre_close','adj_factor','Y'])

print(train)

gp1 = SymbolicTransformer(  
                            generations=3, #整数，可选(默认值=20)要进化的代数
                            population_size=1000,# 整数，可选(默认值=1000)，每一代群体中的公式数量
                            hall_of_fame=100, 
                            n_components=100,
                            function_set=function_set+init_function ,
                            parsimony_coefficient=0.0005,
                            max_samples=0.9, 
                            verbose=1,
                            const_range = (10.0,20.0),
                            feature_names=list('$'+n for n in train.columns),
                            random_state=int(time.time()),  # 随机数种子
                            n_jobs=-1
                    )




# gp1 = SymbolicTransformer(generations=20,  # 公式进化的世代数量
#                           population_size=2000,  # 每一代生成因子数量
#                           n_components=100,  # 最终筛选出的最优因子的数量
#                           hall_of_fame=100,  # 备选因子的数量
#                           function_set=function_set+init_function,  # 函数集
#                           parsimony_coefficient=0.001,  # 节俭系数
#                           tournament_size=20,  # 作为父代的数量
#                           init_depth=(2, 6),  # 公式树的初始化深度
#                           metric='pearson', # 适应度指标，可以用make_fitness自定义
#                           const_range = (10.0,20.0), # 因子中常数的取值范围
#                           p_crossover=0.9,  # 交叉变异概率
#                           p_subtree_mutation=0.01,  # 子树变异概率
#                           p_hoist_mutation=0.01,  # Hoist 变异概率
#                           p_point_mutation=0.01,  # 点变异概率
#                           p_point_replace=0.05,  # 点替代概率                         
#                           max_samples=0.9,  # 最大采样比例
#                           verbose=0,
#                           random_state=int(time.time()),  # 随机数种子
#                           n_jobs=-1,  # 并行计算使用的核心数量
#                          )





gp1.fit(train,label)
new_df2 = gp1.transform(train)


alphas=[]

for formula in gp1:
    alphas.append(str(formula))

alphas=list(set(alphas))

df_all_300=df_all_300.set_index(['ts_code','trade_date'])
for alpha in alphas:
    print(alpha)
    alpha=alphaEngine.calc(alpha,df_all_300.copy(),'alpha',True)
    
    if alpha.empty:
        print('err')
        continue
    
    
    df_analys=df_all_300.copy()
    df_analys['alpha']=alpha
    df_analys=df_analys[['close','alpha']]
    
    
    factorAnalyzer.analys('alpha',df_analys)

    print("\n")


print(time.time()-t1)


