import sys
import os
import time
import random
import pandas as pd
import numpy as np
import gplearn as gp
from gplearn.genetic import SymbolicTransformer

from finhack.library.config import Config
from finhack.library.db import DB

import finhack.factor.default.alphaEngine as alphaFunc
from finhack.factor.default.alphaEngine import alphaEngine
from finhack.factor.default.factorAnalyzer import factorAnalyzer
from finhack.factor.default.factorManager import factorManager
from runtime.constant import *
from finhack.library.ai import AI



class factorMining():

    def kimi(prompt,model,stock_list):
        return factorMining.openai(prompt,model,stock_list,'kimi')
        pass

    def gpt(prompt,model,stock_list):
        return factorMining.openai(prompt,model,stock_list,'gpt')
        pass

    def openai(prompt,model,stock_list,s="gpt"):
        
        flist=factorManager.getFactorsList()+['open','high','low','close','amount','volume','vwap','returns']
        
        while True:
            print("本批次alpha公式生成中...\n")
            prompt=AI.load_prompt('autoalpha')
            if s=="gpt":
                res=AI.ChatGPT(prompt,model)
            elif s=="kimi":
                res=AI.Kimi(prompt,model)
            lines = res.splitlines()
            alphas = [line for line in lines if '$' in line and '(' in line]
            # 遍历列表，逐行输出
            
            print("\n".join(alphas))
            print("\n已自动生成如上alpha公式，正在对有效因子进行分析...\n")
            
            for alpha in alphas:
                col_list=alphaEngine.get_col_list(alpha)
                
                valid=True
                for col in col_list:
                    if col[1:] not in flist:
                        valid=False
                        break
                if not valid:
                    continue
                
                
                diff_date,max_date,df_check=alphaEngine.get_df(formula=alpha,df=pd.DataFrame(),name='alpha',check=False,ignore_notice=True,stock_list=stock_list,diff=False)
                df_alpha=alphaEngine.calc(formula=alpha,df=df_check,name='alpha',check=False,save=False,ignore_notice=True)
                if df_alpha.empty:
                    #print('err')
                    continue
                
                df_analys=df_check.copy()
                df_analys['alpha']=df_alpha
                df_analys=df_analys[['alpha']]
                df_base=factorManager.getFactors(factor_list=['open','close'],cache=True)

                merged_df = df_analys.merge(df_base, left_index=True, right_index=True, how='left')

                factorAnalyzer.analys('alpha',df=merged_df,formula=alpha,source='chatgpt',table='factors_mining',ignore_error=True)
                #print("\n")



    def gplearn(train,label,_df_tmp,df_check,source='gplearn'):
        df_tmp=_df_tmp

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

        
        gp1 = SymbolicTransformer(  
                                    generations=10, #整数，可选(默认值=20)要进化的代数
                                    population_size=500,# 整数，可选(默认值=1000)，每一代群体中的公式数量
                                    hall_of_fame=100, # 备选因子的数量
                                    n_components=50,#最终筛选出的最优因子的数量
                                    function_set=function_set+init_function , # 函数集
                                    parsimony_coefficient=0.002, # 节俭系数
                                    tournament_size=20,  # 作为父代的数量
                                    init_depth=(2, 5),  # 公式树的初始化深度
                                    max_samples=0.9, 
                                    verbose=1,
                                    #const_range = (0,0),
                                    p_crossover=0.9,  # 交叉变异概率
                                    p_subtree_mutation=0.01,  # 子树变异概率
                                    p_hoist_mutation=0.01,  # Hoist 变异概率
                                    p_point_mutation=0.01,  # 点变异概率
                                    p_point_replace=0.05,  # 点替代概率                             
                                    feature_names=list('$'+n for n in train.columns),
                                    random_state=int(time.time()),  # 随机数种子
                                    n_jobs=1
                            )
        
        
        gp1.fit(train,label)
        new_df2 = gp1.transform(train)
        
        
        alphas=[]
        
        for formula in gp1:
            alphas.append(str(formula))
        
        alphas=list(set(alphas))
        
        print(alphas)
         
        for alpha in alphas:
            df_alpha=alphaEngine.calc(formula=alpha,df=df_check.copy(),name='alpha',check=False,save=False,ignore_notice=True,diff=False)
            if df_alpha.empty:
                #print('err')
                continue
            
            df_analys=df_check.copy()
            df_analys['alpha']=df_alpha
            df_analys=df_analys[['open','close','alpha']]
            factorAnalyzer.analys('alpha',df=df_analys,formula=alpha,source=source,table='factors_mining',ignore_error=True)
            #print("\n")
        
         
        
