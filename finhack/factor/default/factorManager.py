import os
import re
import time
import sys
import datetime
import hashlib
import threading
import pandas as pd
import numpy as np
from numpy.lib import recfunctions
from datetime import datetime, timedelta
import traceback

from finhack.library.mydb import mydb
from runtime.constant import *
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor, wait, ALL_COMPLETED
import finhack.library.log as Log

class factorManager:
    #获取
    def getAnalysedIndicatorsList(top=200,valid=True):
        flist1=mydb.selectToDf('select * from factors_analysis where factor_name not like "alpha%"','finhack')
        flist1=flist1['factor_name'].tolist()
        result=[]
        if valid==True:
            flist2=mydb.selectToDf('select * from factors_list where check_type=11','finhack')   
            flist2=flist2['factor_name'].values
            
            for factor in flist1:
                factor_name=factor.split('_')[0]
                if factor_name in flist2:
                    result.append(factor)
            
        else:
            result=flist1
        return result
        
    def getTopAnalysedFactorsList(top=200,valid=True):
        flist1=mydb.selectToDf('select * from factors_analysis order by score desc limit '+str(top),'finhack')
        flist1=flist1['factor_name'].tolist()
        result=[]
    
        
        if valid==True:
            flist2=mydb.selectToDf('select * from factors_list where check_type=11','finhack')   
            flist2=flist2['factor_name'].values
            
            for factor in flist1:
                if 'alpha' in factor:
                    factor_name=factor
                else:
                    factor_name=factor.split('_')[0]
                


                if factor_name in flist2:
                    result.append(factor)
            
        else:
            result=flist1
        return result
    
    
    #获取因子列表
    def getFactorsList(valid=True,ignore=True):
        flist1=[]
        result=[]
        ignore_list=[]
        if ignore:
            ignore_list=['close','vol','volume','open','low','high','pct_chg','amount','pre_close','vwap','stop','lh']
        for subfile in os.listdir(SINGLE_FACTORS_DIR):
            if not '__' in subfile and not subfile.replace('.csv','') in ignore_list:
                flist1.append(subfile.replace('.csv',''))
                
 
                
        result=[]
        if valid==True:
            flist2=mydb.selectToDf('select * from factors_list where check_type=11','finhack')   
            flist2=flist2['factor_name'].values
            
            for factor in flist1:
                if "alpha" not in factor:
                    factor_name=factor.split('_')[0]
                else:
                    factor_name=factor
                if factor_name in flist2:
                    result.append(factor)
            
        else:
            result=flist1
        result.sort()
        return result
 
 
     # 获取因子数据
    def getFactors(factor_list, stock_list=[], start_date='', end_date='', cache=False):
        df_factor = pd.DataFrame()
        single_factors_pkl_dir = SINGLE_FACTORS_PKL_DIR
    
        # 加载索引
        index_pkl_path = SINGLE_FACTORS_PKL_DIR + 'index.pkl'
        index_df = pd.read_pickle(index_pkl_path)
        index_df['trade_date'] = index_df['trade_date'].astype(str)
    
        if start_date != "":
            index_df = index_df[index_df['trade_date'] >= start_date]
    
        if end_date != "":
            index_df = index_df[index_df['trade_date'] <= end_date]
    
        if index_df.empty:
            return index_df
    
        # 获取日期范围的位置
        start_index = index_df.index[0]
        end_index = index_df.index[-1]
    
        factor_dfs = []
    
        # 逐个读取因子 .pkl 文件
        for factor in factor_list:
            factor = factor.replace('$', '')
            factor_file = os.path.join(single_factors_pkl_dir, f'{factor}.pkl')
            if os.path.isfile(factor_file):
                # 加载因子数据
                factor_data = pd.read_pickle(factor_file)
                factor_data[factor] = pd.to_numeric(factor_data[factor], errors='coerce')
    
                # 筛选对应日期的因子数据
                factor_data = factor_data.iloc[start_index:end_index+1]
                factor_dfs.append(factor_data)
            else:
                print(f"Warning: {factor_file} not found")
    
        # 将所有因子数据合并为一个 DataFrame
        combined_factors_df = pd.concat(factor_dfs, axis=1)
    
        df_factor = index_df.join(combined_factors_df, how='left')
        df_factor = df_factor.set_index(['ts_code', 'trade_date'])
        df_factor = df_factor.sort_index()    
    
        if stock_list != []:
            df_list = []
            for ts_code in stock_list:
                df_tmp = combined_factors_df[combined_factors_df['ts_code'] == ts_code]
                df_list.append(df_tmp)
    
            df_factor = pd.concat(df_list)
        else:
            
            pass

    
        return df_factor


    
    
    #获取alpha列表的列表
    def getAlphaLists():
        alphalists=[]
        path = CONFIG_DIR+"/factorlist/alphalist/"
        for subfile in os.listdir(path):
            if not '__' in subfile:
                listname=subfile
                alphalists.append(subfile)
        return alphalists
    
    #根据alpha列表获取alpha
    def getAlphaList(listname):
        path = CONFIG_DIR+"/factorlist/alphalist/"+listname
        with open(path, 'r', encoding='utf-8') as f:
            return f.readlines()
            

    def getIndicatorsList():
        return_fileds=[]
        path = INDICATORS_DIR
        for subfile in os.listdir(path):
            if not '__' in subfile:
                indicators=subfile.split('.py')
                indicators=indicators[0]
                function_name=''
                code=''
                find=False
                with open(path+subfile) as filecontent:
                    for line in filecontent:
                        if(line.strip()[0:1]=='#'):
                            code=code+"\n"+line
                            continue
                        #提取当前函数名
                        if('def ' in line):
                            function_name=line.split('def ')
                            function_name=function_name[1]
                            function_name=function_name.split('(')
                            function_name=function_name[0]
                            function_name=function_name.strip()
                            code=line
                        else:
                            code=code+"\n"+line
                        left=line.split('=')
                        left=left[0]
                        
                        pattern = re.compile(r"df\[\'([A-Za-z0-9_\-]*?)\'\]")   # 查找数字
                        
                        flist = pattern.findall(left)
                        
                        for f in flist.copy():
                            #前缀是tmp_的都是临时因子，不管
                            if f[:4]=='tmp_':
                                flist.remove(f)
                        return_fileds=return_fileds+flist
                        
         
        path = CONFIG_DIR+"/factorlist/indicatorlist/"
        with open(path+'all','w') as file_object:
            file_object.write("\n".join(return_fileds))  
        
        #print("\n".join(return_fileds))  
        return return_fileds