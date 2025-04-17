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
    # #获取
    # def getAnalysedIndicatorsList(valid=True):
    #     flist1=mydb.selectToDf('select * from factors_analysis where factor_name not like "alpha%"','finhack')
    #     flist1=flist1['factor_name'].tolist()
    #     result=[]
    #     if valid==True:
    #         flist2=mydb.selectToDf('select * from factors_list where check_type=11','finhack')   
    #         flist2=flist2['factor_name'].values
            
    #         for factor in flist1:
    #             factor_name=factor.split('_')[0]
    #             if factor_name in flist2:
    #                 result.append(factor)
            
    #     else:
    #         result=flist1
    #     return result
        
    # def getTopAnalysedFactorsList(top=200,valid=True):
    #     flist1=mydb.selectToDf('select * from factors_analysis order by score desc limit '+str(top),'finhack')
    #     flist1=flist1['factor_name'].tolist()
    #     result=[]
    
        
    #     if valid==True:
    #         flist2=mydb.selectToDf('select * from factors_list where check_type=11','finhack')   
    #         flist2=flist2['factor_name'].values
            
    #         for factor in flist1:
    #             if 'alpha' in factor:
    #                 factor_name=factor
    #             else:
    #                 factor_name=factor.split('_')[0]
                


    #             if factor_name in flist2:
    #                 result.append(factor)
            
    #     else:
    #         result=flist1
    #     return result
    
    
    # #获取因子列表，含indicators和alphas
    # def getFactorsList(valid=True,ignore=True):
    #     flist1=[]
    #     result=[]
    #     ignore_list=[]
    #     if ignore:
    #         ignore_list=['close','vol','volume','open','low','high','pct_chg','amount','pre_close','vwap','stop','lh']
    #     for subfile in os.listdir(SINGLE_FACTORS_DIR):
    #         if not '__' in subfile and not subfile.replace('.csv','') in ignore_list:
    #             flist1.append(subfile.replace('.csv',''))

    #     result=[]
    #     if valid==True:
    #         flist2=mydb.selectToDf('select * from factors_list where check_type=11','finhack')   
    #         flist2=flist2['factor_name'].values
            
    #         for factor in flist1:
    #             if "alpha" not in factor:
    #                 factor_name=factor.split('_')[0]
    #             else:
    #                 factor_name=factor
    #             if factor_name in flist2:
    #                 result.append(factor)
            
    #     else:
    #         result=flist1
    #     result.sort()
    #     return result
 
 
    # def getFactors()


    #  # 获取因子数据
    # def getFactors(factor_list, stock_list=[], start_date='', end_date='', cache=False):
    #     df_factor = pd.DataFrame()
    #     df_tmp= pd.DataFrame()
    #     single_factors_pkl_dir = SINGLE_FACTORS_PKL_DIR
    
    #     # 加载索引
    #     index_pkl_path = SINGLE_FACTORS_PKL_DIR + 'index.pkl'
    #     index_df = pd.read_pickle(index_pkl_path)
    #     index_df['trade_date'] = index_df['trade_date'].astype(str)
    
    #     if start_date != "":
    #         index_df = index_df[index_df['trade_date'] >= start_date]
    
    #     if end_date != "":
    #         index_df = index_df[index_df['trade_date'] <= end_date]
    
    #     if index_df.empty:
    #         return index_df
    
    #     # 获取日期范围的位置
    #     start_index = index_df.index[0]
    #     end_index = index_df.index[-1]
    
    #     factor_dfs = []
    
    #     # 逐个读取因子 .pkl 文件
    #     for factor in factor_list:
    #         factor = factor.replace('$', '')
    #         factor_file = os.path.join(single_factors_pkl_dir, f'{factor}.pkl')
    #         if (os.path.isfile(factor_file)):
    #             # 加载因子数据
    #             factor_data = pd.read_pickle(factor_file)
    #             factor_data[factor] = pd.to_numeric(factor_data[factor], errors='coerce')
    
    #             # 筛选对应日期的因子数据
    #             factor_data = factor_data.iloc[start_index:end_index+1]
    #             factor_dfs.append(factor_data)
                
    #             del factor_data
    #         else:
    #             print(f"Warning: {factor_file} not found")
    
    #     # 将所有因子数据合并为一个 DataFrame
    #     combined_factors_df = pd.concat(factor_dfs, axis=1)
    
    #     df_factor = index_df.join(combined_factors_df, how='left')

    #     if stock_list != []:
    #         df_list = []
    #         combined_factors_df=df_factor.copy()
    #         for ts_code in stock_list:
    #             df_tmp = combined_factors_df[combined_factors_df['ts_code'] == ts_code]
    #             df_list.append(df_tmp)
    
    #         df_factor = pd.concat(df_list)
    #         del df_list
    #     else:
            
    #         pass

        
    #     del combined_factors_df,index_df,factor_dfs,df_tmp

    #     df_factor = df_factor.set_index(['ts_code', 'trade_date'])
    #     df_factor = df_factor.sort_index()           
        
    #     return df_factor



    def loadFactors(matrix_list=[],vector_list=[],code_list=[],market='cn_stock',freq='1m',start_date="20200101",end_date="20201231",cache=False):
        """
        加载因子数据，支持矩阵(matrix)和向量(vector)两种类型的因子
        
        参数:
            matrix_list: 矩阵类型的因子列表
            vector_list: 向量类型的因子列表
            code_list: 股票代码列表，为空则获取所有股票
            market: 市场，默认为cn_stock
            freq: 频率，如1m, 5m, 1d, 1w等
            start_date: 开始日期，格式为"YYYYMMDD"
            end_date: 结束日期，格式为"YYYYMMDD"，如果为"now"则使用当前日期
            cache: 是否使用缓存
            
        返回:
            包含所有请求因子的DataFrame
        """
        # 处理end_date为'now'的情况
        if end_date == 'now':
            end_date = datetime.now().strftime("%Y%m%d")
        
        # 处理缓存逻辑
        if cache:
            # 创建参数的哈希值
            params_str = f"{','.join(sorted(matrix_list))}-{','.join(sorted(vector_list))}-{','.join(sorted(code_list))}-{market}-{freq}-{start_date}-{end_date}"
            hash_str = hashlib.md5(params_str.encode()).hexdigest()
            
            # 构建缓存文件路径
            cache_dir = f"{FACTORS_CACHE_DIR}/{market}/{freq}"
            os.makedirs(cache_dir, exist_ok=True)
            cache_file = f"{cache_dir}/factors_{hash_str}.pkl"
            
            # 检查缓存是否存在
            if os.path.exists(cache_file):
                try:
                    Log.info(f"Loading factors from cache: {cache_file}")
                    return pd.read_pickle(cache_file)
                except Exception as e:
                    Log.error(f"Error loading cache file: {str(e)}")
                    # 如果读取缓存失败，继续执行原流程
        
        # 转换日期格式
        
        start_dt = datetime.strptime(start_date, "%Y%m%d")
        end_dt = datetime.strptime(end_date, "%Y%m%d")
        
        # 确定数据存储的目录结构
        base_matrix_path = FACTORS_DIR+f"/matrix/{market}/{freq}"
        base_vector_path = FACTORS_DIR+f"/vector/{market}/{freq}"
        
        # 根据频率确定目录结构
        time_dirs = []
        if 'w' in freq or 'd' in freq:
            # 按年存储
            years = range(start_dt.year, end_dt.year + 1)
            for year in years:
                time_dirs.append(f"{year}")
        elif 'h' in freq or 'm' in freq:
            # 按年/月存储
            current_dt = start_dt
            while current_dt <= end_dt:
                time_dirs.append(f"{current_dt.year}/{current_dt.month:02d}")
                # 移动到下个月
                if current_dt.month == 12:
                    current_dt = datetime(current_dt.year + 1, 1, 1)
                else:
                    current_dt = datetime(current_dt.year, current_dt.month + 1, 1)
        elif 's' in freq:
            # 按年/月/日存储
            current_dt = start_dt
            while current_dt <= end_dt:
                time_dirs.append(f"{current_dt.year}/{current_dt.month:02d}/{current_dt.day:02d}")
                # 移动到下一天
                current_dt += timedelta(days=1)
        
        # 使用多线程读取各个时间目录下的因子数据
        result_dfs = []
        
        def process_time_dir(time_dir):
            matrix_dir = f"{base_matrix_path}/{time_dir}"
            
            # 获取当前时间目录下的所有code
            available_codes = []
            if os.path.exists(matrix_dir):
                available_codes = [d for d in os.listdir(matrix_dir) if os.path.isdir(os.path.join(matrix_dir, d))]
            
            # 如果code_list不为空，则筛选
            if code_list:
                codes_to_process = [code for code in available_codes if code in code_list]
            else:
                codes_to_process = available_codes
            
            if not codes_to_process:
                return None
            
            # 读取每个code的因子数据
            code_dfs = []
            for code in codes_to_process:
                code_dir = f"{matrix_dir}/{code}"
                index_path = f"{code_dir}/index.pkl"
                
                if not os.path.exists(index_path):
                    continue
                
                # 读取索引数据
                try:
                    index_df = pd.read_pickle(index_path)
                    
                    # 确保trade_date列为字符串格式
                    if 'trade_date' in index_df.columns:
                        index_df['trade_date'] = index_df['trade_date'].astype(str)
                        
                    # 根据日期范围过滤
                    if 'trade_date' in index_df.columns:
                        index_df = index_df[(index_df['trade_date'] >= start_date) & 
                                          (index_df['trade_date'] <= end_date)]
                    
                    if index_df.empty:
                        continue
                    
                    # 读取指定的matrix因子
                    for factor in matrix_list:
                        factor_path = f"{code_dir}/{factor}.pkl"
                        if os.path.exists(factor_path):
                            factor_data = pd.read_pickle(factor_path)
                            # 将因子数据添加到index_df中
                            index_df[factor] = factor_data
                    
                    code_dfs.append(index_df)
                    
                except Exception as e:
                    Log.error(f"Error loading data for {code} in {time_dir}: {str(e)}")
                    traceback.print_exc()
            
            if not code_dfs:
                return None
            
            # 合并同一时间目录下的所有代码数据
            time_df = pd.concat(code_dfs, axis=0)
            return time_df
        
        # 使用线程池并行处理各个时间目录
        with ThreadPoolExecutor(max_workers=min(10, len(time_dirs))) as executor:
            futures = [executor.submit(process_time_dir, time_dir) for time_dir in time_dirs]
            wait(futures, return_when=ALL_COMPLETED)
            
            for future in futures:
                if future.result() is not None:
                    result_dfs.append(future.result())
        
        # 合并所有时间目录的数据
        if not result_dfs:
            # 如果没有矩阵因子数据，返回空DataFrame
            final_df = pd.DataFrame()
        else:
            final_df = pd.concat(result_dfs, axis=0)
            final_df = final_df.reset_index(drop=True)
        
        # 处理向量因子
        if vector_list and not final_df.empty:
            for factor in vector_list:
                vector_path = f"{base_vector_path}/{factor}.pkl"
                if os.path.exists(vector_path):
                    try:
                        vector_df = pd.read_pickle(vector_path)
                        
                        # 确保trade_date列为字符串格式
                        if 'trade_date' in vector_df.columns:
                            vector_df['trade_date'] = vector_df['trade_date'].astype(str)
                            
                        # 根据日期范围过滤
                        if 'trade_date' in vector_df.columns:
                            vector_df = vector_df[(vector_df['trade_date'] >= start_date) & 
                                                (vector_df['trade_date'] <= end_date)]
                        
                        # 如果code_list不为空，则筛选
                        if code_list and 'ts_code' in vector_df.columns:
                            vector_df = vector_df[vector_df['ts_code'].isin(code_list)]
                        
                        # 设置索引以便与final_df合并
                        if 'ts_code' in vector_df.columns and 'trade_date' in vector_df.columns:
                            vector_df = vector_df.set_index(['ts_code', 'trade_date'])
                            
                            # 确保final_df也有相同的索引
                            if not set(['ts_code', 'trade_date']).issubset(final_df.columns):
                                continue
                                
                            final_df = final_df.set_index(['ts_code', 'trade_date'])
                            
                            # 合并向量因子数据
                            final_df = final_df.join(vector_df[[factor]], how='left')
                            
                            # 重置索引
                            final_df = final_df.reset_index()
                    except Exception as e:
                        Log.error(f"Error loading vector factor {factor}: {str(e)}")
                        traceback.print_exc()
        
        # 设置最终的索引并排序
        if not final_df.empty and 'ts_code' in final_df.columns and 'trade_date' in final_df.columns:
            final_df = final_df.set_index(['ts_code', 'trade_date'])
            final_df = final_df.sort_index()
        
        # 保存到缓存
        if cache and not final_df.empty:
            try:
                Log.info(f"Saving factors to cache: {cache_file}")
                final_df.to_pickle(cache_file)
            except Exception as e:
                Log.error(f"Error saving to cache: {str(e)}")
        
        return final_df
    
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