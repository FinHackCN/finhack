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
    def inspectFactor(factor_name, factor_type="matrix", market='cn_stock', freq='1m', start_date=None, end_date=None,only_exists=False):
        """
        检查因子的基本信息，包括开始日期、结束日期、文件大小和代码数量
        
        参数:
            factor_name: 因子名称
            factor_type: 因子类型，"matrix"或"vector"
            market: 市场类型，如cn_stock
            freq: 频率，如1m, 5m, 1d等
            start_date: 开始日期，格式为"YYYYMMDD"，None表示不限制开始日期
            end_date: 结束日期，格式为"YYYYMMDD"，None表示不限制结束日期
            
        返回:
            包含因子信息的字典，如不存在则返回None
        """
        try:
            result = {
                "factor_name": factor_name,
                "factor_type": factor_type,
                "market": market,
                "freq": freq,
                "start_date": None,
                "end_date": None,
                "total_size_mb": 0,
                "code_count": 0,
                "exists": False
            }
            
            # 如果start_date和end_date不为None，转换为datetime对象
            start_dt = None
            end_dt = None
            if start_date is not None:
                start_dt = datetime.strptime(start_date, "%Y%m%d")
            if end_date is not None:
                end_dt = datetime.strptime(end_date, "%Y%m%d")
            
            print("start_date: {}, end_date: {}".format(start_date, end_date))

            if factor_type == "matrix":
                # 矩阵型因子的检查逻辑
                base_path = f"{FACTORS_DIR}/matrix/{market}/{freq}"
                
                # 获取所有可能的时间目录
                time_dirs = []
                
                if os.path.exists(base_path):
                    if 'w' in freq or 'd' in freq:
                        # 按年存储
                        all_years = [f"{year}" for year in os.listdir(base_path) 
                                     if os.path.isdir(os.path.join(base_path, str(year)))]
                        
                        # 根据日期范围筛选年份目录
                        if start_dt is not None or end_dt is not None:
                            for year in all_years:
                                year_int = int(year)
                                # 如果年份在范围内，则添加到time_dirs
                                if (start_dt is None or year_int >= start_dt.year) and \
                                   (end_dt is None or year_int <= end_dt.year):
                                    time_dirs.append(year)
                        else:
                            time_dirs = all_years
                            
                    elif 'h' in freq or 'm' in freq:
                        # 按年/月存储
                        for year in os.listdir(base_path):
                            year_path = os.path.join(base_path, year)
                            year_int = int(year)
                            
                            # 根据日期范围筛选年份
                            if start_dt is not None and year_int < start_dt.year:
                                continue
                            if end_dt is not None and year_int > end_dt.year:
                                continue
                                
                            if os.path.isdir(year_path):
                                for month in os.listdir(year_path):
                                    month_path = os.path.join(year_path, month)
                                    month_int = int(month)
                                    
                                    # 根据日期范围筛选月份
                                    if start_dt is not None and year_int == start_dt.year and month_int < start_dt.month:
                                        continue
                                    if end_dt is not None and year_int == end_dt.year and month_int > end_dt.month:
                                        continue
                                        
                                    if os.path.isdir(month_path):
                                        time_dirs.append(f"{year}/{month}")
                                        
                    elif 's' in freq:
                        # 按年/月/日存储
                        for year in os.listdir(base_path):
                            year_path = os.path.join(base_path, year)
                            year_int = int(year)
                            
                            # 根据日期范围筛选年份
                            if start_dt is not None and year_int < start_dt.year:
                                continue
                            if end_dt is not None and year_int > end_dt.year:
                                continue
                                
                            if os.path.isdir(year_path):
                                for month in os.listdir(year_path):
                                    month_path = os.path.join(year_path, month)
                                    month_int = int(month)
                                    
                                    # 根据日期范围筛选月份
                                    if start_dt is not None and year_int == start_dt.year and month_int < start_dt.month:
                                        continue
                                    if end_dt is not None and year_int == end_dt.year and month_int > end_dt.month:
                                        continue
                                        
                                    if os.path.isdir(month_path):
                                        for day in os.listdir(month_path):
                                            day_path = os.path.join(month_path, day)
                                            day_int = int(day)
                                            
                                            # 根据日期范围筛选日期
                                            if start_dt is not None and year_int == start_dt.year and month_int == start_dt.month and day_int < start_dt.day:
                                                continue
                                            if end_dt is not None and year_int == end_dt.year and month_int == end_dt.month and day_int > end_dt.day:
                                                continue
                                                
                                            if os.path.isdir(day_path):
                                                time_dirs.append(f"{year}/{month}/{day}")
                
                if not time_dirs:
                    return result
                
                # 排序时间目录以确定开始和结束日期
                time_dirs.sort()
                
                all_times = []
                all_codes = set()
                total_size = 0
                factor_exists = False
                
                # 遍历每个时间目录
                for time_dir in time_dirs:
                    dir_path = f"{base_path}/{time_dir}"
                    
                    # 获取该时间目录下的所有代码
                    codes = [d for d in os.listdir(dir_path) if os.path.isdir(os.path.join(dir_path, d))]
                    
                    for code in codes:
                        factor_path = f"{dir_path}/{code}/{factor_name}.pkl"
                        index_path = f"{dir_path}/{code}/index.pkl"
                        
                        # 检查因子文件是否存在
                        #print(f"Checking {factor_path}...")
                        if os.path.exists(factor_path):
                            if only_exists:
                                factor_exists = True
                                break
                            total_size += os.path.getsize(factor_path)
                            
                            # 从索引文件中获取时间信息
                            if os.path.exists(index_path):
                                try:
                                    index_df = pd.read_pickle(index_path)
                                    # 检查索引结构，使用time而不是time
                                    if  'time' in index_df.index.names:
                                        times = index_df.index.get_level_values('time')
                                        # 如果有日期范围限制，则进行过滤
                                        if start_dt is not None or end_dt is not None:
                                            valid_times = times
                                            if start_dt is not None:
                                                start_dt_tz = pd.Timestamp(start_dt).tz_localize('Asia/Shanghai')
                                                valid_times = valid_times[valid_times >= start_dt_tz]
                                            if end_dt is not None:
                                                end_dt_tz = pd.Timestamp(end_dt).tz_localize('Asia/Shanghai')
                                                valid_times = valid_times[valid_times <= end_dt_tz]

                                            if len(valid_times) > 0:
                                                all_times.extend(valid_times)
                                                all_codes.add(code)
                                                factor_exists = True
                                        else:
                                            # 无日期限制，添加全部时间
                                            all_times.extend(times)
                                            all_codes.add(code)
                                            factor_exists = True
                                except Exception as e:
                                    Log.error(f"Error reading index file {index_path}: {str(e)}")
                
                if factor_exists:
                    result["exists"] = True
                    result["code_count"] = len(all_codes)
                    result["total_size_mb"] = round(total_size / (1024 * 1024), 2)  # 转换为MB
                    
                    if all_times:
                        all_times = sorted(all_times)
                        # 将时间戳转换为字符串格式YYYYMMDD
                        result["start_date"] = all_times[0].strftime("%Y%m%d")
                        result["end_date"] = all_times[-1].strftime("%Y%m%d")
                
            elif factor_type == "vector":
                # 向量型因子的检查逻辑
                factor_path = f"{FACTORS_DIR}/vector/{market}/{freq}/{factor_name}.pkl"
                
                if os.path.exists(factor_path):
                    result["exists"] = True
                    if only_exists:
                        return result
                    
                    result["total_size_mb"] = round(os.path.getsize(factor_path) / (1024 * 1024), 2)  # 转换为MB
                    
                    try:
                        # 读取向量因子数据
                        vector_df = pd.read_pickle(factor_path)
                        
                        if not vector_df.empty:
                            # 获取代码数量
                            result["code_count"] = 0
                            
                            # 检查索引格式
                            if isinstance(vector_df.index, pd.MultiIndex) and 'time' in vector_df.index.names and 'code' in vector_df.index.names:
                                times = vector_df.index.get_level_values('time')
                                
                                # 如果有日期范围限制，则进行过滤
                                if start_dt is not None or end_dt is not None:
                                    valid_times = times
                                    if start_dt is not None:
                                        start_dt_tz = pd.Timestamp(start_dt).tz_localize('Asia/Shanghai')
                                        valid_times = valid_times[valid_times >= start_dt_tz]
                                    if end_dt is not None:
                                        end_dt_tz = pd.Timestamp(end_dt).tz_localize('Asia/Shanghai')
                                        valid_times = valid_times[valid_times <= end_dt_tz]
                                    
                                    if len(valid_times) > 0:
                                        result["start_date"] = valid_times.min().strftime("%Y%m%d")
                                        result["end_date"] = valid_times.max().strftime("%Y%m%d")
                                        result["code_count"] = len(vector_df.index.get_level_values('code').unique())
                                else:
                                    # 无日期限制，使用全部时间
                                    result["start_date"] = times.min().strftime("%Y%m%d")
                                    result["end_date"] = times.max().strftime("%Y%m%d")
                                    result["code_count"] = len(vector_df.index.get_level_values('code').unique())
                    except Exception as e:
                        Log.error(f"Error reading vector factor {factor_path}: {str(e)}")
            
            return result
        
        except Exception as e:
            Log.error(f"Error inspecting factor {factor_name}: {str(e)}")
            traceback.print_exc()
            return None


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
                        
                        # 确保time列为字符串格式
                        if 'time' in vector_df.columns:
                            vector_df['time'] = vector_df['time'].astype(str)
                            
                        # 根据日期范围过滤
                        if 'time' in vector_df.columns:
                            vector_df = vector_df[(vector_df['time'] >= start_date) & 
                                                (vector_df['time'] <= end_date)]
                        
                        # 如果code_list不为空，则筛选
                        if code_list and 'code' in vector_df.columns:
                            vector_df = vector_df[vector_df['code'].isin(code_list)]
                        
                        # 设置索引以便与final_df合并
                        if 'code' in vector_df.columns and 'time' in vector_df.columns:
                            vector_df = vector_df.set_index(['code', 'time'])
                            
                            # 确保final_df也有相同的索引
                            if not set(['code', 'time']).issubset(final_df.columns):
                                continue
                                
                            final_df = final_df.set_index(['code', 'time'])
                            
                            # 合并向量因子数据
                            final_df = final_df.join(vector_df[[factor]], how='left')
                            
                            # 重置索引
                            final_df = final_df.reset_index()
                    except Exception as e:
                        Log.error(f"Error loading vector factor {factor}: {str(e)}")
                        traceback.print_exc()
        
        # 设置最终的索引并排序
        if not final_df.empty and 'code' in final_df.columns and 'time' in final_df.columns:
            final_df = final_df.set_index(['code', 'time'])
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
                        
                        # 修改正则表达式，确保df前面没有任何字母、数字、下划线或横杠
                        pattern = re.compile(r"(?<![A-Za-z0-9_\-])df\[\'([A-Za-z0-9_\-]*?)\'\]")   # 查找数字
                        
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