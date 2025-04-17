import hashlib
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from runtime.constant import *
import glob
from tqdm import tqdm
import logging
import concurrent.futures
from functools import partial

def inspectKline(market='cn_stock', freq='1m'):
    """
    获取某market freq下K线数据的基本信息
    
    参数:
    market: 市场类型，如 cn_stock, cn_future 等
    freq: 频率，如 1m, 5m, 15m, 60m, 1d 等
    
    返回:
    包含K线数据信息的字典，包括开始日期、结束日期、代码数量、文件数量和大小
    """
    try:
        result = {
            "market": market,
            "freq": freq,
            "start_date": None,
            "end_date": None,
            "code_count": 0,
            "codebased_files": 0,
            "codebased_size_mb": 0,
            "timebased_files": 0,
            "timebased_size_mb": 0,
            "total_size_mb": 0
        }
        
        # 定义K线目录
        KLINE_DIR = f"{DATA_DIR}/market/kline"
        
        # 检查codebased数据
        codebased_path = f"{KLINE_DIR}/codebased/{market}/{freq}"
        # print(codebased_path)
        # exit()
        all_codes = set()
        all_dates = []
        
        if os.path.exists(codebased_path):
            # 遍历年份目录
            for year in os.listdir(codebased_path):
                year_path = os.path.join(codebased_path, year)
                if os.path.isdir(year_path):
                    # 遍历代码文件
                    for code_file in os.listdir(year_path):
                        if code_file.endswith('.csv'):
                            code = code_file.replace('.csv', '')
                            file_path = os.path.join(year_path, code_file)
                            result["codebased_files"] += 1
                            file_size = os.path.getsize(file_path) / (1024 * 1024)  # 转换为MB
                            result["codebased_size_mb"] += file_size
                            all_codes.add(code)
                            
                            # 获取日期范围（读取文件的第一行和最后一行）
                            try:
                                with open(file_path, 'r') as f:
                                    first_line = f.readline().strip()
                                    if first_line:
                                        first_date = first_line.split(',')[0]
                                        all_dates.append(first_date)
                                    
                                    # 跳到文件末尾获取最后一行
                                    f.seek(0, os.SEEK_END)
                                    pos = f.tell() - 2
                                    while pos > 0 and f.read(1) != '\n':
                                        pos -= 1
                                        f.seek(pos, os.SEEK_SET)
                                    if pos > 0:
                                        last_line = f.readline().strip()
                                        if last_line:
                                            last_date = last_line.split(',')[0]
                                            all_dates.append(last_date)
                            except Exception as e:
                                logging.warning(f"Error reading date range from {file_path}: {str(e)}")
        
        # 检查timebased数据
        timebased_path = f"{KLINE_DIR}/timebased/{market}/{freq}"
        
        if os.path.exists(timebased_path):
            # 递归遍历目录结构
            for root, dirs, files in os.walk(timebased_path):
                for file in files:
                    if file.endswith('.csv'):
                        file_path = os.path.join(root, file)
                        result["timebased_files"] += 1
                        file_size = os.path.getsize(file_path) / (1024 * 1024)  # 转换为MB
                        result["timebased_size_mb"] += file_size
                        
                        # 提取日期信息从文件路径
                        path_parts = file_path.split('/')
                        if len(path_parts) >= 7:  # 确保路径长度足够
                            year = path_parts[-4]
                            month = path_parts[-3]
                            day = path_parts[-2]
                            date_str = f"{year}-{month}-{day}"
                            all_dates.append(date_str)
                        
                        # 从文件中提取代码信息（读取部分数据）
                        try:
                            # 只读取前1000行来节省时间
                            df_sample = pd.read_csv(file_path, header=None, nrows=1000,
                                               names=['time', 'code', 'open', 'high', 'low', 'close', 'volume', 'amount'])
                            if not df_sample.empty:
                                codes = df_sample['code'].unique()
                                all_codes.update(codes)
                        except Exception as e:
                            logging.warning(f"Error extracting codes from {file_path}: {str(e)}")
        
        # 计算总结果
        result["code_count"] = len(all_codes)
        result["total_size_mb"] = round(result["codebased_size_mb"] + result["timebased_size_mb"], 2)
        result["codebased_size_mb"] = round(result["codebased_size_mb"], 2)
        result["timebased_size_mb"] = round(result["timebased_size_mb"], 2)
        
        # 处理日期范围
        if all_dates:
            # 转换为datetime对象进行排序
            datetime_dates = []
            for date_str in all_dates:
                try:
                    # 处理不同格式的日期字符串
                    if 'T' in date_str or '+' in date_str:
                        # ISO格式日期 - 转换为不带时区的日期
                        dt = pd.to_datetime(date_str).replace(tzinfo=None)
                        datetime_dates.append(dt)
                    elif '-' in date_str:
                        # YYYY-MM-DD 格式
                        parts = date_str.split('-')
                        if len(parts) == 3:
                            dt = datetime(int(parts[0]), int(parts[1]), int(parts[2]))
                            datetime_dates.append(dt)
                except Exception as e:
                    logging.debug(f"Error parsing date '{date_str}': {str(e)}")
            
            if datetime_dates:
                datetime_dates.sort()
                result["start_date"] = datetime_dates[0].strftime("%Y%m%d")
                result["end_date"] = datetime_dates[-1].strftime("%Y%m%d")
        
        return result
    
    except Exception as e:
        logging.error(f"Error in inspectKline for {market}/{freq}: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            "market": market,
            "freq": freq,
            "error": str(e)
        }

def loadKline(market='cn_stock', freq='1m', start_date="20200101", end_date="20201231", code_list=[], cache=False, max_workers=8):
    """
    加载K线数据，根据参数智能选择最优加载方式，使用多线程/多进程加速
    
    参数:
    market: 市场类型，如 cn_stock, cn_future 等
    freq: 频率，如 1m, 5m, 15m, 60m, 1d 等
    start_date: 开始日期，格式为YYYYMMDD
    end_date: 结束日期，格式为YYYYMMDD
    code_list: 代码列表
    cache: 是否使用缓存
    max_workers: 最大工作线程/进程数
    """
    # 处理缓存逻辑
    if cache:
        # 创建参数的哈希值
        params_str = f"{','.join(sorted(code_list))}-{market}-{freq}-{start_date}-{end_date}"
        hash_str = hashlib.md5(params_str.encode()).hexdigest()
        
        # 构建缓存文件路径
        cache_dir = f"{FACTORS_CACHE_DIR}/{market}/{freq}"
        os.makedirs(cache_dir, exist_ok=True)
        cache_file = f"{cache_dir}/kline_{hash_str}.pkl"
        
        # 检查缓存是否存在
        if os.path.exists(cache_file):
            try:
                logging.info(f"Loading factors from cache: {cache_file}")
                return pd.read_pickle(cache_file)
            except Exception as e:
                logging.error(f"Error loading cache file: {str(e)}")
                # 如果读取缓存失败，继续执行原流程
    
    # 转换日期格式
    start_dt = datetime.strptime(start_date, "%Y%m%d")
    end_dt = datetime.strptime(end_date, "%Y%m%d")
    
    # 判断使用哪种加载方式
    use_codebased = len(code_list) <= 100 or (end_dt - start_dt).days > 30
    
    kline_data = None
    
    # 定义K线目录
    KLINE_DIR = f"{DATA_DIR}/market/kline"
    
    # 使用 codebased 方式加载 - 使用线程池加速
    if use_codebased:
        def load_code_data(code, start_dt, end_dt, market, freq, KLINE_DIR):
            code_dfs = []
            years = range(start_dt.year, end_dt.year + 1)
            
            for year in years:
                file_path = f"{KLINE_DIR}/codebased/{market}/{freq}/{year}/{code}.csv"
                
                if os.path.exists(file_path):
                    try:
                        df = pd.read_csv(file_path, header=None, 
                                         names=['time', 'code', 'open', 'high', 'low', 'close', 'volume', 'amount'])
                        
                        df['time'] = pd.to_datetime(df['time'])
                        mask = (df['time'] >= start_dt) & (df['time'] <= end_dt + timedelta(days=1))
                        df = df[mask]
                        
                        if not df.empty:
                            code_dfs.append(df)
                    except Exception as e:
                        logging.error(f"Error loading {file_path}: {str(e)}")
            
            if code_dfs:
                return pd.concat(code_dfs, ignore_index=True)
            return None
        
        # 创建线程池，使用线程池处理IO密集型任务
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 使用偏函数固定其他参数
            load_func = partial(load_code_data, start_dt=start_dt, end_dt=end_dt, 
                               market=market, freq=freq, KLINE_DIR=KLINE_DIR)
            
            # 提交所有任务到线程池
            future_to_code = {executor.submit(load_func, code): code for code in code_list}
            
            # 收集结果
            dfs = []
            for future in tqdm(concurrent.futures.as_completed(future_to_code), 
                              total=len(code_list), desc="Loading codebased data"):
                try:
                    result = future.result()
                    if result is not None:
                        dfs.append(result)
                except Exception as e:
                    code = future_to_code[future]
                    logging.error(f"Error processing code {code}: {str(e)}")
            
            if dfs:
                kline_data = pd.concat(dfs, ignore_index=True)
    
    # 使用 timebased 方式加载 - 使用进程池加速
    else:
        def load_day_data(day_tuple, market, freq, code_list, KLINE_DIR):
            current_dt, _ = day_tuple
            year = current_dt.strftime("%Y")
            month = current_dt.strftime("%m")
            day = current_dt.strftime("%d")
            
            file_path = f"{KLINE_DIR}/timebased/{market}/{freq}/{year}/{month}/{day}/{market}_kline_merged.csv"
            
            if not os.path.exists(file_path):
                pattern = f"{KLINE_DIR}/timebased/{market}/{freq}/{year}/{month}/{day}/{market}_kline_*.csv"
                files = glob.glob(pattern)
                if files:
                    file_path = files[0]
            
            if os.path.exists(file_path):
                try:
                    df = pd.read_csv(file_path, header=None, 
                                     names=['time', 'code', 'open', 'high', 'low', 'close', 'volume', 'amount'])
                    
                    if code_list:
                        df = df[df['code'].isin(code_list)]
                    
                    if not df.empty:
                        df['time'] = pd.to_datetime(df['time'])
                        return df
                except Exception as e:
                    logging.error(f"Error loading {file_path}: {str(e)}")
            
            return None
        
        # 创建日期范围
        days = []
        current_dt = start_dt
        while current_dt <= end_dt:
            days.append((current_dt, f"{current_dt.year}{current_dt.month:02d}{current_dt.day:02d}"))
            current_dt += timedelta(days=1)
        
        # 使用进程池处理
        dfs = []
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            load_func = partial(load_day_data, market=market, freq=freq, 
                               code_list=code_list, KLINE_DIR=KLINE_DIR)
            
            # 提交所有任务到进程池
            future_to_day = {executor.submit(load_func, day): day for day in days}
            
            # 收集结果
            for future in tqdm(concurrent.futures.as_completed(future_to_day), 
                              total=len(days), desc="Loading timebased data"):
                try:
                    result = future.result()
                    if result is not None:
                        dfs.append(result)
                except Exception as e:
                    day = future_to_day[future][1]
                    logging.error(f"Error processing day {day}: {str(e)}")
            
            if dfs:
                kline_data = pd.concat(dfs, ignore_index=True)
    
    # 如果没有数据，返回空DataFrame
    if kline_data is None:
        kline_data = pd.DataFrame(columns=['time', 'code', 'open', 'high', 'low', 'close', 'volume', 'amount'])
    else:
        # 排序数据
        kline_data = kline_data.sort_values(['code', 'time']).reset_index(drop=True)
    
    # 保存到缓存
    if cache and kline_data is not None:
        try:
            kline_data.to_pickle(cache_file)
            logging.info(f"Saved kline data to cache: {cache_file}")
        except Exception as e:
            logging.error(f"Error saving to cache file: {str(e)}")
    
    return kline_data