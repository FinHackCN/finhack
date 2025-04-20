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
    code_list: 代码列表，如果为空则加载所有可用代码
    cache: 是否使用缓存
    max_workers: 最大工作线程/进程数
    """
    # 定义K线目录
    KLINE_DIR = f"{DATA_DIR}/market/kline"
    
    # 转换日期格式为没有时区的datetime对象
    start_dt = datetime.strptime(start_date, "%Y%m%d")
    end_dt = datetime.strptime(end_date, "%Y%m%d")
    
    # 如果code_list为空，获取所有可用的code
    if not code_list:
        logging.info("Code list is empty, loading all available codes...")
        all_codes = set()
        
        # 检查codebased目录获取可用代码
        codebased_path = f"{KLINE_DIR}/codebased/{market}/{freq}"
        if os.path.exists(codebased_path):
            # 遍历年份目录
            for year in range(start_dt.year, end_dt.year + 1):
                year_path = os.path.join(codebased_path, str(year))
                if os.path.isdir(year_path):
                    # 遍历代码文件
                    for code_file in os.listdir(year_path):
                        if code_file.endswith('.csv'):
                            code = code_file.replace('.csv', '')
                            all_codes.add(code)
        
        # 如果codebased找不到代码，检查timebased目录
        if not all_codes:
            timebased_path = f"{KLINE_DIR}/timebased/{market}/{freq}"
            if os.path.exists(timebased_path):
                current_dt = start_dt
                
                # 从一部分文件中获取代码（最多采样5天数据）
                sample_files = []
                while current_dt <= end_dt and len(sample_files) < 5:
                    year = current_dt.strftime("%Y")
                    month = current_dt.strftime("%m")
                    day = current_dt.strftime("%d")
                    
                    # 首先尝试获取merged文件
                    merged_file = f"{timebased_path}/{year}/{month}/{day}/{market}_kline_merged.csv"
                    if os.path.exists(merged_file):
                        sample_files.append(merged_file)
                    else:
                        # 否则获取任意一个源文件
                        file_pattern = f"{timebased_path}/{year}/{month}/{day}/{market}_kline_*.csv"
                        files = glob.glob(file_pattern)
                        if files:
                            sample_files.append(files[0])
                    
                    current_dt += timedelta(days=1)
                
                # 从样本文件中提取代码
                for file_path in sample_files:
                    try:
                        df_sample = pd.read_csv(file_path, header=None, nrows=1000,
                                         names=['time', 'code', 'open', 'high', 'low', 'close', 'volume', 'amount'])
                        if not df_sample.empty:
                            codes = df_sample['code'].unique()
                            all_codes.update(codes)
                    except Exception as e:
                        logging.warning(f"Error extracting codes from {file_path}: {str(e)}")
        
        code_list = list(all_codes)
        if code_list:
            logging.info(f"Found {len(code_list)} available codes")
        else:
            logging.warning(f"No codes found for {market}/{freq} between {start_date} and {end_date}")
            return pd.DataFrame(columns=['time', 'code', 'open', 'high', 'low', 'close', 'volume', 'amount'])
    
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
    
    # 判断使用哪种加载方式
    use_codebased = len(code_list) <= 100 or (end_dt - start_dt).days > 30
    
    kline_data = None
    
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
                        
                        # 转换时间并处理时区
                        df['time'] = pd.to_datetime(df['time'])
                        
                        # 确保start_dt和end_dt没有时区信息
                        local_start_dt = start_dt
                        local_end_dt = end_dt + timedelta(days=1) - timedelta(seconds=1)  # 设置为当天结束时间
                        
                        # 处理时区问题 - 移除时区信息以便进行比较
                        if hasattr(df['time'].dt, 'tz') and df['time'].dt.tz is not None:
                            # 将带时区的时间转换为不带时区的本地时间
                            df['time'] = df['time'].dt.tz_localize(None)
                        
                        # 过滤日期范围
                        mask = (df['time'] >= local_start_dt) & (df['time'] <= local_end_dt)
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
            
            # 首先尝试加载merged文件
            file_path = f"{KLINE_DIR}/timebased/{market}/{freq}/{year}/{month}/{day}/{market}_kline_merged.csv"
            
            if not os.path.exists(file_path):
                # 如果没有merged文件，尝试找到任意一个源文件
                pattern = f"{KLINE_DIR}/timebased/{market}/{freq}/{year}/{month}/{day}/{market}_kline_*.csv"
                files = glob.glob(pattern)
                if files:
                    file_path = files[0]
            
            if os.path.exists(file_path):
                try:
                    df = pd.read_csv(file_path, header=None, 
                                     names=['time', 'code', 'open', 'high', 'low', 'close', 'volume', 'amount'])
                    
                    # 过滤代码
                    if code_list:
                        df = df[df['code'].isin(code_list)]
                    
                    if not df.empty:
                        # 处理时间和时区
                        df['time'] = pd.to_datetime(df['time'])
                        if hasattr(df['time'].dt, 'tz') and df['time'].dt.tz is not None:
                            df['time'] = df['time'].dt.tz_localize(None)
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
        
        # 为时间添加中国标准时间时区信息 (+08:00)
        if 'time' in kline_data.columns and not kline_data.empty:
            if pd.api.types.is_datetime64_dtype(kline_data['time']):
                # 确保移除任何现有时区，然后添加中国标准时间时区
                kline_data['time'] = kline_data['time'].dt.tz_localize(None)
                kline_data['time'] = kline_data['time'].dt.tz_localize('Asia/Shanghai')
                
                # 将时间格式化为带时区的字符串格式
                # 这会产生如 '2025-01-02 09:40:00+08:00' 的格式
                kline_data['time'] = kline_data['time'].dt.strftime('%Y-%m-%d %H:%M:%S%z')
                # 插入冒号使格式成为 +08:00 而不是 +0800
                kline_data['time'] = kline_data['time'].str.replace(r'(\+\d{2})(\d{2})$', r'\1:\2', regex=True)
    
    # 保存到缓存
    if cache and kline_data is not None and not kline_data.empty:
        try:
            kline_data.to_pickle(cache_file)
            logging.info(f"Saved kline data to cache: {cache_file}")
        except Exception as e:
            logging.error(f"Error saving to cache file: {str(e)}")
    
    return kline_data