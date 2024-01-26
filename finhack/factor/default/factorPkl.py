import os
import pandas as pd
import multiprocessing
import shutil
from runtime.constant import *
import dask.dataframe as dd
from finhack.market.astock.astock import AStock
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
class factorPkl:
    def process_file(file_path, output_dir, all_dates_df):
        # 读取 CSV 文件
        factor_name = os.path.basename(file_path).split('.')[0]
        df = pd.read_csv(file_path, names=['ts_code', 'trade_date', factor_name])
        df.set_index(['ts_code', 'trade_date'], inplace=True)
    
        # 对齐索引并填充缺失数据
        aligned_df = all_dates_df.join(df, how='left')
        aligned_df[factor_name] = aligned_df[factor_name].fillna(method='ffill')
    
        # 按照索引排序
        aligned_df.sort_index(inplace=True)
    
        # 去除索引
        aligned_df.reset_index(drop=True, inplace=True)
    
        # 仅保存因子值到 pkl 文件
        aligned_df[[factor_name]].to_pickle(os.path.join(output_dir, f'{factor_name}.pkl'))

        
        
        
        
    def save():
        # 设置 CSV 文件所在的目录
        directory = SINGLE_FACTORS_DIR
        output_dir = SINGLE_FACTORS_PKL_TMP_DIR
        open_path = SINGLE_FACTORS_DIR+'open.csv'  # 替换为 open_0.csv 文件的实际路径
        open_df = pd.read_csv(open_path, names=['ts_code', 'trade_date', 'open'])

        # 设置 'ts_code' 和 'trade_date' 为索引
        open_df.set_index(['ts_code', 'trade_date'], inplace=True)
        
        # 创建一个基于这些索引的 DataFrame
        all_dates_df = pd.DataFrame(index=open_df.index)
        # all_dates_df = pd.DataFrame(index=all_indices)


                
        # 检查 output_dir是否存在
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)

        os.makedirs(output_dir)        
        
        
        # 使用多进程处理文件
        with ProcessPoolExecutor() as executor:
            for filename in os.listdir(directory):
                if filename.endswith('.csv'):
                    file_path = os.path.join(directory, filename)
                    executor.submit(factorPkl.process_file, file_path, output_dir, all_dates_df)
                    #factorPkl.process_file(file_path, output_dir, all_dates_df)
                    
        # 单独保存全量索引
        index_df = all_dates_df.reset_index()
        index_df['trade_date']=index_df['trade_date'].astype(str)
        index_df.to_pickle(output_dir+'index.pkl')
        
        if os.path.exists(SINGLE_FACTORS_PKL_OLD_DIR):
            shutil.rmtree(SINGLE_FACTORS_PKL_OLD_DIR)
        
        os.rename(SINGLE_FACTORS_PKL_DIR, SINGLE_FACTORS_PKL_OLD_DIR)
        os.rename(SINGLE_FACTORS_PKL_TMP_DIR, SINGLE_FACTORS_PKL_DIR)
        shutil.rmtree(SINGLE_FACTORS_PKL_OLD_DIR)