import os
import traceback
import pandas as pd
from datetime import datetime
from finhack.library.mydb import mydb
from finhack.library.config import Config
import finhack.library.log as Log

class TushareSaver:
    def __init__(self):
        # 初始化配置
        cfgTS = Config.get_config('ts')
        self.db = cfgTS['db']
        
    def save_data_to_csv(self):
        """将数据库中的数据导出到CSV文件"""
        try:
            Log.logger.info("开始导出数据到CSV文件...")
            
            # 定义数据表与dataname的映射关系
            table_dataname_map = {
                'astock_price_daily': 'cn_stock',
                'astock_index_daily': 'cn_index',
                'cb_daily': 'cn_cb',
                'fund_daily': 'cn_fund',
                'fx_daily': 'global_fx',
                'hk_daily': 'hk_stock'
            }
            
            base_dir = os.path.join(os.getcwd(), "data")
            freq = "1d"

            for table_name, dataname in table_dataname_map.items():
                try:
                    Log.logger.info(f"开始处理表 {table_name}...")
                    
                    # 验证表是否存在
                    if not mydb.tableExists(table_name, self.db):
                        Log.logger.warning(f"表 {table_name} 不存在，跳过")
                        continue

                    # 获取所有数据
                    sql = f"SELECT * FROM {table_name} ORDER BY trade_date"
                    df = mydb.selectToDf(sql, self.db)
                    
                    if df.empty:
                        Log.logger.warning(f"表 {table_name} 没有数据")
                        continue

                    # 获取列映射
                    column_mapping = self._get_column_mapping(table_name)
                    
                    # 按年月日分组处理数据
                    df['year'] = df['trade_date'].astype(str).str[:4]
                    df['month'] = df['trade_date'].astype(str).str[4:6]
                    df['day'] = df['trade_date'].astype(str).str[6:8]
                    
                    for year in df['year'].unique():
                        year_df = df[df['year'] == year]
                        for month in year_df['month'].unique():
                            month_df = year_df[year_df['month'] == month]
                            for day in month_df['day'].unique():
                                day_df = month_df[month_df['day'] == day]
                                
                                # 处理数据格式，传入dataname以确定时间格式
                                current_date = datetime.strptime(f"{year}{month}{day}", '%Y%m%d')
                                processed_df = self._process_dataframe(day_df, column_mapping, current_date, dataname)
                                
                                if not processed_df.empty:
                                    # 保存timebased数据
                                    timebased_dir = os.path.join(base_dir, 'market', 'kline', 'timebased', 
                                                               dataname, freq, year, month, day)
                                    os.makedirs(timebased_dir, exist_ok=True)
                                    
                                    timebased_file = os.path.join(timebased_dir, f"{dataname}_kline_tushare.csv")
                                    merged_file = os.path.join(timebased_dir, f"{dataname}_kline_merged.csv")
                                    
                                    self._save_or_update_csv(processed_df, timebased_file)
                                    self._save_or_update_csv(processed_df, merged_file)
                                    
                                    # 保存codebased数据
                                    for code in processed_df['code'].unique():
                                        code_df = processed_df[processed_df['code'] == code].copy(deep=True)
                                        codebased_dir = os.path.join(base_dir, 'market', 'kline', 'codebased',
                                                                   dataname, freq, year)
                                        os.makedirs(codebased_dir, exist_ok=True)
                                        
                                        code_file = os.path.join(codebased_dir, f"{code}.csv")
                                        self._save_or_update_csv(code_df, code_file, sort=True)
                    
                    Log.logger.info(f"表 {table_name} 处理完成")
                    
                except Exception as e:
                    Log.logger.error(f"处理表 {table_name} 时发生错误: {str(e)}")
                    Log.logger.error(traceback.format_exc())
                    continue
                    
            Log.logger.info("数据导出完成")
            return True
            
        except Exception as e:
            Log.logger.error(f"数据导出过程中发生错误: {str(e)}")
            Log.logger.error(traceback.format_exc())
            return False

    def _get_column_mapping(self, table_name):
        """获取不同表的列映射关系"""
        base_mapping = {
            'ts_code': 'code',
            'trade_date': 'time',
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close',
            'vol': 'volume',
            'amount': 'amount'
        }
        
        if table_name == 'fx_daily':
            return {
                'ts_code': 'code',
                'trade_date': 'time',
                'bid_open': 'open',
                'bid_high': 'high',
                'bid_low': 'low',
                'bid_close': 'close',
                'tick_qty': 'volume',
                'amount': 'amount'  # FX数据可能没有amount
            }
            
        return base_mapping

    def _process_dataframe(self, df, column_mapping, current_date, dataname):
        """处理DataFrame格式"""
        # 创建新的DataFrame
        new_df = pd.DataFrame()
        
        # 处理trade_date列为datetime格式
        df['trade_date'] = pd.to_datetime(df['trade_date'].astype(str), format='%Y%m%d')
        
        # 根据dataname确定时间格式
        if dataname in ['cn_stock', 'cn_index', 'cn_cb', 'cn_fund']:
            time_format = '%Y-%m-%d 09:30:00+08:00'
        elif dataname == 'hk_stock':
            time_format = '%Y-%m-%d 10:00:00+08:00'
        elif dataname == 'global_fx':
            time_format = '%Y-%m-%d 00:00:00+08:00'
        else:
            time_format = '%Y-%m-%d 00:00:00+08:00'
        
        # 映射其他列
        for source_col, target_col in column_mapping.items():
            if source_col in df.columns:
                if source_col == 'trade_date':
                    # 根据数据类型设置不同的时间格式
                    new_df['time'] = df[source_col].dt.strftime(time_format)
                elif source_col in ['open', 'high', 'low', 'close', 'vol', 'amount',
                                'bid_open', 'bid_high', 'bid_low', 'bid_close']:
                    new_df[target_col] = pd.to_numeric(df[source_col], errors='coerce')
                elif source_col == 'ts_code':
                    new_df['code'] = df[source_col]  # 不再移除 .SH 和 .SZ 后缀，保留原始代码格式
        
        # 确保amount列存在
        if 'amount' not in new_df.columns:
            new_df['amount'] = 0.0
            
        # 确保列顺序正确
        columns = ['time', 'code', 'open', 'high', 'low', 'close', 'volume', 'amount']
        new_df = new_df[columns]
        
        return new_df

    def _save_or_update_csv(self, df, filepath, sort=False):
        """保存或更新CSV文件"""
        try:
            # 创建传入 DataFrame 的深层副本，避免修改原始数据
            df = df.copy(deep=True)
            
            # 根据filepath确定数据类型，并设置相应的时间格式
            data_type = None
            if 'cn_stock' in filepath or 'cn_index' in filepath or 'cn_cb' in filepath or 'cn_fund' in filepath:
                time_format = '%Y-%m-%d 09:30:00+08:00'
                data_type = 'cn'
            elif 'hk_stock' in filepath:
                time_format = '%Y-%m-%d 10:00:00+08:00'
                data_type = 'hk'
            elif 'global_fx' in filepath:
                time_format = '%Y-%m-%d 00:00:00+08:00'
                data_type = 'fx'
            else:
                time_format = '%Y-%m-%d 00:00:00+08:00'
                data_type = 'other'
                
            if os.path.exists(filepath):
                # 读取现有文件
                existing_df = pd.read_csv(filepath, header=None, 
                                        names=['time', 'code', 'open', 'high', 'low', 
                                              'close', 'volume', 'amount'])
                
                # 尝试解析时间列，如果失败则保留原始格式
                try:
                    # 检查时间列格式，可能是yyyymmdd格式
                    if existing_df['time'].astype(str).str.match(r'^\d{8}$').any():
                        # 根据数据类型设置不同的时间格式
                        existing_df.loc[:, 'time'] = pd.to_datetime(
                            existing_df['time'].astype(str).str.replace(
                                r'^(\d{4})(\d{2})(\d{2})$', r'\1-\2-\3', regex=True)
                        ).dt.strftime(time_format)
                    else:
                        # 对于已经有时间部分的数据，保留原有时间
                        date_only_mask = existing_df['time'].astype(str).str.match(r'^\d{4}-\d{2}-\d{2}$')
                        if date_only_mask.any():
                            # 如果只有日期部分，加上时间
                            existing_df.loc[date_only_mask, 'time'] = pd.to_datetime(
                                existing_df.loc[date_only_mask, 'time']
                            ).dt.strftime(time_format)
                    
                    # 标准化时间格式
                    existing_df.loc[:, 'time'] = pd.to_datetime(existing_df['time'], errors='coerce')
                    df.loc[:, 'time'] = pd.to_datetime(df['time'], errors='coerce')
                except Exception as e:
                    Log.logger.warning(f"时间列转换出错: {str(e)}")
                
                # 合并数据并去除重复项
                combined_df = pd.concat([existing_df, df])
                combined_df = combined_df.drop_duplicates(subset=['time', 'code'], keep='last')
                
                if sort:
                    # 按代码和时间排序
                    combined_df = combined_df.sort_values(['code', 'time'])
                
                # 将时间转换回字符串格式 - 添加类型检查和备用方案
                if pd.api.types.is_datetime64_any_dtype(combined_df['time']):
                    combined_df.loc[:, 'time'] = combined_df['time'].dt.strftime(time_format)
                else:
                    # 如果不是datetime类型，尝试使用字符串处理方法格式化
                    try:
                        # 尝试使用正则表达式格式化yyyymmdd格式
                        time_str = combined_df['time'].astype(str)
                        mask = time_str.str.match(r'^\d{8}$')
                        if mask.any():
                            combined_df.loc[mask, 'time'] = pd.to_datetime(
                                time_str[mask].str.replace(r'^(\d{4})(\d{2})(\d{2})$', r'\1-\2-\3', regex=True)
                            ).dt.strftime(time_format)
                        
                        # 再次尝试转换为datetime后格式化
                        temp_time = pd.to_datetime(combined_df['time'], errors='coerce')
                        valid_mask = ~temp_time.isna()
                        if valid_mask.any():
                            combined_df.loc[valid_mask, 'time'] = temp_time[valid_mask].dt.strftime(time_format)
                    except Exception as e:
                        Log.logger.warning(f"时间格式化出错，保持原格式: {str(e)}")
                
                # 保存文件
                combined_df.to_csv(filepath, index=False, header=False)
                Log.logger.info(f"更新文件: {filepath}")
            else:
                # 对新文件的处理
                try:
                    # 如果是纯数字日期格式，转换为标准格式
                    time_str = df['time'].astype(str)
                    mask = time_str.str.match(r'^\d{8}$')
                    if mask.any():
                        df.loc[mask, 'time'] = pd.to_datetime(
                            time_str[mask].str.replace(r'^(\d{4})(\d{2})(\d{2})$', r'\1-\2-\3', regex=True)
                        ).dt.strftime(time_format)
                    
                    # 对于已有日期格式，但无时间的数据
                    date_only_mask = df['time'].astype(str).str.match(r'^\d{4}-\d{2}-\d{2}$')
                    if date_only_mask.any():
                        df.loc[date_only_mask, 'time'] = pd.to_datetime(
                            df.loc[date_only_mask, 'time']
                        ).dt.strftime(time_format)
                    
                    # 转换时间列并处理可能的错误
                    try:
                        datetime_time = pd.to_datetime(df['time'], errors='coerce')
                        valid_mask = ~datetime_time.isna()
                        
                        if sort and valid_mask.any():
                            # 只对有效的datetime值排序
                            df = df.copy()
                            df.loc[valid_mask, 'time'] = datetime_time[valid_mask]
                            df = df.sort_values(['code', 'time'])
                            # 确保是datetime类型才使用dt访问器
                            if pd.api.types.is_datetime64_any_dtype(df['time']):
                                df.loc[:, 'time'] = df['time'].dt.strftime(time_format)
                            else:
                                # 分别处理datetime和非datetime值
                                df.loc[valid_mask, 'time'] = datetime_time[valid_mask].dt.strftime(time_format)
                        else:
                            # 不排序，直接格式化有效的datetime值
                            if valid_mask.any():
                                df.loc[valid_mask, 'time'] = datetime_time[valid_mask].dt.strftime(time_format)
                    except Exception as e:
                        Log.logger.warning(f"时间格式化处理出错: {str(e)}")
                        if sort:
                            # 如果格式化失败，仍尝试按原始值排序
                            df = df.sort_values(['code', 'time'])
                        
                except Exception as e:
                    Log.logger.warning(f"新文件时间处理出错: {str(e)}")
                    if sort:
                        df = df.sort_values(['code', 'time'])
                
                df.to_csv(filepath, index=False, header=False)
                Log.logger.info(f"创建文件: {filepath}")
                
        except Exception as e:
            Log.logger.error(f"保存文件 {filepath} 时发生错误: {str(e)}")
            Log.logger.error(traceback.format_exc())
 
# 为了向后兼容，保留这个函数
def save_tushare_data():
    """提供给外部调用的保存函数"""
    saver = TushareSaver()
    return saver.save_data_to_csv()