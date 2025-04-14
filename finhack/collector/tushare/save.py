import os
import traceback
import pandas as pd
from datetime import datetime
import concurrent.futures
import numpy as np
from finhack.library.mydb import mydb
from finhack.library.config import Config
import finhack.library.log as Log

class TushareSaver:
    def __init__(self):
        # 初始化配置
        cfgTS = Config.get_config('ts')
        self.db = cfgTS['db']
        
        # 定义数据表与dataname的映射关系
        self.table_dataname_map = {
            'astock_price_daily': 'cn_stock',
            'astock_index_daily': 'cn_index',
            'cb_daily': 'cn_cb',
            'fund_daily': 'cn_fund',
            'fx_daily': 'global_fx',
            'hk_daily': 'hk_stock'
        }
        
        # 定义数据类型对应的时间格式
        self.time_format_map = {
            'cn_stock': '%Y-%m-%d 09:30:00+08:00',
            'cn_index': '%Y-%m-%d 09:30:00+08:00',
            'cn_cb': '%Y-%m-%d 09:30:00+08:00',
            'cn_fund': '%Y-%m-%d 09:30:00+08:00',
            'hk_stock': '%Y-%m-%d 10:00:00+08:00',
            'global_fx': '%Y-%m-%d 00:00:00+08:00'
        }
        
        self.base_dir = os.path.join(os.getcwd(), "data")
        self.freq = "1d"
        self.start_year = 1998  # 历史数据开始年份
        self.current_year = datetime.now().year
        
    def save_kline_to_csv(self):
        """将数据库中的数据导出到CSV文件，优化版本"""
        try:
            Log.logger.info("开始导出数据到CSV文件...")
            
            # 使用线程池并行处理不同的表
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(6, len(self.table_dataname_map))) as executor:
                futures = {executor.submit(self._process_table_optimized, table_name, dataname): (table_name, dataname) 
                          for table_name, dataname in self.table_dataname_map.items()}
                
                for future in concurrent.futures.as_completed(futures):
                    table_name, dataname = futures[future]
                    try:
                        result = future.result()
                        Log.logger.info(f"表 {table_name}({dataname}) 处理完成")
                    except Exception as exc:
                        Log.logger.error(f"处理表 {table_name} 时发生错误: {str(exc)}")
            
            Log.logger.info("所有数据导出完成")
            return True
            
        except Exception as e:
            Log.logger.error(f"数据导出过程中发生错误: {str(e)}")
            Log.logger.error(traceback.format_exc())
            return False
    
    def _process_table_optimized(self, table_name, dataname):
        """优化的单表处理方法，分别处理codebased和timebased数据"""
        try:
            Log.logger.info(f"开始处理表 {table_name}...")
            
            # 验证表是否存在
            if not mydb.tableExists(table_name, self.db):
                Log.logger.warning(f"表 {table_name} 不存在，跳过")
                return False
                
            # 获取列映射
            column_mapping = self._get_column_mapping(table_name)
            time_format = self.time_format_map.get(dataname, '%Y-%m-%d 00:00:00+08:00')
            
            # 1. 处理codebased数据 - 按年份处理
            self._process_codebased_data(table_name, dataname, column_mapping, time_format)
            
            # 2. 处理timebased数据 - 只处理最新数据
            self._process_timebased_data(table_name, dataname, column_mapping, time_format)
            
            return True
                
        except Exception as e:
            Log.logger.error(f"处理表 {table_name} 时发生错误: {str(e)}")
            Log.logger.error(traceback.format_exc())
            return False

    def _process_codebased_data(self, table_name, dataname, column_mapping, time_format):
        """按年份处理codebased数据"""
        try:
            # 确保基础目录存在
            codebased_base_dir = os.path.join(self.base_dir, 'market', 'kline', 'codebased',
                                      dataname, self.freq)
            os.makedirs(codebased_base_dir, exist_ok=True)
            
            # 处理每一年的数据
            for year in range(self.start_year, self.current_year + 1):
                year_str = str(year)
                year_dir = os.path.join(codebased_base_dir, year_str)
                
                # 如果不是当前年且目录已存在，则跳过
                if year != self.current_year and os.path.exists(year_dir):
                    Log.logger.info(f"年份 {year} 的codebased数据已存在，跳过处理")
                    continue
                    
                # 创建年份目录
                os.makedirs(year_dir, exist_ok=True)
                
                # 查询该年份的数据
                start_date = f"{year}0101"
                end_date = f"{year}1231"
                sql = f"""
                    SELECT * FROM {table_name} 
                    WHERE trade_date >= '{start_date}' AND trade_date <= '{end_date}'
                    ORDER BY ts_code, trade_date
                """
                
                Log.logger.info(f"查询 {year} 年 {table_name} 表数据...")
                df = mydb.selectToDf(sql, self.db)
                
                if df.empty:
                    Log.logger.warning(f"{year} 年 {table_name} 表没有数据")
                    continue
                
                # 处理数据格式
                processed_df = self._process_dataframe_batch(df, column_mapping, dataname, time_format)
                if processed_df.empty:
                    continue
                
                # 按代码分组处理
                code_groups = processed_df.groupby('code')
                total_codes = len(code_groups)
                processed_count = 0
                
                Log.logger.info(f"开始处理 {year} 年 {total_codes} 个代码的数据...")
                
                # 并行处理不同代码
                with concurrent.futures.ThreadPoolExecutor(max_workers=min(30, total_codes)) as executor:
                    futures = []
                    
                    for code, code_df in code_groups:
                        # 构建文件路径
                        code_file = os.path.join(year_dir, f"{code}.csv")
                        
                        # 如果是当前年份且文件已存在，先删除
                        if year == self.current_year and os.path.exists(code_file):
                            try:
                                os.remove(code_file)
                            except Exception as e:
                                Log.logger.warning(f"删除文件失败: {code_file}, 错误: {str(e)}")
                        
                        # 提交保存任务
                        futures.append(executor.submit(
                            self._save_csv, code_df, code_file, sort=True, sort_by=['time']))
                    
                    # 等待所有任务完成
                    for future in concurrent.futures.as_completed(futures):
                        try:
                            future.result()
                            processed_count += 1
                            if processed_count % 100 == 0:
                                Log.logger.info(f"已处理 {processed_count}/{total_codes} 个代码")
                        except Exception as e:
                            Log.logger.error(f"保存文件时发生错误: {str(e)}")
                
                Log.logger.info(f"完成 {year} 年 {table_name} 表的codebased数据处理，共处理 {processed_count} 个代码")
                
        except Exception as e:
            Log.logger.error(f"处理codebased数据时发生错误: {str(e)}")
            Log.logger.error(traceback.format_exc())

    def _process_timebased_data(self, table_name, dataname, column_mapping, time_format):
        """处理timebased数据，只处理最新数据"""
        try:
            # 确定timebased基础目录
            timebased_base_dir = os.path.join(self.base_dir, 'market', 'kline', 'timebased',
                                      dataname, self.freq)
            os.makedirs(timebased_base_dir, exist_ok=True)
            
            # 获取最新日期
            latest_date = self._find_latest_date_directory(timebased_base_dir)
            
            if latest_date:
                latest_date_str = latest_date.replace('-', '')
                
                # 查询最新日期及之后的数据
                sql = f"""
                    SELECT * FROM {table_name} 
                    WHERE trade_date >= '{latest_date_str}'
                    ORDER BY trade_date
                """
            else:
                # 如果没有现有数据，则获取所有数据
                sql = f"""
                    SELECT * FROM {table_name} 
                    ORDER BY trade_date
                """
            
            Log.logger.info(f"查询 {table_name} 表最新数据...")
            df = mydb.selectToDf(sql, self.db)
            
            if df.empty:
                Log.logger.warning(f"{table_name} 表没有最新数据")
                return
            
            # 处理数据格式
            processed_df = self._process_dataframe_batch(df, column_mapping, dataname, time_format)
            if processed_df.empty:
                return
                
            # 提取日期部分并按日期分组
            processed_df['date'] = pd.to_datetime(processed_df['time']).dt.strftime('%Y-%m-%d')
            date_groups = processed_df.groupby('date')
            
            for date, group in date_groups:
                year, month, day = date.split('-')
                
                # 构建目录路径
                day_dir = os.path.join(timebased_base_dir, year, month, day)
                os.makedirs(day_dir, exist_ok=True)
                
                # 构建文件路径
                source_file = os.path.join(day_dir, f"{dataname}_kline_tushare.csv")
                merged_file = os.path.join(day_dir, f"{dataname}_kline_merged.csv")
                
                # 移除辅助列
                day_df = group.drop(columns=['date'], errors='ignore')
                
                # 保存数据
                #self._save_csv(day_df, source_file)
                self._save_csv(day_df, merged_file)
                
                Log.logger.info(f"已保存 {date} 的timebased数据到 {source_file}")
                
        except Exception as e:
            Log.logger.error(f"处理timebased数据时发生错误: {str(e)}")
            Log.logger.error(traceback.format_exc())
    
    def _find_latest_date_directory(self, base_dir):
        """查找最新的日期目录"""
        try:
            latest_date = None
            latest_year = None
            latest_month = None
            latest_day = None
            
            # 遍历年份目录
            for year in sorted(os.listdir(base_dir), reverse=True):
                if not os.path.isdir(os.path.join(base_dir, year)) or not year.isdigit():
                    continue
                    
                year_dir = os.path.join(base_dir, year)
                
                # 遍历月份目录
                for month in sorted(os.listdir(year_dir), reverse=True):
                    if not os.path.isdir(os.path.join(year_dir, month)) or not month.isdigit():
                        continue
                        
                    month_dir = os.path.join(year_dir, month)
                    
                    # 遍历日期目录
                    for day in sorted(os.listdir(month_dir), reverse=True):
                        if not os.path.isdir(os.path.join(month_dir, day)) or not day.isdigit():
                            continue
                            
                        # 找到最新日期
                        latest_year = year
                        latest_month = month
                        latest_day = day
                        break
                        
                    if latest_day:
                        break
                        
                if latest_year:
                    break
            
            if latest_year and latest_month and latest_day:
                latest_date = f"{latest_year}-{latest_month}-{latest_day}"
                Log.logger.info(f"找到最新的数据日期: {latest_date}")
            else:
                Log.logger.info("未找到现有数据，将处理所有数据")
                
            return latest_date
            
        except Exception as e:
            Log.logger.error(f"查找最新日期时发生错误: {str(e)}")
            Log.logger.error(traceback.format_exc())
            return None

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

    def _process_dataframe_batch(self, df, column_mapping, dataname, time_format):
        """批量处理DataFrame格式"""
        try:
            # 创建新的DataFrame
            new_df = pd.DataFrame()
            
            # 处理trade_date列为datetime格式
            df['trade_date'] = pd.to_datetime(df['trade_date'].astype(str), format='%Y%m%d')
            
            # 映射其他列
            for source_col, target_col in column_mapping.items():
                if source_col in df.columns:
                    if source_col == 'trade_date':
                        new_df['time'] = df[source_col].dt.strftime(time_format)
                    elif source_col in ['open', 'high', 'low', 'close', 'vol', 'amount',
                                    'bid_open', 'bid_high', 'bid_low', 'bid_close', 'tick_qty']:
                        # 对于FX数据，需要特殊处理bid_columns作为OHLC
                        if source_col.startswith('bid_'):
                            target_col = source_col.replace('bid_', '')
                        
                        # 确保数值正确转换
                        new_df[target_col] = pd.to_numeric(df[source_col], errors='coerce')
                    elif source_col == 'ts_code':
                        new_df['code'] = df[source_col]
            
            # 确保所有必要的列都存在
            columns = ['time', 'code', 'open', 'high', 'low', 'close', 'volume', 'amount']
            for col in columns:
                if col not in new_df.columns:
                    if col in ['open', 'high', 'low', 'close', 'volume', 'amount']:
                        # 对于FX数据，如果volume列不存在，用tick_qty替代
                        if col == 'volume' and 'tick_qty' in column_mapping:
                            new_df[col] = pd.to_numeric(df['tick_qty'], errors='coerce')
                        else:
                            new_df[col] = 0.0
                    elif col == 'code':
                        # 如果没有code列，这是异常情况，尝试创建一个默认值
                        new_df[col] = 'unknown'
                    elif col == 'time':
                        # 如果没有time列，这也是异常情况
                        Log.logger.warning(f"未找到时间列，使用当前时间")
                        new_df[col] = datetime.now().strftime(time_format)
            
            # 确保列顺序正确
            new_df = new_df[columns]
            
            return new_df
            
        except Exception as e:
            Log.logger.error(f"处理DataFrame时发生错误: {str(e)}")
            Log.logger.error(traceback.format_exc())
            return pd.DataFrame()
    
    def _save_csv(self, df, filepath, sort=False, sort_by=None):
        """优化的保存CSV文件方法"""
        try:
            # 避免修改原始数据
            df = df.copy()
            
            # 排序
            if sort and sort_by:
                df = df.sort_values(sort_by)
            
            # 保存文件
            df.to_csv(filepath, index=False, header=False)
                
        except Exception as e:
            Log.logger.error(f"保存文件 {filepath} 时发生错误: {str(e)}")
            Log.logger.error(traceback.format_exc())
 
# 为了向后兼容，保留这个函数
def save_tushare_data():
    """提供给外部调用的保存函数"""
    saver = TushareSaver()
    return saver.save_data_to_csv()