import os
import traceback
import pandas as pd
from datetime import datetime
import concurrent.futures
import numpy as np
from finhack.library.db import DB
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
        
        # 定义代码列表表名与输出映射
        self.list_tables_map = {
            'astock_basic': {'output_path': 'market/reference/cn_stock/cn_stock_list.csv', 'region': 'cn', 'type': 'stock'},
            'astock_index_basic': {'output_path': 'market/reference/cn_index/cn_index_list.csv', 'region': 'cn', 'type': 'index'},
            'cb_basic': {'output_path': 'market/reference/cn_cb/cn_cb_list.csv', 'region': 'cn', 'type': 'cb'},
            'fund_basic': {'output_path': 'market/reference/cn_fund/cn_fund_list.csv', 'region': 'cn', 'type': 'fund'},
            'futures_basic': {'output_path': 'market/reference/cn_future/cn_futr_list.csv', 'region': 'cn', 'type': 'future'},
            'fx_basic': {'output_path': 'market/reference/global_forex/global_forex_list.csv', 'region': 'global', 'type': 'forex'},
            'hk_basic': {'output_path': 'market/reference/hk_stock/hk_stock_list.csv', 'region': 'hk', 'type': 'stock'}
        }
        
        # 定义复权因子表与输出映射
        self.adj_tables_map = {
            'astock_price_adj_factor': {'output_path': 'market/reference/cn_stock/cn_stock_adj.csv'},
            'fund_adj': {'output_path': 'market/reference/cn_fund/cn_fund_adj.csv'}
        }
        
        # 定义交易日历表与输出映射
        self.calendar_tables_map = {
            'astock_trade_cal': [
                {'output_path': 'market/reference/cn_stock/cn_stock_calendar.csv'},
                {'output_path': 'market/reference/cn_index/cn_index_calendar.csv'},
                {'output_path': 'market/reference/cn_fund/cn_fund_calendar.csv'},
                {'output_path': 'market/reference/cn_cb/cn_cb_calendar.csv'}
            ],
            'futures_trade_cal': [
                {'output_path': 'market/reference/cn_future/cn_future_calendar.csv'}
            ],
            'hk_tradecal': [
                {'output_path': 'market/reference/hk_stock/hk_stock_calendar.csv', 'add_exchange': 'hk'}
            ]
        }
        
        # 定义数据表与输出映射
        self.finance_tables_map = {
            'astock_finance_audit': {'output_path': 'market/reference/cn_stock/astock_finance_audit.csv'},
            'astock_finance_balancesheet': {'output_path': 'market/reference/cn_stock/astock_finance_balancesheet.csv'},
            'astock_finance_cashflow': {'output_path': 'market/reference/cn_stock/astock_finance_cashflow.csv'},
            'astock_finance_disclosure_date': {'output_path': 'market/reference/cn_stock/astock_finance_disclosure_date.csv'},
            'astock_finance_dividend': {'output_path': 'market/reference/cn_stock/astock_finance_dividend.csv'},
            'astock_finance_express': {'output_path': 'market/reference/cn_stock/astock_finance_express.csv'},
            'astock_finance_forecast': {'output_path': 'market/reference/cn_stock/astock_finance_forecast.csv'},
            'astock_finance_income': {'output_path': 'market/reference/cn_stock/astock_finance_income.csv'},
            'astock_finance_indicator': {'output_path': 'market/reference/cn_stock/astock_finance_indicator.csv'},
            'astock_finance_mainbz': {'output_path': 'market/reference/cn_stock/astock_finance_mainbz.csv'},
            'astock_price_daily_basic': {'output_path': 'market/reference/cn_stock/astock_price_daily_basic.csv'}
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
            Log.logger.info("开始导出K线数据到CSV文件...")
            
            # 使用线程池并行处理不同的表
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(10, len(self.table_dataname_map))) as executor:
                futures = {executor.submit(self._process_table_optimized, table_name, dataname): (table_name, dataname) 
                          for table_name, dataname in self.table_dataname_map.items()}
                
                for future in concurrent.futures.as_completed(futures):
                    table_name, dataname = futures[future]
                    try:
                        result = future.result()
                        Log.logger.info(f"表 {table_name}({dataname}) 处理完成")
                    except Exception as exc:
                        Log.logger.error(f"处理表 {table_name} 时发生错误: {str(exc)}")
            
            Log.logger.info("所有K线数据导出完成")
            return True
            
        except Exception as e:
            Log.logger.error(f"K线数据导出过程中发生错误: {str(e)}")
            Log.logger.error(traceback.format_exc())
            return False
    
    def save_lists_to_csv(self):
        """将代码列表数据导出到CSV文件"""
        try:
            Log.logger.info("开始导出代码列表数据到CSV文件...")
            
            # 使用线程池并行处理不同的表
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(10, len(self.list_tables_map))) as executor:
                futures = {executor.submit(self._process_list_table, table_name, config): table_name 
                          for table_name, config in self.list_tables_map.items()}
                
                for future in concurrent.futures.as_completed(futures):
                    table_name = futures[future]
                    try:
                        result = future.result()
                        Log.logger.info(f"代码列表表 {table_name} 处理完成")
                    except Exception as exc:
                        Log.logger.error(f"处理代码列表表 {table_name} 时发生错误: {str(exc)}")
            
            Log.logger.info("所有代码列表数据导出完成")
            return True
            
        except Exception as e:
            Log.logger.error(f"代码列表数据导出过程中发生错误: {str(e)}")
            Log.logger.error(traceback.format_exc())
            return False

    def save_adj_factors_to_csv(self):
        """将复权因子数据导出到CSV文件"""
        try:
            Log.logger.info("开始导出复权因子数据到CSV文件...")
            
            # 使用线程池并行处理不同的表
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(10, len(self.adj_tables_map))) as executor:
                futures = {executor.submit(self._process_adj_table, table_name, config): table_name 
                          for table_name, config in self.adj_tables_map.items()}
                
                for future in concurrent.futures.as_completed(futures):
                    table_name = futures[future]
                    try:
                        result = future.result()
                        Log.logger.info(f"复权因子表 {table_name} 处理完成")
                    except Exception as exc:
                        Log.logger.error(f"处理复权因子表 {table_name} 时发生错误: {str(exc)}")
            
            Log.logger.info("所有复权因子数据导出完成")
            return True
            
        except Exception as e:
            Log.logger.error(f"复权因子数据导出过程中发生错误: {str(e)}")
            Log.logger.error(traceback.format_exc())
            return False

    def save_calendars_to_csv(self):
        """将交易日历数据导出到CSV文件"""
        try:
            Log.logger.info("开始导出交易日历数据到CSV文件...")
            
            # 处理每个交易日历表
            for table_name, output_configs in self.calendar_tables_map.items():
                try:
                    self._process_calendar_table(table_name, output_configs)
                    Log.logger.info(f"交易日历表 {table_name} 处理完成")
                except Exception as exc:
                    Log.logger.error(f"处理交易日历表 {table_name} 时发生错误: {str(exc)}")
            
            Log.logger.info("所有交易日历数据导出完成")
            return True
            
        except Exception as e:
            Log.logger.error(f"交易日历数据导出过程中发生错误: {str(e)}")
            Log.logger.error(traceback.format_exc())
            return False

    def save_table_data_to_csv(self):
        """将数据表导出到CSV文件"""
        try:
            Log.logger.info("开始导出数据到CSV文件...")
            
            # 使用线程池并行处理不同的表
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(10, len(self.finance_tables_map))) as executor:
                futures = {executor.submit(self._process_finance_table, table_name, config): table_name 
                          for table_name, config in self.finance_tables_map.items()}
                
                for future in concurrent.futures.as_completed(futures):
                    table_name = futures[future]
                    try:
                        result = future.result()
                        Log.logger.info(f"表 {table_name} 处理完成")
                    except Exception as exc:
                        Log.logger.error(f"处理表 {table_name} 时发生错误: {str(exc)}")
            
            Log.logger.info("所有数据导出完成")
            return True
            
        except Exception as e:
            Log.logger.error(f"数据导出过程中发生错误: {str(e)}")
            Log.logger.error(traceback.format_exc())
            return False

    def _process_list_table(self, table_name, config):
        """处理单个代码列表表"""
        try:
            # 验证表是否存在
            if not DB.table_exists(table_name, self.db):
                Log.logger.warning(f"表 {table_name} 不存在，跳过")
                return False
                
            # 获取输出路径
            output_path = config['output_path']
            output_file = os.path.join(self.base_dir, output_path)
            
            # 确保输出目录存在
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            # 检查是否存在CSV文件，以及是否可以进行增量更新
            max_date = None
            condition = ""
            if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
                try:
                    # 读取现有CSV文件，检查是否有trade_date列
                    existing_df = pd.read_csv(output_file)
                    if 'trade_date' in existing_df.columns and not existing_df.empty:
                        max_date = existing_df['trade_date'].max()
                        if pd.notna(max_date):
                            condition = f" WHERE trade_date > '{max_date}'"
                            Log.logger.info(f"发现现有文件 {output_file}，将只查询 {max_date} 之后的数据")
                except Exception as e:
                    Log.logger.warning(f"读取现有CSV文件时发生错误，将执行全量导出: {str(e)}")
            
            # 查询数据
            Log.logger.info(f"查询表 {table_name} 的代码列表数据...")
            
            # 查询语句，根据是否有最大日期构建不同的SQL
            sql = f"SELECT * FROM {table_name}{condition}"
            df = DB.select_to_df(sql, self.db)
            
            if df.empty:
                Log.logger.info(f"表 {table_name} 没有新数据需要更新")
                return True  # 没有新数据也算成功
            
            # 根据表名处理特定的映射逻辑
            processed_df = self._process_list_dataframe(df, table_name, config)
            
            # 保存数据到CSV
            mode = 'a' if max_date is not None else 'w'  # 如果是增量更新则追加模式，否则覆盖模式
            header = not (max_date is not None and os.path.exists(output_file))  # 增量更新不需要表头
            processed_df.to_csv(output_file, index=False, mode=mode, header=header)
            
            Log.logger.info(f"已{'追加' if mode == 'a' else '保存'}代码列表数据到 {output_file}")
            return True
            
        except Exception as e:
            Log.logger.error(f"处理代码列表表 {table_name} 时发生错误: {str(e)}")
            Log.logger.error(traceback.format_exc())
            return False

    def _process_adj_table(self, table_name, config):
        """处理单个复权因子表"""
        try:
            # 验证表是否存在
            if not DB.table_exists(table_name, self.db):
                Log.logger.warning(f"表 {table_name} 不存在，跳过")
                return False
                
            # 获取输出路径
            output_path = config['output_path']
            output_file = os.path.join(self.base_dir, output_path)
            
            # 确保输出目录存在
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            # 查询数据
            Log.logger.info(f"查询表 {table_name} 的复权因子数据...")
            
            # 查询语句
            sql = f"SELECT * FROM {table_name}"
            df = DB.select_to_df(sql, self.db)
            
            if df.empty:
                Log.logger.warning(f"表 {table_name} 没有数据")
                return False
            
            # 重命名列
            if 'ts_code' in df.columns:
                df.rename(columns={'ts_code': 'code'}, inplace=True)
            if 'trade_date' in df.columns:
                df.rename(columns={'trade_date': 'date'}, inplace=True)
            
            # 保存数据到CSV
            self._save_csv(df, output_file, header=True)
            
            Log.logger.info(f"已保存复权因子数据到 {output_file}")
            return True
            
        except Exception as e:
            Log.logger.error(f"处理复权因子表 {table_name} 时发生错误: {str(e)}")
            Log.logger.error(traceback.format_exc())
            return False

    def _process_calendar_table(self, table_name, output_configs):
        """处理单个交易日历表，支持多个输出位置"""
        try:
            # 验证表是否存在
            if not DB.table_exists(table_name, self.db):
                Log.logger.warning(f"表 {table_name} 不存在，跳过")
                return False
                
            # 查询数据
            Log.logger.info(f"查询表 {table_name} 的交易日历数据...")
            
            # 查询语句
            sql = f"SELECT * FROM {table_name}"
            df = DB.select_to_df(sql, self.db)
            
            if df.empty:
                Log.logger.warning(f"表 {table_name} 没有数据")
                return False
            
            # 处理各个输出配置
            for config in output_configs:
                output_path = config['output_path']
                output_file = os.path.join(self.base_dir, output_path)
                
                # 确保输出目录存在
                os.makedirs(os.path.dirname(output_file), exist_ok=True)
                
                # 复制数据框以避免修改原始数据
                processed_df = df.copy()
                
                # 是否需要添加exchange列
                if 'add_exchange' in config:
                    processed_df['exchange'] = config['add_exchange']
                
                # 保存数据到CSV
                self._save_csv(processed_df, output_file, header=True)
                
                Log.logger.info(f"已保存交易日历数据到 {output_file}")
            
            return True
            
        except Exception as e:
            Log.logger.error(f"处理交易日历表 {table_name} 时发生错误: {str(e)}")
            Log.logger.error(traceback.format_exc())
            return False

    def _process_finance_table(self, table_name, config):
        """处理单个数据表"""
        try:
            # 验证表是否存在
            if not DB.table_exists(table_name, self.db):
                Log.logger.warning(f"表 {table_name} 不存在，跳过")
                return False
                
            # 获取输出路径
            output_path = config['output_path']
            output_file = os.path.join(self.base_dir, output_path)
            
            # 确保输出目录存在
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            # 查询数据
            Log.logger.info(f"查询表 {table_name} 的数据...")
            
            # 查询语句
            sql = f"SELECT * FROM {table_name}"
            df = DB.select_to_df(sql, self.db)
            
            if df.empty:
                Log.logger.warning(f"表 {table_name} 没有数据")
                return False
            
            # 如果ts_code列存在，重命名为code
            if 'ts_code' in df.columns:
                df.rename(columns={'ts_code': 'code'}, inplace=True)
            
            # 保存数据到CSV
            self._save_csv(df, output_file, header=True)
            
            Log.logger.info(f"已保存数据到 {output_file}")
            return True
            
        except Exception as e:
            Log.logger.error(f"处理表 {table_name} 时发生错误: {str(e)}")
            Log.logger.error(traceback.format_exc())
            return False

    def _process_list_dataframe(self, df, table_name, config):
        """根据表名处理代码列表数据框的映射"""
        try:
            # 创建新的DataFrame
            new_df = pd.DataFrame()
            
            # 基础列
            new_df['code'] = df['ts_code'] if 'ts_code' in df.columns else ''
            new_df['name'] = df['name'] if 'name' in df.columns else ''
            
            # 填充固定值
            new_df['region'] = config['region']
            new_df['type'] = config['type']
            
            # 根据表名特殊处理
            if table_name == 'astock_basic':
                new_df['exchange'] = df['exchange']
                new_df['market'] = df['market']
                new_df['category'] = df['market']
                new_df['list_date'] = df['list_date']
                new_df['delist_date'] = df['delist_date']
                new_df['ext_1'] = ''
                new_df['ext_2'] = ''
                
            elif table_name == 'astock_index_basic':
                new_df['exchange'] = df['market']
                new_df['market'] = df['market']
                new_df['category'] = ''
                new_df['list_date'] = df['list_date'] if 'list_date' in df.columns else ''
                new_df['delist_date'] = df['exp_date'] if 'exp_date' in df.columns else ''
                new_df['ext_1'] = ''
                new_df['ext_2'] = ''
                
            elif table_name == 'cb_basic':
                new_df['exchange'] = df['exchange'].str.replace('SH', 'SSE').str.replace('SZ', 'SZSE')
                new_df['market'] = df['exchange']
                new_df['category'] = ''
                new_df['list_date'] = df['list_date']
                new_df['delist_date'] = df['delist_date']
                new_df['ext_1'] = ''
                new_df['ext_2'] = ''
                
            elif table_name == 'fund_basic':
                new_df['exchange'] = df['exchange'] if 'exchange' in df.columns else ''
                new_df['market'] = df['market'] if 'market' in df.columns else 'E'
                new_df['category'] = ''
                new_df['list_date'] = df['list_date'].fillna(df['purc_startdate']) if 'list_date' in df.columns and 'purc_startdate' in df.columns else ''
                new_df['delist_date'] = df['delist_date'] if 'delist_date' in df.columns else ''
                new_df['ext_1'] = ''
                new_df['ext_2'] = ''
                
            elif table_name == 'futures_basic':
                new_df['exchange'] = df['exchange']
                new_df['market'] = ''
                # 从code提取类别
                import re
                def extract_category(code):
                    match = re.match(r'([a-zA-Z]+)', code)
                    return match.group(1) if match else ''
                new_df['category'] = new_df['code'].apply(extract_category)
                new_df['list_date'] = df['list_date']
                new_df['delist_date'] = df['delist_date']
                new_df['ext_1'] = ''
                new_df['ext_2'] = ''
                
            elif table_name == 'fx_basic':
                new_df['exchange'] = df['exchange']
                new_df['market'] = ''
                new_df['category'] = df['classify']
                new_df['list_date'] = ''
                new_df['delist_date'] = ''
                new_df['ext_1'] = ''
                new_df['ext_2'] = ''
                
            elif table_name == 'hk_basic':
                new_df['exchange'] = 'HKEX'
                new_df['market'] = df['market']
                new_df['category'] = df['market']
                new_df['list_date'] = df['list_date']
                new_df['delist_date'] = df['delist_date']
                new_df['ext_1'] = df['isin'] if 'isin' in df.columns else ''
                new_df['ext_2'] = ''
                
            # 确保所有必要的列都存在
            required_columns = ['code', 'name', 'region', 'exchange', 'market', 'type', 'category', 'list_date', 'delist_date', 'ext_1', 'ext_2']
            for col in required_columns:
                if col not in new_df.columns:
                    new_df[col] = ''
            
            # 设置列顺序
            new_df = new_df[required_columns]
            
            return new_df
            
        except Exception as e:
            Log.logger.error(f"处理代码列表数据框时发生错误: {str(e)}")
            Log.logger.error(traceback.format_exc())
            return pd.DataFrame()

    def _process_table_optimized(self, table_name, dataname):
        """优化的单表处理方法，分别处理codebased和timebased数据"""
        try:
            Log.logger.info(f"开始处理表 {table_name}...")
            
            # 验证表是否存在
            if not DB.table_exists(table_name, self.db):
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
                df = DB.select_to_df(sql, self.db)
                
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
            df = DB.select_to_df(sql, self.db)
            
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
    
    def _save_csv(self, df, filepath, sort=False, sort_by=None, header=False):
        """优化的保存CSV文件方法"""
        try:
            # 避免修改原始数据
            df = df.copy()
            
            # 排序
            if sort and sort_by:
                df = df.sort_values(sort_by)
            
            # 保存文件
            df.to_csv(filepath, index=False, header=header)
                
        except Exception as e:
            Log.logger.error(f"保存文件 {filepath} 时发生错误: {str(e)}")
            Log.logger.error(traceback.format_exc())
 
# 为了向后兼容，保留这个函数
def save_tushare_data():
    """提供给外部调用的保存函数"""
    saver = TushareSaver()
    return saver.save_kline_to_csv()