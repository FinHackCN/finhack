import sys
import time
import datetime
import traceback
import pandas as pd
import os
import json

from finhack.library.db import DB
from finhack.library.alert import alert
from finhack.library.monitor import tsMonitor
from finhack.collector.tushare.helper import tsSHelper
import finhack.library.log as Log
from runtime.constant import *

class tsAStockPrice:
    # 用于保存数据采集进度的路径
    CHECKPOINT_DIR = CHECKPOINT_DIR
    
    # 确保目录存在
    try:
        if not os.path.exists(CHECKPOINT_DIR):
            os.makedirs(CHECKPOINT_DIR, exist_ok=True)
            Log.logger.info(f"已创建检查点目录: {CHECKPOINT_DIR}")
    except Exception as e:
        Log.logger.error(f"创建检查点目录失败: {str(e)}")
    
    @classmethod
    def get_checkpoint_path(cls, api, table):
        """获取检查点文件路径"""
        if not os.path.exists(cls.CHECKPOINT_DIR):
            os.makedirs(cls.CHECKPOINT_DIR, exist_ok=True)
        return os.path.join(cls.CHECKPOINT_DIR, f"{api}_{table}_checkpoint.json")
    
    @classmethod
    def save_checkpoint(cls, api, table, last_date, last_ts_code=None):
        """保存检查点信息"""
        checkpoint_path = cls.get_checkpoint_path(api, table)
        data = {
            "last_date": last_date,
            "last_ts_code": last_ts_code,
            "update_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        with open(checkpoint_path, "w") as f:
            json.dump(data, f)
        Log.logger.info(f"{api}: 已保存检查点 {last_date}")
    
    @classmethod
    def load_checkpoint(cls, api, table):
        """加载检查点信息"""
        checkpoint_path = cls.get_checkpoint_path(api, table)
        if not os.path.exists(checkpoint_path):
            return None
        try:
            with open(checkpoint_path, "r") as f:
                data = json.load(f)
            Log.logger.info(f"{api}: 从检查点恢复 {data['last_date']}")
            return data
        except Exception as e:
            Log.logger.error(f"{api}: 加载检查点失败: {str(e)}")
            return None
            
    @classmethod
    def reset_checkpoint(cls, api, table):
        """重置检查点"""
        checkpoint_path = cls.get_checkpoint_path(api, table)
        if os.path.exists(checkpoint_path):
            try:
                os.remove(checkpoint_path)
                Log.logger.warning(f"{api}: 表 {table} 不存在，已重置检查点")
                return True
            except Exception as e:
                Log.logger.error(f"{api}: 删除检查点文件失败: {str(e)}")
                return False
        return True
    
    @classmethod
    def verify_checkpoint(cls, api, table, db):
        """验证检查点，如果表不存在则重置检查点"""
        # 检查表是否存在
        if not DB.table_exists(table, db):
            Log.logger.warning(f"{api}: 表 {table} 不存在，将重置检查点")
            return cls.reset_checkpoint(api, table)
        return True

    def getPrice(pro, api, table, db):
        # 检查表是否存在，如果不存在则重置检查点
        tsAStockPrice.verify_checkpoint(api, table, db)
        
        # 获取最后日期，及检查是否有检查点
        checkpoint = tsAStockPrice.load_checkpoint(api, table)
        
        if checkpoint and checkpoint.get("last_date"):
            # 从检查点恢复
            lastdate = checkpoint["last_date"]
            Log.logger.info(f"{api}: 从检查点恢复，使用日期 {lastdate}")
            
            # 删除检查点日期的所有数据，确保重新获取完整数据
            try:
                DB.delete(f"DELETE FROM {table} WHERE trade_date = '{lastdate}'", db)
                Log.logger.info(f"{api}: 已删除日期 {lastdate} 的所有数据，准备重新获取")
            except Exception as e:
                Log.logger.error(f"{api}: 删除日期 {lastdate} 的数据时出错: {str(e)}")
        else:
            # 从数据库获取最后日期并删除可能的重复数据
            lastdate = tsSHelper.getLastDateAndDelete(table=table, filed='trade_date', ts_code="", db=db)
            Log.logger.info(f"{api}: 从数据库获取最后日期 {lastdate}")
        
        start_date = datetime.datetime.strptime(lastdate, "%Y%m%d").date()
        end_date = datetime.datetime.now().date()
        
        # 按批次获取数据
        batch_days = 1  # 每批次获取1天的数据，Tushare有限制
        current_start_date = start_date
        
        while current_start_date <= end_date:
            # 计算当前批次的结束日期
            current_end_date = min(current_start_date + datetime.timedelta(days=batch_days), end_date)
            current_start_str = current_start_date.strftime('%Y%m%d')
            current_end_str = current_end_date.strftime('%Y%m%d')
            
            Log.logger.info(f"{api}: 获取数据, 日期范围: {current_start_str} 至 {current_end_str}")
            
            # 涨跌停接口更换处理
            current_api = api
            if api == 'limit_list' and current_start_str > "20210101":
                current_api = 'limit_list_d'
                Log.logger.info(f"接口更换: 对于{current_start_str}之后的数据，使用{current_api}接口代替{api}")
                
            try:
                # 尝试使用起止日期范围获取数据
                f = getattr(pro, current_api)
                df = f(start_date=current_start_str, end_date=current_end_str)
                
                # 检查和去除重复记录
                if df is not None and not df.empty:
                    # 计算数据大小
                    original_count = len(df)
                    
                    # 按股票代码和交易日期去重（如果存在这两个字段）
                    if 'ts_code' in df.columns and 'trade_date' in df.columns:
                        df = df.drop_duplicates(subset=['ts_code', 'trade_date'], keep='first')
                        
                        # 输出去重结果
                        removed_count = original_count - len(df)
                        if removed_count > 0:
                            Log.logger.warning(f"{api}: 在日期范围 {current_start_str} 至 {current_end_str} 的源数据中发现 {removed_count} 条重复记录已被去除")
                    
                    # 预处理数据，确保关键字段为字符串类型 - 修复SettingWithCopyWarning
                    for col in df.columns:
                        if col in ['ts_code', 'symbol', 'code', 'trade_date'] or \
                           'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                            # 使用.loc来避免SettingWithCopyWarning
                            df.loc[:, col] = df[col].fillna('').astype(str)
                    
                    # 处理可能的空字符串转换问题
                    # 对于数值类型的列，将空字符串转换为None
                    numeric_cols = df.select_dtypes(include=['float', 'int']).columns
                    for col in numeric_cols:
                        if col in df.columns:
                            df[col] = df[col].replace('', None)
                    
                    # 特别处理limit_amount列，确保它不包含空字符串
                    if 'limit_amount' in df.columns:
                        df['limit_amount'] = df['limit_amount'].replace('', None)
                    
                    # 检查未来日期数据
                    today_str = datetime.datetime.now().strftime('%Y%m%d')
                    if 'trade_date' in df.columns:
                        future_data = df[df['trade_date'] > today_str]
                        if not future_data.empty:
                            Log.logger.warning(f"{api}: 发现 {len(future_data)} 条未来日期数据, 最大日期: {future_data['trade_date'].max()}")
                            
                            # 移除未来日期数据
                            df = df[df['trade_date'] <= today_str]
                            Log.logger.info(f"{api}: 已移除未来日期数据，剩余 {len(df)} 条记录")
                    
                    # 打印列名，帮助调试
                    if current_api != api:
                        Log.logger.info(f"{current_api}: 返回的列: {', '.join(df.columns)}")
                    
                    # 如果数据量很大，分批写入数据库
                    chunk_size = 5000
                    total_chunks = (len(df) + chunk_size - 1) // chunk_size
                    
                    for i in range(total_chunks):
                        start_idx = i * chunk_size
                        end_idx = min((i + 1) * chunk_size, len(df))
                        chunk_df = df.iloc[start_idx:end_idx].copy()  # 创建明确的副本
                        
                        Log.logger.info(f"{api}: 写入第 {i+1}/{total_chunks} 批数据，{len(chunk_df)} 条到 {table} 表")
                        DB.safe_to_sql(chunk_df, table, db, index=False, if_exists='append', chunksize=5000)
                    
                    # 批次写入完成后，保存检查点
                    if 'trade_date' in df.columns and not df.empty:
                        max_date = df['trade_date'].max()
                        # 确保使用正确的API名称保存检查点
                        tsAStockPrice.save_checkpoint(api, table, max_date)
                    
                    Log.logger.info(f"{api}: 日期 {current_start_str} 至 {current_end_str} 数据写入完成，共 {len(df)} 条")
                else:
                    Log.logger.warning(f"{api}: 未获取到任何数据，日期范围: {current_start_str} 至 {current_end_str}")
                
            except Exception as e:
                if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                    Log.logger.warning(f"{api}: 触发最多访问。\n{str(e)}")
                    # 保存当前进度作为检查点
                    tsAStockPrice.save_checkpoint(api, table, current_start_str)
                    return
                elif "最多访问" in str(e):
                    Log.logger.warning(f"{api}: 触发限流，等待重试。\n{str(e)}")
                    time.sleep(30)
                    continue
                else:
                    Log.logger.error(f"{api}: 获取数据失败: {str(e)}")
                    # 保存当前进度作为检查点
                    tsAStockPrice.save_checkpoint(api, table, current_start_str)
                    # 继续处理下一批次，不中断整个流程
            
            # 移动到下一个批次
            current_start_date = current_end_date + datetime.timedelta(days=1)
            
            # 防止过快请求导致API限制，每批次间隔一些时间
            time.sleep(1)

    @tsMonitor
    def daily(pro,db):
        table='astock_price_daily'
        # 检查表是否存在，如果不存在则重置检查点
        if not tsAStockPrice.verify_checkpoint("daily", table, db):
            Log.logger.warning(f"daily: 表 {table} 不存在或检查点重置失败，将使用初始日期")
        
        # 获取检查点
        checkpoint = tsAStockPrice.load_checkpoint("daily", table)
        
        if checkpoint and checkpoint.get("last_date"):
            # 从检查点恢复
            last_date = checkpoint["last_date"]
            Log.logger.info(f"daily: 从检查点恢复，使用日期 {last_date}")
            
            # 删除检查点日期的所有数据，确保重新获取完整数据
            try:
                DB.delete(f"DELETE FROM {table} WHERE trade_date = '{last_date}'", db)
                Log.logger.info(f"daily: 已删除日期 {last_date} 的所有数据，准备重新获取")
            except Exception as e:
                # 如果是表不存在错误，则继续执行
                if "no such table" in str(e).lower():
                    Log.logger.warning(f"daily: 表 {table} 不存在，将使用初始日期")
                else:
                    Log.logger.error(f"daily: 删除日期 {last_date} 的数据时出错: {str(e)}")
        else:
            # 从数据库获取最后日期并删除可能的重复数据
            last_date = tsSHelper.getLastDateAndDelete(table=table, filed='trade_date', ts_code='000001.SZ', db=db)
            Log.logger.info(f"daily: 从数据库获取最后日期 {last_date}")
        
        start_date = datetime.datetime.strptime(last_date, '%Y%m%d').date()
        end_date = datetime.datetime.now().date()
        
        # 按批次获取数据
        batch_days = 1  # 每批次获取1天的数据，Tushare有限制
        current_start_date = start_date
        
        while current_start_date <= end_date:
            # 计算当前批次的结束日期
            current_end_date = min(current_start_date + datetime.timedelta(days=batch_days), end_date)
            current_start_str = current_start_date.strftime('%Y%m%d')
            current_end_str = current_end_date.strftime('%Y%m%d')
            
            Log.logger.info(f"获取日线数据, 日期范围: {current_start_str} 至 {current_end_str}")
            
            try:
                # 使用日期范围获取数据
                df = pro.daily(start_date=current_start_str, end_date=current_end_str)
                
                # 检查和去除重复记录
                if not df.empty:
                    # 计算数据大小
                    original_count = len(df)
                    
                    # 按股票代码和交易日期去重
                    df = df.drop_duplicates(subset=['ts_code', 'trade_date'], keep='first')
                    
                    # 输出去重结果
                    removed_count = original_count - len(df)
                    if removed_count > 0:
                        Log.logger.warning(f"在日期范围 {current_start_str} 至 {current_end_str} 的源数据中发现 {removed_count} 条重复记录已被去除")
                    
                    # 预处理数据，确保股票代码等字段为字符串类型 - 修复SettingWithCopyWarning
                    for col in df.columns:
                        if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
                           'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                            # 使用.loc来避免SettingWithCopyWarning
                            df.loc[:, col] = df[col].fillna('').astype(str)
                    
                    # 处理可能的空字符串转换问题
                    # 对于数值类型的列，将空字符串转换为None
                    numeric_cols = df.select_dtypes(include=['float', 'int']).columns
                    for col in numeric_cols:
                        if col in df.columns:
                            df[col] = df[col].replace('', None)
                    
                    # 检查未来日期数据
                    today_str = datetime.datetime.now().strftime('%Y%m%d')
                    future_data = df[df['trade_date'] > today_str]
                    if not future_data.empty:
                        Log.logger.warning(f"发现 {len(future_data)} 条未来日期数据, 最大日期: {future_data['trade_date'].max()}")
                        
                        # 移除未来日期数据
                        df = df[df['trade_date'] <= today_str]
                        Log.logger.info(f"已移除未来日期数据，剩余 {len(df)} 条记录")
                    
                    # 如果数据量很大，分批写入数据库
                    chunk_size = 5000
                    total_chunks = (len(df) + chunk_size - 1) // chunk_size
                    
                    for i in range(total_chunks):
                        start_idx = i * chunk_size
                        end_idx = min((i + 1) * chunk_size, len(df))
                        chunk_df = df.iloc[start_idx:end_idx].copy()  # 创建明确的副本
                        
                        Log.logger.info(f"写入第 {i+1}/{total_chunks} 批日线数据，{len(chunk_df)} 条到 {table} 表")
                        DB.safe_to_sql(chunk_df, table, db, index=False, if_exists='append', chunksize=5000)
                    
                    # 批次写入完成后，保存检查点
                    if 'trade_date' in df.columns and not df.empty:
                        max_date = df['trade_date'].max()
                        tsAStockPrice.save_checkpoint("daily", table, max_date)
                    
                    Log.logger.info(f"日期 {current_start_str} 至 {current_end_str} 数据写入完成，共 {len(df)} 条")
                else:
                    Log.logger.warning(f"未获取到任何日线数据，日期范围: {current_start_str} 至 {current_end_str}")
            
            except Exception as e:
                if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                    Log.logger.warning(f"触发Tushare访问限制，今日额度已用完: {str(e)}")
                    # 保存当前进度作为检查点
                    tsAStockPrice.save_checkpoint("daily", table, current_start_str)
                    return
                elif "最多访问" in str(e):
                    Log.logger.warning(f"触发Tushare访问限制，等待重试: {str(e)}")
                    time.sleep(30)
                    continue
                else:
                    Log.logger.error(f"获取日线数据失败: {str(e)}")
                    # 保存当前进度作为检查点
                    tsAStockPrice.save_checkpoint("daily", table, current_start_str)
                    # 继续处理下一批次，不中断整个流程
            
            # 移动到下一个批次
            current_start_date = current_end_date + datetime.timedelta(days=1)
            
            # 防止过快请求导致API限制，每批次间隔一些时间
            time.sleep(1)

    @tsMonitor
    def weekly(pro,db):
        """获取周线行情数据"""
        table = 'astock_price_weekly'
        # 检查表是否存在，如果不存在则重置检查点
        if not tsAStockPrice.verify_checkpoint("weekly", table, db):
            Log.logger.warning(f"weekly: 表 {table} 不存在或检查点重置失败，将使用初始日期")
        
        # 获取检查点
        checkpoint = tsAStockPrice.load_checkpoint("weekly", table)
        
        # 获取开始日期
        if checkpoint and checkpoint.get("last_date"):
            last_date = checkpoint["last_date"]
            Log.logger.info(f"weekly: 从检查点恢复，使用日期 {last_date}")
            
            # 删除检查点日期的所有数据，确保重新获取完整数据
            try:
                DB.delete(f"DELETE FROM {table} WHERE trade_date = '{last_date}'", db)
                Log.logger.info(f"weekly: 已删除日期 {last_date} 的所有数据，准备重新获取")
            except Exception as e:
                # 如果是表不存在错误，则继续执行
                if "no such table" in str(e).lower():
                    Log.logger.warning(f"weekly: 表 {table} 不存在，将使用初始日期")
                else:
                    Log.logger.error(f"weekly: 删除日期 {last_date} 的数据时出错: {str(e)}")
        else:
            last_date = tsSHelper.getLastDateAndDelete(table=table, filed='trade_date', ts_code='000001.SZ', db=db)
            Log.logger.info(f"weekly: 从数据库获取最后日期 {last_date}")
        
        start_date = datetime.datetime.strptime(last_date, '%Y%m%d').date()
        end_date = datetime.datetime.now().date()
        
        # 按批次获取数据
        batch_days = 1  # 每批次获取1天的数据，Tushare有限制
        current_start_date = start_date
            
        while current_start_date <= end_date:
            # 计算当前批次的结束日期
            current_end_date = min(current_start_date + datetime.timedelta(days=batch_days), end_date)
            current_start_str = current_start_date.strftime('%Y%m%d')
            current_end_str = current_end_date.strftime('%Y%m%d')
            
            Log.logger.info(f"获取周线数据, 日期: {current_start_str}")
            
            try:
                # 直接使用trade_date参数获取当天所有股票的周线数据
                df = pro.weekly(trade_date=current_start_str)
                
                if not df.empty:
                    # 预处理数据，确保股票代码等字段为字符串类型 - 修复SettingWithCopyWarning
                    for col in df.columns:
                        if col in ['ts_code', 'symbol', 'code', 'trade_date'] or \
                           'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                            # 使用.loc来避免SettingWithCopyWarning
                            df.loc[:, col] = df[col].fillna('').astype(str)
                    
                    # 处理可能的空字符串转换问题
                    # 对于数值类型的列，将空字符串转换为None
                    numeric_cols = df.select_dtypes(include=['float', 'int']).columns
                    for col in numeric_cols:
                        if col in df.columns:
                            df[col] = df[col].replace('', None)
                    
                    # 按股票代码和交易日期去重
                    df = df.drop_duplicates(subset=['ts_code', 'trade_date'], keep='first')
                    
                    # 检查未来日期数据
                    today_str = datetime.datetime.now().strftime('%Y%m%d')
                    future_data = df[df['trade_date'] > today_str]
                    if not future_data.empty:
                        Log.logger.warning(f"发现 {len(future_data)} 条未来日期数据, 最大日期: {future_data['trade_date'].max()}")
                        
                        # 移除未来日期数据
                        df = df[df['trade_date'] <= today_str]
                        Log.logger.info(f"已移除未来日期数据，剩余 {len(df)} 条记录")
                    
                    # 如果数据量很大，分批写入数据库
                    chunk_size = 5000
                    total_chunks = (len(df) + chunk_size - 1) // chunk_size
                    
                    for i in range(total_chunks):
                        start_idx = i * chunk_size
                        end_idx = min((i + 1) * chunk_size, len(df))
                        chunk_df = df.iloc[start_idx:end_idx].copy()  # 创建明确的副本
                        
                        Log.logger.info(f"写入第 {i+1}/{total_chunks} 批周线数据，{len(chunk_df)} 条到 {table} 表")
                        DB.safe_to_sql(chunk_df, table, db, index=False, if_exists='append', chunksize=5000)
                    
                    Log.logger.info(f"日期 {current_start_str} 数据写入完成，共 {len(df)} 条")
                else:
                    Log.logger.warning(f"未获取到任何周线数据，日期: {current_start_str}")
                
                # 每天的数据处理完后，保存检查点
                tsAStockPrice.save_checkpoint("weekly", table, current_start_str)
                
            except Exception as e:
                if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                    Log.logger.warning(f"触发Tushare访问限制，今日额度已用完: {str(e)}")
                    # 保存当前进度作为检查点
                    tsAStockPrice.save_checkpoint("weekly", table, current_start_str)
                    return
                elif "最多访问" in str(e):
                    Log.logger.warning(f"触发Tushare访问限制，等待重试: {str(e)}")
                    time.sleep(30)
                    continue
                else:
                    Log.logger.error(f"获取周线数据失败: {str(e)}")
                    # 保存当前进度作为检查点
                    tsAStockPrice.save_checkpoint("weekly", table, current_start_str)
                    # 继续处理下一批次，不中断整个流程
            
            # 移动到下一个批次
            current_start_date = current_end_date + datetime.timedelta(days=1)
            
            # 防止过快请求导致API限制，每批次间隔一些时间
            time.sleep(1)

    @tsMonitor
    def monthly(pro,db):
        """获取月线行情数据"""
        table = 'astock_price_monthly'
        # 检查表是否存在，如果不存在则重置检查点
        if not tsAStockPrice.verify_checkpoint("monthly", table, db):
            Log.logger.warning(f"monthly: 表 {table} 不存在或检查点重置失败，将使用初始日期")
        
        # 获取检查点
        checkpoint = tsAStockPrice.load_checkpoint("monthly", table)
        
        # 获取开始日期
        if checkpoint and checkpoint.get("last_date"):
            last_date = checkpoint["last_date"]
            Log.logger.info(f"monthly: 从检查点恢复，使用日期 {last_date}")
            
            # 删除检查点日期的所有数据，确保重新获取完整数据
            try:
                DB.delete(f"DELETE FROM {table} WHERE trade_date = '{last_date}'", db)
                Log.logger.info(f"monthly: 已删除日期 {last_date} 的所有数据，准备重新获取")
            except Exception as e:
                # 如果是表不存在错误，则继续执行
                if "no such table" in str(e).lower():
                    Log.logger.warning(f"monthly: 表 {table} 不存在，将使用初始日期")
                else:
                    Log.logger.error(f"monthly: 删除日期 {last_date} 的数据时出错: {str(e)}")
        else:
            last_date = tsSHelper.getLastDateAndDelete(table=table, filed='trade_date', ts_code='000001.SZ', db=db)
            Log.logger.info(f"monthly: 从数据库获取最后日期 {last_date}")
        
        start_date = datetime.datetime.strptime(last_date, '%Y%m%d').date()
        end_date = datetime.datetime.now().date()
        
        # 按批次获取数据
        batch_days = 1  # 每批次获取1天的数据，Tushare有限制
        current_start_date = start_date
            
        while current_start_date <= end_date:
            # 计算当前批次的结束日期
            current_end_date = min(current_start_date + datetime.timedelta(days=batch_days), end_date)
            current_start_str = current_start_date.strftime('%Y%m%d')
            current_end_str = current_end_date.strftime('%Y%m%d')
            
            Log.logger.info(f"获取月线数据, 日期: {current_start_str}")
            
            try:
                # 直接使用trade_date参数获取当天所有股票的月线数据
                df = pro.monthly(trade_date=current_start_str)
                
                if not df.empty:
                    # 预处理数据，确保股票代码等字段为字符串类型 - 修复SettingWithCopyWarning
                    for col in df.columns:
                        if col in ['ts_code', 'symbol', 'code', 'trade_date'] or \
                           'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                            # 使用.loc来避免SettingWithCopyWarning
                            df.loc[:, col] = df[col].fillna('').astype(str)
                    
                    # 处理可能的空字符串转换问题
                    # 对于数值类型的列，将空字符串转换为None
                    numeric_cols = df.select_dtypes(include=['float', 'int']).columns
                    for col in numeric_cols:
                        if col in df.columns:
                            df[col] = df[col].replace('', None)
                    
                    # 按股票代码和交易日期去重
                    df = df.drop_duplicates(subset=['ts_code', 'trade_date'], keep='first')
                    
                    # 检查未来日期数据
                    today_str = datetime.datetime.now().strftime('%Y%m%d')
                    future_data = df[df['trade_date'] > today_str]
                    if not future_data.empty:
                        Log.logger.warning(f"发现 {len(future_data)} 条未来日期数据, 最大日期: {future_data['trade_date'].max()}")
                        
                        # 移除未来日期数据
                        df = df[df['trade_date'] <= today_str]
                        Log.logger.info(f"已移除未来日期数据，剩余 {len(df)} 条记录")
                    
                    # 如果数据量很大，分批写入数据库
                    chunk_size = 5000
                    total_chunks = (len(df) + chunk_size - 1) // chunk_size
                    
                    for i in range(total_chunks):
                        start_idx = i * chunk_size
                        end_idx = min((i + 1) * chunk_size, len(df))
                        chunk_df = df.iloc[start_idx:end_idx].copy()  # 创建明确的副本
                        
                        Log.logger.info(f"写入第 {i+1}/{total_chunks} 批月线数据，{len(chunk_df)} 条到 {table} 表")
                        DB.safe_to_sql(chunk_df, table, db, index=False, if_exists='append', chunksize=5000)
                    
                    Log.logger.info(f"日期 {current_start_str} 数据写入完成，共 {len(df)} 条")
                else:
                    Log.logger.warning(f"未获取到任何月线数据，日期: {current_start_str}")
                
                # 每天的数据处理完后，保存检查点
                tsAStockPrice.save_checkpoint("monthly", table, current_start_str)
                
            except Exception as e:
                if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                    Log.logger.warning(f"触发Tushare访问限制，今日额度已用完: {str(e)}")
                    # 保存当前进度作为检查点
                    tsAStockPrice.save_checkpoint("monthly", table, current_start_str)
                    return
                elif "最多访问" in str(e):
                    Log.logger.warning(f"触发Tushare访问限制，等待重试: {str(e)}")
                    time.sleep(30)
                    continue
                else:
                    Log.logger.error(f"获取月线数据失败: {str(e)}")
                    # 保存当前进度作为检查点
                    tsAStockPrice.save_checkpoint("monthly", table, current_start_str)
                    # 继续处理下一批次，不中断整个流程
            
            # 移动到下一个批次
            current_start_date = current_end_date + datetime.timedelta(days=1)
            
            # 防止过快请求导致API限制，每批次间隔一些时间
            time.sleep(1)

    @tsMonitor
    def adj_factor(pro,db):
        tsAStockPrice.getPrice(pro,'adj_factor','astock_price_adj_factor',db)
    
    @tsMonitor
    def suspend_d(pro,db):
        tsAStockPrice.getPrice(pro,'suspend_d','astock_price_suspend_d',db)
    
    @tsMonitor
    def daily_basic(pro,db):
        tsAStockPrice.getPrice(pro,'daily_basic','astock_price_daily_basic',db)
    
    @tsMonitor
    def moneyflow(pro,db):
        tsAStockPrice.getPrice(pro,'moneyflow','astock_price_moneyflow',db)
    
    @tsMonitor
    def stk_limit(pro,db):
        tsAStockPrice.getPrice(pro,'stk_limit','astock_price_stk_limit',db)
    
    @tsMonitor
    def limit_list(pro,db):
        """
        获取涨跌停股票，适配新旧两个API接口
        - limit_list: 旧版接口，2021年以前数据
        - limit_list_d: 新版接口，2021年以后数据
        """
        table = 'astock_price_limit_list'
        
        # 获取表中最后一条数据的日期
        lastdate = tsSHelper.getLastDateAndDelete(table=table, filed='trade_date', ts_code="", db=db)
        Log.logger.info(f"limit_list: 从数据库获取最后日期 {lastdate}")
        
        # 判断是否使用新接口
        use_new_api = False
        if lastdate > "20210101":
            use_new_api = True
            Log.logger.info(f"limit_list: 使用新接口 limit_list_d，从 {lastdate} 开始获取数据")
        else:
            Log.logger.info(f"limit_list: 使用旧接口 limit_list，从 {lastdate} 开始获取数据")
        
        # 设置起止日期
        start_date = lastdate
        end_date = datetime.datetime.now().strftime('%Y%m%d')
        
        # 按照日期分批获取数据
        current_date = datetime.datetime.strptime(start_date, '%Y%m%d').date()
        end_date_obj = datetime.datetime.strptime(end_date, '%Y%m%d').date()
        
        while current_date <= end_date_obj:
            # 获取当前处理日期
            current_date_str = current_date.strftime('%Y%m%d')
            
            # 根据日期决定使用哪个API
            if current_date_str > "20210101":
                current_api = 'limit_list_d'
                use_new_api = True
                Log.logger.info(f"limit_list: 处理日期 {current_date_str}，使用新接口 limit_list_d")
            else:
                current_api = 'limit_list'
                use_new_api = False
                Log.logger.info(f"limit_list: 处理日期 {current_date_str}，使用旧接口 limit_list")
            
            try:
                # 获取数据
                f = getattr(pro, current_api)
                df = f(trade_date=current_date_str)
                
                if df is not None and not df.empty:
                    # 数据预处理
                    for col in df.columns:
                        if col in ['ts_code', 'trade_date'] or 'code' in col.lower() or 'date' in col.lower():
                            df[col] = df[col].fillna('').astype(str)
                    
                    # 处理limit_amount列的空字符串
                    if 'limit_amount' in df.columns:
                        df['limit_amount'] = df['limit_amount'].replace('', None)
                        
                    # 打印列名，帮助调试新增列
                    if use_new_api:
                        Log.logger.info(f"limit_list: 新接口返回的列: {', '.join(df.columns)}")
                    
                    # 写入数据库
                    Log.logger.info(f"limit_list: 写入 {len(df)} 条数据到表 {table}, 日期: {current_date_str}")
                    DB.safe_to_sql(df, table, db, index=False, if_exists='append', chunksize=5000)
                else:
                    Log.logger.warning(f"limit_list: 日期 {current_date_str} 未获取到数据")
            
            except Exception as e:
                if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                    Log.logger.warning(f"limit_list: 触发访问限制: {str(e)}")
                    return
                elif "最多访问" in str(e):
                    Log.logger.warning(f"limit_list: 触发限流，等待重试: {str(e)}")
                    time.sleep(30)
                    continue
                else:
                    Log.logger.error(f"limit_list: 获取数据失败: {str(e)}")
                    # 继续处理下一天，而不是整体中断
            
            # 前进一天
            current_date += datetime.timedelta(days=1)
            # 防止请求过快
            time.sleep(1)

    @tsMonitor
    def moneyflow_hsgt(pro,db):
        tsAStockPrice.getPrice(pro,'moneyflow_hsgt','astock_price_moneyflow_hsgt',db)
    
    @tsMonitor
    def hsgt_top10(pro,db):
        tsAStockPrice.getPrice(pro,'hsgt_top10','astock_price_hsgt_top10',db)
    
    @tsMonitor
    def ggt_top10(pro,db):
        """获取港股通十大成交股"""
        table = 'astock_price_ggt_top10'
        # 检查表是否存在，如果不存在则重置检查点
        if not tsAStockPrice.verify_checkpoint("ggt_top10", table, db):
            Log.logger.warning(f"ggt_top10: 表 {table} 不存在或检查点重置失败，将使用初始日期")
        
        # 获取检查点
        checkpoint = tsAStockPrice.load_checkpoint("ggt_top10", table)
        
        # 获取开始日期和市场类型
        if checkpoint and checkpoint.get("last_date"):
            last_date = checkpoint["last_date"]
            last_market_type = checkpoint.get("last_ts_code", "")
            Log.logger.info(f"ggt_top10: 从检查点恢复，使用日期 {last_date}，市场类型 {last_market_type}")
            
            # 删除检查点日期的所有数据，确保重新获取完整数据
            try:
                DB.delete(f"DELETE FROM {table} WHERE trade_date = '{last_date}'", db)
                Log.logger.info(f"ggt_top10: 已删除日期 {last_date} 的所有数据，准备重新获取")
            except Exception as e:
                # 如果是表不存在错误，则继续执行
                if "no such table" in str(e).lower():
                    Log.logger.warning(f"ggt_top10: 表 {table} 不存在，将使用初始日期")
                else:
                    Log.logger.error(f"ggt_top10: 删除日期 {last_date} 的数据时出错: {str(e)}")
        else:
            last_date = tsSHelper.getLastDateAndDelete(table=table, filed='trade_date', db=db)
            last_market_type = ""
            Log.logger.info(f"ggt_top10: 从数据库获取最后日期 {last_date}")
        
        start_date = last_date
        end_date = datetime.datetime.now().strftime('%Y%m%d')
        
        Log.logger.info(f"获取港股通十大成交股数据, 日期范围: {start_date} 至 {end_date}")
        
        try:
            # 分别获取沪市和深市的港股通十大成交股
            market_types = ['1', '3']  # 1:沪市 3:深市
            
            # 如果有上次处理的市场类型，从该市场后开始处理
            if last_market_type == '1':
                market_types = ['3']
            elif last_market_type == '3':
                # 如果已经处理完'3'，则表示全部处理完毕
                market_types = []
            
            for market_type in market_types:
                try:
                    # 一次性获取所有数据
                    df = pro.ggt_top10(market_type=market_type, start_date=start_date, end_date=end_date)
                    
                    if not df.empty:
                        # 预处理数据，确保股票代码等字段为字符串类型 - 修复SettingWithCopyWarning
                        for col in df.columns:
                            if col in ['ts_code', 'symbol', 'code', 'trade_date'] or \
                               'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                                # 使用.loc来避免SettingWithCopyWarning
                                df.loc[:, col] = df[col].fillna('').astype(str)
                        
                        # 如果数据量很大，分批写入数据库
                        chunk_size = 5000
                        total_chunks = (len(df) + chunk_size - 1) // chunk_size
                        
                        for i in range(total_chunks):
                            start_idx = i * chunk_size
                            end_idx = min((i + 1) * chunk_size, len(df))
                            chunk_df = df.iloc[start_idx:end_idx].copy()  # 创建明确的副本
                            
                            DB.safe_to_sql(chunk_df, table, db, index=False, if_exists='append', chunksize=5000)
                            Log.logger.info(f"为市场类型 {market_type} 写入第 {i+1}/{total_chunks} 批数据，{len(chunk_df)} 条港股通十大成交股数据")
                            
                            # 保存检查点
                            tsAStockPrice.save_checkpoint("ggt_top10", table, start_date, market_type)
                            
                            # 防止数据库压力过大
                            #time.sleep(0.5)
                        
                        Log.logger.info(f"为市场类型 {market_type} 写入 {len(df)} 条港股通十大成交股数据")
                    else:
                        Log.logger.warning(f"未获取到市场类型 {market_type} 在日期范围 {start_date} 至 {end_date} 的港股通十大成交股数据")
                    
                    # 保存检查点
                    tsAStockPrice.save_checkpoint("ggt_top10", table, start_date, market_type)
                    
                    # 避免频繁调用API
                    time.sleep(1)
                    
                except Exception as e:
                    if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                        Log.logger.warning(f"触发Tushare访问限制，今日额度已用完: {str(e)}")
                        # 保存当前进度作为检查点
                        tsAStockPrice.save_checkpoint("ggt_top10", table, start_date, market_type)
                        return
                    elif "最多访问" in str(e):
                        Log.logger.warning(f"触发Tushare访问限制，等待重试: {str(e)}")
                        time.sleep(30)
                        # 保存当前进度作为检查点
                        tsAStockPrice.save_checkpoint("ggt_top10", table, start_date, market_type)
                        continue
                    else:
                        Log.logger.error(f"获取市场类型 {market_type} 的港股通十大成交股数据失败: {str(e)}")
                        # 保存当前进度作为检查点
                        tsAStockPrice.save_checkpoint("ggt_top10", table, start_date, market_type)
                        time.sleep(3)
                        continue
            
            # 全部处理完成后，记录检查点
            tsAStockPrice.save_checkpoint("ggt_top10", table, end_date, "")
            
        except Exception as e:
            Log.logger.error(f"处理港股通十大成交股数据时发生错误: {str(e)}")
            # 保存当前进度
            if 'market_type' in locals():
                tsAStockPrice.save_checkpoint("ggt_top10", table, start_date, market_type)
    
    @tsMonitor
    def hk_hold(pro,db):
        tsAStockPrice.getPrice(pro,'hk_hold','astock_price_hk_hold',db)
    
    @tsMonitor
    def ggt_daily(pro,db):
        tsSHelper.getDataAndReplace(pro,'ggt_daily','astock_price_ggt_daily',db)
    
    @tsMonitor
    def ggt_monthly(pro,db):
        tsSHelper.getDataAndReplace(pro,'ggt_monthly','astock_price_ggt_monthly',db)
    
    @tsMonitor
    def ccass_hold_detail(pro,db):
        pass #积分不够
        #tsStockPrice.getPrice(pro,'ccass_hold_detail','astock_price_ccass_hold_detail',db)
    
    # @tsMonitor
    # def pro_bar(pro,db):
    #     tsStockPrice.getPrice(pro,'daily','astock_price_daily',db)

 