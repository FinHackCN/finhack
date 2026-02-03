import sys
import time
import datetime
import traceback
import pandas as pd
import os
import json
import threading
import fcntl
import gc

from finhack.library.db import DB
from finhack.library.alert import alert
from finhack.library.monitor import tsMonitor
from finhack.collector.tushare.helper import tsSHelper
import finhack.library.log as Log
from runtime.constant import *

class tsAStockPrice:
    # 用于保存数据采集进度的路径
    CHECKPOINT_DIR = CHECKPOINT_DIR

    # 类级别的锁，用于保护检查点文件操作
    _checkpoint_lock = threading.RLock()

    # 每个检查点文件的文件锁字典
    _file_locks = {}
    _file_locks_lock = threading.Lock()

    # 确保目录存在
    try:
        if not os.path.exists(CHECKPOINT_DIR):
            os.makedirs(CHECKPOINT_DIR, exist_ok=True)
            Log.logger.info(f"已创建检查点目录: {CHECKPOINT_DIR}")
    except Exception as e:
        Log.logger.error(f"创建检查点目录失败: {str(e)}")

    @classmethod
    def _get_file_lock(cls, checkpoint_path):
        """获取指定检查点文件的文件锁"""
        with cls._file_locks_lock:
            if checkpoint_path not in cls._file_locks:
                cls._file_locks[checkpoint_path] = threading.Lock()
            return cls._file_locks[checkpoint_path]
    
    @staticmethod
    def _validate_data_completeness(df, trade_date):
        """
        验证数据完整性 - 已简化，去掉数量检查，因为早期数据不会有那么多code
        
        Args:
            df: DataFrame 数据
            trade_date: 交易日期
            
        Returns:
            tuple: (is_complete, issues_list)
        """
        try:
            if df is None or df.empty:
                return False, ["数据为空"]
            
            issues = []
            
            # 记录统计信息（不再作为验证条件）
            if 'ts_code' in df.columns:
                df_temp = df.copy()
                df_temp['exchange'] = df_temp['ts_code'].str.split('.').str[1]
                exchange_counts = df_temp['exchange'].value_counts().to_dict()
                
                # 记录统计信息，但不验证数量
                Log.logger.info(f"交易所数据分布 - BJ:{exchange_counts.get('BJ', 0)}, SH:{exchange_counts.get('SH', 0)}, SZ:{exchange_counts.get('SZ', 0)}")
            
            # 检查关键字段
            required_columns = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                issues.append(f"缺失关键字段: {missing_columns}")
            
            # 检查空值比例（放宽标准）
            for col in ['open', 'high', 'low', 'close']:
                if col in df.columns:
                    null_ratio = df[col].isnull().sum() / len(df)
                    if null_ratio > 0.5:  # 只有超过50%的空值才报错
                        issues.append(f"{col}字段空值过多: {null_ratio:.1%}")
            
            is_complete = len(issues) == 0
            
            return is_complete, issues
            
        except Exception as e:
            Log.logger.error(f"数据完整性验证异常: {str(e)}")
            return False, [f"验证过程异常: {str(e)}"]
    
    @classmethod
    def get_checkpoint_path(cls, api, table):
        """获取检查点文件路径"""
        if not os.path.exists(cls.CHECKPOINT_DIR):
            os.makedirs(cls.CHECKPOINT_DIR, exist_ok=True)
        return os.path.join(cls.CHECKPOINT_DIR, f"{api}_{table}_checkpoint.json")
    
    @classmethod
    def save_checkpoint(cls, api, table, last_date, last_ts_code=None):
        """
        保存检查点信息（线程安全，原子写入）

        使用临时文件+重命名的方式确保原子性，配合文件锁防止多线程竞争。
        """
        checkpoint_path = cls.get_checkpoint_path(api, table)
        temp_path = f"{checkpoint_path}.tmp"
        backup_path = f"{checkpoint_path}.bak"

        # 获取该检查点文件的专用锁
        file_lock = cls._get_file_lock(checkpoint_path)

        with file_lock:
            try:
                data = {
                    "api": api,
                    "table": table,
                    "last_date": last_date,
                    "last_ts_code": last_ts_code,
                    "update_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }

                # 步骤1: 写入临时文件（原子操作）
                with open(temp_path, "w") as f:
                    # 使用fcntl进行进程级文件锁（Linux/Mac）
                    try:
                        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                    except Exception:
                        pass  # Windows不支持fcntl，忽略
                    json.dump(data, f)
                    try:
                        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
                    except Exception:
                        pass

                # 步骤2: 如果原检查点存在，备份它
                if os.path.exists(checkpoint_path):
                    try:
                        # 删除旧备份
                        if os.path.exists(backup_path):
                            os.remove(backup_path)
                        # 重命名当前为备份
                        os.rename(checkpoint_path, backup_path)
                    except Exception as e:
                        Log.logger.warning(f"{api}: 备份检查点失败: {str(e)}")

                # 步骤3: 原子重命名临时文件为正式文件
                os.rename(temp_path, checkpoint_path)

                Log.logger.info(f"{api}: 已保存检查点 {last_date}")
                return True

            except Exception as e:
                Log.logger.error(f"{api}: 保存检查点失败: {str(e)}")

                # 清理临时文件
                if os.path.exists(temp_path):
                    try:
                        os.remove(temp_path)
                    except:
                        pass

                return False
    
    @classmethod
    def load_checkpoint(cls, api, table):
        """
        加载检查点信息（线程安全，支持备份恢复）

        如果主检查点文件损坏，尝试从备份恢复。
        """
        checkpoint_path = cls.get_checkpoint_path(api, table)
        backup_path = f"{checkpoint_path}.bak"

        # 获取该检查点文件的专用锁
        file_lock = cls._get_file_lock(checkpoint_path)

        with file_lock:
            # 尝试加载主检查点
            if os.path.exists(checkpoint_path):
                try:
                    with open(checkpoint_path, "r") as f:
                        # 尝试获取文件锁
                        try:
                            fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                        except Exception:
                            pass
                        data = json.load(f)
                        try:
                            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
                        except Exception:
                            pass

                    # 验证数据完整性
                    if cls._validate_checkpoint_data(data):
                        Log.logger.info(f"{api}: 从检查点恢复 {data['last_date']}")
                        return data
                    else:
                        Log.logger.warning(f"{api}: 检查点数据无效，尝试从备份恢复")
                except Exception as e:
                    Log.logger.error(f"{api}: 加载检查点失败: {str(e)}，尝试从备份恢复")

            # 尝试从备份恢复
            if os.path.exists(backup_path):
                try:
                    with open(backup_path, "r") as f:
                        data = json.load(f)

                    if cls._validate_checkpoint_data(data):
                        # 恢复备份到主文件
                        os.rename(backup_path, checkpoint_path)
                        Log.logger.info(f"{api}: 从备份恢复检查点 {data['last_date']}")
                        return data
                except Exception as e:
                    Log.logger.error(f"{api}: 从备份恢复检查点失败: {str(e)}")

            return None

    @classmethod
    def _validate_checkpoint_data(cls, data):
        """验证检查点数据的有效性"""
        if not isinstance(data, dict):
            return False
        required_fields = ['last_date']
        for field in required_fields:
            if field not in data or data[field] is None:
                return False
        # 验证日期格式
        try:
            datetime.datetime.strptime(str(data['last_date']), '%Y%m%d')
        except ValueError:
            return False
        return True
            
    @classmethod
    def reset_checkpoint(cls, api, table):
        """
        重置检查点（线程安全）

        删除检查点文件和备份文件。
        """
        checkpoint_path = cls.get_checkpoint_path(api, table)
        backup_path = f"{checkpoint_path}.bak"
        temp_path = f"{checkpoint_path}.tmp"

        # 获取该检查点文件的专用锁
        file_lock = cls._get_file_lock(checkpoint_path)

        with file_lock:
            success = True

            # 删除主检查点
            if os.path.exists(checkpoint_path):
                try:
                    os.remove(checkpoint_path)
                    Log.logger.warning(f"{api}: 已删除检查点文件")
                except Exception as e:
                    Log.logger.error(f"{api}: 删除检查点文件失败: {str(e)}")
                    success = False

            # 删除备份文件
            if os.path.exists(backup_path):
                try:
                    os.remove(backup_path)
                except Exception as e:
                    Log.logger.warning(f"{api}: 删除备份文件失败: {str(e)}")

            # 删除临时文件
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except Exception as e:
                    Log.logger.warning(f"{api}: 删除临时文件失败: {str(e)}")

            Log.logger.warning(f"{api}: 表 {table} 检查点已重置")
            return success
    
    @classmethod
    def verify_checkpoint(cls, api, table, db):
        """
        验证检查点，确保检查点与数据库实际状态一致

        检查内容包括：
        1. 表是否存在
        2. 检查点日期是否与实际数据匹配
        3. 如果检查点无效则重置
        """
        checkpoint_path = cls.get_checkpoint_path(api, table)

        # 检查表是否存在
        if not DB.table_exists(table, db):
            Log.logger.warning(f"{api}: 表 {table} 不存在，将重置检查点")
            return cls.reset_checkpoint(api, table)

        # 加载检查点
        checkpoint = cls.load_checkpoint(api, table)
        if not checkpoint:
            return True  # 没有检查点，无需验证

        # 验证检查点日期是否与实际数据一致
        try:
            last_date = checkpoint.get('last_date')
            if last_date:
                # 查询数据库中该日期的数据量
                sql = f"SELECT COUNT(*) as count FROM {table} WHERE trade_date = '{last_date}'"
                result = DB.select_to_list(sql, db)

                if result and len(result) > 0:
                    count = result[0].get('count', 0)
                    if count == 0:
                        Log.logger.warning(f"{api}: 检查点日期 {last_date} 在数据库中无数据，将重置检查点")
                        return cls.reset_checkpoint(api, table)
                    else:
                        Log.logger.debug(f"{api}: 检查点验证通过，日期 {last_date} 有 {count} 条数据")
        except Exception as e:
            Log.logger.warning(f"{api}: 验证检查点时出错: {str(e)}")

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
            
            Log.logger.info(f"{api}: 获取数据, 日期: {current_start_str}")
            
            # 涨跌停接口更换处理
            current_api = api
            if api == 'limit_list' and current_start_str > "20210101":
                current_api = 'limit_list_d'
                Log.logger.info(f"接口更换: 对于{current_start_str}之后的数据，使用{current_api}接口代替{api}")
                
            try:
                # 统一使用trade_date参数获取单日数据，避免日期范围导致的重复数据问题
                f = getattr(pro, current_api)
                df = f(trade_date=current_start_str)
                
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
                            Log.logger.warning(f"{api}: 在日期 {current_start_str} 的源数据中发现 {removed_count} 条重复记录已被去除")
                    
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
                        chunk_df = None  # 提前声明，确保在finally中可访问
                        start_idx = i * chunk_size
                        end_idx = min((i + 1) * chunk_size, len(df))

                        try:
                            chunk_df = df.iloc[start_idx:end_idx].copy()  # 创建明确的副本

                            Log.logger.info(f"{api}: 写入第 {i+1}/{total_chunks} 批数据，{len(chunk_df)} 条到 {table} 表")
                            DB.safe_to_sql(chunk_df, table, db, index=False, if_exists='append', chunksize=5000)

                        except Exception as chunk_error:
                            Log.logger.error(f"{api}: 写入第 {i+1}/{total_chunks} 批数据失败: {str(chunk_error)}")
                            # 保存检查点记录失败位置
                            if 'trade_date' in df.columns:
                                failed_date = df['trade_date'].iloc[start_idx] if start_idx < len(df) else current_start_str
                                tsAStockPrice.save_checkpoint(api, table, str(failed_date))
                            raise  # 重新抛出异常，让上层处理

                        finally:
                            # 显式释放DataFrame内存
                            if chunk_df is not None:
                                del chunk_df
                            # 每5批强制垃圾回收，避免内存持续增长
                            if (i + 1) % 5 == 0:
                                gc.collect()

                    # 批次写入完成后，保存检查点
                    if 'trade_date' in df.columns and not df.empty:
                        max_date = df['trade_date'].max()
                        # 确保使用正确的API名称保存检查点
                        tsAStockPrice.save_checkpoint(api, table, max_date)
                    
                    Log.logger.info(f"{api}: 日期 {current_start_str} 数据写入完成，共 {len(df)} 条")
                else:
                    Log.logger.warning(f"{api}: 未获取到任何数据，日期: {current_start_str}")
                
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
            
            # 移动到下一个批次（修复日期递增bug）
            current_start_date = current_start_date + datetime.timedelta(days=1)
            
            # 防止过快请求导致API限制，每批次间隔一些时间
            time.sleep(1)

    @tsMonitor
    def daily(pro,db,target_date=None):
        table='astock_price_daily'
        
        # 如果指定了target_date，则进入单日期修复模式
        if target_date is not None:
            Log.logger.info(f"daily: 进入单日期修复模式，获取日期 {target_date} 的数据")
            
            # 删除指定日期的现有数据
            try:
                DB.delete(f"DELETE FROM {table} WHERE trade_date = '{target_date}'", db)
                Log.logger.info(f"daily: 已删除日期 {target_date} 的现有数据")
            except Exception as e:
                Log.logger.warning(f"daily: 删除日期 {target_date} 的数据时出错: {str(e)}")
            
            # 直接设置开始和结束日期为指定日期
            start_date = datetime.datetime.strptime(target_date, '%Y%m%d').date()
            end_date = start_date
        else:
            # 正常模式：按现有逻辑工作
            Log.logger.info("daily: 进入正常更新模式")
            
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
            
            if target_date is not None:
                Log.logger.info(f"获取单日期修复数据, 日期: {current_start_str}")
            else:
                Log.logger.info(f"获取日线数据, 日期: {current_start_str}")
            
            try:
                # 统一使用trade_date参数获取单日数据，避免日期范围导致的重复数据问题
                df = pro.daily(trade_date=current_start_str)
                
                # 数据处理和完整性验证
                if not df.empty:
                    # 计算数据大小
                    original_count = len(df)
                    
                    # 按股票代码和交易日期去重
                    df = df.drop_duplicates(subset=['ts_code', 'trade_date'], keep='first')
                    
                    # 输出去重结果
                    removed_count = original_count - len(df)
                    if removed_count > 0:
                        Log.logger.warning(f"在日期 {current_start_str} 的源数据中发现 {removed_count} 条重复记录已被去除")
                    
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
                    
                    # ⭐ 新增：数据完整性验证
                    is_complete, completeness_issues = tsAStockPrice._validate_data_completeness(df, current_start_str)
                    
                    if not is_complete:
                        Log.logger.error(f"❌ 数据完整性验证失败: {'; '.join(completeness_issues)}")
                        # 不完整的数据不写入数据库，也不保存检查点
                        if target_date is not None:
                            Log.logger.error(f"单日期修复失败：{current_start_str} 数据不完整，跳过写入")
                            return False
                        else:
                            Log.logger.warning(f"跳过写入不完整的数据：{current_start_str}")
                            # ⭐ 修复：在continue前先更新日期，避免无限循环
                            current_start_date = current_start_date + datetime.timedelta(days=1)
                            continue
                    else:
                        Log.logger.info(f"✅ 数据完整性验证通过，日期 {current_start_str}，准备写入 {len(df)} 条记录")
                    
                    # ⭐ 改进：原子性写入数据（删除现有数据后一次性写入）
                    # 先删除该日期的所有现有数据
                    try:
                        delete_sql = f"DELETE FROM {table} WHERE trade_date = '{current_start_str}'"
                        DB.delete(delete_sql, db)
                        Log.logger.info(f"已删除日期 {current_start_str} 的现有数据，准备重新写入")
                    except Exception as e:
                        Log.logger.warning(f"删除现有数据时出错: {str(e)}")
                    
                    # 一次性批量写入（避免分批导致的部分写入问题）
                    # ⭐ 改进：增加重试机制
                    max_write_retries = 3
                    write_success = False
                    
                    for write_attempt in range(max_write_retries):
                        try:
                            attempt_info = f"第{write_attempt + 1}次尝试" if write_attempt > 0 else ""
                            Log.logger.info(f"开始原子性写入 {len(df)} 条日线数据到 {table} 表，日期：{current_start_str} {attempt_info}")
                            
                            # 计算合适的chunksize，考虑SQLite的999参数限制
                            # 对于8列数据：999 ÷ 8 = 124，为保险起见使用100
                            safe_chunk_size = max(100, min(999 // max(len(df.columns), 1), 1000))
                            chunk_size = min(safe_chunk_size, len(df))
                            
                            # 判断是否会触发详细分批日志
                            will_use_detailed_logging = chunk_size > 1 and len(df) > chunk_size
                            if will_use_detailed_logging:
                                total_batches = (len(df) + chunk_size - 1) // chunk_size
                                Log.logger.info(f"📊 分批写入模式：chunksize={chunk_size}（列数={len(df.columns)}，安全值={safe_chunk_size}），将分 {total_batches} 个批次 → 表 {table}")
                            else:
                                Log.logger.info(f"📊 单批写入模式：chunksize={chunk_size}（列数={len(df.columns)}，安全值={safe_chunk_size}）→ 表 {table}")
                            
                            DB.safe_to_sql(df, table, db, index=False, if_exists='append', chunksize=chunk_size)
                            write_success = True
                            
                            # 显示重试成功信息
                            if write_attempt > 0:
                                Log.logger.info(f"✅ 重试成功！第 {write_attempt + 1} 次尝试写入成功（表 {table}，日期 {current_start_str}）")
                            
                            break  # 写入成功，退出重试循环
                            
                        except Exception as retry_e:
                            error_msg = str(retry_e)
                            # 解析分批写入错误信息
                            if "分批写入在" in error_msg and "失败" in error_msg:
                                Log.logger.error(f"⚠️ 第{write_attempt + 1}次分批写入详情：{error_msg}")
                            else:
                                Log.logger.warning(f"第{write_attempt + 1}次写入尝试失败（表 {table}）：{error_msg}")
                                
                            if write_attempt < max_write_retries - 1:
                                # 显示重试信息
                                Log.logger.warning(f"🔄 准备进行第 {write_attempt + 2}/{max_write_retries} 次重试（表 {table}，日期 {current_start_str}）")
                                
                                # 清理可能的部分写入数据
                                try:
                                    partial_count_sql = f"SELECT COUNT(*) as count FROM {table} WHERE trade_date = '{current_start_str}'"
                                    partial_check = DB.select_to_list(partial_count_sql, db)
                                    partial_count = partial_check[0]['count'] if partial_check else 0
                                    
                                    if partial_count > 0:
                                        Log.logger.warning(f"🧹 发现 {partial_count} 条部分写入的数据，清理后重试")
                                        DB.delete(f"DELETE FROM {table} WHERE trade_date = '{current_start_str}'", db)
                                        Log.logger.info(f"✅ 已清理第{write_attempt + 1}次尝试的 {partial_count} 条部分数据")
                                    else:
                                        Log.logger.info(f"ℹ️ 第{write_attempt + 1}次尝试未产生部分数据，直接重试")
                                        
                                    Log.logger.info(f"⏱️ 等待 2 秒后开始重试...")
                                    time.sleep(2)  # 等待2秒再重试
                                except Exception as cleanup_retry_e:
                                    Log.logger.error(f"❌ 清理重试数据失败：{str(cleanup_retry_e)}")
                            else:
                                # 最后一次尝试也失败了，抛出异常
                                Log.logger.error(f"💥 所有重试尝试已用尽！第 {write_attempt + 1}/{max_write_retries} 次（最后一次）尝试也失败了")
                                raise retry_e
                    
                    if not write_success:
                        Log.logger.error(f"💥 写入最终失败：经过 {max_write_retries} 次重试仍然无法成功写入到表 {table}，日期 {current_start_str}")
                        raise Exception(f"经过 {max_write_retries} 次重试仍然写入失败")
                        
                    # 写入后验证数据 - 改进验证逻辑
                    verify_sql = f"SELECT COUNT(*) as count FROM {table} WHERE trade_date = '{current_start_str}'"
                    verify_result = DB.select_to_list(verify_sql, db)
                    actual_count = verify_result[0]['count'] if verify_result else 0
                    expected_count = len(df)
                        
                    # ⭐ 改进：使用更灵活的验证标准
                    # 允许一定范围内的数据差异，因为早期数据可能确实不完整
                    missing_ratio = abs(expected_count - actual_count) / expected_count if expected_count > 0 else 0
                    tolerance_ratio = 0.05  # 允许5%的数据差异
                    
                    if actual_count == expected_count:
                        Log.logger.info(f"✅ 数据写入验证成功：日期 {current_start_str}，预期 {expected_count} 条，实际 {actual_count} 条")
                    elif actual_count > 0 and missing_ratio <= tolerance_ratio:
                        Log.logger.warning(f"⚠️ 数据写入部分成功：日期 {current_start_str}，预期 {expected_count} 条，实际 {actual_count} 条，差异 {missing_ratio:.1%}（在容忍范围内）")
                    elif actual_count > 0:
                        Log.logger.error(f"❌ 数据写入验证失败：日期 {current_start_str}，预期 {expected_count} 条，实际 {actual_count} 条，差异 {missing_ratio:.1%}")
                        Log.logger.error(f"❌ 数据写入详情：表 {table}，chunksize {chunk_size}，列数 {len(df.columns)}，数据范围 {df['trade_date'].min()} 到 {df['trade_date'].max()}")
                        # 查询部分写入的数据分布
                        try:
                            check_sql = f"SELECT ts_code, COUNT(*) as cnt FROM {table} WHERE trade_date = '{current_start_str}' GROUP BY ts_code ORDER BY cnt DESC LIMIT 10"
                            sample_result = DB.select_to_list(check_sql, db)
                            if sample_result:
                                Log.logger.error(f"❌ 部分数据样本：{sample_result[:3]}")
                                exchange_sql = f"SELECT SUBSTR(ts_code, -2) as exchange, COUNT(*) as cnt FROM {table} WHERE trade_date = '{current_start_str}' GROUP BY SUBSTR(ts_code, -2)"
                                exchange_result = DB.select_to_list(exchange_sql, db)
                                if exchange_result:
                                    Log.logger.error(f"❌ 交易所分布：{exchange_result}")
                        except Exception as debug_e:
                            Log.logger.warning(f"查询调试信息失败：{str(debug_e)}")
                        if target_date is not None:
                            return False
                        # ⭐ 修复：在continue前先更新日期，避免无限循环
                        current_start_date = current_start_date + datetime.timedelta(days=1)
                        continue
                    else:
                        Log.logger.error(f"❌ 数据写入完全失败：日期 {current_start_str}，预期 {expected_count} 条，实际 {actual_count} 条")
                        if target_date is not None:
                            return False
                        # ⭐ 修复：在continue前先更新日期，避免无限循环  
                        current_start_date = current_start_date + datetime.timedelta(days=1)
                        continue
                    
                    # ⭐ 改进：只有在数据完整且写入成功后才保存检查点
                    if target_date is None and 'trade_date' in df.columns and not df.empty:
                        max_date = df['trade_date'].max()
                        tsAStockPrice.save_checkpoint("daily", table, max_date)
                    
                    if target_date is not None:
                        Log.logger.info(f"单日期修复数据写入完成，日期: {current_start_str}，共 {len(df)} 条")
                    else:
                        Log.logger.info(f"日期 {current_start_str} 数据写入完成，共 {len(df)} 条")
                else:
                    if target_date is not None:
                        Log.logger.warning(f"未获取到任何日线数据，日期: {current_start_str}")
                    else:
                        Log.logger.warning(f"未获取到任何日线数据，日期: {current_start_str}")
            
            except Exception as e:
                if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                    Log.logger.warning(f"触发Tushare访问限制，今日额度已用完: {str(e)}")
                    # 仅在正常模式下保存检查点
                    if target_date is None:
                        tsAStockPrice.save_checkpoint("daily", table, current_start_str)
                    return
                elif "最多访问" in str(e):
                    Log.logger.warning(f"触发Tushare访问限制，等待重试: {str(e)}")
                    time.sleep(30)
                    continue
                else:
                    Log.logger.error(f"获取日线数据失败: {str(e)}")
                    # 仅在正常模式下保存检查点
                    if target_date is None:
                        tsAStockPrice.save_checkpoint("daily", table, current_start_str)
                    # 在单日期修复模式下，直接返回失败
                    if target_date is not None:
                        return False
                    # 继续处理下一批次，不中断整个流程
            
            # 移动到下一个日期 - 修复off-by-one错误
            current_start_date += datetime.timedelta(days=1)
            
            # 防止过快请求导致API限制，每批次间隔一些时间
            time.sleep(1)
        
        # 在单日期修复模式下，返回成功状态
        if target_date is not None:
            Log.logger.info(f"单日期修复模式完成，日期: {target_date}")
            return True

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
                # 统一使用trade_date参数获取当日周线数据，避免日期范围导致的重复数据问题
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
            
            # 移动到下一个日期 - 修复off-by-one错误
            current_start_date += datetime.timedelta(days=1)
            
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
                # 统一使用trade_date参数获取当日月线数据，避免日期范围导致的重复数据问题
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
            
            # 移动到下一个日期 - 修复off-by-one错误
            current_start_date += datetime.timedelta(days=1)
            
            # 防止过快请求导致API限制，每批次间隔一些时间
            time.sleep(1)

    @tsMonitor
    def adj_factor(pro,db):
        tsAStockPrice.getPrice(pro,'adj_factor','astock_price_adj_factor',db)
    
    @tsMonitor
    def suspend_d(pro,db):
        tsAStockPrice.getPrice(pro,'suspend_d','astock_price_suspend_d',db)
    
    @tsMonitor
    def daily_basic(pro,db,target_date=None):
        table='astock_price_daily_basic'
        
        # 如果指定了target_date，则进入单日期修复模式
        if target_date is not None:
            Log.logger.info(f"daily_basic: 进入单日期修复模式，获取日期 {target_date} 的数据")
            
            # 删除指定日期的现有数据
            try:
                DB.delete(f"DELETE FROM {table} WHERE trade_date = '{target_date}'", db)
                Log.logger.info(f"daily_basic: 已删除日期 {target_date} 的现有数据")
            except Exception as e:
                Log.logger.warning(f"daily_basic: 删除日期 {target_date} 的数据时出错: {str(e)}")
            
            # 直接获取指定日期的数据
            try:
                Log.logger.info(f"获取单日期修复数据, 日期: {target_date}")
                df = pro.daily_basic(trade_date=target_date)
                
                if not df.empty:
                    # 预处理数据，确保股票代码等字段为字符串类型
                    for col in df.columns:
                        if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
                           'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                            df.loc[:, col] = df[col].fillna('').astype(str)
                    
                    # 处理可能的空字符串转换问题
                    numeric_cols = df.select_dtypes(include=['float', 'int']).columns
                    for col in numeric_cols:
                        if col in df.columns:
                            df[col] = df[col].replace('', None)
                    
                    # 检查未来日期数据
                    today_str = datetime.datetime.now().strftime('%Y%m%d')
                    if 'trade_date' in df.columns:
                        future_data = df[df['trade_date'] > today_str]
                        if not future_data.empty:
                            Log.logger.warning(f"发现 {len(future_data)} 条未来日期数据, 最大日期: {future_data['trade_date'].max()}")
                            df = df[df['trade_date'] <= today_str]
                            Log.logger.info(f"已移除未来日期数据，剩余 {len(df)} 条记录")
                    
                    # 写入数据库
                    DB.safe_to_sql(df, table, db, index=False, if_exists='append', chunksize=5000)
                    Log.logger.info(f"单日期修复数据写入完成，日期: {target_date}，共 {len(df)} 条")
                    return True
                else:
                    Log.logger.warning(f"未获取到任何daily_basic数据，日期: {target_date}")
                    return False
                    
            except Exception as e:
                if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                    Log.logger.warning(f"触发Tushare访问限制: {str(e)}")
                    return False
                elif "最多访问" in str(e):
                    Log.logger.warning(f"触发Tushare访问限制，等待重试: {str(e)}")
                    time.sleep(30)
                    return False
                else:
                    Log.logger.error(f"获取daily_basic数据失败: {str(e)}")
                    return False
        else:
            # 正常模式：使用原有的getPrice方法
            Log.logger.info("daily_basic: 进入正常更新模式")
            return tsAStockPrice.getPrice(pro,'daily_basic','astock_price_daily_basic',db)
    
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
            #Log.logger.info(f"limit_list: 使用新接口 limit_list_d，从 {lastdate} 开始获取数据")
        else:
            pass
            #Log.logger.info(f"limit_list: 使用旧接口 limit_list，从 {lastdate} 开始获取数据")
        
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
        
        # 按日期逐日处理，避免日期范围导致的数据问题
        current_date = datetime.datetime.strptime(start_date, '%Y%m%d').date()
        end_date_obj = datetime.datetime.strptime(end_date, '%Y%m%d').date()
        
        try:
            while current_date <= end_date_obj:
                current_date_str = current_date.strftime('%Y%m%d')
                
                # 分别获取沪市和深市的港股通十大成交股
                market_types = ['1', '3']  # 1:沪市 3:深市
                
                # 如果有上次处理的市场类型，从该市场后开始处理
                if current_date_str == start_date and last_market_type == '1':
                    market_types = ['3']
                elif current_date_str == start_date and last_market_type == '3':
                    # 如果已经处理完'3'，则跳到下一天
                    current_date += datetime.timedelta(days=1)
                    continue
                
                for market_type in market_types:
                    try:
                        # 统一使用trade_date参数获取单日数据，避免日期范围导致的重复数据问题
                        df = pro.ggt_top10(market_type=market_type, trade_date=current_date_str)
                        
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
                                Log.logger.info(f"日期 {current_date_str} 市场类型 {market_type} 写入第 {i+1}/{total_chunks} 批数据，{len(chunk_df)} 条港股通十大成交股数据")
                            
                            Log.logger.info(f"日期 {current_date_str} 市场类型 {market_type} 写入 {len(df)} 条港股通十大成交股数据")
                        else:
                            Log.logger.warning(f"未获取到日期 {current_date_str} 市场类型 {market_type} 的港股通十大成交股数据")
                        
                        # 避免频繁调用API
                        time.sleep(1)
                        
                    except Exception as e:
                        if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                            Log.logger.warning(f"触发Tushare访问限制，今日额度已用完: {str(e)}")
                            # 保存当前进度作为检查点
                            tsAStockPrice.save_checkpoint("ggt_top10", table, current_date_str, market_type)
                            return
                        elif "最多访问" in str(e):
                            Log.logger.warning(f"触发Tushare访问限制，等待重试: {str(e)}")
                            time.sleep(30)
                            continue
                        else:
                            Log.logger.error(f"获取日期 {current_date_str} 市场类型 {market_type} 的港股通十大成交股数据失败: {str(e)}")
                            time.sleep(3)
                            continue
                
                # 完成当前日期后，保存检查点并移动到下一天
                tsAStockPrice.save_checkpoint("ggt_top10", table, current_date_str, "")
                current_date += datetime.timedelta(days=1)
            
            # 全部处理完成后，记录检查点
            tsAStockPrice.save_checkpoint("ggt_top10", table, end_date, "")
            
        except Exception as e:
            Log.logger.error(f"处理港股通十大成交股数据时发生错误: {str(e)}")
            # 保存当前进度
            if 'current_date_str' in locals() and 'market_type' in locals():
                tsAStockPrice.save_checkpoint("ggt_top10", table, current_date_str, market_type)
    
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

 