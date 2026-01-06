import os
import sys
import time
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from pathlib import Path
import glob
import hashlib
from concurrent.futures import ProcessPoolExecutor, as_completed
import signal
from runtime.constant import *
import finhack.library.log as Log
from finhack.library.data import MarketConfig

class DefaultKline:
    def __init__(self, args):
        self.args = args
        self.project_path = BASE_DIR
        
        # 获取参数
        self.market = getattr(args, 'market', 'cn_stock')
        self.freq = getattr(args, 'freq', '1m')
        self.year = getattr(args, 'year', None)
        self.force = getattr(args, 'force', False)
        self.append = getattr(args, 'append', False)
        
        # 如果year参数为空字符串，也视为None
        if self.year == '':
            self.year = None
            
        # 设置日志
        self.logger = Log.logger
        
        # 中断标志
        self.interrupted = False
        
        # 注册信号处理器
        self._setup_signal_handlers()
        
    def _setup_signal_handlers(self):
        """设置信号处理器，捕获中断信号"""
        def signal_handler(signum, frame):
            self.logger.warning(f"\n接收到中断信号 {signum}，正在优雅退出...")
            self.interrupted = True
            
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
    def run(self):
        """默认运行方法，执行缓存操作"""
        self.cache()
        
    def cache(self):
        """执行K线数据缓存操作"""
        start_time = time.time()
        
        self.logger.info("=" * 80)
        self.logger.info("开始K线数据缓存操作")
        self.logger.info("=" * 80)
        self.logger.info(f"市场: {self.market}")
        self.logger.info(f"频率: {self.freq}")
        self.logger.info(f"年份: {self.year}")
        self.logger.info(f"强制重建: {self.force}")
        self.logger.info(f"追加模式: {self.append}")
        
        # 确定要处理的市场列表
        if self.market == 'all':
            markets_to_process = list(MarketConfig.MARKET_FREQ_SUPPORT.keys())
            self.logger.info(f"将处理所有市场: {markets_to_process}")
        else:
            markets_to_process = [self.market]
        
        # 遍历每个市场
        total_processed = False
        for market in markets_to_process:
            self.logger.info(f"\n{'=' * 80}")
            self.logger.info(f"处理市场: {market}")
            self.logger.info(f"{'=' * 80}")
            
            # 确定该市场支持的频率
            if self.freq == 'all':
                freqs_to_process = MarketConfig.MARKET_FREQ_SUPPORT.get(market, [])
                self.logger.info(f"将处理所有频率: {freqs_to_process}")
            else:
                freqs_to_process = [self.freq]
            
            # 遍历每个频率
            for freq in freqs_to_process:
                self.logger.info(f"\n处理频率: {freq}")
                
                # 如果没有指定年份，处理所有年份
                if self.year is None:
                    years = self._get_available_years(market, freq)
                    if not years:
                        self.logger.warning(f"市场 {market}/{freq} 没有找到可用的年份数据")
                        continue
                else:
                    years = [self.year]
                
                self.logger.info(f"将处理年份: {years}")
                
                # 处理每个年份
                market_processed = False
                for year in years:
                    if self._process_year(market, freq, year):
                        market_processed = True
                        total_processed = True
                
                if not market_processed:
                    self.logger.warning(f"市场 {market}/{freq} 没有处理任何数据，请检查数据目录和参数")
                
        if not total_processed:
            self.logger.warning("没有处理任何数据，请检查数据目录和参数")
            
        elapsed = time.time() - start_time
        self.logger.info("=" * 80)
        self.logger.info(f"K线数据缓存完成，总耗时: {elapsed:.2f}秒")
        self.logger.info("=" * 80)
        
    def _get_available_years(self, market=None, freq=None):
        """获取可用的年份数据"""
        if market is None:
            market = self.market
        if freq is None:
            freq = self.freq
            
        source_dir = os.path.join(DATA_DIR, f"market/kline/codebased/{market}/{freq}")
        
        if not os.path.exists(source_dir):
            self.logger.debug(f"源数据目录不存在: {source_dir}")
            return []
            
        # 获取所有年份目录
        years = []
        try:
            for item in os.listdir(source_dir):
                item_path = os.path.join(source_dir, item)
                if os.path.isdir(item_path) and item.isdigit():
                    years.append(int(item))
        except PermissionError as e:
            self.logger.error(f"无法读取目录 {source_dir}: {e}")
            return []
                
        return sorted(years)
        
    def _enumerate_all_years(self):
        """手动枚举所有可能的年份"""
        source_base_dir = os.path.join(DATA_DIR, f"market/kline/codebased/{self.market}/{self.freq}")
        
        if not os.path.exists(source_base_dir):
            self.logger.error(f"源数据目录不存在: {source_base_dir}")
            return []
            
        years = []
        for item in os.listdir(source_base_dir):
            item_path = os.path.join(source_base_dir, item)
            if os.path.isdir(item_path) and item.isdigit():
                years.append(int(item))
                
        return sorted(years)
        
    def _process_year(self, market, freq, year):
        """处理指定年份的数据，支持从中断处恢复"""
        self.logger.info(f"\n开始处理 {market}/{freq} {year} 年数据...")
        
        # 源目录和目标文件
        source_dir = os.path.join(DATA_DIR, f"market/kline/codebased/{market}/{freq}/{year}")
        target_file = os.path.join(DATA_DIR, f"market/kline/codebased/{market}/{freq}/{year}.parquet")
        recovery_file = target_file + '.recovery'
        
        # 确保目标目录存在
        os.makedirs(os.path.dirname(target_file), exist_ok=True)
        
        # 检查是否有恢复文件
        csv_files_to_process = []
        if os.path.exists(recovery_file) and not self.force:
            self.logger.info(f"发现恢复文件 {recovery_file}，尝试继续处理...")
            try:
                with open(recovery_file, 'r') as f:
                    processed_files = set(line.strip() for line in f if line.strip())
                
                # 获取所有CSV文件
                all_csv_files = set(f for f in os.listdir(source_dir) if f.endswith('.csv'))
                
                # 找出未处理的文件
                csv_files_to_process = list(all_csv_files - processed_files)
                
                if csv_files_to_process:
                    self.logger.info(f"从恢复点继续，已处理 {len(processed_files)} 个文件，剩余 {len(csv_files_to_process)} 个文件")
                else:
                    self.logger.info("所有文件已处理完成")
                    # 删除恢复文件
                    try:
                        os.remove(recovery_file)
                        self.logger.info("已删除恢复文件")
                    except:
                        pass
                    # 验证现有文件
                    if os.path.exists(target_file):
                        self._verify_parquet(target_file, len(all_csv_files))
                        return True
                    else:
                        # 如果目标文件不存在，重新处理
                        csv_files_to_process = list(all_csv_files)
                        self.logger.info("目标文件不存在，重新处理所有文件")
            except Exception as e:
                self.logger.error(f"读取恢复文件失败: {e}，将重新处理所有文件")
                csv_files_to_process = [f for f in os.listdir(source_dir) if f.endswith('.csv')]
        else:
            # 检查是否需要处理
            if not self._should_process_year(source_dir, target_file):
                self.logger.info(f"跳过 {year} 年，缓存文件已是最新")
                return False # 表示未处理
            
            # 获取所有CSV文件
            try:
                csv_files_to_process = [f for f in os.listdir(source_dir) if f.endswith('.csv')]
            except PermissionError as e:
                self.logger.error(f"无法读取目录 {source_dir}: {e}")
                return False
        
        if not csv_files_to_process:
            self.logger.warning(f"{year} 年没有找到CSV文件")
            return False # 表示未处理
            
        self.logger.info(f"找到 {len(csv_files_to_process)} 个CSV文件需要处理")
        
        try:
            # 处理数据
            if self.append and os.path.exists(target_file):
                # 追加模式
                if csv_files_to_process:
                    self._append_to_parquet(source_dir, target_file, csv_files_to_process)
                else:
                    self.logger.info("没有新文件需要追加")
            
            else:
                # 完整重建模式（包括从中断恢复）
                self._create_parquet(source_dir, target_file, csv_files_to_process)
                
            # 处理成功后，删除恢复文件
            if os.path.exists(recovery_file):
                try:
                    os.remove(recovery_file)
                    self.logger.info("已删除恢复文件")
                except Exception as e:
                    self.logger.warning(f"删除恢复文件失败: {e}")
            
            # 验证结果
            self._verify_parquet(target_file, len(os.listdir(source_dir)) - 1 if os.path.exists(target_file) else len(os.listdir(source_dir)))
            
            return True # 表示已处理
            
        except KeyboardInterrupt:
            # 用户中断，不删除恢复文件，方便下次继续
            self.logger.warning("处理被用户中断")
            return False
            
        except Exception as e:
            # 其他错误，也保留恢复文件
            self.logger.error(f"处理失败: {e}")
            return False
        
    def _should_process_year(self, source_dir, target_file):
        """判断是否需要处理指定年份的数据"""
        # 如果强制重建，总是处理
        if self.force:
            return True
            
        # 如果目标文件不存在，需要处理
        if not os.path.exists(target_file):
            return True
            
        # 检查源目录和目标文件的时间戳
        target_mtime = os.path.getmtime(target_file)
        
        # 获取源目录中所有CSV文件的最新修改时间
        source_mtime = 0
        for file in os.listdir(source_dir):
            if file.endswith('.csv'):
                file_path = os.path.join(source_dir, file)
                file_mtime = os.path.getmtime(file_path)
                source_mtime = max(source_mtime, file_mtime)
                
        # 如果Parquet文件比所有CSV文件都新，不需要处理
        if target_mtime >= source_mtime:
            self.logger.info(f"Parquet文件({datetime.fromtimestamp(target_mtime)})比CSV文件({datetime.fromtimestamp(source_mtime)})新，跳过处理")
            return False
            
        # 否则需要处理
        return True
        
    def _create_parquet(self, source_dir, target_file, csv_files):
        """创建新的Parquet文件，支持中断恢复"""
        # 使用临时文件进行原子写入
        temp_target_file = target_file + '.tmp'
        self.logger.info(f"创建 {temp_target_file}")
        
        # 定义数据类型 - 使用更灵活的类型定义
        dtypes = {
            'time': 'str',
            'code': 'str',
            'open': 'float32',
            'high': 'float32',
            'low': 'float32',
            'close': 'float32',
            'volume': 'float64',  # 先用float64读取，后面再转换
            'amount': 'float64'
        }
        
        # 分批处理CSV文件
        batch_size = 50  # 每批处理50个文件
        writer = None
        processed_files = []
        
        try:
            for i in range(0, len(csv_files), batch_size):
                # 检查中断标志
                if self.interrupted:
                    self.logger.warning("检测到中断信号，正在停止处理...")
                    raise KeyboardInterrupt("用户中断")
                
                batch_files = csv_files[i:i+batch_size]
                self.logger.info(f"处理批次 {i//batch_size + 1}/{(len(csv_files)-1)//batch_size + 1}，包含 {len(batch_files)} 个文件")
                
                # 读取批次中的所有CSV文件
                dfs = []
                batch_processed_files = []
                for csv_file in batch_files:
                    file_path = os.path.join(source_dir, csv_file)
                    try:
                        df = pd.read_csv(
                            file_path,
                            header=None,
                            names=['time', 'code', 'open', 'high', 'low', 'close', 'volume', 'amount'],
                            dtype=dtypes
                        )
                        dfs.append(df)
                        batch_processed_files.append(csv_file)
                    except Exception as e:
                        self.logger.error(f"读取文件失败 {csv_file}: {e}")
                        
                if not dfs:
                    continue
                    
                # 合并批次数据
                batch_df = pd.concat(dfs, ignore_index=True)
                
                # 转换时间戳
                batch_df['time'] = pd.to_datetime(batch_df['time'])
                
                # 转换volume列为int64（处理可能的float值和NaN）
                if 'volume' in batch_df.columns:
                    # 先填充NaN为0，然后转换为int64
                    batch_df['volume'] = batch_df['volume'].fillna(0)
                    # 四舍五入到整数
                    batch_df['volume'] = batch_df['volume'].round().astype('Int64')
                
                # 创建Arrow表
                table = pa.Table.from_pandas(batch_df)
                
                # 写入Parquet文件
                if writer is None:
                    # 第一批，创建writer
                    writer = pq.ParquetWriter(
                        temp_target_file,
                        table.schema,
                        compression='snappy',
                        write_statistics=True
                    )
                
                # 写入数据
                writer.write_table(table)
                
                # 记录已处理的文件
                processed_files.extend(batch_processed_files)
                    
                # 释放内存
                del batch_df, dfs, table
            
            # 关闭writer
            if writer is not None:
                writer.close()
            
            # 验证临时文件
            if not os.path.exists(temp_target_file):
                self.logger.error("临时文件创建失败")
                return False
            
            # 原子性重命名：只有在完整写入后才重命名
            if os.path.exists(target_file):
                os.remove(target_file)
            os.rename(temp_target_file, target_file)
            
            # 计算文件大小
            file_size_mb = os.path.getsize(target_file) / (1024 * 1024)
            self.logger.info(f"创建完成，文件大小: {file_size_mb:.1f}MB")
            
            return True
            
        except KeyboardInterrupt:
            # 处理键盘中断
            self.logger.warning("处理被中断，正在清理临时文件...")
            if writer is not None:
                try:
                    writer.close()
                except:
                    pass
            
            # 删除不完整的临时文件
            if os.path.exists(temp_target_file):
                try:
                    os.remove(temp_target_file)
                    self.logger.info("已删除不完整的临时文件")
                except Exception as e:
                    self.logger.error(f"删除临时文件失败: {e}")
            
            # 记录已处理的文件信息，便于后续恢复
            if processed_files:
                recovery_file = target_file + '.recovery'
                try:
                    with open(recovery_file, 'w') as f:
                        f.write('\n'.join(processed_files))
                    self.logger.info(f"已保存处理进度到 {recovery_file}")
                    self.logger.info(f"已处理 {len(processed_files)} 个文件，共 {len(csv_files)} 个")
                except Exception as e:
                    self.logger.error(f"保存恢复信息失败: {e}")
            
            raise
            
        except Exception as e:
            # 处理其他异常
            self.logger.error(f"创建Parquet文件失败: {e}")
            if writer is not None:
                try:
                    writer.close()
                except:
                    pass
            
            # 删除临时文件
            if os.path.exists(temp_target_file):
                try:
                    os.remove(temp_target_file)
                except Exception as e2:
                    self.logger.error(f"删除临时文件失败: {e2}")
            
            return False
        
    def _append_to_parquet(self, source_dir, target_file, csv_files):
        """追加数据到现有Parquet文件"""
        # 检查目标文件是否存在
        if not os.path.exists(target_file):
            self.logger.info(f"Parquet文件不存在，直接创建 {target_file}")
            self._create_parquet(source_dir, target_file, csv_files)
            return
            
        self.logger.info(f"检查是否需要追加数据到 {target_file}")
        
        # 获取现有Parquet文件中的代码列表
        existing_codes = set()
        try:
            # 读取现有文件的代码信息
            existing_table = pq.read_table(target_file, columns=['code'])
            existing_df = existing_table.to_pandas()
            existing_codes = set(existing_df['code'].unique())
            self.logger.info(f"现有Parquet文件中包含 {len(existing_codes)} 个唯一股票代码")
                    
        except Exception as e:
            self.logger.error(f"读取现有Parquet文件失败: {e}")
            # 如果读取失败，回退到创建新文件
            self._create_parquet(source_dir, target_file, csv_files)
            return
            
        # 筛选需要追加的文件
        new_files = []
        for csv_file in csv_files:
            code = csv_file.replace('.csv', '')
            if code not in existing_codes:
                new_files.append(csv_file)
                
        if not new_files:
            self.logger.info("没有新数据需要追加，所有股票代码已存在于Parquet文件中")
            return
            
        self.logger.info(f"找到 {len(new_files)} 个新文件需要追加")
        
        # 处理新文件
        self._create_parquet(source_dir, target_file + ".tmp", new_files)
        
        # 合并现有文件和临时文件
        try:
            # 读取现有文件
            existing_table = pq.read_table(target_file)
            
            # 读取临时文件
            temp_table = pq.read_table(target_file + ".tmp")
            
            # 合并表
            merged_table = pa.concat_tables([existing_table, temp_table])
            
            # 写入合并后的文件
            pq.write_table(
                merged_table,
                target_file,
                compression='snappy',
                write_statistics=True
            )
            
            # 删除临时文件
            os.remove(target_file + ".tmp")
            
            # 计算文件大小
            file_size_mb = os.path.getsize(target_file) / (1024 * 1024)
            self.logger.info(f"追加完成，文件大小: {file_size_mb:.1f}MB")
            
        except Exception as e:
            self.logger.error(f"合并文件失败: {e}")
            # 清理临时文件
            if os.path.exists(target_file + ".tmp"):
                os.remove(target_file + ".tmp")
            # 回退到完全重建
            self.logger.info("回退到完全重建...")
            self._create_parquet(source_dir, target_file, csv_files)
        
    def _verify_parquet(self, target_file, expected_file_count):
        """验证Parquet文件"""
        try:
            # 读取文件信息
            parquet_file = pq.ParquetFile(target_file)
            
            # 获取行数
            row_count = parquet_file.metadata.num_rows
            
            # 获取文件大小
            file_size_mb = os.path.getsize(target_file) / (1024 * 1024)
            
            self.logger.info(f"验证结果:")
            self.logger.info(f"  文件: {target_file}")
            self.logger.info(f"  行数: {row_count:,}")
            self.logger.info(f"  文件大小: {file_size_mb:.1f}MB")
            self.logger.info(f"  预期文件数: {expected_file_count}")
            
            # 尝试读取少量数据进行验证
            sample_table = pq.read_table(target_file).slice(0, 10)
            sample_df = sample_table.to_pandas()
            
            self.logger.info(f"  样本数据预览:")
            self.logger.info(f"{sample_df.head()}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"验证Parquet文件失败: {e}")
            return False
