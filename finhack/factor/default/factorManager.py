import os
import re
import time
import sys
import hashlib
import threading
import pandas as pd
import numpy as np
from numpy.lib import recfunctions
import datetime
import traceback

from finhack.library.db import DB
from finhack.library.data import get_data_interface
from runtime.constant import *
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor, wait, ALL_COMPLETED
import finhack.library.log as Log

class factorManager:
    @staticmethod
    def loadFactorsAuto(matrix_list=[], vector_list=[], code_list=[], market='cn_stock', freq='1d',
                        start_date="20200101", end_date="20201231", chunk_size=50, cache=True):
        """freq 感知加载（统一入口）。高频('m'/'s')按 code chunk 迭代；低频全量 yield 一次。
        调用方统一 `for df in loadFactorsAuto(...)` 遍历，无需关心 freq。"""
        if ('m' in freq) or ('s' in freq):
            yield from factorManager.loadFactorsByCodeChunk(
                matrix_list=matrix_list, code_list=code_list, market=market, freq=freq,
                start_date=start_date, end_date=end_date, chunk_size=chunk_size)
        else:
            df = factorManager.loadFactors(
                matrix_list=matrix_list, vector_list=vector_list, code_list=code_list,
                market=market, freq=freq, start_date=start_date, end_date=end_date, cache=cache)
            if df is not None and not df.empty:
                yield df

    @staticmethod
    def loadFactorsByCodeChunk(matrix_list=[], code_list=[], market='cn_stock', freq='1d',
                               start_date="20200101", end_date="20201231", chunk_size=50):
        """按 code chunk 迭代加载因子（1m 等大数据集用，避免全量进内存）。
        返回生成器，每 chunk 一个 DataFrame（索引 time,code）。"""
        import glob
        di = get_data_interface()
        codes = code_list if code_list else None
        yield from di._iter_matrix_factors_by_code_chunk(
            matrix_list, codes, market, freq, start_date, end_date, chunk_size)

    @staticmethod
    def inspectFactor(factor_name, factor_type="matrix", market='cn_stock', freq='1m', start_date=None, end_date=None, only_exists=False):
        """
        检查因子的基本信息，包括开始日期、结束日期、文件大小和代码数量
        
        参数:
            factor_name: 因子名称
            factor_type: 因子类型，"matrix"或"vector"
            market: 市场类型，如cn_stock
            freq: 频率，如1m, 5m, 1d等
            start_date: 开始日期，格式为"YYYYMMDD"，None表示不限制开始日期
            end_date: 结束日期，格式为"YYYYMMDD"，None表示不限制结束日期
            only_exists: 是否只检查存在性
            
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
            
            # 转换日期格式
            if start_date is not None:
                start_date = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
            if end_date is not None:
                end_date = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
            
            # only_exists 时走文件存在性检查（避免 1m 全市场全量加载 OOM）
            if only_exists:
                import glob
                # 按日期区间过滤年份目录：原 '*' 全局 glob 不看区间，任一年份有旧文件
                # 就判 exists → computeAlpha 的区间补算被挡（2020-2025 补算被 2016 旧文件误跳过）
                if start_date is not None and end_date is not None:
                    y1, y2 = int(start_date[:4]), int(end_date[:4])
                    year_dirs = [os.path.join(DATA_DIR, 'factors', factor_type, market, freq, str(y))
                                 for y in range(y1, y2 + 1)]
                else:
                    year_dirs = [os.path.join(DATA_DIR, 'factors', factor_type, market, freq, '*')]
                for yd in year_dirs:
                    for pat in (f"{factor_name}.pkl", f"{factor_name}_0.pkl"):
                        if glob.glob(os.path.join(yd, '*', pat)):
                            result["exists"] = True
                            return result
                return result

            # 获取数据接口
            data_interface = get_data_interface()

            try:
                # 尝试获取因子数据以检查是否存在
                factor_df = data_interface.get_factors(
                    factor_names=[factor_name],
                    codes=None,  # 获取所有codes
                    market=market,
                    freq=freq,
                    start_date=start_date,
                    end_date=end_date,
                    factor_type=factor_type,
                    use_cache=False  # 检查时不使用缓存
                )
                
                if not factor_df.empty:
                    result["exists"] = True
                    
                    if only_exists:
                        return result
                    
                    # 获取基本统计信息
                    result["code_count"] = len(factor_df.index.get_level_values('code').unique())
                    
                    # 获取时间范围
                    times = factor_df.index.get_level_values('time')
                    result["start_date"] = times.min().strftime("%Y%m%d")
                    result["end_date"] = times.max().strftime("%Y%m%d")
                    
                    # 估算文件大小（基于DataFrame内存使用）
                    memory_usage = factor_df.memory_usage(deep=True).sum()
                    result["total_size_mb"] = round(memory_usage / (1024 * 1024), 2)
                    
            except Exception as e:
                Log.warning(f"Failed to load factor {factor_name}: {str(e)}")
                # 如果无法加载，仍然返回基本信息
                result["exists"] = False
            
            return result
        
        except Exception as e:
            Log.error(f"Error inspecting factor {factor_name}: {str(e)}")
            traceback.print_exc()
            return None


    @staticmethod
    def loadFactors(matrix_list=[], vector_list=[], code_list=[], market='cn_stock', freq='1m', start_date="20200101", end_date="20201231", cache=False):
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
        try:
            print("start_date: {}, end_date: {}".format(start_date, end_date))
            print("matrix_list: {}, vector_list: {}".format(matrix_list, vector_list))
            print("code_list: {}, market: {}, freq: {}".format(code_list, market, freq))
            
            # 处理end_date为'now'的情况
            if end_date == 'now':
                end_date = datetime.datetime.now().strftime("%Y%m%d")
            
            # 转换日期格式为YYYY-MM-DD
            start_date_formatted = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
            end_date_formatted = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
            
            # 获取数据接口
            data_interface = get_data_interface()
            
            result_dfs = []
            
            # 加载矩阵因子
            if matrix_list:
                Log.info(f"加载矩阵因子: {matrix_list}")
                matrix_df = data_interface.get_factors(
                    factor_names=matrix_list,
                    codes=code_list if code_list else None,
                    market=market,
                    freq=freq,
                    start_date=start_date_formatted,
                    end_date=end_date_formatted,
                    factor_type='matrix',
                    use_cache=cache
                )
                if not matrix_df.empty:
                    result_dfs.append(matrix_df)
            
            # 加载向量因子
            if vector_list:
                Log.info(f"加载向量因子: {vector_list}")
                vector_df = data_interface.get_factors(
                    factor_names=vector_list,
                    codes=code_list if code_list else None,
                    market=market,
                    freq=freq,
                    start_date=start_date_formatted,
                    end_date=end_date_formatted,
                    factor_type='vector',
                    use_cache=cache
                )
                if not vector_df.empty:
                    result_dfs.append(vector_df)
            
            # 合并所有因子数据
            if result_dfs:
                if len(result_dfs) == 1:
                    final_df = result_dfs[0]
                else:
                    # 按索引合并多个DataFrame
                    final_df = result_dfs[0]
                    for df in result_dfs[1:]:
                        final_df = final_df.join(df, how='outer')
                
                Log.info(f"成功加载因子数据，形状: {final_df.shape}")
                return final_df
            else:
                Log.warning("未找到任何因子数据")
                return pd.DataFrame()
        
        except Exception as e:
            Log.error(f"加载因子数据失败: {str(e)}")
            traceback.print_exc()
            return pd.DataFrame()
    

    @staticmethod
    def saveFactors(df_factors, factor_list, market, freq, max_workers=8, buffer_size=50):
        """
        保存因子数据到存储系统
        
        参数:
            df_factors: 计算得到的因子数据DataFrame
            factor_list: 需要保存的因子列表
            market: 市场类型
            freq: 频率
            max_workers: 最大线程数（已集成到数据接口中）
            buffer_size: 内存缓冲区大小（已集成到数据接口中）
        """
        try:
            # 检查索引格式
            if not isinstance(df_factors.index, pd.MultiIndex) or 'time' not in df_factors.index.names or 'code' not in df_factors.index.names:
                Log.error("因子数据索引格式不正确，需要包含time和code")
                return df_factors
            
            # 获取数据接口
            data_interface = get_data_interface()
            
            # 使用统一数据接口保存因子数据
            success = data_interface.save_factors(
                df_factors=df_factors,
                factor_list=factor_list,
                market=market,
                freq=freq,
                factor_type='matrix',  # 默认保存为matrix格式
                max_workers=max_workers,
                buffer_size=buffer_size
            )
            
            if success:
                Log.info(f"因子保存完成: {len(factor_list)} 个因子, 数据形状: {df_factors.shape}")
            else:
                Log.error("因子保存失败")
            
            return df_factors
            
        except Exception as e:
            Log.error(f"保存因子数据失败: {str(e)}")
            traceback.print_exc()
            return df_factors


    @staticmethod
    def timeSplit(start_date, end_date, freq):
        """
        根据频率拆分时间区间，返回时间段列表
        
        参数:
            start_date: 开始日期，格式为"YYYYMMDD"
            end_date: 结束日期，格式为"YYYYMMDD"
            freq: 频率，如1m, 5m, 1d, 1w等
            
        返回:
            time_ranges: 列表，包含时间区间元组 [(start1, end1), (start2, end2), ...]
        """
        # 动态处理end_date为'now'的情况
        if end_date == 'now':
            end_date = datetime.datetime.now().strftime("%Y%m%d")
            
        # 转换日期格式
        start_dt = datetime.datetime.strptime(start_date, "%Y%m%d")
        end_dt = datetime.datetime.strptime(end_date, "%Y%m%d")
        
        time_ranges = []
        
        # 根据频率确定分割粒度
        if 'w' in freq or 'd' in freq:
            # 按年拆分
            current_year = start_dt.year
            while current_year <= end_dt.year:
                # 计算当年的开始和结束日期
                year_start = datetime.datetime(current_year, 1, 1)
                # year_start = max(start_dt, datetime.datetime(current_year, 1, 1))
                year_end = min(end_dt, datetime.datetime(current_year, 12, 31))
                
                # 添加时间范围
                time_ranges.append((
                    year_start.strftime("%Y%m%d"),
                    year_end.strftime("%Y%m%d")
                ))
                
                current_year += 1
                
        elif 'h' in freq or 'm' in freq:
            # 按月拆分
            current_dt = datetime.datetime(start_dt.year, start_dt.month, 1)
            while current_dt <= end_dt:
                # 计算当月的最后一天
                if current_dt.month == 12:
                    next_month = datetime.datetime(current_dt.year + 1, 1, 1)
                else:
                    next_month = datetime.datetime(current_dt.year, current_dt.month + 1, 1)
                month_end = next_month - datetime.timedelta(days=1)
                
                # 计算当月的开始和结束日期
                # month_start = max(start_dt, current_dt)
                month_start = current_dt
                month_end = min(end_dt, month_end)
                
                # 添加时间范围
                time_ranges.append((
                    month_start.strftime("%Y%m%d"),
                    month_end.strftime("%Y%m%d")
                ))
                
                # 移动到下个月
                if current_dt.month == 12:
                    current_dt = datetime.datetime(current_dt.year + 1, 1, 1)
                else:
                    current_dt = datetime.datetime(current_dt.year, current_dt.month + 1, 1)
                
        elif 's' in freq:
            # 按天拆分
            current_dt = start_dt
            while current_dt <= end_dt:
                # 添加时间范围（每天一个范围）
                time_ranges.append((
                    current_dt.strftime("%Y%m%d"),
                    current_dt.strftime("%Y%m%d")
                ))
                
                # 移动到下一天
                current_dt += datetime.timedelta(days=1)
        
        return time_ranges

    @staticmethod
    def adjustStartDateByFreq(start_date, freq):
        """
        根据频率调整起始日期，确保有足够的历史数据用于计算指标
        
        Args:
            start_date (str): 原始起始日期，格式如 '20200101'
            freq (str): 频率，如 's'(秒), 'm'(分钟), 'h'(小时), 'd'(日), 'w'(周)
        
        Returns:
            str: 调整后的起始日期
        """
        # 将字符串日期转换为datetime对象
        if len(start_date) == 8:  # 格式为 '20200101'
            date_format = '%Y%m%d'
        elif len(start_date) == 10 and '-' in start_date:  # 格式为 '2020-01-01'
            date_format = '%Y-%m-%d'
        else:
            # 如果格式不匹配，返回原始日期
            return start_date
        
        try:
            dt_start = datetime.datetime.strptime(start_date, date_format)
            
            # 根据不同频率调整日期
            if 'w' in freq.lower():
                # 对于周频率，使用上一年的同一周
                adjusted_dt = datetime.datetime(dt_start.year - 1, dt_start.month, dt_start.day)
            elif 'd' in freq.lower():
                # 对于日频率，使用上一年的同一天
                adjusted_dt = datetime.datetime(dt_start.year - 1, dt_start.month, dt_start.day)
            elif 'h' in freq.lower():
                # 对于小时频率，使用上个月的同一天
                year = dt_start.year
                month = dt_start.month - 1
                if month < 1:
                    month = 12
                    year -= 1
                # 处理月末问题（如1月31日回溯到上年12月时保持有效日期）
                try:
                    adjusted_dt = datetime.datetime(year, month, dt_start.day)
                except ValueError:
                    # 如果日期无效（如2月30日），使用该月的最后一天
                    if month == 12:
                        next_month = datetime.datetime(year + 1, 1, 1)
                    else:
                        next_month = datetime.datetime(year, month + 1, 1)
                    adjusted_dt = next_month - datetime.timedelta(days=1)
            elif 'm' in freq.lower():
                # 对于分钟频率，使用上个月的同一天
                year = dt_start.year
                month = dt_start.month - 1
                if month < 1:
                    month = 12
                    year -= 1
                # 处理月末问题
                try:
                    adjusted_dt = datetime.datetime(year, month, dt_start.day)
                except ValueError:
                    # 如果日期无效，使用该月的最后一天
                    if month == 12:
                        next_month = datetime.datetime(year + 1, 1, 1)
                    else:
                        next_month = datetime.datetime(year, month + 1, 1)
                    adjusted_dt = next_month - datetime.timedelta(days=1)
            elif 's' in freq.lower():
                # 对于秒级频率，使用前一天
                adjusted_dt = dt_start - datetime.timedelta(days=7)  # 调整为前7天，提供更多历史数据
            else:
                # 默认情况，不调整
                return start_date
            
            # 转换回字符串格式
            if len(start_date) == 8:
                return adjusted_dt.strftime('%Y%m%d')
            else:
                return adjusted_dt.strftime('%Y-%m-%d')
        except Exception as e:
            print(f"调整日期时出错: {str(e)}")
            # 如果转换出错，返回原始日期
            return start_date



    #获取alpha列表的列表
    @staticmethod
    def getAlphaLists():
        alphalists=[]
        path = CONFIG_DIR+"/factorlist/alphalist/"
        for subfile in os.listdir(path):
            if not '__' in subfile:
                listname=subfile
                alphalists.append(subfile)
        return alphalists
    
    #根据alpha列表获取alpha
    @staticmethod
    def getAlphaList(listname):
        path = CONFIG_DIR+"/factorlist/alphalist/"+listname
        with open(path, 'r', encoding='utf-8') as f:
            return f.readlines()
            

    @staticmethod
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

    @staticmethod
    def list_factors(market='cn_stock', freq='1d', factor_type='matrix'):
        """列出已入库的因子名（扫 factors/{factor_type}/{market}/{freq}/ 目录）。
        替代旧版依赖 MySQL factors_list 表的 getFactorsList，纯目录扫描、市场参数化。"""
        base = os.path.join(DATA_DIR, 'factors', factor_type, market, freq)
        factors = set()
        if os.path.exists(base):
            for root, dirs, files in os.walk(base):
                for f in files:
                    if f.endswith('.pkl') and not f.endswith('index.pkl'):
                        factors.add(f[:-4])
        return sorted(factors)

    # ============ 因子库管理（dashboard 因子管理用，纯 inode 操作不加载 pkl） ============

    @staticmethod
    def inspectFactorsLight(market='cn_stock', freq='1d', factor_type='matrix'):
        """轻量批量 inspect：glob+stat 扫 pkl，不加载内容（防 1m 全市场 OOM）。
        每因子聚合 year 范围 / code 数 / 磁盘大小 / 文件数。
        返回 [{name, type, year_min, year_max, code_count, size_mb, file_count}]。"""
        base = os.path.join(DATA_DIR, 'factors', factor_type, market, freq)
        info = {}  # name -> {years:set, codes:set, size:int, files:int}
        if not os.path.isdir(base):
            return []
        for root, dirs, files in os.walk(base):
            for fn in files:
                if not fn.endswith('.pkl') or fn.endswith('index.pkl'):
                    continue
                fpath = os.path.join(root, fn)
                rel = os.path.relpath(fpath, base)
                parts = rel.split(os.sep)
                if len(parts) < 3:  # 期望 year/code/name.pkl
                    continue
                year, code = parts[0], parts[1]
                name = fn[:-4]
                try:
                    sz = os.path.getsize(fpath)
                except OSError:
                    sz = 0
                d = info.setdefault(name, {'years': set(), 'codes': set(), 'size': 0, 'files': 0})
                d['years'].add(year)
                d['codes'].add(code)
                d['size'] += sz
                d['files'] += 1
        out = []
        for name, d in info.items():
            years = sorted(y for y in d['years'] if y.isdigit())
            out.append({
                'name': name, 'type': factor_type,
                'year_min': int(years[0]) if years else None,
                'year_max': int(years[-1]) if years else None,
                'code_count': len(d['codes']),
                'size_mb': round(d['size'] / 1048576, 2),
                'file_count': d['files'],
            })
        out.sort(key=lambda x: x['name'])
        return out

    @staticmethod
    def coverageFactor(name, market='cn_stock', freq='1d', factor_type='matrix', max_codes=80):
        """单因子覆盖度（年份×代码稀疏矩阵），用于热力图。
        匹配 {name}.pkl 与 {name}_N.pkl 分片。codes 过多时截断前 max_codes 个。
        返回 {years, codes, present:[[year_idx,code_idx],...], code_total, cell_total}。"""
        base = os.path.join(DATA_DIR, 'factors', factor_type, market, freq)
        pat = re.compile(rf'^{re.escape(name)}(_\d+)?\.pkl$')
        yc = set()
        if os.path.isdir(base):
            for root, dirs, files in os.walk(base):
                for fn in files:
                    if pat.match(fn):
                        rel = os.path.relpath(os.path.join(root, fn), base)
                        parts = rel.split(os.sep)
                        if len(parts) >= 3:
                            yc.add((parts[0], parts[1]))
        years = sorted({y for y, _ in yc})
        all_codes = sorted({c for _, c in yc})
        codes = all_codes[:max_codes]
        yi = {y: i for i, y in enumerate(years)}
        ci = {c: i for i, c in enumerate(codes)}
        present = [[yi[y], ci[c]] for y, c in yc if c in ci]
        return {'years': years, 'codes': codes, 'present': present,
                'code_total': len(all_codes), 'cell_total': len(yc)}

    @staticmethod
    def deleteFactorFiles(name, market='cn_stock', freq='1d'):
        """删除某因子所有 pkl（matrix+vector，含 _N 分片）。正则精确匹配防误删
        （删 alpha191_001 不会连带删 alpha191_002）。返回删除文件数。"""
        pat = re.compile(rf'^{re.escape(name)}(_\d+)?\.pkl$')
        removed = 0
        for ftype in ('matrix', 'vector'):
            base = os.path.join(DATA_DIR, 'factors', ftype, market, freq)
            if not os.path.isdir(base):
                continue
            for root, dirs, files in os.walk(base):
                for fn in files:
                    if pat.match(fn):
                        try:
                            os.remove(os.path.join(root, fn))
                            removed += 1
                        except OSError:
                            pass
        return removed