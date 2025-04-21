import os
import re
import gc
import time
import objgraph
import hashlib
import datetime
import warnings
import traceback
import numpy as np
import pandas as pd
from functools import lru_cache
from importlib import import_module
from multiprocessing import cpu_count 
import importlib
from runtime.constant import *
from finhack.library.mydb import mydb
from finhack.library.config import Config
from finhack.factor.default.factorManager import factorManager
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor, wait, ALL_COMPLETED
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)
import finhack.library.log as Log
 


class indicatorEngine():
    @lru_cache(None)
    def getIndicatorList(market,freq,task_list):
        """
        获取所有的指标列表
        """
        indicator_list = []
        for root, dirs, files in os.walk(CONFIG_DIR + f"/factorlist/indicatorlist/{market}/{freq}/"):
            for file in files:
                if file in task_list.split(','):
                    with open(os.path.join(root, file), 'r', encoding='utf-8') as f:
                        lines = [line.strip() for line in f.readlines()]
                        indicator_list.extend(lines)
        for root, dirs, files in os.walk(CONFIG_DIR + f"/factorlist/indicatorlist/{market}/x{freq}/"):
            for file in files:
                if file in task_list.split(','):
                    with open(os.path.join(root, file), 'r', encoding='utf-8') as f:
                        lines = [line.strip() for line in f.readlines()]
                        indicator_list.extend(lines)
        return indicator_list


    def getAllIndicatorRelation(indicator_list, market, freq):
        """
        分析指标之间的依赖关系，构建计算顺序和并行组
        
        参数:
            indicator_list: 所有需要计算的指标列表
            market: 市场类型
            freq: 频率
            
        返回:
            dict: 包含以下键值对:
                - 'dependency_graph': 每个指标的依赖关系字典
                - 'calculation_levels': 按计算层级分组的指标列表
                - 'parallel_groups': 可并行计算的指标组，按代码执行分组
                - 'base_fields': 基础数据字段列表
        """
        # 构建指标依赖图
        dependency_graph = {}
        field_producers = {}  # 记录每个字段的生产者（指标）
        field_references = {}  # 记录每个字段被哪些指标引用
        
        # 代码执行分组 - 存储相同模块和函数的指标
        execution_groups = {}
        
        # 第一步：获取每个指标的信息和依赖关系
        for indicator in indicator_list:
            indicator_field = indicator.split('_')[0]
            
            module_name, func_name, code, return_fields, referenced_fields = indicatorEngine.getIndicatorInfo(indicator, market, freq)
            if not module_name:  # 如果获取失败，跳过
                continue
            
            # 确保return_fields和referenced_fields不为空
            if not return_fields:
                return_fields = [indicator_field]
            if not referenced_fields:
                referenced_fields = []
            
            # 记录该指标的依赖字段
            dependency_graph[indicator] = {
                'module': module_name,
                'function': func_name,
                'returns': return_fields,
                'depends_on': referenced_fields,
                'dependent_indicators': [],  # 将存储依赖于该指标的其他指标
                'depends_on_indicators': [],  # 存储当前指标依赖的其他指标
                'is_base_field': False  # 标记是否为基础字段指标
            }
            
            # 记录每个字段的生产者
            for field in return_fields:
                field_producers[field] = indicator
            
            # 记录每个引用字段被哪些指标使用
            for field in referenced_fields:
                if field not in field_references:
                    field_references[field] = [indicator]
                else:
                    field_references[field].append(indicator)
            
            # 构建执行分组键
            execution_key = f"{module_name}_{func_name}"
            if execution_key not in execution_groups:
                execution_groups[execution_key] = {
                    'module': module_name,
                    'function': func_name,
                    'indicators': []
                }
            execution_groups[execution_key]['indicators'].append(indicator)
        
        # 第二步：确定所有基础字段（被引用但没有生产者的字段）
        base_fields = set()
        for field in field_references:
            if field not in field_producers:
                base_fields.add(field)
        
        # 确保常见基础字段被包含在内
        common_base_fields = {'open', 'high', 'low', 'close', 'volume'}
        for field in common_base_fields:
            base_fields.add(field)
            
        # 第三步：为所有基础字段创建虚拟指标，并设置为最高优先级
        for field in base_fields:
            base_indicator = f"{field}_0"
            dependent_indicators = field_references.get(field, [])
            
            dependency_graph[base_indicator] = {
                'module': 'base_data',
                'function': 'get_base_data',
                'returns': [field],
                'depends_on': [],
                'dependent_indicators': dependent_indicators.copy(),
                'depends_on_indicators': [],
                'is_base_field': True  # 标记为基础字段指标
            }
        
        # 第四步：构建指标之间的完整依赖关系
        # 特别处理：确保WILLR依赖于close等基础字段
        special_dependencies = {
            'WILLR': ['close', 'high', 'low']
        }
        
        for indicator, info in list(dependency_graph.items()):
            # 处理特殊指标的依赖关系
            indicator_base = indicator.split('_')[0]
            if indicator_base in special_dependencies and 'base_data' not in info['module']:
                for dep_field in special_dependencies[indicator_base]:
                    if dep_field not in info['depends_on']:
                        info['depends_on'].append(dep_field)
            
            # 对于每个依赖字段，找到其生产者并建立依赖关系
            for ref_field in info['depends_on']:
                if ref_field in field_producers:
                    # 字段有生产者，建立指标间的依赖
                    producer = field_producers[ref_field]
                    
                    if producer != indicator:  # 避免自依赖
                        # 添加依赖指标
                        if producer not in info['depends_on_indicators']:
                            info['depends_on_indicators'].append(producer)
                        
                        # 添加被依赖关系
                        if indicator not in dependency_graph[producer]['dependent_indicators']:
                            dependency_graph[producer]['dependent_indicators'].append(indicator)
                else:
                    # 字段是基础字段，建立与虚拟指标的依赖
                    base_indicator = f"{ref_field}_0"
                    
                    if base_indicator in dependency_graph:
                        # 添加依赖指标
                        if base_indicator not in info['depends_on_indicators']:
                            info['depends_on_indicators'].append(base_indicator)
                        
                        # 添加被依赖关系
                        if indicator not in dependency_graph[base_indicator]['dependent_indicators']:
                            dependency_graph[base_indicator]['dependent_indicators'].append(indicator)
        
        # 第五步：拓扑排序，构建计算层级
        # 首先处理基础字段指标，然后是一级指标，再是二级指标
        calculation_levels = []
        
        # 第一层：只处理基础字段指标
        base_indicators = [ind for ind, info in dependency_graph.items() if info['is_base_field']]
        if base_indicators:
            # 按字段名称排序以保持稳定顺序（先处理常见基础字段）
            priority_order = {field: i for i, field in enumerate(['close', 'high', 'low', 'open', 'volume'])}
            base_indicators.sort(key=lambda x: priority_order.get(x.split('_')[0], 999))
            calculation_levels.append(base_indicators)
        
        # 处理剩余指标
        remaining_indicators = {ind for ind in dependency_graph.keys() if not dependency_graph[ind]['is_base_field']}
        processed_indicators = set(base_indicators)
        
        while remaining_indicators:
            # 找出当前层可以计算的指标（所有依赖都已处理）
            current_level = set()
            
            for indicator in list(remaining_indicators):
                dependencies = dependency_graph[indicator]['depends_on_indicators']
                if all(dep in processed_indicators for dep in dependencies):
                    current_level.add(indicator)
            
            # 如果无法找到可以计算的指标但仍有剩余指标，说明存在循环依赖
            if not current_level and remaining_indicators:
                # 寻找依赖最少的指标来打破循环
                min_deps = float('inf')
                min_indicator = next(iter(remaining_indicators))
                
                for indicator in remaining_indicators:
                    unprocessed_deps = [dep for dep in dependency_graph[indicator]['depends_on_indicators'] 
                                        if dep in remaining_indicators]
                    if len(unprocessed_deps) < min_deps:
                        min_deps = len(unprocessed_deps)
                        min_indicator = indicator
                
                print(f"检测到循环依赖，强制打破循环选择指标: {min_indicator}，依赖数: {min_deps}")
                current_level = {min_indicator}
            
            # 将当前层添加到计算层级
            sorted_level = sorted(list(current_level))
            calculation_levels.append(sorted_level)
            processed_indicators.update(current_level)
            remaining_indicators -= current_level
        
        # 第六步：基于代码执行分组构建并行计算组
        parallel_groups = []
        
        # 对每个计算层级，根据相同代码执行进行分组
        for level in calculation_levels:
            level_execution_groups = {}
            
            for indicator in level:
                module_name = dependency_graph[indicator]['module']
                func_name = dependency_graph[indicator]['function']
                execution_key = f"{module_name}_{func_name}"
                
                if execution_key not in level_execution_groups:
                    level_execution_groups[execution_key] = []
                level_execution_groups[execution_key].append(indicator)
            
            # 将该层级的执行组添加到并行组
            level_parallel_groups = list(level_execution_groups.values())
            parallel_groups.append(level_parallel_groups)
        
        # 打印调试信息
        # print("\n基础字段:", base_fields)
        # print("\nWILLR和close的生产者:", {k: v for k, v in field_producers.items() if k in ['WILLR', 'close']})
        # print("\n计算层级:")
        # for i, level in enumerate(calculation_levels):
        #     print(f"层级 {i}:", level[:5], "..." if len(level) > 5 else "")
        
        return {
            'dependency_graph': dependency_graph,
            'calculation_levels': calculation_levels,
            'parallel_groups': parallel_groups,
            'execution_groups': execution_groups,
            'base_fields': list(base_fields)
        }


    @lru_cache(None)
    def getIndicatorInfo(indicator_name,market,freq):
        indicator=indicator_name.split('_')
        indicator_filed=indicator[0]
        path = INDICATORS_DIR+f"/{market}/x{freq}/"
        for subfile in os.listdir(path):
            if not '__' in subfile:
                indicators=subfile.split('.py')
                indicators=indicators[0]
                function_name=''
                indicator_code=''
                return_fileds=[]
                find=False
                with open(path+subfile) as filecontent:
                    for line in filecontent:
                        #提取当前函数名
                        if('def ' in line):
                            function_name=line.split('def ')
                            function_name=function_name[1]
                            function_name=function_name.split('(')
                            function_name=function_name[0]
                            function_name=function_name.strip()
                            indicator_code=line
                        else:
                            indicator_code=indicator_code+"\n"+line
                        left=line.split('=')
                        left=left[0]
                        
                        #pattern = re.compile(r"df\[\'(\w*?)\'\]")   # 查找数字
                        pattern = re.compile(r"df\[\'([A-Za-z0-9_\-]*?)\'\]")   # 查找数字
                        flist = pattern.findall(left)
                        return_fileds=return_fileds+flist

                        if("df['"+indicator_filed+"_") in left or ("df['"+indicator_filed+"']") in left:
                            find=True
                        if 'return ' in  line:
                            if find:
                                # 收集代码中所有出现的字段
                                all_fields_pattern = re.compile(r"df\[\'([A-Za-z0-9_\-]*?)\'\]")
                                all_fields = all_fields_pattern.findall(indicator_code)
                                all_fields = list(set(all_fields))  # 去重
                                
                                # 计算引用的字段（所有字段减去返回的字段）
                                referenced_fields = list(set(all_fields) - set(return_fileds))
                                
                                return indicators, function_name, indicator_code, return_fileds, referenced_fields
                            else:
                                indicator_code=''
                                return_fileds=[]
                         

        print('wrong indicator_name:'+indicator_name)       
        return False, False, False, [], []


    def computeIndicatorBatch(market,freq,indicator_list,process_num="auto",start_date="",end_date="",code_list=[]):
        indicator_relation = indicatorEngine.getAllIndicatorRelation(indicator_list, market, freq)
        parallel_groups=indicator_relation['parallel_groups']
        for parallel_group in parallel_groups:
            indicatorEngine.computeIndicatorByLevelGroup(market, freq,parallel_group,process_num, start_date, end_date, code_list)

    
    #根据批量计算当前层级组下的指标
    def computeIndicatorByLevelGroup(market, freq , parallel_group, process_num="auto", start_date="20200101",end_date="20201231",code_list=[]):
        print('计算指标组:', parallel_group)
        
        # 确定进程数量
        if process_num == "auto":
            # 获取CPU核心数并减1，至少为1
            cpu_cores = cpu_count()
            process_num = max(1, cpu_cores - 1)
        else:
            # 如果指定了进程数，确保它是整数
            process_num = int(process_num)
        
        print(f"使用进程数: {process_num}")
        
        # 使用进程池执行指标计算
        with ProcessPoolExecutor(max_workers=process_num) as executor:
            futures = []
            
            # 为每个指标组提交计算任务
            for indicator_group in parallel_group:
                future = executor.submit(
                    indicatorEngine.computeIndicator,
                    market,
                    freq,
                    indicator_group,
                    1,  # 在子进程中只用单进程执行
                    start_date,
                    end_date,
                    code_list
                )
                futures.append(future)
            
            # 等待所有任务完成
            wait(futures, return_when=ALL_COMPLETED)
            
            # 检查任务是否有异常
            for future in futures:
                try:
                    future.result()  # 获取结果，如果有异常会引发
                except Exception as e:
                    print(f"指标计算出错: {str(e)}")
                    traceback.print_exc()
        
        print(f"指标组计算完成")

    #这里的list是同代码返回的字段，本意是希望只需要计算一次，但后来发现还有不同参数的情况
    def computeIndicator(market, freq, indicator_list, process_num="auto", start_date="", end_date="", code_list=[], save=True):
        """
        计算指标，支持按时间分块计算
        
        参数:
            market: 市场类型，如 'cn_stock'
            freq: 频率，如 '1d', '1m', '1s' 等
            indicator_list: 需要计算的指标列表
            process_num: 进程数量，默认为 'auto'（自动确定）
            start_date: 开始日期，格式为 'YYYYMMDD'
            end_date: 结束日期，格式为 'YYYYMMDD'，如果为 'now' 则使用当前日期
            code_list: 股票代码列表，为 None 则处理所有股票
            save: 是否保存计算结果
        """
        # 理论上不应该出现空的指标列表，但为了安全起见，还是加上判断
        if not indicator_list:
            print("指标列表为空，无需计算")
            return
            
        try:
            print(f"计算指标: {indicator_list}")
            
            # 处理 end_date 为 'now' 的情况
            if end_date == 'now':
                end_date = datetime.datetime.now().strftime("%Y%m%d")
                
            # 1. 获取指标的计算函数和依赖关系
            indicator = indicator_list[0]  # 取第一个指标作为代表
            module_name, func_name, indicator_code, return_fields, referenced_fields = indicatorEngine.getIndicatorInfo(indicator, market, freq)

            if not module_name or not func_name:
                print(f"无法获取指标信息: {indicator}")
                return
                
            print("依赖字段:", referenced_fields)
            
            # 2. 加载指标计算函数
            try:
                # 定义文件路径
                file_path = f"{INDICATORS_DIR}/{market}/x{freq}/{module_name}.py"
                
                # 使用importlib.util更精确地加载模块
                import importlib.util
                spec = importlib.util.spec_from_file_location(module_name, file_path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                
                # 获取计算函数
                func = getattr(module, func_name, None)
                
                if func is None:
                    print(f"无法找到指标函数: {func_name}")
                    return
            except Exception as e:
                print(f"加载指标模块或函数失败: {str(e)}")
                traceback.print_exc()
                return

            # 3. 确定模块类型
            module_type = module_name.split('_')[0]  # mix, ts, cs等
            
            # 4. 使用timeSplit进行时间拆分
            time_ranges = indicatorEngine.timeSplit(start_date, end_date, freq)
            print(f"时间范围拆分为 {len(time_ranges)} 个区间")
            print(time_ranges)
            for range_start, range_end in time_ranges:
                print(f"处理时间区间: {range_start} - {range_end}")
                # 加载该时间区间的依赖字段数据
                df_ref = factorManager.loadFactors(
                        matrix_list=referenced_fields,
                        vector_list=[],
                        code_list=code_list,
                        market=market,
                        freq=freq,
                        start_date=range_start,
                        end_date=range_end,
                        cache=True  # 使用缓存加速
                )
                    
                if df_ref.empty and module_type != "mix":
                    print(f"时间区间 {range_start} - {range_end} 无数据")
                    continue
                    
                # 根据模块类型计算指标
                result_df = None

                if module_type == "mix":
                    # 混合因子，单次计算
                    result_df = func(df_ref,[range_start, range_end])
                elif module_type == "ts":
                    # 时间序列因子，对每个股票代码分别计算
                    if code_list:
                        stocks = code_list
                    else:
                        stocks = df_ref.index.get_level_values('ts_code').unique().tolist()
                        
                    code_results = []
                    for code in stocks:
                        code_df = df_ref.xs(code, level='ts_code')
                        if not code_df.empty:
                            try:
                                code_result = func(code_df, indicator_list)
                                code_result['ts_code'] = code
                                code_results.append(code_result)
                            except Exception as e:
                                print(f"计算股票 {code} 的时间序列指标失败: {str(e)}")
                        
                    if code_results:
                        result_df = pd.concat(code_results)
                elif module_type == "cs":
                        # 横截面因子，对每个交易日分别计算
                    dates = df_ref.index.get_level_values('trade_date').unique().tolist()
                        
                    date_results = []
                    for date in dates:
                        date_df = df_ref.xs(date, level='trade_date')
                        if not date_df.empty:
                            try:
                                date_result = func(date_df, indicator_list)
                                date_result['trade_date'] = date
                                date_results.append(date_result)
                            except Exception as e:
                                print(f"计算日期 {date} 的横截面指标失败: {str(e)}")
                        
                    if date_results:
                        result_df = pd.concat(date_results)
                
                # 处理指标结果 - 重命名和 shift
                if result_df is not None and not result_df.empty:
                    # 处理每个指标的重命名和shift
                    for indicator in indicator_list:
                        parts = indicator.split('_')
                        factor_name = parts[0]
                        
                        # 检查该列是否存在
                        if factor_name in result_df.columns:
                            # 确定后缀
                            suffix = '_'.join(parts[1:]) if len(parts) > 1 else '0'
                            new_column_name = f"{factor_name}_{suffix}"
                            
                            # 重命名列
                            result_df.rename(columns={factor_name: new_column_name}, inplace=True)
                            
                            # 检查是否需要shift
                            if len(parts) > 1 and parts[-1] != '0':
                                shift_value = int(parts[-1])
                                result_df[new_column_name] = result_df[new_column_name].shift(shift_value)
                    
                    # 如果计算成功且需要保存
                    if save:
                        indicatorEngine.saveFactors(result_df, indicator_list, market, freq)
                    
                    print(f"计算指标组 {indicator_list} 成功，结果数据量: {len(result_df)}")
                    print(result_df)
                    exit()
        except Exception as e:
            print(f"指标计算过程出现未预期异常: {str(e)}")
            traceback.print_exc()
    
    def saveFactors(df_factors, indicator_list, market, freq, max_workers=100):
        """
        保存因子数据到指定目录结构 (多线程版本)
        
        参数:
            df_factors: 计算得到的因子数据DataFrame
            indicator_list: 需要保存的指标列表
            market: 市场类型
            freq: 频率
            max_workers: 最大线程数
            
        返回:
            保存的DataFrame
        """
        try:
            # 按照频率确定存储路径
            base_path = FACTORS_DIR + f"/matrix/{market}/{freq}"
            
            # 确保索引中有time和code
            if not isinstance(df_factors.index, pd.MultiIndex) or 'time' not in df_factors.index.names or 'code' not in df_factors.index.names:
                print("因子数据索引格式不正确，需要包含time和code")
                return df_factors
            
            # 创建额外保存的基础指标列表
            basic_indicators = ['open_0', 'high_0', 'low_0', 'close_0', 'volume_0', 'amount_0']
            basic_mapping = {
                'open_0': 'open',
                'high_0': 'high',
                'low_0': 'low',
                'close_0': 'close',
                'volume_0': 'volume',
                'amount_0': 'amount'
            }
            
            # 对时间索引标准化处理（解决时区问题）
            df_factors = df_factors.copy()
            df_factors.index = df_factors.index.set_levels(
                [pd.DatetimeIndex(df_factors.index.levels[0]).normalize(), 
                 df_factors.index.levels[1]]
            )
            
            # 根据频率给每个时间点分配存储路径组
            def get_time_group(time_obj):
                if 'w' in freq or 'd' in freq:
                    # 按年分组
                    return f"{time_obj.year}"
                elif 'h' in freq or 'm' in freq:
                    # 按年月分组
                    return f"{time_obj.year}{time_obj.month:02d}"
                elif 's' in freq:
                    # 按年月日分组
                    return f"{time_obj.year}{time_obj.month:02d}{time_obj.day:02d}"
                else:
                    return "default"
            
            # 为DataFrame添加分组列
            df_factors['_time_group'] = df_factors.index.get_level_values('time').map(get_time_group)
            
            # 定义保存单个股票因子数据的函数
            def save_stock_factor_group(time_group, group_df, indicators):
                # 按股票代码分组处理
                for code, code_df in group_df.groupby(level='code'):
                    # 获取第一个时间对象来确定目录路径
                    sample_time = code_df.index.get_level_values('time')[0]
                    
                    # 根据频率确定存储路径
                    if 'w' in freq or 'd' in freq:
                        date_path = f"{sample_time.year}"
                    elif 'h' in freq or 'm' in freq:
                        date_path = f"{sample_time.year}/{sample_time.month:02d}"
                    elif 's' in freq:
                        date_path = f"{sample_time.year}/{sample_time.month:02d}/{sample_time.day:02d}"
                    else:
                        print(f"不支持的频率类型: {freq}")
                        continue
                    
                    # 创建目录
                    save_dir = f"{base_path}/{date_path}/{code}"
                    os.makedirs(save_dir, exist_ok=True)
                    
                    # 保存索引（第一次）
                    index_path = f"{save_dir}/index.pkl"
                    if not os.path.exists(index_path):
                        # 创建索引DataFrame
                        index_df = pd.DataFrame(index=code_df.index.get_level_values('time').unique())
                        index_df['code'] = code
                        index_df['time'] = index_df.index
                        index_df.to_pickle(index_path)
                    
                    # 对每个指标处理
                    for indicator in indicators:
                        if indicator in code_df.columns:
                            factor_path = f"{save_dir}/{indicator}.pkl"
                            
                            # 创建该指标在所有时间点的Series
                            factor_series = pd.Series(dtype=float)
                            for time_obj, row in code_df.iterrows():
                                time_obj = time_obj[0]  # 获取时间部分
                                factor_series[time_obj] = row[indicator]
                            
                            # 如果文件已存在，读取并更新
                            if os.path.exists(factor_path):
                                try:
                                    existing_series = pd.read_pickle(factor_path)
                                    # 合并现有数据和新数据
                                    factor_series = pd.concat([existing_series, factor_series]).drop_duplicates()
                                    factor_series.to_pickle(factor_path)
                                except Exception as e:
                                    print(f"更新因子文件失败: {factor_path}, 错误: {str(e)}")
                                    factor_series.to_pickle(factor_path)
                            else:
                                # 创建新文件
                                factor_series.to_pickle(factor_path)
                            
                            # 对基础指标额外保存一份不带后缀的版本
                            if indicator in basic_indicators:
                                base_name = basic_mapping[indicator]
                                base_factor_path = f"{save_dir}/{base_name}.pkl"
                                
                                # 与上面相同的逻辑
                                if os.path.exists(base_factor_path):
                                    try:
                                        existing_series = pd.read_pickle(base_factor_path)
                                        base_factor_series = pd.concat([existing_series, factor_series]).drop_duplicates()
                                        base_factor_series.to_pickle(base_factor_path)
                                    except Exception as e:
                                        print(f"更新基础因子文件失败: {base_factor_path}, 错误: {str(e)}")
                                        factor_series.to_pickle(base_factor_path)
                                else:
                                    factor_series.to_pickle(base_factor_path)
            
            # 按时间分组进行保存
            groups = df_factors.groupby('_time_group')
            
            # 创建任务列表
            tasks = []
            for time_group, group_df in groups:
                # 去掉临时列
                group_df = group_df.drop('_time_group', axis=1)
                # 添加到任务列表
                tasks.append((time_group, group_df, indicator_list))
            
            # 使用线程池并行保存
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                for task in tasks:
                    future = executor.submit(save_stock_factor_group, *task)
                    futures.append(future)
                
                # 等待所有任务完成
                for future in futures:
                    try:
                        future.result()
                    except Exception as e:
                        print(f"保存因子时出错: {str(e)}")
                        traceback.print_exc()
            
            print(f"因子保存完成，共处理 {len(tasks)} 个时间组")
            return df_factors
            
        except Exception as e:
            print(f"保存因子数据失败: {str(e)}")
            traceback.print_exc()
            return df_factors

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


    def computeIndicatorByCode():
        pass