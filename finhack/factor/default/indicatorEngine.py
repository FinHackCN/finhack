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
from finhack.library.db import DB
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
                        # 对于不包含'_'的指标，在尾部添加_0
                        processed_lines = []
                        for line in lines:
                            if '_' not in line and line.strip() not in ['open', 'high', 'low', 'close', 'volume']:
                                processed_lines.append(f"{line}_0")
                            else:
                                processed_lines.append(line)
                        indicator_list.extend(processed_lines)
        for root, dirs, files in os.walk(CONFIG_DIR + f"/factorlist/indicatorlist/{market}/x{freq}/"):
            for file in files:
                if file in task_list.split(','):
                    with open(os.path.join(root, file), 'r', encoding='utf-8') as f:
                        lines = [line.strip() for line in f.readlines()]
                        # 对于不包含'_'的指标，在尾部添加_0
                        processed_lines = []
                        for line in lines:
                            if '_' not in line and line.strip() not in ['open', 'high', 'low', 'close', 'volume']:
                                processed_lines.append(f"{line}_0")
                            else:
                                processed_lines.append(line)
                        indicator_list.extend(processed_lines)
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
                            function_name=function_name[0].strip()
                            indicator_code=line
                        else:
                            indicator_code=indicator_code+"\n"+line
                        left=line.split('=')
                        left=left[0]
                        
                        #pattern = re.compile(r"df\[\'(\w*?)\'\]")   # 查找数字
                        pattern = re.compile(r"\bdf\[\'([A-Za-z0-9_\-]*?)\'\]")  # 查找数字
                        flist = pattern.findall(left)
                        return_fileds=return_fileds+flist

                        if("df['"+indicator_filed+"_") in left or ("df['"+indicator_filed+"']") in left:
                            find=True
                        if 'return ' in  line:
                            if find:
                                # 收集代码中所有出现的字段
                                all_fields_pattern = re.compile(r"\bdf\[\'([A-Za-z0-9_\-]*?)\'\]")
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
            time_ranges = factorManager.timeSplit(start_date, end_date, freq)
            print(f"时间范围拆分为 {len(time_ranges)} 个区间")
            print(time_ranges)
            
            # 遍历时间区间
            for i, (range_start, range_end) in enumerate(time_ranges):
                print(f"处理时间区间: {range_start} - {range_end}")
                
                # 判断当前是否为最后一个time_ranges元素
                is_last_range = (i == len(time_ranges) - 1)
                
                # 如果不是最后一个区间，检查因子是否已存在
                if not is_last_range:
                    should_skip = True
                    for indicator in indicator_list:
                        factor_info = factorManager.inspectFactor(indicator, market=market, freq=freq,start_date=range_start, end_date=range_end,only_exists=True)
                        # print(factor_info)
                        if not factor_info["exists"]:
                            # 只要有一个因子不存在，就不跳过
                            should_skip = False
                            break
                    
                    if should_skip:
                        print(f"时间区间 {range_start} - {range_end} 的因子已存在，跳过计算")
                        continue
                
                # 加载该时间区间的依赖字段数据
                # 根据频率调整起始日期，确保有足够的历史数据用于计算
                adjusted_start_date = factorManager.adjustStartDateByFreq(range_start, freq)
                print(f"原始起始日期: {range_start}, 调整后的起始日期: {adjusted_start_date}")
                
                df_ref = factorManager.loadFactors(
                        matrix_list=referenced_fields,
                        vector_list=[],
                        code_list=code_list,
                        market=market,
                        freq=freq,
                        start_date=adjusted_start_date,  # 使用调整后的起始日期
                        end_date=range_end,
                        cache=True  # 使用缓存加速
                )

                
                #exit()
                    
                if df_ref.empty:
                    if module_type != "mix":
                        print(f"时间区间 {range_start} - {range_end} 无数据")
                        continue
                    if len(df_ref.index)>0:
                        df_ref['placeholder']=0
                print(f"加载数据量: {len(df_ref)}")
                print(df_ref)
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
                        # 检查df_ref是否为MultiIndex
                        if isinstance(df_ref.index, pd.MultiIndex) and 'code' in df_ref.index.names:
                            stocks = df_ref.index.get_level_values('code').unique().tolist()
                        else:
                            # 如果不是MultiIndex或没有code级别，检查是否有code列
                            if 'code' in df_ref.columns:
                                stocks = df_ref['code'].unique().tolist()
                            else:
                                print("警告：无法从df_ref中获取股票代码列表，数据结构不符合预期")
                                print(f"索引名称: {df_ref.index.names}")
                                print(f"列名: {df_ref.columns.tolist()}")
                                stocks = []
                        
                    code_results = []
                    params=indicator.split('_')[:-1]
                    # print("df_ref:")
                    # print(df_ref)

                    for code in stocks:
                        code_df = df_ref.xs(code, level='code')
                        if not code_df.empty:
                            try:
                                # 创建DataFrame的副本而非引用，避免SettingWithCopyWarning
                                code_df_copy = code_df.copy()
                                code_result = func(code_df_copy, params)
                                code_result['code'] = code
                                code_results.append(code_result)
                            except Exception as e:
                                print(f"计算股票 {code} 的时间序列指标失败: {str(e)}")
                        
                    if code_results:
                        result_df = pd.concat(code_results)
                elif module_type == "cs":
                        # 横截面因子，对每个交易日分别计算
                    dates = df_ref.index.get_level_values('time').unique().tolist()
                        
                    date_results = []
                    params=indicator.split('_')[:-1]
                    for date in dates:
                        date_df = df_ref.xs(date, level='time')
                        if not date_df.empty:
                            try:
                                date_result = func(date_df, params)
                                date_result['time'] = date
                                date_results.append(date_result)
                            except Exception as e:
                                print(f"计算日期 {date} 的横截面指标失败: {str(e)}")
                        
                    if date_results:
                        result_df = pd.concat(date_results)
                else:
                    print(f"未知因子类型: {module_type}")
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
                    print(result_df)
                    
                    # 过滤结果，确保只保存在指定日期范围内的数据
                    if not result_df.empty:
                        # 使用当前处理的时间范围进行过滤，而不是整个任务的日期范围
                        current_start_date = range_start
                        current_end_date = range_end
                        
                        # 确保end_date处理
                        if current_end_date == 'now':
                            current_end_date = datetime.datetime.now().strftime("%Y%m%d")
                            
                        try:
                            # 获取结果DataFrame中的时间索引
                            if isinstance(result_df.index, pd.MultiIndex):
                                times = result_df.index.get_level_values('time')
                            else:
                                # 如果不是MultiIndex，检查是否有time列
                                if 'time' in result_df.columns:
                                    times = result_df['time']
                                else:
                                    # 假设整个索引就是时间
                                    times = result_df.index
                            
                            # 确保times是datetime类型
                            if not pd.api.types.is_datetime64_any_dtype(times):
                                try:
                                    # 尝试将times转换为datetime
                                    times = pd.to_datetime(times)
                                except Exception as e:
                                    print(f"无法将时间索引转换为datetime: {str(e)}")
                                    print("跳过日期过滤，使用原始数据")
                                    raise
                            
                            # 统一时区处理
                            # 1. 先判断times是否有时区信息
                            has_tz = False
                            if hasattr(times, 'tz') and times.tz is not None:
                                has_tz = True
                                tz_info = times.tz
                                print(f"检测到时区信息: {tz_info}")
                            
                            # 2. 转换输入的日期为datetime对象
                            start_datetime = pd.to_datetime(current_start_date)
                            end_datetime = pd.to_datetime(current_end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
                            
                            # 3. 根据times的时区情况统一处理
                            if has_tz:
                                # 如果times有时区，将start_datetime和end_datetime本地化为相同时区
                                try:
                                    start_datetime = start_datetime.tz_localize(tz_info)
                                    end_datetime = end_datetime.tz_localize(tz_info)
                                except TypeError:  # 已经有时区信息的情况
                                    start_datetime = start_datetime.tz_convert(tz_info)
                                    end_datetime = end_datetime.tz_convert(tz_info)
                            else:
                                # 如果times没有时区，移除所有时区信息
                                if hasattr(times, 'dt'):  # Series类型
                                    times = times.dt.tz_localize(None)
                                else:  # DatetimeIndex类型
                                    times = times.tz_localize(None)
                                
                                # 确保start_datetime和end_datetime也没有时区信息
                                if hasattr(start_datetime, 'tzinfo') and start_datetime.tzinfo is not None:
                                    start_datetime = start_datetime.tz_localize(None)
                                if hasattr(end_datetime, 'tzinfo') and end_datetime.tzinfo is not None:
                                    end_datetime = end_datetime.tz_localize(None)
                            
                            # 创建日期过滤条件
                            date_mask = (times >= start_datetime) & (times <= end_datetime)
                            
                            # 应用过滤
                            filtered_df = result_df[date_mask]
                            
                            # 如果过滤后结果为空，则记录警告
                            if filtered_df.empty and not result_df.empty:
                                print(f"警告：过滤后没有符合日期范围 {current_start_date} 到 {current_end_date} 的数据")
                                print(f"时间范围信息: start={start_datetime}, end={end_datetime}")
                                print(f"样本时间: {times.iloc[0] if hasattr(times, 'iloc') else times[0]}")
                            else:
                                print(f"日期过滤前数据量: {len(result_df)}, 过滤后数据量: {len(filtered_df)}")
                                result_df = filtered_df
                        except Exception as e:
                            print(f"日期过滤过程出错: {str(e)}")
                            print("跳过日期过滤，使用原始数据")
                            # 添加更详细的错误信息
                            print(f"本批次范围 - 开始日期: {current_start_date}, 结束日期: {current_end_date}")
                            if 'times' in locals():
                                print(f"时间数据类型: {type(times)}")
                                print(f"时区信息: {getattr(times, 'tz', None)}")
                                print(f"样本时间: {times.iloc[0] if hasattr(times, 'iloc') else times[0] if len(times) > 0 else None}")
                            traceback.print_exc()
                    
                    if 'time' in result_df.columns:
                        result_df = result_df.reset_index(drop=True)
                    else:
                        result_df = result_df.reset_index(drop=False)
                    result_df = result_df.sort_values(by=['time', 'code'])
                    result_df = result_df.set_index(['time', 'code'])

                    if save:
                        factorManager.saveFactors(result_df, indicator_list, market, freq)
                    
                    print(f"计算指标组 {indicator_list} 成功，结果数据量: {len(result_df)}")
                    print(result_df)
                    # exit()
        except Exception as e:
            print(f"指标计算过程出现未预期异常: {str(e)}")
            traceback.print_exc()
    


    def computeIndicatorByCode():
        pass