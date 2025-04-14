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


    def getAllIndicatorRelation(indicatorlist, market, freq):
        """
        分析指标之间的依赖关系，构建计算顺序和并行组
        
        参数:
            indicatorlist: 所有需要计算的指标列表
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
        for indicator in indicatorlist:
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
        print("\n基础字段:", base_fields)
        print("\nWILLR和close的生产者:", {k: v for k, v in field_producers.items() if k in ['WILLR', 'close']})
        print("\n计算层级:")
        for i, level in enumerate(calculation_levels):
            print(f"层级 {i}:", level[:5], "..." if len(level) > 5 else "")
        
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
                code=''
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
                            code=line
                        else:
                            code=code+"\n"+line
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
                                all_fields = all_fields_pattern.findall(code)
                                all_fields = list(set(all_fields))  # 去重
                                
                                # 计算引用的字段（所有字段减去返回的字段）
                                referenced_fields = list(set(all_fields) - set(return_fileds))
                                
                                return indicators, function_name, code, return_fileds, referenced_fields
                            else:
                                code=''
                                return_fileds=[]
                         

        print('wrong indicator_name:'+indicator_name)       
        return False, False, False, [], []
