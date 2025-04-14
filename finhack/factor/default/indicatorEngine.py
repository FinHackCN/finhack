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


    def getAllIndicatorRelation(indicatorlist,market,freq):
        pass


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
