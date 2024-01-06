# coding=utf-8
import pandas as pd
import sys
import datetime
import os
import traceback
import numpy as np
import hashlib
import time
import threading
# 股票信息获取模块
from datetime import timedelta
from runtime.constant import *
from finhack.library.mydb import mydb
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor, wait, ALL_COMPLETED


import importlib
from types import FunctionType

class AStockMeta(type):
    def __new__(cls, name, bases, dct):
        # 创建新类时动态添加方法
        def create_dynamic_method(method_name):
            @staticmethod
            def dynamic_method(*args, **kwargs):
                # 动态导入对应的模块
                source="tushare"
                module_name = f"finhack.market.astock.{source}.astock"
                module = importlib.import_module(module_name)
                # 获取模块中的函数并调用
                func = getattr(module, method_name)
                return func(*args, **kwargs)
            return dynamic_method

        # 假设我们知道所有可能的方法名称列表
        # 或者可以从某处动态获取它们
        method_names = ['getStockCodeList', 'getStockIndustry', 'getIndexMember', 'getIndexPrice', 'getTableDataByCode', 'getTableData', 'getStockDailyPrice', 'checkLimit', 'getStockDailyPriceByCode', 'alignStockFactors']  # 示例方法列表
        for method_name in method_names:
            # 为每个方法名创建一个动态方法并添加到类字典中
            dct[method_name] = create_dynamic_method(method_name)

        return type.__new__(cls, name, bases, dct)

class AStock(metaclass=AStockMeta):
    pass
