import pandas as pd
from finhack.factor.default.factorManager import factorManager
from math import e
import traceback
import os
import importlib
from runtime.constant import *
from finhack.library.db import DB
import numpy as np
from datetime import datetime


class Trainer:
    """训练器基类：提供训练/预测数据准备。market-aware，索引 (time, code)。"""

    @staticmethod
    def getPredData(market, freq, model_id, start_date, end_date, norm=False):
        """按 model_id 从 auto_train 表取特征列表，loadFactors 加载预测数据。"""
        model_info = DB.select_to_df(f"SELECT * FROM `auto_train` WHERE `hash`='{model_id}'", 'finhack')
        if model_info is None or model_info.empty:
            print("model error:", model_id)
            return pd.DataFrame()
        row = model_info.iloc[0]
        # auto_train.features 列存 "f1,f2,..."（= matrix_list + vector_list 合并）
        features = [f for f in str(row.get('features', '')).split(',') if f]
        df = factorManager.loadFactors(market=market, freq=freq, matrix_list=features,
                                        start_date=start_date, end_date=end_date)
        if df is None or df.empty:
            return pd.DataFrame()
        df = df.reset_index().ffill().fillna(0)
        if norm and features:
            g = df.groupby('time')[features]
            df[features] = (df[features] - g.transform('min')) / (g.transform('max') - g.transform('min'))
        return df

    def getTrainData(self, market='cn_stock', freq='1d', start_date='20000101', valid_date="20080101",
                     end_date='20100101', matrix_list=[], vector_list=[], label='abs', shift=10,
                     filter_name='', dropna=False, norm=False, pred_date=""):
        data_path = DATA_DIR
        if pred_date == "":
            end_year = int(end_date[:4]) + 3
            pred_date = str(end_year) + end_date[4:]
        df = factorManager.loadFactors(market=market, freq=freq,
                                        matrix_list=matrix_list + ['open', 'close', 'high', 'low'],
                                        vector_list=vector_list, start_date=start_date, end_date=pred_date)
        if df is None or df.empty:
            return None, None, None, None, pd.DataFrame(), data_path
        df = df.reset_index().sort_values('time').ffill().fillna(0)

        # 过滤器（项目级 strategies/filters.py）
        if filter_name != '':
            try:
                filters_module = importlib.import_module('strategies.filters')
                func_filter = getattr(filters_module.filters, filter_name)
                df = func_filter(df)
            except Exception as e:
                print(f"filter {filter_name} 加载失败(跳过): {e}")

        # 标签 Y：未来收益
        if label == 'abs':
            df['label'] = df.groupby('code', group_keys=False).apply(
                lambda x: x['close'].shift(-1 * shift) / x['open'].shift(-1))
        elif label == 'mv':
            from finhack.factor.default.alphaEngine import alphaEngine
            formula = ("mean(shift($close,%s),%s)/mean(shift($close,%s),%s)/shift($open,1)+1"
                       % (str(-1 * shift), str(shift - 1), str(-1 * shift), str(shift - 1)))
            df_tmp = df.set_index(['time', 'code'])[['open', 'close']]
            df['label'] = alphaEngine.calc(formula=formula, df=df_tmp, market=market, freq=freq)

        df = df[df['high'] != df['low']]  # 剔除一字板
        if 'high' not in matrix_list and 'high' in df.columns:
            df = df.drop('high', axis=1)
        if 'low' not in matrix_list and 'low' in df.columns:
            df = df.drop('low', axis=1)

        # 时间切分（train/valid/pred）
        df['time_str'] = df['time'].dt.strftime('%Y%m%d') if pd.api.types.is_datetime64_any_dtype(df['time']) else df['time'].astype(str)
        df_train = df[(df.time_str >= start_date) & (df.time_str < valid_date)]
        df_valid = df[(df.time_str >= valid_date) & (df.time_str < end_date)]
        df_pred = df[df.time_str >= end_date]

        if dropna:
            df_train = df_train.replace([np.inf, -np.inf], np.nan).dropna()
            df_valid = df_valid.replace([np.inf, -np.inf], np.nan).dropna()
        if norm and matrix_list:
            g = df_valid.groupby('time')[matrix_list]
            df_valid[matrix_list] = (df_valid[matrix_list] - g.transform('min')) / (g.transform('max') - g.transform('min'))

        def split_xy(d):
            y = d['label']
            x = d.drop(['label', 'close', 'open', 'time_str'], axis=1, errors='ignore')
            return x, y

        x_train, y_train = split_xy(df_train)
        x_valid, y_valid = split_xy(df_valid)
        return x_train, y_train, x_valid, y_valid, df_pred, data_path
