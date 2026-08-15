# -*- coding: utf-8 -*-
"""
通用 sklearn 系 Trainer：一套契约支持多种经典 ML 模型
======================================================

与 LightgbmTrainer 完全同契约：start_train → {'model_id': md5, 'cached': bool}，
pred 落盘为因子 pred_<md5>（回测 get_factors 可读），auto_train 入库（algorithm 列标具体算法）。

支持算法（model_type 传入）：
  xgboost        XGBoost 回归（梯度提升，sklearn API）
  random_forest  随机森林（bagging 树，抗过拟合）
  extra_trees    极端随机树（比 RF 更随机，方差更低）
  ridge          岭回归（L2 线性基线，带标准化）
  lasso          Lasso（L1 稀疏选择，带标准化）
  linear         普通最小二乘（带标准化，最简基线）
  mlp            多层感知机（sklearn MLPRegressor，带标准化）

线性/MLP 走 Pipeline(StandardScaler, model)——因子量纲差异大，不标准化会被大量纲特征主导。
"""
import os
import json
import hashlib
import traceback
import numpy as np
import pandas as pd
import joblib

from runtime.constant import DATA_DIR
from finhack.library.db import DB
from finhack.factor.default.factorManager import factorManager
from finhack.trainer.trainer import Trainer


def _build_model(algorithm, param, loss='mse'):
    """按算法名构造模型。param 中的超参键直接透传给模型构造器；
    loss 在可换损失的模型上生效（xgb objective / RF-ET criterion）。"""

    from sklearn.linear_model import Ridge, Lasso, LinearRegression
    from sklearn.ensemble import RandomForestRegressor, ExtraTreesRegressor
    from sklearn.neural_network import MLPRegressor
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler

    p = dict(param or {})

    def _pop_int(k, d):
        try:
            return int(p.pop(k, d))
        except Exception:
            return d

    def _pop_f(k, d):
        try:
            return float(p.pop(k, d))
        except Exception:
            return d

    if algorithm == 'xgboost':
        from xgboost import XGBRegressor
        # 损失映射：mse/mae/huber/quantile（quantile_alpha 用 loss_alpha，huber 用 huber_sleeve 默认）
        xgb_obj = {'mse': 'reg:squarederror', 'mae': 'reg:absoluteerror',
                   'huber': 'reg:pseudohubererror', 'quantile': 'reg:quantileerror'}.get(loss, 'reg:squarederror')
        kw = {'objective': xgb_obj}
        if xgb_obj == 'reg:quantileerror':
            kw['quantile_alpha'] = _pop_f('loss_alpha', 0.9)
        return XGBRegressor(
            n_estimators=_pop_int('n_estimators', 300),
            max_depth=_pop_int('max_depth', 6),
            learning_rate=_pop_f('learning_rate', 0.05),
            subsample=_pop_f('subsample', 0.9),
            colsample_bytree=_pop_f('colsample_bytree', 0.9),
            reg_lambda=_pop_f('reg_lambda', 1.0),
            min_child_weight=_pop_int('min_child_weight', 5),
            n_jobs=16, random_state=42, tree_method='hist',
            early_stopping_rounds=30,
            **kw, **{k: v for k, v in p.items()}), 'xgb'
    # RF/ET 分裂准则可换：mse(squared_error) / mae(absolute_error，更慢但抗离群)
    crit = 'absolute_error' if loss == 'mae' else 'squared_error'
    if algorithm == 'random_forest':
        return RandomForestRegressor(
            n_estimators=_pop_int('n_estimators', 300),
            max_depth=_pop_int('max_depth', 12),
            min_samples_leaf=_pop_int('min_samples_leaf', 20),
            max_features=_pop_f('max_features', 0.5),
            criterion=crit, n_jobs=16, random_state=42,
            **{k: v for k, v in p.items()}), 'rf'
    if algorithm == 'extra_trees':
        return ExtraTreesRegressor(
            n_estimators=_pop_int('n_estimators', 300),
            max_depth=_pop_int('max_depth', 14),
            min_samples_leaf=_pop_int('min_samples_leaf', 20),
            max_features=_pop_f('max_features', 0.5),
            criterion=crit, n_jobs=16, random_state=42,
            **{k: v for k, v in p.items()}), 'et'
    if algorithm == 'ridge':
        return Pipeline([('sc', StandardScaler()),
                         ('m', Ridge(alpha=_pop_f('alpha', 1.0)))]), 'ridge'
    if algorithm == 'lasso':
        return Pipeline([('sc', StandardScaler()),
                         ('m', Lasso(alpha=_pop_f('alpha', 0.001), max_iter=5000,
                                     selection='random'))]), 'lasso'
    if algorithm == 'linear':
        return Pipeline([('sc', StandardScaler()),
                         ('m', LinearRegression())]), 'linear'
    if algorithm == 'mlp':
        hidden = p.pop('hidden_layers', '64,32')
        try:
            layers = tuple(int(x) for x in str(hidden).split(',') if x.strip())
        except Exception:
            layers = (64, 32)
        return Pipeline([('sc', StandardScaler()),
                         ('m', MLPRegressor(hidden_layer_sizes=layers,
                                            alpha=_pop_f('alpha', 1e-4),
                                            learning_rate_init=_pop_f('learning_rate', 1e-3),
                                            max_iter=_pop_int('max_iter', 50),
                                            early_stopping=True, n_iter_no_change=5,
                                            batch_size=1024, random_state=42))]), 'mlp'
    raise ValueError(f'未知算法 {algorithm}')


class SklearnTrainer(Trainer):

    def start_train(self, market='cn_stock', freq='1d', start_date='20000101', valid_date="20080101",
                    end_date='20100101', matrix_list=[], vector_list=[], label='abs', shift=10,
                    param={}, loss='mse', filter_name='', replace=False, algorithm='ridge'):
        """algorithm 由 functional.train 按 model_type 传入。返回 {'model_id','cached'}。"""
        print(f"start train [{algorithm}]: loss={loss}")
        param_str = json.dumps(param, sort_keys=True) if isinstance(param, dict) else str(param)
        # hash 含算法名：同参数不同算法 = 不同模型
        hashstr = f"{market}-{freq}" + start_date + "-" + valid_date + "-" + end_date + "-" + \
                  ",".join(matrix_list) + ",".join(vector_list) + "," + label + "," + str(shift) + \
                  f",[{algorithm}]," + param_str + "," + str(loss) + filter_name
        md5 = hashlib.md5(hashstr.encode('utf-8')).hexdigest()

        has = DB.select_to_df('select * from auto_train where hash="%s"' % md5, 'finhack')
        if has is not None and not has.empty and not replace:
            print(f"命中已有模型 {md5}（参数未变，跳过训练；需重训传 replace=True）")
            return {'model_id': md5, 'cached': True}

        x_train, y_train, x_valid, y_valid, df_pred, data_path = self.getTrainData(
            market=market, freq=freq, start_date=start_date, valid_date=valid_date,
            end_date=end_date, matrix_list=matrix_list, vector_list=vector_list,
            label=label, shift=shift, filter_name=filter_name)
        if x_train is None:
            raise ValueError('训练数据为空：检查特征因子名/日期区间/市场数据是否存在')

        self.train(x_train, y_train, x_valid, y_valid, data_path, md5, algorithm, param, loss)
        self.pred(df_pred, data_path, md5, market, freq, algorithm, save=True)

        features = ','.join(matrix_list + vector_list)
        insert_sql = ("INSERT INTO auto_train (start_date, valid_date, end_date, features, label, "
                      "shift, param, hash, loss, algorithm, filter, market, freq) "
                      "VALUES ('%s','%s','%s','%s','%s',%s,'%s','%s','%s','%s','%s','%s','%s')"
                      % (start_date, valid_date, end_date, features, label, str(shift),
                         str(param).replace("'", '"'), md5, loss, algorithm, filter_name, market, freq))
        try:
            if has is None or has.empty:
                DB.exec(insert_sql, 'finhack')
            else:
                DB.exec("DELETE FROM auto_train WHERE hash='%s'" % md5, 'finhack')
                DB.exec(insert_sql, 'finhack')
        except Exception as ex:
            print("auto_train 入库降级: " + str(ex))
        self.score(md5, market, freq, algorithm)
        return {'model_id': md5, 'cached': False}

    def train(self, x_train, y_train, x_valid, y_valid, data_path=DATA_DIR, md5='test',
              algorithm='ridge', param={}, loss='mse'):
        for c in ('code', 'time'):
            if c in x_train.columns:
                x_train = x_train.drop(c, axis=1)
            if x_valid is not None and c in getattr(x_valid, 'columns', []):
                x_valid = x_valid.drop(c, axis=1)
        x_train = x_train.select_dtypes(include=[np.number]).replace([np.inf, -np.inf], np.nan).fillna(0)
        model, tag = _build_model(algorithm, param, loss)
        t0 = __import__('time').time()
        if algorithm == 'xgboost' and x_valid is not None:
            x_valid = x_valid.select_dtypes(include=[np.number]).replace([np.inf, -np.inf], np.nan).fillna(0)
            model.fit(x_train, y_train, eval_set=[(x_valid, y_valid)], verbose=False)
        else:
            model.fit(x_train, y_train)
        print(f"[{algorithm}] 训练完成 ({__import__('time').time()-t0:.1f}s)")
        model_file = data_path + f'/models/ml_{algorithm}_' + md5 + '.pkl'
        os.makedirs(os.path.dirname(model_file), exist_ok=True)
        joblib.dump(model, model_file)
        print('Saved model: ' + model_file)

    def pred(self, df_pred, data_path=DATA_DIR, md5='test', market='cn_stock', freq='1d',
             algorithm='ridge', save=False):
        if df_pred is None or df_pred.empty:
            return pd.DataFrame()
        df = df_pred.copy()
        for c in ('label', 'close', 'open'):
            if c in df.columns:
                df = df.drop(c, axis=1)
        model_file = data_path + f'/models/ml_{algorithm}_' + md5 + '.pkl'
        model = joblib.load(model_file)
        meta = df[['code', 'time']].copy()
        x = df.select_dtypes(include=[np.number]).replace([np.inf, -np.inf], np.nan).fillna(0)
        x = x.drop(['time_str'], axis=1, errors='ignore')
        meta['pred'] = model.predict(x)
        # T 日预测用于 T+1 操作：按 code shift(1)（与 lgb 版一致）
        meta = meta.sort_values(['time', 'code'])
        meta['pred'] = meta.groupby('code')['pred'].shift(1)
        meta = meta.dropna(subset=['pred'])
        if save and not meta.empty:
            os.makedirs(data_path + '/preds', exist_ok=True)
            meta.to_pickle(data_path + f'/preds/ml_{algorithm}_' + md5 + '_pred.pkl')
            pname = f'pred_{md5}'
            pdf = meta.set_index(['time', 'code'])[['pred']].rename(columns={'pred': pname})
            factorManager.saveFactors(pdf, [pname], market, freq)
            print(f"pred 入库为因子: {pname} ({len(pdf)} 行)")
        return meta

    def score(self, md5='test', market='cn_stock', freq='1d', algorithm='ridge'):
        """score = pred 与未来 shift 日收益的日均横截面 spearman IC（与 lgb 版同语义）。"""
        pred_file = DATA_DIR + f'/preds/ml_{algorithm}_' + md5 + '_pred.pkl'
        if not os.path.exists(pred_file):
            print(pred_file + " not found!")
            return False
        df_preded = pd.read_pickle(pred_file).set_index(['time', 'code'])
        model = DB.select_to_df('select * from auto_train where hash="' + md5 + '"', 'finhack')
        if model is None or model.empty:
            return False
        shift = int(model.iloc[0]['shift'])
        end_date = str(model.iloc[0]['end_date'] or '')
        s_date = end_date if len(end_date) == 8 else '20200101'
        e_year = int(s_date[:4]) + 3
        e_date = str(e_year) + s_date[4:]
        base = factorManager.loadFactors(matrix_list=['open', 'close'], market=market, freq=freq,
                                         start_date=s_date, end_date=e_date)
        if base is None or base.empty:
            print("score: open/close 加载为空（区间 %s~%s）" % (s_date, e_date))
            return False
        df = df_preded.join(base[['open', 'close']], how='left')
        df['label'] = df.groupby('code', group_keys=False).apply(
            lambda x: x['close'].shift(-shift) / x['open'].shift(-1))
        df = df.replace({'label': [np.inf, -np.inf]}, np.nan).dropna(subset=['pred', 'label'])
        if df.empty:
            return False
        from scipy.stats import spearmanr

        def _daily_ic(g):
            if len(g) < 10:
                return np.nan
            return spearmanr(g['pred'], g['label'])[0]
        ic = df.groupby(level='time').apply(_daily_ic).dropna()
        score = float(ic.mean()) if len(ic) else 0.0
        try:
            DB.exec("UPDATE auto_train SET score=%s WHERE hash='%s'" % (round(score, 6), md5), 'finhack')
            print(f"score(IC)={score:.4f} ({len(ic)} 个交易日)")
        except Exception as ex:
            print("score 入库降级: " + str(ex))
        os.remove(pred_file)
        return score


def _start_train_wrapper(algorithm, market, freq, start_date, valid_date, end_date,
                         matrix_list, vector_list, label, shift, param, loss, filter_name, replace):
    try:
        SklearnTrainer().start_train(
            algorithm=algorithm, market=market, freq=freq, start_date=start_date,
            valid_date=valid_date, end_date=end_date, matrix_list=matrix_list,
            vector_list=vector_list, label=label, shift=shift, param=param,
            loss=loss, filter_name=filter_name, replace=replace)
    except Exception as ex:
        print(f"train wrapper error: {ex}")
        traceback.print_exc()
