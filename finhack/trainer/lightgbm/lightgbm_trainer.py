import pandas as pd
import traceback
import hashlib
import lightgbm as lgb
import json
from math import e
import numpy as np
from runtime.constant import *
from finhack.library.db import DB
import os
import runtime.global_var as global_var
from finhack.factor.default.factorManager import factorManager
from finhack.trainer.trainer import Trainer


class LightgbmTrainer(Trainer):

    def auto(self):
        args = global_var.args
        max_processes = int(args.process)
        from multiprocessing import Process, current_process
        processes = []
        while True:
            processes = [p for p in processes if p.is_alive()]
            if len(processes) < max_processes:
                try:
                    flist = factorManager.list_factors(market=getattr(args, 'market', 'cn_stock'),
                                                       freq=getattr(args, 'freq', '1d'))
                    import random
                    random.shuffle(flist)
                    n = random.randint(int(args.min_f), int(args.max_f))
                    factor_list = [flist.pop() for _ in range(n)]
                    df = factorManager.loadFactors(matrix_list=factor_list + ['open', 'close'],
                                                    market=getattr(args, 'market', 'cn_stock'),
                                                    freq=getattr(args, 'freq', '1d'))
                    corr = df.corr(numeric_only=True)
                    new_factor_list = []
                    for f in factor_list:
                        if f in ['open', 'close']:
                            continue
                        if all(abs(corr[f][f2]) <= 0.7 for f2 in new_factor_list):
                            new_factor_list.append(f)
                    p = Process(target=_start_train_wrapper, args=(
                        getattr(args, 'market', 'cn_stock'), getattr(args, 'freq', '1d'),
                        args.start_date, args.valid_date, args.end_date,
                        new_factor_list, [], args.label, int(args.shift),
                        json.loads(args.param) if getattr(args, 'param', '') else {},
                        args.loss, getattr(args, 'filter_name', ''), getattr(args, 'replace', False)))
                    p.start()
                    processes.append(p)
                except Exception as ex:
                    print("auto error: " + str(ex))
                    traceback.print_exc()
            else:
                for p in processes:
                    p.join(timeout=0.1)
                    if not p.is_alive():
                        break

    def run(self):
        global_var.args = self.args
        args = self.args
        return self.start_train(
            market=getattr(args, 'market', 'cn_stock'),
            freq=getattr(args, 'freq', '1d'),
            start_date=args.start_date, valid_date=args.valid_date, end_date=args.end_date,
            matrix_list=args.matrix_list.split(',') if getattr(args, 'matrix_list', '') else [],
            vector_list=args.vector_list.split(',') if getattr(args, 'vector_list', '') else [],
            label=getattr(args, 'label', 'abs'), shift=int(args.shift),
            param=json.loads(args.param) if getattr(args, 'param', '') else {},
            loss=getattr(args, 'loss', 'ds'), filter_name=getattr(args, 'filter_name', ''),
            replace=getattr(args, 'replace', False))

    def start_train(self, market='cn_stock', freq='1d', start_date='20000101', valid_date="20080101",
                    end_date='20100101', matrix_list=[], vector_list=[], label='abs', shift=10,
                    param={}, loss='ds', filter_name='', replace=False):
        print("start train:loss=%s" % loss)
        try:
            hashstr = f"{market}-{freq}" + start_date + "-" + valid_date + "-" + end_date + "-" + \
                      ",".join(matrix_list) + ",".join(vector_list) + "," + label + "," + str(shift) + \
                      "," + str(param) + "," + str(loss) + filter_name
            md5 = hashlib.md5(hashstr.encode('utf-8')).hexdigest()

            has = DB.select_to_df('select * from auto_train where hash="%s"' % md5, 'finhack')
            if has is not None and not has.empty and not replace:
                return md5

            data_train, data_valid, df_pred, data_path = self.getLGBTrainData(
                market=market, freq=freq, start_date=start_date, valid_date=valid_date,
                end_date=end_date, matrix_list=matrix_list, vector_list=vector_list,
                label=label, shift=shift, filter_name=filter_name)
            if data_train is None:
                print("训练数据为空")
                return None

            self.train(data_train, data_valid, data_path, md5, loss, param)
            self.pred(df_pred, data_path, md5, market, freq, save=True)

            features = ','.join(matrix_list + vector_list)
            insert_sql = ("INSERT INTO auto_train (start_date, valid_date, end_date, features, label, "
                          "shift, param, hash, loss, algorithm, filter) VALUES ('%s','%s','%s','%s','%s',%s,'%s','%s','%s','%s','%s')"
                          % (start_date, valid_date, end_date, features, label, str(shift),
                             str(param).replace("'", '"'), md5, loss, 'lgb', filter_name))
            try:
                if has is None or has.empty:
                    DB.exec(insert_sql, 'finhack')
            except Exception as ex:
                print("auto_train 入库降级: " + str(ex))
            self.score(md5, market, freq)
            return md5
        except Exception as ex:
            print("start_train error: " + str(ex))
            traceback.print_exc()

    def getLGBTrainData(self, market='cn_stock', freq='1d', start_date='20000101', valid_date="20080101",
                        end_date='20100101', matrix_list=[], vector_list=[], label='abs', shift=10, filter_name=''):
        x_train, y_train, x_valid, y_valid, df_pred, data_path = self.getTrainData(
            market, freq, start_date, valid_date, end_date, matrix_list, vector_list, label, shift, filter_name)
        if x_train is None:
            return None, None, None, None
        for c in ('code', 'time'):
            if c in x_train.columns:
                x_train = x_train.drop(c, axis=1)
            if c in x_valid.columns:
                x_valid = x_valid.drop(c, axis=1)
        data_train = lgb.Dataset(x_train, y_train)
        data_valid = lgb.Dataset(x_valid, y_valid)
        return data_train, data_valid, df_pred, data_path

    def custom_obj(self, y_pred, dataset):
        y_true = dataset.get_label()
        grad = 100 * e ** (y_pred - y_true) - 100
        hess = 100 * e ** (y_pred - y_true)
        return grad, hess

    def custom_eval(self, y_pred, dataset):
        y_true = dataset.get_label()
        loss = 100 * e ** (-(y_true - y_pred)) + 100 * (y_true - y_pred) - 100
        return "ds", np.mean(loss), False

    def train(self, data_train, data_valid, data_path=DATA_DIR, md5='test', loss="ds", param={}):
        args = global_var.args
        params = {
            'boosting_type': 'gbdt', 'max_depth': 7, 'num_leaves': 64,
            'learning_rate': 0.1, 'feature_fraction': 0.9, 'bagging_fraction': 0.9,
            'bagging_freq': 5, 'verbose': -1, 'verbosity': -1,
            'min_child_samples': 20,
            'device': getattr(args, 'device', 'cpu'),
        }
        params.update(param)
        # 从 param 提取非 lgb-params 的训练控制参数
        num_boost_round = int(params.pop('n_estimators', 100))
        early_stopping_rounds = int(params.pop('early_stopping', 30))
        callbacks = [lgb.early_stopping(early_stopping_rounds, verbose=0), lgb.log_evaluation(period=0)]
        if loss == "ds":
            params['objective'] = self.custom_obj
            gbm = lgb.train(params, data_train, num_boost_round=num_boost_round, valid_sets=data_valid,
                            callbacks=callbacks, feval=self.custom_eval)
        else:
            gbm = lgb.train(params, data_train, num_boost_round=num_boost_round, valid_sets=data_valid,
                            callbacks=callbacks)
        model_file = data_path + '/models/lgb_model_' + md5 + '.txt'
        os.makedirs(os.path.dirname(model_file), exist_ok=True)
        gbm.save_model(model_file)
        print('Saved model: ' + model_file)

    def pred(self, df_pred, data_path=DATA_DIR, md5='test', market='cn_stock', freq='1d', save=False):
        if df_pred is None or df_pred.empty:
            return pd.DataFrame()
        df = df_pred.copy()
        if 'label' in df.columns:
            df = df.drop('label', axis=1)
        for c in ('close', 'open'):
            if c in df.columns:
                df = df.drop(c, axis=1)
        gbm = lgb.Booster(model_file=data_path + '/models/lgb_model_' + md5 + '.txt')
        meta = df[['code', 'time']].copy()
        x = df.drop(['code', 'time', 'time_str', 'label'], axis=1, errors='ignore')
        x = x.select_dtypes(include=[np.number])
        meta['pred'] = gbm.predict(x, num_iteration=gbm.best_iteration)
        # T 日预测用于 T+1 操作：按 code shift(1)
        meta = meta.sort_values(['time', 'code'])
        meta['pred'] = meta.groupby('code')['pred'].shift(1)
        meta = meta.dropna(subset=['pred'])
        if save and not meta.empty:
            os.makedirs(data_path + '/preds', exist_ok=True)
            meta.to_pickle(data_path + '/preds/lgb_model_' + md5 + '_pred.pkl')
            # 入库为因子 pred_<md5>（回测 get_factors 读取，统一走 DataInterface）
            pname = f'pred_{md5}'
            pdf = meta.set_index(['time', 'code'])[['pred']].rename(columns={'pred': pname})
            factorManager.saveFactors(pdf, [pname], market, freq)
            print(f"pred 入库为因子: {pname} ({len(pdf)} 行)")
        return meta

    def score(self, md5='test', market='cn_stock', freq='1d'):
        pred_file = DATA_DIR + '/preds/lgb_model_' + md5 + '_pred.pkl'
        if not os.path.exists(pred_file):
            print(pred_file + " not found!")
            return False
        df_preded = pd.read_pickle(pred_file).set_index(['time', 'code'])
        base = factorManager.loadFactors(matrix_list=['open', 'close'], market=market, freq=freq)
        if base is None or base.empty:
            return False
        df = df_preded.join(base[['open', 'close']], how='left')
        model = DB.select_to_df('select * from auto_train where hash="' + md5 + '"', 'finhack')
        if model is None or model.empty:
            return False
        shift = int(model.iloc[0]['shift'])
        df['label'] = df.groupby('code', group_keys=False).apply(
            lambda x: x['close'].shift(-shift) / x['open'].shift(-1))
        df = df.dropna(subset=['pred', 'label'])
        if df.empty:
            return False
        mean_diff = (df['label'] - df['pred']).mean()
        std = (df['label'] - df['pred']).std()
        score = mean_diff / std if std else 0
        try:
            DB.exec("UPDATE auto_train SET score=%s WHERE hash='%s'" % (score, md5), 'finhack')
        except Exception as ex:
            print("score 入库降级: " + str(ex))
        os.remove(pred_file)
        return score


def _start_train_wrapper(market, freq, start_date, valid_date, end_date, matrix_list, vector_list,
                         label, shift, param, loss, filter_name, replace):
    try:
        LightgbmTrainer().start_train(
            market=market, freq=freq, start_date=start_date, valid_date=valid_date, end_date=end_date,
            matrix_list=matrix_list, vector_list=vector_list, label=label, shift=shift,
            param=param, loss=loss, filter_name=filter_name, replace=replace)
    except Exception as ex:
        print(f"train wrapper error: {ex}")
        traceback.print_exc()
