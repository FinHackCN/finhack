from __future__ import division
from __future__ import print_function

import sys
import os
import time
import random
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from factors.indicatorCompute import indicatorCompute
from library.mydb import mydb
from library.config import config
from library.globalvar import *
import pandas as pd
import pickle
import inspect


from collections import defaultdict
import gc
import numpy as np
from typing import Callable, Optional, Text, Union
from sklearn.metrics import roc_auc_score, mean_squared_error
import torch
import torch.nn as nn
import torch.optim as optim
from torch.nn import DataParallel
import traceback
import hashlib
from strategies.filters import filters
from train.trainhelper import trainhelper

class obj(object):
    def __init__(self, d):
        for a, b in d.items():
            if isinstance(b, (list, tuple)):
               setattr(self, a, [obj(x) if isinstance(x, dict) else x for x in b])
            else:
               setattr(self, a, obj(b) if isinstance(b, dict) else b)
               
               
class nntrain:


    def __call__(self, *args, **kwargs) -> object:
        """leverage Python syntactic sugar to make the models' behaviors like functions"""
        return self.predict(*args, **kwargs)



    def run(start_date='20000101',valid_date="20080101",end_date='20100101',features=[],label='abs',shift=10,param={},loss='ds',filter_name='',replace=False):
        print("start nn_train:loss")
        try:
            hashstr=start_date+"-"+valid_date+"-"+end_date+"-"+",".join(features)+","+label+","+str(shift)+","+str(param)+","+str(loss)+filter_name
            md5=hashlib.md5(hashstr.encode(encoding='utf-8')).hexdigest()


            hassql='select * from auto_train where hash="%s"' % (md5)
            has=mydb.selectToDf(hassql,'finhack')

            #有值且不替换
            if(not has.empty):  
                if replace==False:  
                    return md5
                    
            x_train,y_train,x_valid,y_valid,df_pred,data_path=trainhelper.getTrainData(start_date=start_date,valid_date=valid_date,end_date=end_date,features=features,label=label,shift=shift,filter_name='',dropna=True)

            
            params=nntrain.fit(x_train,y_train,x_valid,y_valid,data_path=data_path)
            pred=nntrain.predict(params,df_pred,data_path)
            print(pred)
            # #nntrain.pred(df_pred,data_path,md5)
            
            # insert_sql="INSERT INTO auto_train (start_date, valid_date, end_date, features, label, shift, param, hash,loss,algorithm,filter) VALUES ('%s', '%s', '%s', '%s', '%s', %s, '%s', '%s','%s','%s','%s')" % (start_date,valid_date,end_date,','.join(features),label,str(shift),str(param).replace("'",'"'),md5,loss,'lgb',filter_name)
            # if(has.empty): 
            #     mydb.exec(insert_sql,'finhack')            
            
            # return md5
        except Exception as e:
            print("error:"+str(e))
            print("err exception is %s" % traceback.format_exc())


  


    #data_train,data_valid,data_path='/tmp',md5='test',loss="ds",param={}
    def fit(
        x_train,y_train,x_valid,y_valid,data_path,
        evals_result=dict(),
        verbose=True,
        save_path=None,
        reweighter=None
    ):
        
 
        
        params={
            "lr":0.001,
            "loss":"mse",
            "max_steps":300,
            "batch_size":2000,
            "early_stop_rounds":50,
            "eval_steps":20,
            "lr_decay":0.96,
            "lr_decay_steps":100,
            "optimizer":"gd",
            "loss":"mse",
            "GPU":0,
            "seed":None,
            "weight_decay":0.0, 
            "data_parall":False,
            "scheduler":  "default", 
            "init_model":None,
            "eval_train_metric":False,
            "pt_model_uri":"train.nntrain.Net",
            "pt_model_kwargs":{
                "input_dim": len(x_train.columns),
                "layers": (256,),
            }            
        }
        
        params=obj(params)
  
        if params.GPU>1:
            params.device = torch.device(params.GPU)
        else:
            params.device = torch.device("cuda:%d" % (params.GPU) if torch.cuda.is_available() and params.GPU >= 0 else "cpu")    
        
        if params.seed is not None:
            np.random.seed(params.seed)
            torch.manual_seed(params.seed)

        if params.loss not in {"mse", "binary"}:
            raise NotImplementedError("loss {} is not supported!".format(params.loss))
        params._scorer = mean_squared_error if params.loss == "mse" else roc_auc_score

        if params.init_model is None:
            params.dnn_model =  Net(input_dim=params.pt_model_kwargs.input_dim,  layers=params.pt_model_kwargs.layers)

            if params.data_parall:
                params.dnn_model = DataParallel(params.dnn_model).to(params.device)
        else:
            params.dnn_model = init_model

        print("model:\n{:}".format(params.dnn_model))

        if params.optimizer.lower() == "adam":
            params.train_optimizer = optim.Adam(params.dnn_model.parameters(), lr=params.lr, weight_decay=params.weight_decay)
        elif params.optimizer.lower() == "gd":
            params.train_optimizer = optim.SGD(params.dnn_model.parameters(), lr=params.lr, weight_decay=params.weight_decay)
        else:
            raise NotImplementedError("optimizer {} is not supported!".format(optimizer))

        if params.scheduler == "default":
            # Reduce learning rate when loss has stopped decrease
            params.scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
                params.train_optimizer,
                mode="min",
                factor=0.5,
                patience=10,
                verbose=True,
                threshold=0.0001,
                threshold_mode="rel",
                cooldown=0,
                min_lr=0.00001,
                eps=1e-08,
            )
        elif scheduler is None:
            params.scheduler = None
        else:
            params.scheduler = scheduler(optimizer=params.train_optimizer)

        params.fitted = False
        params.dnn_model.to(params.device)        
        
        
        
      
            
        
        all_df={
            'x':{},
            'y':{},
            'w':{}
        }
        
        all_t={
            'x':{},
            'y':{},
            'w':{}
        }
        
        all_df["x"]['train']=x_train
        all_df["y"]['train']=y_train
        all_df["x"]['valid']=x_valid
        all_df["y"]['valid']=y_valid

        hashstr="test"
        md5=hashlib.md5(hashstr.encode(encoding='utf-8')).hexdigest()
        save_path=data_path+'/models/nn_model_'+md5+'.txt'
        
        has_valid = True
        segments = ["train", "valid"]
        vars = ["x", "y", "w"]
        #all_t = defaultdict(dict)  # tensors
        for seg in segments:
            all_df["w"][seg] = pd.DataFrame(np.ones_like(all_df["y"][seg].values), index=all_df["y"][seg].index)
            for v in vars:
                all_t[v][seg] = torch.from_numpy(all_df[v][seg].values).float()
                all_t[v][seg] = all_t[v][seg].to(params.device)  # This will consume a lot of memory !!!!
            evals_result[seg] = []
        gc.collect()


 
        stop_steps = 0
        train_loss = 0
        best_loss = np.inf
        # train
        fitted = True
        # return
        # prepare training data
        train_num = all_t["y"]["train"].shape[0]

        for step in range(1, params.max_steps + 1):
            if stop_steps >= params.early_stop_rounds:
                if verbose:
                    print("\tearly stop")
                break
            loss = AverageMeter()
            params.dnn_model.train()
            params.train_optimizer.zero_grad()
            choice = np.random.choice(train_num, params.batch_size)
            x_batch_auto = all_t["x"]["train"][choice].to(params.device)
            y_batch_auto = all_t["y"]["train"][choice].to(params.device)
            w_batch_auto = all_t["w"]["train"][choice].to(params.device)

            # forward
            preds = params.dnn_model(x_batch_auto)
            cur_loss = nntrain.get_loss(preds, w_batch_auto, y_batch_auto, params.loss)
            cur_loss.backward()
            params.train_optimizer.step()
            loss.update(cur_loss.item())
 

            # validation
            train_loss += loss.val
            # for evert `eval_steps` steps or at the last steps, we will evaluate the model.
            if step % params.eval_steps == 0 or step == params.max_steps:
                if has_valid:
                    stop_steps += 1
                    train_loss /= params.eval_steps

                    with torch.no_grad():
                        params.dnn_model.eval()

                        # forward
                        preds = nntrain._nn_predict(params,all_t["x"]["valid"], return_cpu=False)
                        cur_loss_val = nntrain.get_loss(preds, all_t["w"]["valid"], all_t["y"]["valid"], params.loss)
                        loss_val = cur_loss_val.item()
                        metric_val = (
                            nntrain.get_metric(
                                preds.reshape(-1), 
                                all_t["y"]["valid"].reshape(-1),
                                all_df["y"]["valid"].index
                            )
                            .detach()
                            .cpu()
                            .numpy()
                            .item()
                        )
 

                        if params.eval_train_metric:
                            metric_train = (
                                params.get_metric(
                                    params._nn_predict(params,all_t["x"]["train"], return_cpu=False),
                                    all_t["y"]["train"].reshape(-1),
                                    all_df["y"]["train"].index,
                                )
                                .detach()
                                .cpu()
                                .numpy()
                                .item()
                            )
                        else:
                            metric_train = np.nan
                    if verbose:
                        print(loss_val)
                        print(
                            f"[Step {step}]: train_loss {train_loss:.6f}, valid_loss {loss_val:.6f}, train_metric {metric_train:.6f}, valid_metric {metric_val:.6f}"
                        )
                    evals_result["train"].append(train_loss)
                    evals_result["valid"].append(loss_val)
                    if loss_val < best_loss:
                        if verbose:
                            print(
                                "\tvalid loss update from {:.6f} to {:.6f}, save checkpoint.".format(
                                    best_loss, loss_val
                                )
                            )
                        best_loss = loss_val
                        params.best_step = step
                        stop_steps = 0
                        torch.save(params.dnn_model.state_dict(), save_path)
                    train_loss = 0
                    # update learning rate
                    if params.scheduler is not None:
                        nntrain.auto_filter_kwargs(params.scheduler.step, warning=False)(metrics=cur_loss_val, epoch=step)
                else:
                    # retraining mode
                    if params.scheduler is not None:
                        params.scheduler.step(epoch=step)

        if has_valid:
            # restore the optimal parameters after training
            params.dnn_model.load_state_dict(torch.load(save_path, map_location=params.device))
        if torch.cuda.is_available() and params.GPU >= 0:
            torch.cuda.empty_cache()

        return params

    def get_lr():
        assert len(params.train_optimizer.param_groups) == 1
        return params.train_optimizer.param_groups[0]["lr"]

    def get_loss(pred, w, target, loss_type):
        pred, w, target = pred.reshape(-1), w.reshape(-1), target.reshape(-1)
        if loss_type == "mse":
            sqr_loss = torch.mul(pred - target, pred - target)
            loss = torch.mul(sqr_loss, w).mean()
            return loss
        elif loss_type == "binary":
            loss = nn.BCEWithLogitsLoss(weight=w)
            return loss(pred, target)
        else:
            raise NotImplementedError("loss {} is not supported!".format(loss_type))

    def get_metric(pred, target, index):
        # NOTE: the order of the index must follow <datetime, instrument> sorted order
        return -ICLoss()(pred, target, index)  # pylint: disable=E1130

    def _nn_predict(params,data, return_cpu=True):
        print(data)
        """Reusing predicting NN.
        Scenarios
        1) test inference (data may come from CPU and expect the output data is on CPU)
        2) evaluation on training (data may come from GPU)
        """
        if not isinstance(data, torch.Tensor):
            if isinstance(data, pd.DataFrame):
                data = data.values
            data = torch.Tensor(data)
        data = data.to(params.device)
        preds = []
        params.dnn_model.eval()
        with torch.no_grad():
            batch_size = 8096
            for i in range(0, len(data), batch_size):
                x = data[i : i + batch_size]
                preds.append(params.dnn_model(x.to(params.device)).detach().reshape(-1))
        if return_cpu:
            preds = np.concatenate([pr.cpu().numpy() for pr in preds])
        else:
            preds = torch.cat(preds, axis=0)
        return preds

    def predict(params,df_pred,data_path):
        df_pred=df_pred.reset_index()
        pred=df_pred[['ts_code','trade_date']]
        x_pred=df_pred.drop('label', axis=1)  
        x_pred= x_pred.drop('close', axis=1) 
        x_pred= x_pred.drop('open', axis=1) 
        x_pred= x_pred.drop('ts_code', axis=1) 
        x_pred= x_pred.drop('trade_date', axis=1) 
        y_pred = nntrain._nn_predict(params,x_pred)
        
        pred['pred']=y_pred
        pred=pred.sort_values('trade_date')
        pred['pred']=pred.groupby('ts_code',group_keys=False).apply(lambda x: x['pred'].shift(1))
        
        pred=pred.dropna()
        
        pred.to_pickle(data_path+'/preds/nn_model_'+'test'+'_pred.pkl')        
        
        return pred


    def auto_filter_kwargs(func: Callable, warning=True) -> Callable:
        def _func(*args, **kwargs):
            spec = inspect.getfullargspec(func)
            new_kwargs = {}
            for k, v in kwargs.items():
                # if `func` don't accept variable keyword arguments like `**kwargs` and have not according named arguments
                if spec.varkw is None and k not in spec.args:
                    if warning:
                        log.warning(f"The parameter `{k}` with value `{v}` is ignored.")
                else:
                    new_kwargs[k] = v
            return func(*args, **new_kwargs)
    
        return _func



    def save(self, filename, **kwargs):
        with save_multiple_parts_file(filename) as model_dir:
            model_path = os.path.join(model_dir, os.path.split(model_dir)[-1])
            # Save model
            torch.save(params.dnn_model.state_dict(), model_path)

    def load(self, buffer, **kwargs):
        with unpack_archive_with_buffer(buffer) as model_dir:
            # Get model name
            _model_name = os.path.splitext(list(filter(lambda x: x.startswith("model.bin"), os.listdir(model_dir)))[0])[
                0
            ]
            _model_path = os.path.join(model_dir, _model_name)
            # Load model
            params.dnn_model.load_state_dict(torch.load(_model_path, map_location=params.device))
        params.fitted = True


class AverageMeter:
    """Computes and stores the average and current value"""

    def __init__(self):
        self.reset()

    def reset(self):
        self.val = 0
        self.avg = 0
        self.sum = 0
        self.count = 0

    def update(self, val, n=1):
        self.val = val
        self.sum += val * n
        self.count += n
        self.avg = self.sum / self.count


class Net(nn.Module):
    def __init__(self, input_dim, output_dim=1, layers=(256,), act="LeakyReLU"):
        super(Net, self).__init__()

        layers = [input_dim] + list(layers)
        dnn_layers = []
        drop_input = nn.Dropout(0.05)
        dnn_layers.append(drop_input)
        hidden_units = input_dim
        for i, (_input_dim, hidden_units) in enumerate(zip(layers[:-1], layers[1:])):
            fc = nn.Linear(_input_dim, hidden_units)
            if act == "LeakyReLU":
                activation = nn.LeakyReLU(negative_slope=0.1, inplace=False)
            elif act == "SiLU":
                activation = nn.SiLU()
            else:
                raise NotImplementedError(f"This type of input is not supported")
            bn = nn.BatchNorm1d(hidden_units)
            seq = nn.Sequential(fc, bn, activation)
            dnn_layers.append(seq)
        drop_input = nn.Dropout(0.05)
        dnn_layers.append(drop_input)
        fc = nn.Linear(hidden_units, output_dim)
        dnn_layers.append(fc)
        # optimizer  # pylint: disable=W0631
        self.dnn_layers = nn.ModuleList(dnn_layers)
        self._weight_init()

    def _weight_init(self):
        for m in self.modules():
            if isinstance(m, nn.Linear):
                nn.init.kaiming_normal_(m.weight, a=0.1, mode="fan_in", nonlinearity="leaky_relu")

    def forward(self, x):
        cur_output = x
        for i, now_layer in enumerate(self.dnn_layers):
            cur_output = now_layer(cur_output)
        return cur_output
        

class ICLoss(nn.Module):
    def forward(self,pred, y, idx, skip_size=50):
        """forward.
        FIXME:
        - Some times it will be a slightly different from the result from `pandas.corr()`
        - It may be caused by the precision problem of model;
        :param pred:
        :param y:
        :param idx: Assume the level of the idx is (date, inst), and it is sorted
        """

        
        EPS=1e-12
        prev = None
        diff_point = []
 
        #这里假设了索引是(date.code)
        for i, (date, inst) in enumerate(idx):
            if date != prev:
                diff_point.append(i)
            prev = date

        diff_point.append(None)
        # The lengths of diff_point will be one more larger then diff_point
 

        ic_all = 0.0
        skip_n = 0
        for start_i, end_i in zip(diff_point, diff_point[1:]):
            pred_focus = pred[start_i:end_i]  # TODO: just for fake
            if pred_focus.shape[0] < skip_size:
                # skip some days which have very small amount of stock.
                skip_n += 1
                continue
            y_focus = y[start_i:end_i]
            if pred_focus.std() < EPS or y_focus.std() < EPS:
                # These cases often happend at the end of test data.
                # Usually caused by fillna(0.)
                skip_n += 1
                continue

            ic_day = torch.dot(
                (pred_focus - pred_focus.mean()) / np.sqrt(pred_focus.shape[0]) / pred_focus.std(),
                (y_focus - y_focus.mean()) / np.sqrt(y_focus.shape[0]) / y_focus.std(),
            )
            ic_all += ic_day
        if len(diff_point) - 1 - skip_n <= 0:
            raise ValueError("No enough data for calculating IC")
        if skip_n > 0:
            get_module_logger("ICLoss").info(
                f"{skip_n} days are skipped due to zero std or small scale of valid samples."
            )
        ic_mean = ic_all / (len(diff_point) - 1 - skip_n)
        
        return -ic_mean  # ic loss