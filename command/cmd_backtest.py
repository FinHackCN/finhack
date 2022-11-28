import pandas as pd
import importlib
import os
import sys
import datetime
sys.path.append('/data/code/finhack')
from library.backtest import bt

from library.mydb import mydb

print('please use command/cmd_autotrain.py')
exit()


features_list='CCI_0,MEDPRICE_0,MIN_0,ROC_0,TSF_0,alpha191_070,alpha191_187,buyLgAmount_0,buyMdVol_0'
train='lgb'
model='lgb_model_271fc9aec36cb2b3b1bcb979ad68fe93_pred.pkl'
strategy='aiTopN'
starttime= datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
endtime= datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
runtime=0


bt_instance=bt.run(
        cash=10000,
        instance_id='271fc9aec36cb2b3b1bcb979ad68fe93',
        start_date='20100101',
        strategy_name="aiTopN",
        data_path="/data/code/finhack/data/preds/lgb_model_271fc9aec36cb2b3b1bcb979ad68fe93_pred.pkl"
    )


risk=bt.analyse(instance=bt_instance,benchmark='000001.SH') 
returns=str(risk['returns'].to_json(orient='index'))
bench_returns=str(risk['bench_returns'].to_json(orient='index'))

mydb.exec('delete from backtest where instance_id="%s"' % (bt_instance['instance_id']),'finhack')
sql="INSERT INTO `finhack`.`backtest`(`instance_id`,`features_list`, `train`, `model`, `strategy`, `start_date`, `end_date`, `init_cash`, `args`, `history`, `returns`, `logs`, `total_value`, `alpha`, `beta`, `annual_return`, `cagr`, `annual_volatility`, `info_ratio`, `downside_risk`, `R2`, `sharpe`, `sortino`, `calmar`, `omega`, `max_down`, `SQN`) VALUES ( '%s','%s', '%s', '%s', '%s', '%s', '%s', %s, '%s', '%s', '%s', '%s', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" % (bt_instance['instance_id'],features_list,train,model,strategy,bt_instance['start_date'],bt_instance['end_date'],str(bt_instance['cash']),str(bt_instance['args']),'history','returns','logs',str(bt_instance['total_value']),str(risk['alpha']),str(risk['beta']),str(risk['annual_return']),str(risk['cagr']),str(risk['annual_volatility']),str(risk['info_ratio']),str(risk['downside_risk']),str(risk['R2']),str(risk['sharpe']),str(risk['sortino']),str(risk['calmar']),str(risk['omega']),str(risk['max_down']),str(risk['sqn']))
mydb.exec(sql,'finhack')




mydb.exec('delete from finhack_backtest_record where id="%s"' % (bt_instance['instance_id']),'woldycvm')
sql="INSERT INTO `finhack`.`finhack_backtest_record`(`n`, `server`, `starttime`, `start_money`, `portvalue`, `alpha`, `beta`, `rnorm`, `sqn`, `info`, `vola`, `omega`, `sharpe`, `sortino`, `calmar`, `drawdown`, `roto`, `trade_num`, `win`, `returns`, `bench_returns`, `trainstart`, `trainend`, `btstart`, `btend`, `factor`, `id`, `endtime`, `runtime`) VALUES (%s, '%s', '%s', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (0,'woldy-PC',starttime,str(bt_instance['cash']),str(bt_instance['total_value']),str(risk['alpha']),str(risk['beta']),str(risk['annual_return']),str(risk['sqn']),str(risk['info_ratio']),str(risk['annual_volatility']),str(risk['omega']),str(risk['sharpe']),str(risk['sortino']),str(risk['calmar']),str(risk['max_down']),str(risk['roto']),str(bt_instance['trade_num']),str(risk['win_ratio']),returns,bench_returns,'20010101','20091231',bt_instance['start_date'],bt_instance['end_date'],features_list,bt_instance['instance_id'],endtime,str(runtime))
mydb.exec(sql,'woldycvm')


