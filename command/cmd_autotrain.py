import sys
import os
import time
import shutil
import random
import traceback
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from train.lgbtrain import lgbtrain
from factors.factorManager import factorManager

def auto_lgbtrain():
    
    t1=time.time()
    flist=factorManager.getTopAnalysedFactorsList()
    random.shuffle(flist)
    n=random.randint(5,15)
    factor_list=[]
    
    for i in range(0,n):
        factor_list.append(flist.pop())
    factor_list.sort()
    
    
    print(factor_list)
    
    md5=lgbtrain.run(features=factor_list)

    features_list=','.join(factor_list)
    train='lgb'
    model='lgb_model_'+md5+'_pred.pkl'
    strategy='aiTopN'
    starttime= datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    runtime=round(time.time()-t1,2)
    
    
    bt_instance=bt.run(
            cash=10000,
            instance_id=md5,
            start_date='20100101',
            strategy_name="aiTopN",
            data_path="/data/code/finhack/data/preds/lgb_model_"+md5+"_pred.pkl"
        )
    
    
    risk=bt.analyse(instance=bt_instance,benchmark='000001.SH') 
    returns=str(risk['returns'].to_json(orient='index'))
    bench_returns=str(risk['bench_returns'].to_json(orient='index'))
    endtime= datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    mydb.exec('delete from backtest where instance_id="%s"' % (bt_instance['instance_id']),'finhack')
    sql="INSERT INTO `finhack`.`backtest`(`instance_id`,`features_list`, `train`, `model`, `strategy`, `start_date`, `end_date`, `init_cash`, `args`, `history`, `returns`, `logs`, `total_value`, `alpha`, `beta`, `annual_return`, `cagr`, `annual_volatility`, `info_ratio`, `downside_risk`, `R2`, `sharpe`, `sortino`, `calmar`, `omega`, `max_down`, `SQN`) VALUES ( '%s','%s', '%s', '%s', '%s', '%s', '%s', %s, '%s', '%s', '%s', '%s', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" % (bt_instance['instance_id'],features_list,train,model,strategy,bt_instance['start_date'],bt_instance['end_date'],str(bt_instance['cash']),str(bt_instance['args']),'history','returns','logs',str(bt_instance['total_value']),str(risk['alpha']),str(risk['beta']),str(risk['annual_return']),str(risk['cagr']),str(risk['annual_volatility']),str(risk['info_ratio']),str(risk['downside_risk']),str(risk['R2']),str(risk['sharpe']),str(risk['sortino']),str(risk['calmar']),str(risk['omega']),str(risk['max_down']),str(risk['sqn']))
    mydb.exec(sql,'finhack')
    
    mydb.exec('delete from finhack_backtest_record where id="%s"' % (bt_instance['instance_id']),'woldycvm')
    sql="INSERT INTO `finhack`.`finhack_backtest_record`(`n`, `server`, `starttime`, `start_money`, `portvalue`, `alpha`, `beta`, `rnorm`, `sqn`, `info`, `vola`, `omega`, `sharpe`, `sortino`, `calmar`, `drawdown`, `roto`, `trade_num`, `win`, `returns`, `bench_returns`, `trainstart`, `trainend`, `btstart`, `btend`, `factor`, `id`, `endtime`, `runtime`) VALUES (%s, '%s', '%s', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (0,'woldy-PC',starttime,str(bt_instance['cash']),str(bt_instance['total_value']),str(risk['alpha']),str(risk['beta']),str(risk['annual_return']),str(risk['sqn']),str(risk['info_ratio']),str(risk['annual_volatility']),str(risk['omega']),str(risk['sharpe']),str(risk['sortino']),str(risk['calmar']),str(risk['max_down']),str(risk['roto']),str(bt_instance['trade_num']),str(risk['win_ratio']),returns,bench_returns,'20010101','20091231',bt_instance['start_date'],bt_instance['end_date'],features_list,bt_instance['instance_id'],endtime,str(runtime))
    mydb.exec(sql,'woldycvm')    


for i in range(0,100):
    try:
        auto_lgbtrain()
    except Exception as e:
        print("error:"+str(e))
        print("err exception is %s" % traceback.format_exc())
        