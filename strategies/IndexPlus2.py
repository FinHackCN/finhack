from  backtest.backtest import bt
import time
from astock.indexHelper import indexHelper
#chunzhizeng
import math
class strategy():
# python command/cmd_backtest.py --model=2713d39ec9c2ccca5876e6b816b50d9a --thread=1 --cash=10000000  --args="{'power':10,'change_threshold':0.3}" --start=20200101 --end=20230101 --filter=MainBoardNoST --benchmark="000001.SH" --fees=0.0003 --min_fees=5 --tax=0.001 --slip=0.005 --replace=1 --log=0 --record=1 --strategy=idxPls3    
    
    def get_arg(key,instance):
        default_args={
            "power":10,
            "change_threshold":0.3
        }
        if key in instance['args']['strategy_args'].keys():
            return instance['args']['strategy_args'][key]
        else:
            return default_args[key]
        
    def run(instance):
        t1=time.time()
        instance['date_range']=list(filter(lambda x: x >= instance['start_date'], instance['date_range']))

        for date in instance['date_range']:
            instance['now_date']=date
            bt.before_market(instance)
            strategy.every_bar(instance)
            bt.after_market(instance)
        return instance
        
    def every_bar(instance):

        power=strategy.get_arg('power',instance)
        change_threshold=strategy.get_arg('change_threshold',instance)
        
        now_date=instance['now_date']
        if(instance['total_value']<100):
            print(111)
            return False

        total_value=instance['total_value']

        idx_weight=indexHelper.get_index_weights('000852.SH',now_date)
        idx_weight=idx_weight.set_index('ts_code',drop=True)


        pred=instance['data'].loc[now_date]
        pred=pred.sort_values(by='pred',ascending=False, inplace=False) 
        pred=pred[~pred.index.duplicated()]
        
        sr=pred
        #低于0.95的全部
        sr.loc[sr.index[200:], 'pred'] = 0

        

        
        for index,row in sr.iterrows():
            symbol=index
            if symbol in idx_weight.index.values:
                idx_weight.loc[symbol,'weight']=float(idx_weight.loc[symbol]['weight'])*math.pow(row['pred'],power)
            
            

                        
        for idx,row in idx_weight.iterrows():
            code=idx
            weight=float(row['weight'])
            target_pos_cash=total_value*weight*0.01
            
            if target_pos_cash<100:
                continue
            now_pos_cash=0
        
            if code in instance['positions'].keys():
                now_pos_cash=instance['positions'][code]['total_value']
            x_cash=target_pos_cash-now_pos_cash
     
            if x_cash<0 and (-x_cash)/(now_pos_cash+1)>change_threshold:
                bt.sell(instance=instance,ts_code=code,value=-x_cash,time='open')
 
        
                
                
                
        for idx,row in idx_weight.iterrows():
            code=idx
            amount=0
            weight=float(row['weight'])
            target_pos_cash=total_value*weight*0.01
                
            if target_pos_cash<100:
                continue
            now_pos_cash=0
        
            if code in instance['positions'].keys():
                now_pos_cash=instance['positions'][code]['total_value']
                amount=instance['positions'][code]['amount']
            x_cash=target_pos_cash-now_pos_cash

            
            if x_cash>0 and  (x_cash)/(now_pos_cash+1)>change_threshold:
                bt.buy(instance=instance,ts_code=code,value=x_cash,time='open')
              
      
 