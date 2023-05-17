from  backtest.backtest import bt
import time
from astock.indexHelper import indexHelper
#chunzhizeng
import math
class strategy():
    def get_arg(key,instance):
        default_args={
            "hold_day":10,
            "hold_n":10
        }
        if key in instance['args']['strategy_args'].keys():
            return instance['args']['strategy_args'][key]
        else:
            return default_args[key]
        
    def run(instance):
        t1=time.time()
        #n用来控制轮仓时间
        instance['g']['n']=0
        instance['date_range']=list(filter(lambda x: x >= instance['start_date'], instance['date_range']))
        
        
        #instance['data']['pred']=instance['data']['pred'].astype('float16')
        
        hold_day=strategy.get_arg('hold_day',instance)
        hold_n=strategy.get_arg('hold_n',instance)
        
        for date in instance['date_range']:
            
            instance['now_date']=date
            bt.before_market(instance)
            strategy.every_bar(instance)
            bt.after_market(instance)
            instance['g']['n']=instance['g']['n']+1
            if instance['g']['n']>=hold_day:
                instance['g']['n']=0
        #print("backtest time: %s , return: %s" % (str(time.time()-t1),str(instance['total_value']/instance['init_cash']-1)))
        return instance
        
    def every_bar(instance):
        hold_day=strategy.get_arg('hold_day',instance)
        hold_n=strategy.get_arg('hold_n',instance)
        
        now_date=instance['now_date']
        if(instance['total_value']<100):
            return False

        total_value=instance['total_value']

        idx_weight=indexHelper.get_index_weights('000852.SH',now_date)
        idx_weight=idx_weight.set_index('ts_code',drop=True)


        pred=instance['data'].loc[now_date]
        pred=pred.sort_values(by='pred',ascending=False, inplace=False) 
        pred=pred[~pred.index.duplicated()]
        
        sr=pred
        del_list=[]
        
        
 
 
        idx_weight['score']=0
        for index,row in sr.iterrows():
            symbol=index
            if symbol in idx_weight.index.values:
                idx_weight.loc[symbol,'weight']=float(idx_weight.loc[symbol]['weight'])*math.pow(row['pred'],hold_n)
                idx_weight.loc[symbol,'score']=row['pred']
                
        
        score_list=list(idx_weight['score'].values)
        score_list.sort(reverse=True) 
        #print(score_list)
        threshold=score_list[int(0.5*len(score_list))]
        idx_weight=idx_weight[idx_weight.score>threshold]
            



            
            #i用来控制持仓数据
        i=0
        for code,postion in instance['positions'].copy().items():
             #不在指数中
             
            # if code=="300157.SZ" and now_date=="20200102":
            #     print(postion['amount'])
            #     exit()
                          
             
            if code not in idx_weight.index.values:
                bt.sell(instance=instance,ts_code=code,amount=postion['amount'],time='open')
            elif code in del_list:
                bt.sell(instance=instance,ts_code=code,amount=postion['amount'],time='open')
            else:
                weight=idx_weight.loc[code]['weight']  
                
  
                        
                        
        for idx,row in idx_weight.iterrows():
            code=idx
            weight=float(row['weight'])
            if code in del_list:
                continue
            target_pos_cash=total_value*weight*0.01*1.05
            
            
            

                
            if target_pos_cash<100:
                continue
            now_pos_cash=0
        
            if code in instance['positions'].keys():
                now_pos_cash=instance['positions'][code]['total_value']
            x_cash=target_pos_cash-now_pos_cash
     
     

            
            
            if x_cash<0:
                bt.sell(instance=instance,ts_code=code,price=-x_cash,time='open')
 
        
                
                
                
        for idx,row in idx_weight.iterrows():
            code=idx
            amount=0
            weight=float(row['weight'])
            if code in del_list:
                continue
            target_pos_cash=total_value*weight*0.01*1.05
                
            if target_pos_cash<100:
                continue
            now_pos_cash=0
        
            if code in instance['positions'].keys():
                now_pos_cash=instance['positions'][code]['total_value']
                amount=instance['positions'][code]['amount']
            x_cash=target_pos_cash-now_pos_cash
            
            

            
            if x_cash>0:
                bt.buy(instance=instance,ts_code=code,value=x_cash,time='open')
                
                

      
 