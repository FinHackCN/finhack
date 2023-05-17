from  backtest.backtest import bt
import time
from astock.indexHelper import indexHelper

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
        
        hold_day=strategy.get_arg('hold_day',instance)
        hold_n=strategy.get_arg('hold_n',instance)
        
        for date in instance['date_range']:
            instance['now_date']=date
            strategy.every_bar(instance)
            bt.update(instance)
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
        #第9日尾盘清仓
        if instance['g']['n'] % hold_day==hold_day-1:
            pred=instance['data'].loc[now_date]
            pred=pred.sort_values(by='pred',ascending=False, inplace=False) 
            pred=pred[~pred.index.duplicated()]
            
            #i用来控制持仓数据
            i=0
            for ts_code,postion in instance['positions'].copy().items():
                bt.sell(instance=instance,ts_code=ts_code,amount=postion['amount'],time='close')
     
        
        #第10日开盘买入
        elif instance['g']['n'] % hold_day==0:
            pred=instance['data'].loc[now_date]
            
            pred=pred.sort_values(by='pred',ascending=False, inplace=False) 
            
            pred=pred.dropna()
            pred=pred[pred.pred>1.05]
            pred=pred[~pred.index.duplicated()]
            #print(pred)
            member_list=indexHelper.get_index_weights('000852.SH',now_date)['ts_code'].tolist()
            #print(member_list)
            
            # pred['flag'] = pred.apply(lambda x: int(x.index in member_list), axis=1)
            # pred=pred[pred.flag==1]
            
            # print(now_date)
            # print(pred)
            
            
            
            pred_list=pred.index.tolist()
            
            
            drop_list=list(set(pred_list) - set(member_list))
            pred = pred.drop(index=drop_list)
 
 
            if len(pred)<hold_n:
                hold_n=len(pred)
            if hold_n<10:
                hold_n=10
            
            # if pred.empty:
            #     print(now_date)
            #i用来控制持仓数据
            
            
            
            i=0
            for ts_code, row in pred.iterrows():
                buy=bt.buy(instance=instance,ts_code=ts_code,value=instance['cash']/(hold_n-i),time='open')
                if buy:
                    i=i+1
                if i==hold_n:
                    break