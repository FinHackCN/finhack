from  library.backtest import bt
import time

hold_day=10
hold_n=10

class strategy():
    def run(instance):
        t1=time.time()
        #n用来控制轮仓时间
        instance['g']['n']=0
        for date in instance['date_range']:
            instance['now_date']=date
            strategy.every_bar(instance)
            bt.update(instance)
            instance['g']['n']=instance['g']['n']+1
            if instance['g']['n']>=hold_day:
                instance['g']['n']=0
        print("strategy time:"+str(time.time()-t1))
        return instance
        
    def every_bar(instance):
        now_date=instance['now_date']
        #第9日尾盘清仓
        if instance['g']['n'] % hold_day==hold_day-1:
            pred=instance['data'].loc[now_date]
            pred=pred.sort_values(by='pred',ascending=False, inplace=False) 
            
            #i用来控制持仓数据
            i=0
            for postion in instance['positions']:
                bt.sell(instance=instance,ts_code=postion['ts_code'],amount=postion['amount'],time='close')
     
        
        #第10日开盘买入
        elif instance['g']['n'] % hold_day==0:
            pred=instance['data'].loc[now_date]
            
            pred=pred.sort_values(by='pred',ascending=False, inplace=False) 
            
            #i用来控制持仓数据
            i=0
            for ts_code, row in pred.iterrows():
                buy=bt.buy(instance=instance,ts_code=ts_code,price=instance['cash']/(hold_n-i),time='open')
                if buy:
                    i=i+1
                if i==hold_n:
                    break