from trader.qmt.data import Data
import finhack.library.log as Log
from trader.qmt.function import *
import pandas as pd
from finhack.trainer.trainer import Trainer
from finhack.trainer.lightgbm.lightgbm_trainer import LightgbmTrainer
class Event:
    def __init__(self):
        pass
    
    def get_event():
        event={}
        event['default']={
            'start_interval':'00:00:00',
            'start_market':'00:00:00',
            'end_market':'23:59:59',
            'end_interval':'23:59:59'    
        }
        
        event['astock']={
            'start_interval':'00:00:00',
            # 'before_market':'09:00:00',
            'before_market':'19:23:00',
            'pre_opening_start':'09:15:00',
            'pre_opening_end':'09:20:00',
            'matching_start':'09:25:00',
            'morning_start':'09:30:00',
            'morning_end':'11:30:00',
            'afternoon_start':'13:00:00',
            'closing_start':'14:57:00',
            'closing_end':'15:00:00',
            'afternoon_end':'15:00:00',
            'after_market':'18:00:00',
            'end_interval':'23:59:59'
        }
       
        event['astock'] = {**event['default'], **event['astock']}
        event['astock'] = sorted(event['astock'].items(), key=lambda x: x[1])    
        return event
        
    def load_event(context,start_time,end_time):
        event_list=[]
        daily_event_list=Event.get_event()
        calendar=context['data']['calendar']
        for date in calendar:
            for event_name,event_time in daily_event_list['astock']:
                if hasattr(Event, event_name):
                    func = getattr(Event, event_name)
                    new_event={
                        'event_name':event_name,
                        'event_func':func,
                        'event_time':date+' '+event_time,
                        'event_type':'market_event'
                    }
                    context['data']['event_list'].append(new_event)
                    context['data']['event_list'].sort(key=lambda x: x['event_time'])                
                    
        return True
        
        
    def start_interval(context):
        now_date=context.current_dt.strftime('%Y%m%d')
        start_date=context.trade.start_time[0:10].replace('-','')
        end_date=context.trade.end_time[0:10].replace('-','')
        print("新的一天开始了"+now_date)

        pass
        
        

    def before_market(context):
        model_id=context.trade.model_id
        preds_data=load_preds_data(model_id)
        clsLgbTrainer=LightgbmTrainer()
        preds=clsLgbTrainer.pred(preds_data,md5=model_id,save=False)
        context.g.preds=preds
  


    
    
    #开盘卖出退市股
    def morning_start(context):
        for code,pos in context.portfolio.positions.items():
            info=Data.get_daily_info(code=code,context=context)
            #print(info)
            if info!=None and "退" in info.name: 
                order_target_value(code,0)
                log(f"{code}即将退市，自动卖出！")
        
        
    
    #盘后更新
    #处理分红事件，计算持仓信息
    def after_market(context):
        Event.update_cash_div_tax(context)

                            
                            
    def update_cash_div_tax(context):            
        pass


                            
    def end_interval(context):
        Event.update_value(context)
        Event.log_turnover(context)
        log('---------------------------------')
        log(f"当前现金："+str(context.portfolio.cash))
        log(f"当前持仓："+str(context.portfolio.positions_value))
        log(f"当前市值："+str(context.portfolio.total_value))
        log(f"昨日市值："+str(context.portfolio.previous_value))
        log(f"今日收益："+str(context.performance.returns[- 1]))
    
    def update_value(context):
        now_date=context.current_dt.strftime('%Y%m%d')
        positions_value=0
        for code,pos2 in context.portfolio.positions.items():
            price=Data.get_price(code=code,context=context)
            pos=context.portfolio.positions[code]
            if price==None:
                pass
            else:
                pos.total_value=pos.amount*price
                pos.last_sale_price=price
            positions_value=positions_value+pos.total_value
        context.portfolio.previous_value=context.portfolio.total_value
        context.portfolio.total_value=positions_value+context.portfolio.cash
        context.portfolio.positions_value=positions_value
        context.performance.returns.append([now_date,context.portfolio.total_value/context.portfolio.previous_value])
        
        
    def log_turnover(context):
        now_date=context.current_dt.strftime('%Y%m%d')
        if now_date not in context.logs.history:
            context.logs.history[now_date]={}
        context.logs.history[now_date]['postion']=context.portfolio.positions
        sell_value = context.logs.history.get(now_date, {}).get('sell_value', 0)
        buy_value = context.logs.history.get(now_date, {}).get('buy_value', 0)
        context.performance.turnover.append(round((sell_value+buy_value)*100/context.portfolio.total_value,2))
        
 
        
        
    #     sell_value = instance['history'].get(now_date, {}).get('sell_value', 0)
    #     buy_value = instance['history'].get(now_date, {}).get('buy_value', 0)
    #     instance['turnover'].append(round((sell_value+buy_value)*100/instance['total_value'],2))
        
    #     bt.log(instance,"账户余额："+str(instance['total_value'])+","+now_date)
    #     if instance['total_value']<0 :
    #         print("余额小于0")
    #         print(instance['cash'])
    #         print(positions_value)
            
    #         exit()

    #     return True