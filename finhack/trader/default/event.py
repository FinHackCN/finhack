from .data import Data
import finhack.library.log as Log
from .function import *
import pandas as pd
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
            'before_market':'09:00:00',
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
        #print("新的一天开始了")
        if context.data.data_source=='file':
            now_date=context.current_dt.strftime('%Y%m%d')
            start_date=context.trade.start_time[0:10].replace('-','')
            end_date=context.trade.end_time[0:10].replace('-','')
            if context.data.daily_info==None:
                data=Data.get_data_from_file(start_date=start_date,end_date=end_date,slice_type='m',now_date=now_date,filename='astock_daily_info',cache=True)
                context.data.daily_info=data
            elif now_date>context.data.daily_info['end_date']:
                data=Data.get_data_from_file(start_date=now_date,end_date=end_date,slice_type='m',now_date=now_date,filename='astock_daily_info',cache=True)
                context.data.daily_info=data
            
        pass
        
        
    #开盘先刷新可用持仓
    def refresh_enable_amount(context):
        for code,pos in context.portfolio.positions.items():
            pos.enable_amount=pos.amount
            
            
    #送股事件  
    def stock_split(context):
        now_date=context.current_dt.strftime('%Y%m%d')
        for code,pos2 in context.portfolio.positions.items():
            pos=context.portfolio.positions[code]
            if "dt_"+now_date in context.data['dividend']:
                if code in context.data['dividend']["dt_"+now_date]:
                    if context.data['dividend']["dt_"+now_date][code]['cash_stk']>0:
                        new_amount=context.data['dividend']["dt_"+now_date][code]['cash_stk']
                        pos.amount=pos.amount+new_amount
                        pos.total_cost=pos.total_cost-new_amount*pos.last_sale_price
                        pos.total_value=pos.amount*pos.last_sale_price
                        pos.cost_basis=pos.total_cost/pos.amount
                        context.portfolio.total_value=context.portfolio.total_value+new_amount*pos.last_sale_price
                        stk_num=str(round(new_amount,2))
                        log(f"{code}发生送股事件，共{stk_num}股")
                        
                        
                        
    def before_market(context):
        Event.refresh_enable_amount(context)
        Event.stock_split(context)
        
  


    
    
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
        now_date=context.current_dt.strftime('%Y%m%d')
        
        
        positions_value=0
        dividend=Data.get_dividend(context)
        if dividend:
            df_div=pd.DataFrame(dividend)   
            #print(df_div)
 

            if not df_div.empty:
                try:
                    df_div['pay_date']=df_div.apply(lambda x: x.ex_date if x.pay_date==None else x.pay_date,axis=1)
                except Exception as e:
                    log(str(e),'error')
                    print(df_div)
                    exit()
                    
            div_cash=0
            for code,pos in context.portfolio.positions.items():
                amount=pos.amount
                if code in df_div.index:
                    stk_div=float(df_div['stk_div'].at[code])
                    cash_div_tax=float(df_div['cash_div_tax'].at[code])
                    ex_date=df_div['ex_date'].at[code]
                    pay_date=df_div['pay_date'].at[code]
                    if pay_date==None:
                        pay_date=ex_date
                    if pay_date==None:
                        continue
                    
                    
                    
                    dt_key="dt_"+pay_date
                    if dt_key not in context.data['dividend']:
                        context.data['dividend'][dt_key]={}
                    context.data['dividend'][dt_key][code]={
                            'cash_tax':amount*cash_div_tax,
                            'cash_stk':amount*stk_div
                        }
                    
                if "dt_"+now_date in context.data['dividend']:
                    if code in context.data['dividend']["dt_"+now_date]:
                        if context.data['dividend']["dt_"+now_date][code]['cash_tax']>0:
                            context.portfolio.cash=context.portfolio.cash+context.data['dividend']["dt_"+now_date][code]['cash_tax']
                            div_v=str(round(context.data['dividend']["dt_"+now_date][code]['cash_tax'],2))
                            log(f"{code}发生分红事件，共{div_v}元")
                            #这里成本其实是下来了，暂时懒得写更新逻辑了                            
                            
                            
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