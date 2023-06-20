import numpy as np
from astock.astock import AStock
import pandas as pd
import importlib
import os
import quantstats as qs
import time
import math
import datetime
import empyrical as ey
import hashlib
from library.mydb import mydb
from astock.market import market
from library.globalvar import *
import multiprocessing
import redis
from library.config import config
import json
import backtest

class bt:
    def run(instance_id='',start_date='20100101',end_date='20230315',fees=0.0003,min_fees=5,tax=0.001,cash=100000,strategy_name="",pred_data_path="",args={},replace=False,type="bt",g={},slip=0.005,benchmark="000001.SH",log_type=False,record_type=9,slice_type='m'):
        t1=time.time()
        starttime=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        bt_instance={
            "instance_id":instance_id,
            "start_date":start_date,
            "end_date":end_date,
            "now_date":'00000000',
            "pred_data_path":pred_data_path,
            "args":args,
            "pred_data":None,
            "g":g,
            "runtime_info":{},
            "positions":{ #仓位列表
                
                
            },
            "history":{
                
            },
            "returns":[
                
            ],
            "trade_returns":[
                
            ],
            "risk":{
                
            },
            "logs":[],
            "setting":{
                "fees":fees,
                "min_fees":min_fees,
                "tax":tax,
                "benchmark":benchmark,
                "slip":slip
            },
            "strategy_name":strategy_name,
            "cash":cash,
            "init_cash":cash,
            "position_value":0,
            "total_value":cash,
            "old_value":cash,
            "trade_num":0,
            'benchmark':benchmark,
            "win":0,
            "type":type
        }
        if instance_id=='':
            instance_id=hashlib.md5(str(bt_instance).encode(encoding='utf-8')).hexdigest()
        hassql='select * from backtest  where instance_id="%s"' % (instance_id)
        has=mydb.selectToDf(hassql,'finhack')
        if(not has.empty):  
            if replace==False:
                print("ignore:"+instance_id)
                return False        
        
        

        bt_instance["instance_id"]=instance_id
        bt_instance['record_type']=record_type
        bt_instance['log_type']=log_type

        cfg=config.getConfig('db','redis')
        redisPool = redis.ConnectionPool(host=cfg['host'],port=int(cfg['port']),password=cfg['password'],db=int(cfg['db']))
        client = redis.Redis(connection_pool=redisPool)   
        bt_instance['cache']=client

        if 'filter' in bt_instance['args']:
            filter_name=bt_instance['args']['filter']  
            filter_module = importlib.import_module('.btfilter',package='backtest')
            bt_instance['filter_func']=getattr(filter_module, filter_name)       
        
        if os.path.isfile(PREDS_DIR+pred_data_path):
            pred_data=pd.read_pickle(PREDS_DIR+pred_data_path)
        else:
            print(PREDS_DIR+data_path+' not found!')
            return False
        
        
        pred_data=pred_data[pred_data.trade_date>=start_date]
        pred_data=pred_data[pred_data.trade_date<=end_date]
        date_range=pred_data['trade_date'].to_list()
        date_range=list(set(date_range))
        date_range=sorted(date_range)
        pred_data=pred_data.set_index(['trade_date','ts_code'])
        bt_instance['pred_data']=pred_data
        bt_instance['date_range']=date_range
        bt_instance['dividend']={} #分红送股数据
        
        bt_instance['slice_type']=slice_type
        bt_instance['market_data']=market.get_data(start_date=start_date,end_date=end_date,slice_type=slice_type,now_date=start_date)


        
        #策略在用户目录下
        #print(USER_DIR+'strategies/'+strategy_name+'.py')
        if os.path.exists(USER_DIR+'strategies/'+strategy_name+'.py'):
            
            strategy_module = importlib.import_module('.'+strategy_name,package='user.strategies')
        else:
            strategy_module = importlib.import_module('.'+strategy_name,package='strategies')

        strategy_instance = getattr(strategy_module, 'strategy')
        bt.log(instance=bt_instance,msg="开始执行策略！",type='info')
        bt_instance=strategy_instance.run(instance=bt_instance)
        
        
        bt_instance['returns']=pd.DataFrame(bt_instance['returns'],columns=['trade_date', 'returns'])
        bt_instance['returns']['values']=bt_instance['returns']['returns'].cumprod()
        bt_instance['returns']["trade_date"] = pd.to_datetime(bt_instance['returns']["trade_date"], format='%Y%m%d')
        bt_instance['returns']=bt_instance['returns'].set_index('trade_date')
            
        bt_instance["runtime_info"]={
                    "starttime":starttime,
                    "endtime":datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "runtime":time.time()-t1
        }        
    
        bt_instance['risk']=bt.analyse(instance=bt_instance,benchmark=bt_instance['setting']['benchmark'],show=False)
        if bt_instance['risk']!=False:
            bt.record(bt_instance)
            rtime=str(round(time.time()-t1,2))
            ret=str(round((bt_instance['total_value']/bt_instance['init_cash']-1)*100,2))
            print("backtest time: %ss , return: %s%%" % (rtime,ret)) 
            if bt_instance['log_type']==2 or bt_instance['log_type']==3 :
                print(bt_instance['risk'])
        else:
            pass
    
    def record(bt_instance):
        risk=bt_instance['risk']
        returns=str(risk['returns'].to_json(orient='index'))
        bench_returns=str(risk['bench_returns'].to_json(orient='index'))        
        features_list=bt_instance['args']['features_list']
        train=bt_instance['args']['train']
        model=bt_instance['args']['model']
        strategy=bt_instance['strategy_name']+'_'+str(list(bt_instance['args']['strategy_args'].values()))
        init_cash=bt_instance['init_cash']
        endtime=bt_instance['runtime_info']['endtime']
        runtime=bt_instance['runtime_info']['runtime']
        starttime=bt_instance['runtime_info']['starttime']
        loss=bt_instance['args']['loss']
        filters_name=''
        if 'filter' in bt_instance['args']:
            filters_name=bt_instance['args']['filter']

        
        cfg_r=config.getConfig('backtest','record')
        tv=risk[cfg_r['filed']]
        sql="select %s %s (select %s(%s) from backtest)" %(tv,cfg_r['compare'],cfg_r['threshold'],cfg_r['filed'])
        th=mydb.selectToDf(sql,'finhack').values[0][0]
        try:
            #th==1,超过阈值
            if int(th)!=1:
                returns='returns'
                bench_returns='bench_returns'
            else:
                pass
            # sql="INSERT INTO `finhack`.`backtest`(`instance_id`,`features_list`, `train`, `model`, `strategy`, `start_date`, `end_date`, `init_cash`, `args`, `history`, `returns`, `logs`, `total_value`, `alpha`, `beta`, `annual_return`, `cagr`, `annual_volatility`, `info_ratio`, `downside_risk`, `R2`, `sharpe`, `sortino`, `calmar`, `omega`, `max_down`, `SQN`,filter,win,server,trade_num,runtime,starttime,endtime,benchReturns,roto,benchmark) VALUES ( '%s','%s', '%s', '%s', '%s', '%s', '%s', %s, '%s', '%s', '%s', '%s', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,'%s',%s,'%s',%s,'%s','%s','%s','%s','%s','%s')" % (bt_instance['instance_id'],features_list,train,model,strategy,bt_instance['start_date'],bt_instance['end_date'],str(init_cash),str(bt_instance['args']).replace("'",'"'),'history',returns,'logs',str(bt_instance['total_value']),str(risk['alpha']),str(risk['beta']),str(risk['annual_return']),str(risk['cagr']),str(risk['annual_volatility']),str(risk['info_ratio']),str(risk['downside_risk']),str(risk['R2']),str(risk['sharpe']),str(risk['sortino']),str(risk['calmar']),str(risk['omega']),str(risk['max_down']),str(risk['sqn']),filters_name,str(risk['win_ratio']),'woldy-PC',str(bt_instance['trade_num']),str(runtime),str(starttime),str(endtime),bench_returns,str(risk['roto']),bt_instance['benchmark'])       
            # print(sql)
        except Exception as e:
            pass
        if bt_instance['type']=='bt':
            sql="INSERT INTO `finhack`.`backtest`(`instance_id`,`features_list`, `train`, `model`, `strategy`, `start_date`, `end_date`, `init_cash`, `args`, `history`, `returns`, `logs`, `total_value`, `alpha`, `beta`, `annual_return`, `cagr`, `annual_volatility`, `info_ratio`, `downside_risk`, `R2`, `sharpe`, `sortino`, `calmar`, `omega`, `max_down`, `SQN`,filter,win,server,trade_num,runtime,starttime,endtime,benchReturns,roto,benchmark) VALUES ( '%s','%s', '%s', '%s', '%s', '%s', '%s', %s, '%s', '%s', '%s', '%s', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,'%s',%s,'%s',%s,'%s','%s','%s','%s','%s','%s')" % (bt_instance['instance_id'],features_list,train,model,strategy,bt_instance['start_date'],bt_instance['end_date'],str(init_cash),str(bt_instance['args']).replace("'",'"'),'history',returns,'logs',str(bt_instance['total_value']),str(risk['alpha']),str(risk['beta']),str(risk['annual_return']),str(risk['cagr']),str(risk['annual_volatility']),str(risk['info_ratio']),str(risk['downside_risk']),str(risk['R2']),str(risk['sharpe']),str(risk['sortino']),str(risk['calmar']),str(risk['omega']),str(risk['max_down']),str(risk['sqn']),filters_name,str(risk['win_ratio']),'woldy-PC',str(bt_instance['trade_num']),str(runtime),str(starttime),str(endtime),bench_returns,str(risk['roto']),bt_instance['benchmark'])       
            mydb.exec('delete from backtest where instance_id="%s"' % (bt_instance['instance_id']),'finhack')
            mydb.exec(sql,'finhack')
  
    
    def buy(instance,ts_code,amount=0,value=0,time='open'):
        flag688=False
        if ts_code[:3]=='688':
            flag688=True

        now_date=instance['now_date']
        price=0
        
        
        #now_price=market.get_price(ts_code,now_date,instance['cache'])
        if 'market_no_'+now_date+'_'+ts_code in instance['market_data']:
            now_price=instance['market_data']['market_no_'+now_date+'_'+ts_code]
        else:
            now_price=None
        
        
        if 'filter' in instance['args']:
            a=instance['filter_func'](now_price)
            if not a:
                #bt.log(instance=instance,ts_code=ts_code,msg="触发规则过滤，不买入！",type='warn')
                return False                   
            
        if now_price!=None and "退" in now_price['name']: 
            bt.log(instance=instance,ts_code=ts_code,msg="即将退市，不买入！",type='warn')
            return False            
        
        if now_price==None:
            bt.log(instance=instance,ts_code=ts_code,msg="价格获取失败，无法买入！",type='warn')
            return False
 
        if time=="open":
            price=now_price['open']
        else:
            price=now_price['close']
        
        stop=int(now_price['stop'])
        
        try:
            price=float(price)
            if(np.isnan(price) or stop==1 or price==0):
                bt.log(instance=instance,ts_code=ts_code,msg="停牌，无法买入！",type='warn')
                return False            
        except Exception as e:
                bt.log(instance=instance,ts_code=ts_code,msg=str(e),type='error')
                return False    
        
 
        upLimit=now_price['upLimit']
        volume=now_price['volume']
        
 
        
        if price>=upLimit*1 and price==now_price['high']:
            bt.log(instance=instance,ts_code=ts_code,msg="%s,%s" % (str(price),str(upLimit)),type='warn')
            bt.log(instance=instance,ts_code=ts_code,msg="涨停，无法买入！",type='warn')
            return False

 
        #设置滑点
        price=price*(1+instance['setting']['slip'])
        if price>=upLimit and upLimit>0:
            price=upLimit


        # if value> instance['cash']-instance['setting']['min_fees']:
        #     value=instance['cash']-instance['setting']['min_fees']


        if amount*price>instance['cash']:
            value=instance['cash']

        
        if value>0:
            #考虑手续费不够的情况
            if value>instance['cash']:
                value=instance['cash']
            if instance['cash']-value<instance['setting']['min_fees'] or instance['cash']-value<value*instance['setting']['fees']:
                if value*instance['setting']['fees']>instance['setting']['min_fees']:
                    value=value-value*instance['setting']['fees']
                else:
                    value=value-instance['setting']['min_fees']
        
                    
            amount=value/price
            
            
        #大于今日成交量的1/4
        if amount>volume*0.25*100:
            amount=int(volume*0.25*100) 



        amount=int(amount)
        #非科创板
        if amount>0 and not flag688:
            if(amount<100):
                amount=0
            if(amount % 100 !=0):
                amount=int(amount/100)*100
            value=amount*price
            if instance['cash']-value<instance['setting']['min_fees'] or instance['cash']-value<value*instance['setting']['fees']:
                amount=amount-100
        elif amount>0 and flag688:
            if(amount<200):
                amount=0
            if instance['cash']-value<instance['setting']['min_fees'] or instance['cash']-value<value*instance['setting']['fees']:
                fees=instance['setting']['min_fees']
                if fees<value*instance['setting']['fees']:
                    fees=value*instance['setting']['fees']
                fees_amount=int(fees/price)+1
                if (amount<200+fees_amount):
                    amount=0
                else:
                    amount=amount-fees_amount
        
        #按该票上次收盘价估算市值，兼容rqalpha
        if ts_code in instance['positions']:
            try:
                last_value=instance['positions'][ts_code]['last_close']*amount
            except Exception as e:
                print(ts_code)
                print(now_date)
                print(instance['positions'][ts_code])
        else:
            last_value=price*amount
        
        #再检测一下，排除买了1手手续费不够的情况
        if amount>0:
            value=amount*price
            fees=5
            if value*instance['setting']['fees']>5:
                fees=value*instance['setting']['fees']
                
            instance['cash']=instance['cash']-value-fees
            
            #理论上total_value是不应该变化的，但是rqalpha似乎是这样做的
            instance['total_value']=instance['total_value']-fees-value+last_value
            
            if ts_code not in instance['positions']:
                instance['positions'][ts_code]={
                    "amount":amount,
                    "avg_price":(value+fees)/amount,
                    "total_value":value+fees,
                    "last_close":price
                }
            else:
                instance['positions'][ts_code]['total_value']=instance['positions'][ts_code]['total_value']
                instance['positions'][ts_code]['amount']=instance['positions'][ts_code]['amount']+amount
                instance['positions'][ts_code]['avg_price']=(instance['positions'][ts_code]['total_value']+price+fees)/instance['positions'][ts_code]['amount']
                instance['positions'][ts_code]['last_close']=price
            instance['position_value']=instance['position_value']+value
        else:
            #bt.log(instance=instance,ts_code=ts_code,msg="钱不够，无法买入！",type='warn')
            return False     

        bt.log(instance=instance,ts_code=ts_code,msg="买入"+str(amount)+"股，当前价格"+str(round(price,2)),type='trade')
        return True
    
    def sell(instance,ts_code,amount=0,value=0,time='close'):

        price=0
        volume=0
        flag688=False
        if ts_code[:3]=='688':
            flag688=True        
        now_date=instance['now_date']

        #now_price=market.get_price(ts_code,now_date,instance['cache'])
        if 'market_no_'+now_date+'_'+ts_code in instance['market_data']:
            now_price=instance['market_data']['market_no_'+now_date+'_'+ts_code]
        else:
            now_price=None
            
        if now_price==None:
            bt.log(instance=instance,ts_code=ts_code,msg="无法获取价格！",type='warn')
            return False               
        
        if time=='close':
            price=now_price['close']
        else:
            price=now_price['open']
        volume=now_price['volume']
        stop=int(now_price['stop'])

        try:
            price=float(price)
            if(np.isnan(price) or stop==1 or price==0):
                bt.log(instance=instance,ts_code=ts_code,msg="停牌，无法卖出！",type='warn')
                return False            
        except Exception as e:
                print(price)
                bt.log(instance,ts_code+":error！"+str(e))
                return False   
        
        
        
        downLimit=now_price['downLimit']
 
        if price<=downLimit and price==now_price['low']:
            bt.log(instance=instance,ts_code=ts_code,msg="跌停版，无法卖出！",type='warn')
            return False
  
 
        #设置滑点
        price=price*(1-instance['setting']['slip'])
        
        #这个步骤还是非常有意义的，只不过现在的涨跌停价格不能确定
        # if value<=downLimit:
        #     value=downLimit
  
    
        if amount==0 and value>0:    
            amount=value/price
    
        if amount>instance['positions'][ts_code]['amount']:
            amount=instance['positions'][ts_code]['amount']
            
 
        
        # 此处做了最大购买量的限制，和其他回测平台不一样了，先去掉这个限制  
        if amount>volume*0.25*100:
            amount=int(volume*0.25*100) 
            
        amount=int(amount)
        #非科创板            
        
        if amount>0 and not flag688:
            if(amount<100):
                amount=0
            if(amount % 100 !=0):
                amount=int(amount/100)*100
            value=amount*price
        elif amount>0 and flag688:
            if(amount<200):
                amount=0

            
        if amount<=0:
            bt.log(instance=instance,ts_code=ts_code,msg="成交量小于等于0！",type='warn')
            return False
            
        value=amount*price
        fees=5
        if value*instance['setting']['fees']>5:
            fees=value*instance['setting']['fees']
        tax=value*instance['setting']['tax']

        last_mv=instance['positions'][ts_code]['last_close']*amount
        #现金中扣除手续费和印花税
        instance['cash']=instance['cash']+value-fees-tax
        instance['total_value']=instance['total_value']-fees-tax+value-last_mv
        
        #卖价>均价
        if price-(fees+tax)/amount>instance['positions'][ts_code]['avg_price']:
            instance['win']=instance['win']+1
        avg_price_old=instance['positions'][ts_code]['avg_price']
        trade_return=(price-(fees+tax)/amount)/(instance['positions'][ts_code]['avg_price'])-1
        instance['trade_returns'].append(trade_return)
        instance['positions'][ts_code]['amount']=instance['positions'][ts_code]['amount']-amount
        instance['positions'][ts_code]['total_value']=instance['positions'][ts_code]['amount']*instance['positions'][ts_code]['last_close']
        if(instance['positions'][ts_code]['amount']==0):
            del instance['positions'][ts_code]
        else:
            instance['positions'][ts_code]['avg_price']=(instance['positions'][ts_code]['total_value']+fees+tax)/instance['positions'][ts_code]['amount']        
        instance['position_value']=instance['position_value']-value
        instance['trade_num']=instance['trade_num']+1
        bt.log(instance=instance,ts_code=ts_code,msg="卖出"+str(amount)+"股，当前价格"+str(round(price,2)),type='trade')
        #+"，每股盈利"+str(round(price-avg_price_old,2))+"，总共盈利"+str(round((price-avg_price_old)*amount,2)),type='trade')
        

        return True
    
    #盘前更新
    #强制出售退市股、处理送股事件
    def before_market(instance):
        sell_list=[]
        now_date=instance['now_date']
        
        #分片加载行情数据
        if now_date>instance['market_data']['end_date']:
            start_date=instance['start_date']
            end_date=instance['end_date']
            slice_type=instance['slice_type']
            instance['market_data']=market.get_data(start_date=start_date,end_date=end_date,slice_type=slice_type,now_date=now_date)

        
        positions_value=0
        for ts_code,position in instance['positions'].items():
            amount=position['amount']
            if "dt_"+now_date in instance['dividend']:
                if ts_code in instance['dividend']["dt_"+now_date]:
                    if instance['dividend']["dt_"+now_date][ts_code]['cash_stk']>0:
                        instance['positions'][ts_code]['amount']=instance['positions'][ts_code]['amount']+instance['dividend']["dt_"+now_date][ts_code]['cash_stk']
                        instance['positions'][ts_code]['last_close']=position['total_value']/instance['positions'][ts_code]['amount']
                        bt.log(instance=instance,ts_code=ts_code,msg="发生送股事件，共%s股" % str(round(instance['dividend']["dt_"+now_date][ts_code]['cash_stk'],2)),type='warn')
            #now_price=market.get_price(ts_code,now_date,instance['cache'])
            if 'market_no_'+now_date+'_'+ts_code in instance['market_data']:
                now_price=instance['market_data']['market_no_'+now_date+'_'+ts_code]
            else:
                now_price=None
            if now_price!=None and "退" in now_price['name']: 
                sell_list.append(ts_code)
        for ts_code in sell_list:
            bt.sell(instance=instance,ts_code=ts_code,amount=instance['positions'][ts_code]['amount'],time='open')
    
    
    
    #盘后更新
    #处理分红事件，计算持仓信息
    def after_market(instance):
        now_date=instance['now_date']
        old_value=instance['old_value']
        positions_value=0

        
        key="dividend_"+now_date
        #div_info=instance['cache'].get(key)
        if key in instance['market_data']:
            div_info=json.loads(instance['market_data'][key])
            df_div=pd.DataFrame(div_info)            
        else:
            df_div=pd.DataFrame()
        # if div_info==None:
        #     df_div=pd.DataFrame()
        # else:
        #     div_info=json.loads(div_info)
        #     df_div=pd.DataFrame(div_info)

        if not df_div.empty:
            try:
                df_div['pay_date']=df_div.apply(lambda x: x.ex_date if x.pay_date==None else x.pay_date,axis=1)
            except Exception as e:
                print(df_div)
                exit()

        #分红现金
        div_cash=0
        for ts_code,position in instance['positions'].items():
            amount=position['amount']
            if ts_code in df_div.index:
                stk_div=float(df_div['stk_div'].at[ts_code])
                cash_div_tax=float(df_div['cash_div_tax'].at[ts_code])
                ex_date=df_div['ex_date'].at[ts_code]
                pay_date=df_div['pay_date'].at[ts_code]
                if pay_date==None:
                    pay_date=ex_date
                if pay_date==None:
                    continue
                dt_key="dt_"+pay_date
                if dt_key not in instance['dividend']:
                    instance['dividend'][dt_key]={}
                instance['dividend'][dt_key][ts_code]={
                    'cash_tax':amount*cash_div_tax,
                    'cash_stk':amount*stk_div
                }
            if "dt_"+now_date in instance['dividend']:
                if ts_code in instance['dividend']["dt_"+now_date]:
                    if instance['dividend']["dt_"+now_date][ts_code]['cash_tax']>0:
                        instance['cash']=instance['cash']+instance['dividend']["dt_"+now_date][ts_code]['cash_tax']
                        bt.log(instance=instance,ts_code=ts_code,msg="发生分红事件，共%s元" % str(round(instance['dividend']["dt_"+now_date][ts_code]['cash_tax'],2)),type='warn')
            try:
                #now_price=market.get_price(ts_code,now_date,instance['cache'])
                if 'market_no_'+now_date+'_'+ts_code in instance['market_data']:
                    now_price=instance['market_data']['market_no_'+now_date+'_'+ts_code]
                else:
                    now_price=None
                value=now_price['close']
            except Exception as e:
                value=0
            try:
                value=float(value)
                if(np.isnan(value) or value==0):
                    #bt.log(instance=instance,ts_code=ts_code,msg="每日统计失败，当做停牌处理",type='warn')
                    value=instance['positions'][ts_code]['last_close']
                else:
                    instance['positions'][ts_code]['last_close']=value
                    instance['positions'][ts_code]['total_value']=instance['positions'][ts_code]['amount']*value
            except Exception as e:
                value=0
            positions_value=positions_value+amount*value
        instance['position_value']=positions_value
        instance['total_value']=instance['cash']+positions_value
        instance['old_value']=instance['cash']+positions_value
        instance['returns'].append([now_date,instance['total_value']/old_value])
        instance['history'][now_date]=instance['positions']
        bt.log(instance,"账户余额："+str(instance['total_value'])+","+now_date)
        if instance['total_value']<0 :
            print("余额小于0")
            print(instance['cash'])
            print(positions_value)
            
            exit()

        return True
       

    def break_point(instance):
        info={
            "total_value":instance['total_value'],
            "positions_value":instance['position_value'],
            "cash":instance["cash"]
        }
        print(info)
        
       
  
    
    def log(instance,msg,ts_code='',type='info'):
        if instance['log_type']==0 or instance['log_type']==3:
            return
        now_date=instance['now_date']
        time=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        msgstr="%s %s %s [%s] %s" % (now_date,ts_code,msg,type,time)
        log_path=LOGS_DIR+"backtest/bt_"+instance['instance_id']+'.log'
        with open(log_path,'a') as f:
            f.writelines(msgstr+"\n")      
        instance['logs'].append(msgstr)
        
        if instance['log_type']==2:
            print(msgstr)
        
        return True
        
    def analyse(instance,benchmark='000001.SH',show=False):
        index=AStock.getIndexPrice(ts_code=benchmark,start_date=instance['start_date'],end_date=instance['end_date'])
        index['returns']=index['close']/index['close'].shift(1)
        index['values']=index['returns'].cumprod()
        index["trade_date"] = pd.to_datetime(index["trade_date"], format='%Y%m%d')
        index=index.set_index('trade_date') 
        
        if show:
            qs.extend_pandas()
            qs.reports.full(instance['returns']['values'], index['values'])

        returns=instance['returns']['returns']-1
        benchReturns=index['returns']-1
        
        try:
            alpha, beta = ey.alpha_beta(returns = returns, factor_returns = benchReturns, annualization=252) 
        except Exception as e:
            print(str(e))
            print(returns)
            print(instance)
            instance['risk']=False
            return False
        result = {}
        result["alpha"] = alpha
        result["beta"] = beta
        result['aggregate_returns']=ey.aggregate_returns(returns,convert_to='yearly')
        result['annual_return']=ey.annual_return(returns=returns, period='daily', annualization=252)
        result['cagr']=ey.cagr(returns)
        result['annual_volatility']=ey.annual_volatility(returns)
        result['info_ratio'] = ey.excess_sharpe(returns =  returns, factor_returns = benchReturns)
        #result['cum_returns(累积收益)']=ey.cum_returns(returns)
        result['downside_risk']=ey.downside_risk(returns)
        result['R2'] =ey.stability_of_timeseries(returns)
        result['sharpe'] = ey.sharpe_ratio(returns = returns, risk_free=0, period='daily', annualization=None)
        result['sortino'] = ey.sortino_ratio(returns = returns,required_return=0, period='daily', annualization=None, _downside_risk=None)
    
        
        result['calmar'] = ey.calmar_ratio(returns = returns,period='daily', annualization=None)
        result['omega'] = ey.omega_ratio(returns = returns, risk_free=0.0, required_return=0.0, annualization=252)
        result['max_down']=ey.max_drawdown(returns)
        
        result['sqn']=math.sqrt(instance['trade_num']) * np.mean(instance['trade_returns']) / np.std(instance['trade_returns'])
        #result['vola']=result['annual_volatility']
        result['rnorm']=result['annual_return']
        result['trade_num']=instance['trade_num']
        result['roto']=instance['total_value']/instance['init_cash']-1
        if(instance['trade_num']>0):
            result['win_ratio']=instance['win']/instance['trade_num']
        else:
            result['win_ratio']=0
        result['returns']=returns
        result['bench_returns']=benchReturns
        for key in result.keys():
            if key not in ['returns','bench_returns']:
                try:
                    if math.isnan(result[key]) or math.isinf(result[key]) :
                        result[key]=0
                except Exception as e:
                    result[key]=0
        
        instance['risk']=result
        return result
        
    def load_index():
        pass