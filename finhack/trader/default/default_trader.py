from runtime.constant import *
import runtime.global_var as global_var
from finhack.library.class_loader import ClassLoader

import os
import importlib
import finhack.library.log as Log
from .calendar import Calendar
from .event import Event
from .data import Data
from .function import *
from .object import *
from .performance import Performance 
import runtime.global_var as global_var
from runtime.constant import LOGS_DIR
from .context import context,g
from datetime import datetime
class DefaultTrader:
    def load_strategy(self,strategy_name):
        if os.path.exists(f"{BASE_DIR}/strategy/{strategy_name}.py"):
            module_spec = importlib.util.spec_from_file_location('strategy', f"{BASE_DIR}/strategy/{strategy_name}.py")
            strategy_module = importlib.util.module_from_spec(module_spec)
            module_spec.loader.exec_module(strategy_module)
            return strategy_module
        else:
            Log.logger.error(f"{BASE_DIR}strategy/{strategy_name}.py  NotFound")
        pass    
    
    def run(self):
        
        init_context(self.args)
        
        
        Log.tlogger=Log.tLog(context.id,logs_dir=LOGS_DIR,background=self.args.background,level=self.args.log_level).logger
        log("正在初始化交易上下文")


        start_time=context['trade']['start_time']
        end_time=context['trade']['end_time']
        market=context['trade']['market']
        
        log("正在获取交易日历")
        calendar=Calendar.get_calendar(start_time,end_time,market=market)
        
        log("正在初始化行情及市场数据")
        Data.init_data(cache=True)
        
        

        
        log("正在加载交易策略")
        strategy=self.load_strategy(context['trade']['strategy'])
        context['data']['calendar']=calendar
        event_list=Event.load_event(context,start_time,end_time)
        
        log("正在绑定交易对象")
        bind_object(strategy)
        
        log("正在绑定交易动作")
        bind_action(strategy)
        strategy.log=Log.logger
        strategy.g=g
        
        log("正在执行策略initialize函数")
        strategy.initialize(context)
        
        #这里后面放到redis里，这样就可以模拟和实盘了
        # print(context)
        # print(context.portfolio)
        
        while context.data.event_list:
            event = context.data.event_list.pop(0)
            #print(event['event_time'],event['event_name'],event['event_type'])
            event_time=datetime.strptime(event['event_time'], '%Y-%m-%d %H:%M:%S')
                # 如果 event_time 大于当前时间，退出循环
            if event_time > datetime.now():
                break
            if context.previous_date==None or context.current_dt==None:
                pass
            elif context.current_dt.date() != event_time.date():
                context.previous_date=context.current_dt.date()
            context.current_dt=event_time
       

            
            event['event_func'](context)
            
            # exit()
        
        Performance.analyse(context)
        
    
    

        

        
        
    
