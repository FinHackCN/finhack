from runtime.constant import *
import runtime.global_var as global_var
from finhack.library.class_loader import ClassLoader
import pickle
import sys
import os
import importlib
import finhack.library.log as Log
from trader.qmt.calendar import Calendar
from trader.qmt.event import Event
from trader.qmt.data import Data
from trader.qmt.function import *
from trader.qmt.object import *
from trader.qmt.context import context,g
from datetime import datetime


#finhack trader run --model_id=4367901764ca783755ed8ccb19504bb7 --strategy=QMTStrategy --vendor=qmt

class QmtTrader:
    def load_strategy(self,strategy_name):
        if os.path.exists(f"{BASE_DIR}/strategy/{strategy_name}.py"):
            module_spec = importlib.util.spec_from_file_location('strategy', f"{BASE_DIR}/strategy/{strategy_name}.py")
            strategy_module = importlib.util.module_from_spec(module_spec)
            module_spec.loader.exec_module(strategy_module)
            return strategy_module
        else:
            Log.logger.error(f"{BASE_DIR}strategy/{strategy_name}.py  NotFound")
        pass    



    def save_context(self,context):
        context_id=context.id
        running_dir = RUNNING_DIR
        event_list_tmp=context.data.event_list
        preds_tmp=context.g.preds
        context.g.preds=None
        context.data.event_list=[]
        context_file_path = os.path.join(running_dir, f"{context_id}.pkl")
        with open(context_file_path, 'wb') as context_file:
            pickle.dump(context, context_file)
        context.data.event_list=event_list_tmp
        context.g.preds=preds_tmp
        Log.logger.info(f"Context saved to {context_file_path}")

    def load_context(self,context):
        context_id=context.id
        running_dir = RUNNING_DIR
        context_file_path = os.path.join(running_dir, f"{context_id}.pkl")
        if os.path.exists(context_file_path):
            with open(context_file_path, 'rb') as context_file:
                loaded_context = pickle.load(context_file)
            Log.logger.info(f"Context loaded from {context_file_path}")
            return loaded_context
        else:
            Log.logger.error(f"Context file {context_file_path} not found")
            return None 


    def run(self,args=None):
        global context
        global g
        if args!=None:
            self.args=args
        t1=time.time()
        starttime=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        init_context(self.args)
        loaded_context = self.load_context(context)
        if loaded_context is not None:
            context = loaded_context
        else:
            pass

        start_time=context['trade']['start_time']
        end_time=context['trade']['end_time']
        market=context['trade']['market']
        

        Log.tlogger=Log.tLog(context.id,logs_dir=LOGS_DIR,background=self.args.background,level=self.args.log_level).logger
        log("正在初始化交易上下文")
        log("正在获取交易日历")

        
       
        calendar=Calendar.get_calendar(start_time,end_time,market=market)
        
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

        
 

        if loaded_context is not None:
            pass
        else:
            strategy.initialize(context)

        self.save_context(context)

        while context.data.event_list:
            event = context.data.event_list.pop(0)
            event_time = datetime.strptime(event['event_time'], '%Y-%m-%d %H:%M:%S')
            current_time = datetime.now()  # 去除毫秒部分
            context.current_dt=current_time
            # 如果 event_time 比当前时间晚，则将事件放回列表并等待
            if event_time > current_time:
                context.data.event_list.insert(0, event)  # 将事件放回列表
                time_to_wait = (event_time - current_time).total_seconds()
                print(f"下次事件时间{event_time}，将在{time_to_wait}秒后执行，正在sleep等待…")
                time.sleep(time_to_wait)  # 等待直到事件时间到达
            else:
                # 如果 event_time 在当前时间的10秒钟以内
                if 0 <= (current_time - event_time).total_seconds() < 10:
                    # 打印信息（可选）
                    print("do something")
                    print(event_time)
                    print(current_time)
                    print(event)
                    strategy.sync(context)
                    print('xxx')
                    event['event_func'](context)  # 执行事件函数
                else:
                    continue  # 如果时间早于当前时间超过10秒，则跳过此事件

            # 更新context的日期信息
            if context.previous_date is None or context.current_dt is None:
                pass
            elif context.current_dt.date() != event_time.date():
                context.previous_date = context.current_dt.date()
            context.current_dt = event_time

            # 打印信息（可选）
            print(event_time)
            print(current_time)
            self.save_context(context)