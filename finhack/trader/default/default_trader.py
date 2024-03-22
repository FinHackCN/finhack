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
from .context import context,g
from datetime import datetime
from finhack.library.mydb import mydb
from multiprocessing import Process,Pool,Semaphore


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
    
    
    
    def show(self):
        # 查询数据库中存储的数据
        sql = 'SELECT * FROM backtest WHERE instance_id="%s"' % (self.args.id)
        result = mydb.selectToDf(sql, 'finhack')

        if not result.empty:
            # 从查询结果中提取数据并还原到 context 变量中
            row = result.iloc[0]

            # 还原 trade 相关的字段
            context.trade.model_id = row['model']
            context.trade.strategy = row['strategy']
            context.trade.start_time = row['start_date']
            context.trade.end_time = row['end_date']
            context.trade.benchmark = row['benchmark']
            context.trade.strategy_code = row['strategy_code']

            # 还原 portfolio 相关的字段
            context.portfolio.starting_cash = row['init_cash']
            context.portfolio.total_value = row['total_value']

            # 还原 performance 相关的字段
            context.performance.returns = list(map(float, row['returns'].split(',')))
            context.performance.bench_returns = list(map(float, row['benchReturns'].split(',')))
            context.performance.indicators.alpha = float(row['alpha'])
            context.performance.indicators.beta = float(row['beta'])
            context.performance.indicators.annual_return = float(row['annual_return'])
            context.performance.indicators.cagr = float(row['cagr'])
            context.performance.indicators.annual_volatility = float(row['annual_volatility'])
            context.performance.indicators.info_ratio = float(row['info_ratio'])
            context.performance.indicators.downside_risk = float(row['downside_risk'])
            context.performance.indicators.R2 = float(row['R2'])
            context.performance.indicators.sharpe = float(row['sharpe'])
            context.performance.indicators.sortino = float(row['sortino'])
            context.performance.indicators.calmar = float(row['calmar'])
            context.performance.indicators.omega = float(row['omega'])
            context.performance.indicators.max_down = float(row['max_down'])
            context.performance.indicators.sqn = float(row['SQN'])
            context.performance.win = float(row['win'])
            context.performance.trade_num = int(row['trade_num'])
            
            calendar=Calendar.get_calendar(context.trade.start_time,context.trade.end_time,market='astock')


            calendar=pd.to_datetime(calendar)
            calendar=calendar[0:len(context.performance.returns)]


            context.performance.returns = pd.DataFrame(context.performance.returns, index=calendar)
            context.performance.bench_returns = pd.DataFrame(context.performance.bench_returns, index=calendar)


            Performance.show_chart(context)
            Performance.show_table(context)
            context.trade.strategy_code="..."
            print(context.trade)
            print("数据已从数据库查询并还原到 context 变量中。")
        else:
            print("未找到与当前实例ID相匹配的数据记录。")
    
    
    def run_trader(self, args):
        with self.semaphore:
            self.run(args)
    
    def auto(self):
        trader = DefaultTrader()
        self.semaphore = Semaphore(2)
        # 定义策略和模型ID的列表
        strategies = [
            ('AITopNStrategy', '4367901764ca783755ed8ccb19504bb7'),
            ('AITopNStrategy', '07b87ac507f3ab689ad3cdca75cb978c'),
            ('AITopNStrategy', '9570af1a7ddcb21663e7c402b599c554'),
            ('AITopNStrategy', '922687d0eedcb66fa43cacfb2b3faacf'),
            ('AITopNStrategy', 'e2971016152ebb19cd6c33f0d8c19765'),
            ('AITopNStrategy', 'd2f4e695254abcd539f058d63baf1074'),
            ('AITopNStrategy', '9211ba85c3a7e47e2552268bed21b15f'),
            ('AITopNStrategy', '7b80d7ccbf816068f137af3bb6861875'),
            # 更多策略和模型ID组合
        ]        
        # 创建并启动多个进程
        processes = []
        for strategy_name, model_id in strategies:
            args = self.args
            args.strategy = strategy_name
            args.model_id = model_id
            p = Process(target=self.run_trader, args=(args,))
            p.start()
            processes.append(p)

        # 等待所有进程完成
        for p in processes:
            p.join()    
    
    
    def run(self,args=None):
        if args!=None:
            self.args=args
        t1=time.time()
        starttime=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        init_context(self.args)
        
        
        Log.tlogger=Log.tLog(context.id,logs_dir=LOGS_DIR,background=self.args.background,level=self.args.log_level).logger
        log("正在初始化交易上下文")

        hassql='select id from backtest where instance_id="%s"' % (context.id)
        has=mydb.selectToDf(hassql,'finhack')
        if(not has.empty): 
            log("存在相同回测记录，本次回测结束！")
            return  




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
        
        

    
    
        returns_float_list = (context.performance.returns.values).tolist()
        returns_string_list = ['{:.8f}'.format(item) for item in returns_float_list]  # 保留8位小数
        returns_string = ','.join(returns_string_list)
        
        bench_float_list = (context.performance.bench_returns.values).tolist()
        bench_string_list = ['{:.8f}'.format(item) for item in bench_float_list]  # 保留8位小数
        bench_string = ','.join(bench_string_list)
        
    
        #from finhack.library.mydb import mydb
        
        
        
        features_list=''
        train=''
        
        if context.trade.model_id!='':
            model=mydb.selectToDf('select * from auto_train where hash="'+context.trade.model_id+'"','finhack')
            if(not model.empty):  
                model=model.iloc[0]
                features_list=model['features']
                train=model['algorithm']+"_"+model['loss']

        sql="INSERT INTO `finhack`.`backtest`(`instance_id`,`features_list`, `train`, `model`, `strategy`, `start_date`, `end_date`, `init_cash`, `args`, `history`, `returns`, `logs`, `total_value`, `alpha`, `beta`, `annual_return`, `cagr`, `annual_volatility`, `info_ratio`, `downside_risk`, `R2`, `sharpe`, `sortino`, `calmar`, `omega`, `max_down`, `SQN`,filter,win,server,trade_num,runtime,starttime,endtime,benchReturns,roto,benchmark,strategy_code) VALUES ( '%s','%s', '%s', '%s', '%s', '%s', '%s', %s, '%s', '%s', '%s', '%s', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,'%s',%s,'%s',%s,'%s','%s','%s','%s','%s','%s','%s')" % \
        (context.id,  \
        features_list,  \
        train,  \
        context.trade.model_id,  \
        context.trade.strategy,  \
        context.trade.start_time,  \
        context.trade.end_time,  \
        context.portfolio.starting_cash,  \
        str(context.args).replace("'",'"'),  \
        'history',  \
        returns_string,  \
        'logs',  \
        context.portfolio.total_value,  \
        str(context.performance.indicators.alpha),  \
        str(context.performance.indicators.beta),  \
        str(context.performance.indicators.annual_return),  \
        str(context.performance.indicators.cagr),
        str(context.performance.indicators.annual_volatility),  \
        str(context.performance.indicators.info_ratio),  \
        str(context.performance.indicators.downside_risk),  \
        str(context.performance.indicators.R2),  \
        str(context.performance.indicators.sharpe),  \
        str(context.performance.indicators.sortino),  \
        str(context.performance.indicators.calmar),  \
        str(context.performance.indicators.omega),  \
        str(context.performance.indicators.max_down),  \
        str(context.performance.indicators.sqn),  \
        'filters_name',  \
        str(context.performance.win),  \
        'woldy-PC',  \
        str(context.performance.trade_num),  \
        str(time.time()-t1),  \
        str(starttime),  \
        str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),  \
        bench_string,  \
        str(context.performance.indicators.roto),  \
        context.trade.benchmark,  \
        context.trade.strategy_code.replace("'", "\\'") \
        
        ) 
        #print(sql)
        mydb.exec('delete from backtest where instance_id="%s"' % (context.id),'finhack') 
        mydb.exec(sql,'finhack')
        
    
