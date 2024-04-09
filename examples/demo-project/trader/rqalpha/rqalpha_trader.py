from runtime.constant import *
import runtime.global_var as global_var
from finhack.library.class_loader import ClassLoader
import cloudpickle as pickle
import sys
import os
import re
import finhack.library.log as Log
from datetime import datetime
import json
from rqalpha import run_code
import hashlib
from finhack.library.mydb import mydb
import numpy as np
#finhack trader run --model_id=f7fd6531b6ec1ad6bc884ec5c6faeedb --strategy=ChatgptAIStrategy --vendor=qmt

class RqalphaTrader:

    myargs={}


    def load_strategy(self,strategy_name):
        if os.path.exists(f"{BASE_DIR}/strategy/{strategy_name}.py"):
            with open(f"{BASE_DIR}/strategy/{strategy_name}.py", 'r') as file:
                content = file.read()
                return content
        else:
            Log.logger.error(f"{BASE_DIR}strategy/{strategy_name}.py  NotFound")
            return ''
        pass   


    def insert_import(self,code):
        import_list=[
            'from finhack.core.classes.dictobj import DictObj',
            'from finhack.trader.default.function import load_preds_data'
        ]
        for line in import_list:
            code=line+"\n"+code
        return code

    def replace_init(self,code):
        replace_list={
            "initialize(context):":"initialize(context):\n    params="+str(self.myargs['params'].to_json())+"\n",
            'inout_cash':'#inout_cash',
            'set_benchmark':"#set_benchmark",
            'set_option':"#set_option",
            'set_order_cost':"#set_order_cost",
            'set_slippage':"#set_slippage",
            'context.trade.model_id':f"'{self.myargs['trade']['model_id']}'",
            "context.get('params', {})":'params',
            'log.info(':'print(',
            #'context.portfolio.positions':'get_positions()',
            'context.portfolio.positions.keys()':'get_position_keys()',
            "09:30":"09:31",
            "15:00":"14:59",
            "g.":"context.g.",
            "context.context":"context",
            "context.current_dt":"context.now",
            'cost_basis':"avg_price"
        }
        for k,v in replace_list.items():
            code=code.replace(k,v)
        return code



    def replace_run_daily_and_collect_names(self,code_str):
        # 编译一个正则表达式模式，用于匹配 run_daily 函数调用
        pattern = re.compile(r'run_daily\((.*?), time="(\d{2}):(\d{2})"\)')
        function_names = []  # 用于收集函数名

        # 定义一个替换函数
        def replacer(match):
            function_name = match.group(1).strip()
            hour = match.group(2)
            minute = match.group(3)
            function_names.append(function_name)  # 收集函数名
            # 返回替换后的字符串
            return f'scheduler.run_daily({function_name}, time_rule=physical_time(hour={int(hour)}, minute={int(minute)}))'

        # 使用正则表达式的 sub 方法进行替换
        new_code_str = pattern.sub(replacer, code_str)
        return new_code_str, function_names



 
    def replace_order_functions(self,code_str):
        # 编译一个正则表达式模式，用于匹配 order_* 函数调用
        # \w+ 匹配任意变量名
        pattern = re.compile(r'order_(target_value|value|buy|sell)\((\w+), ([\w\d]+)\)')


        # 定义一个替换函数
        def replacer(match):
            order_function = match.group(1)
            variable_name = match.group(2)
            value = match.group(3)
            # 返回替换后的字符串

            return f"order_{order_function}(check_stock_code({variable_name},'{self.myargs.trade.rule_list}'), {value}) if check_stock_code({variable_name},'{self.myargs.trade.rule_list}') else False"

        # 使用正则表达式的 sub 方法进行替换
        return pattern.sub(replacer, code_str)
    

    def append_code(self,code):
        # 获取当前文件的完整路径
        current_file_path = os.path.abspath(__file__)
        # 获取当前文件所在的目录
        current_dir = os.path.dirname(current_file_path)
        with open(f"{current_dir}/append_code.py", 'r') as file:
            content = file.read()
            code=code+"\n"+content
        return code



    def prase_args(self):
        args=self.args.__dict__
        from finhack.trader.default.context import context as mycontext
        mycontext['params']=args['params']
        mycontext['trade']['market']=args['market']
        mycontext['trade']['start_time']=args['start_time']
        mycontext['trade']['end_time']=args['end_time']
        mycontext['trade']['benchmark']=args['benchmark']
        mycontext['trade']['strategy']=args['strategy']
        strategy_path=f"{BASE_DIR}/strategy/{args['strategy']}.py"
        with open(strategy_path, 'r', encoding='utf-8') as file:
            mycontext['trade']['strategy_code'] = file.read()

        if args['params']!=None and args['params']!='':
            params=json.loads(args['params'])
            mycontext['params']=params
            if 'model_id' in params:
                mycontext['trade']['model_id']=params['model_id']
        else:
            mycontext['params']={}
        if args['model_id']!='':
            mycontext['trade']['model_id']=args['model_id']

        mycontext['trade']['slip']=float(args['slip'])
        mycontext['trade']['sliptype']=args['sliptype']
        mycontext['trade']['rule_list']=args['rule_list']

        mycontext['account']['username']=args['username']
        mycontext['account']['password']=args['password']
        mycontext['account']['account_id']=''


        mycontext['account']['open_tax']=args['open_tax']
        mycontext['account']['close_tax']=args['close_tax']
        mycontext['account']['open_commission']=args['open_commission']
        mycontext['account']['close_commission']=args['close_commission']
        mycontext['account']['close_today_commission']=args['close_today_commission']
        mycontext['account']['min_commission']=args['min_commission']


        mycontext['portfolio']['previous_value']=float(args['cash'])
        mycontext['portfolio']['total_value']=float(args['cash'])
        mycontext['portfolio']['cash']=float(args['cash'])
        mycontext['portfolio']['starting_cash']=float(args['cash'])

        context_json = str(args)+str(mycontext['trade'])+str(mycontext['account'])+str(mycontext['portfolio']['cash'])
        context_json = context_json.replace("'vendor': 'rqalpha'","'vendor': None")

        hash_value = hashlib.md5(context_json.encode()).hexdigest()
        if args['id']!='':
            mycontext['id']=args['id']
        else:
            mycontext['id']=hash_value

        self.myargs=mycontext


        # print(context_json)
        # print(hash_value)
        # exit()
        return self.myargs



    def insert_strategy_results(self,data, instance_id):
        try:
            data['excess_max_drawdown_duration']=''
            data['max_drawdown_duration']=''

            # 删除已存在的instance_id记录
            delete_query = f"DELETE FROM rqalpha WHERE instance_id = '{instance_id}';"
            mydb.exec(delete_query,'finhack')
            # 准备插入数据的SQL语句
            # 构建列名和占位符

            # 转换None或NaN为NULL，并将字符串值用引号包围
            def format_value(value):
                if value is None or (isinstance(value, float) and np.isnan(value)):
                    return 'NULL'
                elif isinstance(value, str):
                    value = value.replace("'", "''")  # 处理字符串中的单引号
                    return f"'{value}'"
                else:
                    return str(value)

            # 构建列名和值
            columns = ', '.join(data.keys())
            formatted_values = ', '.join(format_value(value) for value in data.values())

            # 构建完整的SQL语句
            insert_sql = f"INSERT INTO rqalpha (instance_id, {columns}) VALUES ('{instance_id}', {formatted_values});"

            print(insert_sql)
            mydb.exec(insert_sql,'finhack')
            # 提交到数据库执行
   
        except Exception as e:
            print("Error while connecting to MySQL", e)



    def run(self):
        self.prase_args()
        code=self.load_strategy(self.myargs['trade']['strategy'])
        code=self.insert_import(code)
        code=self.replace_init(code)
        code,func_names=self.replace_run_daily_and_collect_names(code)
        for func_name in func_names:
            code=code.replace(f"{func_name}(context)",f"{func_name}(context,bar_dict)")
        code=self.append_code(code)
        code=self.replace_order_functions(code)
        # print(code)
        # #print(self.myargs['params'])
        # exit()

        config = {
            "base": {
                "start_date": self.myargs['trade']['start_time'][0:10],
                "end_date": self.myargs['trade']['end_time'][0:10],
                "accounts": {
                    "stock": self.myargs['portfolio']['cash']
                }
            },
            "extra": {
                "log_level": "error",#verbose | code:info | warning | error
            },
            "mod": {
                "sys_progress": {
                        "enabled": True,
                        "show": True
                },
                "sys_analyser": {
                    "enabled": True,
                    "plot": True,
                    "output_file": f"{REPORTS_DIR}static/rqalpha/"+self.myargs['id']+".pkl",
                    "plot_save_file":f"{REPORTS_DIR}static/rqalpha/"+self.myargs['id']+".png",
                    "benchmark":self.myargs['trade']['benchmark'].replace('SH','XSHG')
                },
                "sys_transaction_cost":{
                    "enabled": True,
                    "cn_stock_min_commission": 5,
                    "commission_multiplier": 0.375,
                    "tax_multiplier": 1
                }
            }
        }


        
        if os.path.exists(f"{REPORTS_DIR}static/rqalpha/"+self.myargs['id']+".pkl") and self.args.__dict__['replace'].lower()[0:1]=='f':
            pass
        else:
            #print(code)
            run_code(code, config)
            result_dict = pickle.load(open(f"{REPORTS_DIR}static/rqalpha/"+self.myargs['id']+".pkl", "rb"))   # 从输出pickle中读取数据
            #print(result_dict["summary"])
            self.insert_strategy_results(result_dict["summary"],self.myargs['id'])