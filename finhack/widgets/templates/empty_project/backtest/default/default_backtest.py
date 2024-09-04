import os
import multiprocessing
from finhack.trader.default.function import load_preds_data,delete_preds_data
from finhack.library.mydb import mydb
from finhack.trader.default.data import Data
from finhack.factor.default.factorManager import factorManager
from runtime.constant import *
from finhack.library.config import Config 
import itertools
import json
import psutil
import time
import glob
import gc
import sys
import re
from finhack.core.classes.dictobj import DictObj

class DefaultBacktest():
    def __init__(self):
        pass


    def del_model_if_idle():
        
        keyword = "finhack"
        file_pattern = os.path.join(PREDS_DIR, 'model_*_pred.pkl')
        for file_path in glob.glob(file_pattern):
            # 从文件路径中提取文件名
            file_name = os.path.basename(file_path)
            model_id = file_name[len('model_'):-len('_pred.pkl')]
            idle=True
            #print(model_id)
            for process in psutil.process_iter(['pid', 'name', 'cmdline']):
                # 检查进程名称或命令行中是否包含关键字
                if keyword.lower() in process.info['name'].lower() or \
                any(keyword.lower() in cmd_part.lower() for cmd_part in process.info['cmdline']):
                    if model_id in str(process.info):
                        idle=False
                        break
            if idle:
                pred_data_path=PREDS_DIR+f"model_{model_id}_pred.pkl"
                if os.path.exists(pred_data_path):
                    # 获取文件的最后修改时间
                    last_modified_time = os.path.getmtime(pred_data_path)
                    # 获取当前时间
                    current_time = time.time()
                    # 计算时间差，以秒为单位
                    time_diff = current_time - last_modified_time
                    # 将时间差转换为分钟
                    time_diff_minutes = time_diff / 60
                    if time_diff_minutes > 30:
                        #print(f"文件 {pred_data_path} 最后修改时间超过30分钟。")
                        os.remove(pred_data_path)

    def run_command_with_semaphore(self, cmd, semaphore):
        try:
            DefaultBacktest.del_model_if_idle()
        except Exception as e:
            print(f'An error occurred: {e}')
            
        with semaphore:
            try:
                os.system(cmd)
            except Exception as e:
                print(f'An error occurred: {e}')


    def rqalpha(self):

        if hasattr(sys, 'frozen'):  # 特别检查，如果是使用 PyInstaller 等工具打包的可执行文件
            entry_file_path = os.path.realpath(sys.executable)
        else:
            entry_file_path = os.path.realpath(sys.argv[0])
        
        sql=""
        path=f"{BASE_DIR}/data/config/sqllist/backtest/bt_list.sql"
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as file:
                sql= file.read()
        else:
            sql="SELECT id, instance_id, features_list, train, model, strategy, start_date, end_date, init_cash, params, total_value, alpha, beta, annual_return, cagr, annual_volatility, info_ratio, downside_risk, R2, sharpe, sortino, calmar, omega, max_down, SQN, created_at, filter, win, server, trade_num, runtime, starttime, endtime,  roto, simulate, benchmark, strategy_code FROM `finhack`.`backtest`  order by sharpe desc LIMIT 1000"
        bt_list=mydb.selectToList(sql,'finhack')   
        
        def to_json(s):
            dict_str = re.sub(r"DictObj\((.*?)\)", r"{\1}", s)
            dict_str = re.sub(r"(\w+)=('[^']*'|\"[^\"]*\")", r'"\1": \2', dict_str)
            dict_str = dict_str.replace("'", '"')
            return dict_str
        semaphore = multiprocessing.Semaphore(3) 
        processes = []
        for bt in bt_list:
            cmd=f"{entry_file_path} trader run --strategy={bt['strategy']} --log_level=ERROR --id={bt['instance_id']} --model_id={bt['model']} --cash={int(bt['init_cash'])}   --project_path={BASE_DIR}  --params='{to_json(bt['params'])}' --replace=True --vendor=rqalpha"
            p = multiprocessing.Process(target=self.run_command_with_semaphore, args=(cmd, semaphore))
            processes.append(p)
            p.start()
           

    def run(self):
        # 获取入口文件的完整路径
        if hasattr(sys, 'frozen'):  # 特别检查，如果是使用 PyInstaller 等工具打包的可执行文件
            entry_file_path = os.path.realpath(sys.executable)
        else:
            entry_file_path = os.path.realpath(sys.argv[0])

        Data.init_data(cache=True)
        cash_list = self.args.cash.split(',')
        strategy_list = self.args.strategy.split(',')
        model_list = mydb.selectToDf('select * from auto_train order by rand()', 'finhack')
        
        semaphore = multiprocessing.Semaphore(int(self.args.process))  # 创建一个信号量，最大允许process个进程同时运行
        # print(model_list)
        # exit()


        for row in model_list.itertuples():
            model_hash = getattr(row, 'hash')
            features = getattr(row, 'features')
            features=features.split(',')
            factor_list=factorManager.getFactorsList(valid=False,ignore=False)
            features_set = set(features)
            factor_set = set(factor_list)

            if not features_set.issubset(factor_set):
                complement_set = features_set - factor_set
                print("choose factor error:"+str(complement_set))
                continue

            cfgTrade=Config.get_config('args','trader')

            pt = multiprocessing.Process(target=load_preds_data, args=(model_hash, True, 'lightgbm', cfgTrade['start_time'], cfgTrade['end_time']))
            # 启动进程
            pt.start()
            # 等待进程完成
            pt.join()           
            # preds=load_preds_data(model_id=model_hash, cache=True,trainer='lightgbm',start_time=cfgTrade['start_time'],end_time=cfgTrade['end_time'])
            # del preds
            # gc.collect()
            #continue
            processes = []
            for cash in cash_list:
                for strategy_name in strategy_list:
                    s_params={}

                    strategy_params=Config.get_config('backtest','params')
                    for k,v in strategy_params.items():
                        s_params[k]=v

                    strategy_params=Config.get_config('backtest',strategy_name)
                    for k,v in strategy_params.items():
                        s_params[k]=v
                    # 将每个键对应的字符串分割为列表
                    split_values = {k: v.split(',') for k, v in s_params.items()}
                    # 使用itertools.product生成所有可能的组合
                    product_combinations = itertools.product(*split_values.values())
                    # 为每个组合创建一个字典，并将它们组成一个列表
                    params_list = [dict(zip(split_values.keys(), combination)) for combination in product_combinations]
                    # 打印结果
                    for params in params_list:
                        time.sleep(1)
                        active_processes = len(multiprocessing.active_children())
                        available_memory=1
                        total_memory=100
                        while  available_memory/total_memory<0.1 or active_processes>int(self.args.process):
                            time.sleep(1)
                            active_processes = len(multiprocessing.active_children())
                            total_memory = psutil.virtual_memory().total
                            available_memory = psutil.virtual_memory().available
                            #print(available_memory/total_memory)
                        cmd = f"{entry_file_path} trader run --strategy={strategy_name} --log_level=ERROR --model_id={model_hash}  --cash={cash} --project_path={BASE_DIR} --params='{json.dumps(params)}'"
                        # 创建Process对象，传入函数和需要的参数，包括信号量
                        p = multiprocessing.Process(target=self.run_command_with_semaphore, args=(cmd, semaphore))
                        processes.append(p)
                        p.start()
    
            # # 等待当前model的所有进程完成
            # for p in processes:
            #     p.join()
    
            # 当前model的所有进程执行完毕，可以进行下一个model的处理
            # delete_preds_data(model_hash)
    
     