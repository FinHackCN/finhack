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
class DefaultBacktest():
    def __init__(self):
        pass

    def run_command_with_semaphore(self, cmd, semaphore):
        with semaphore:
            try:
                os.system(cmd)
            except Exception as e:
                print(f'An error occurred: {e}')

    def run(self):
        Data.init_data(cache=True)
        cash_list = self.args.cash.split(',')
        strategy_list = self.args.strategy.split(',')
        model_list = mydb.selectToDf('select * from auto_train order by score desc', 'finhack')
        
        semaphore = multiprocessing.Semaphore(int(self.args.p))  # 创建一个信号量，最大允许p个进程同时运行
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

            load_preds_data(model_hash, True)
            processes = []
    



    
            for cash in cash_list:
                for strategy_name in strategy_list:
                    s_args={}

                    strategy_args=Config.get_config('backtest','args')
                    for k,v in strategy_args.items():
                        s_args[k]=v

                    strategy_args=Config.get_config('backtest',strategy_name)
                    for k,v in strategy_args.items():
                        s_args[k]=v
                    # 将每个键对应的字符串分割为列表
                    split_values = {k: v.split(',') for k, v in s_args.items()}
                    # 使用itertools.product生成所有可能的组合
                    product_combinations = itertools.product(*split_values.values())
                    # 为每个组合创建一个字典，并将它们组成一个列表
                    args_list = [dict(zip(split_values.keys(), combination)) for combination in product_combinations]
                    # 打印结果
                    for args in args_list:
                        cmd = f"finhack trader run --strategy={strategy_name} --log_level=ERROR --model_id={model_hash}  --cash={cash} --project_path={BASE_DIR} --args='{json.dumps(args)}'"
                        # 创建Process对象，传入函数和需要的参数，包括信号量
                        p = multiprocessing.Process(target=self.run_command_with_semaphore, args=(cmd, semaphore))
                        processes.append(p)
                        p.start()
    
            # 等待当前model的所有进程完成
            for p in processes:
                p.join()
    
            # 当前model的所有进程执行完毕，可以进行下一个model的处理
            delete_preds_data(model_hash)
    
     