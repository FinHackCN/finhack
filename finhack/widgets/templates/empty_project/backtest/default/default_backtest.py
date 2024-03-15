import os
import multiprocessing
from finhack.trader.default.function import load_preds_data,delete_preds_data
from finhack.library.mydb import mydb
from finhack.trader.default.data import Data
from finhack.factor.default.factorManager import factorManager
from runtime.constant import *

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
    

        for row in model_list.itertuples():
            model_hash = getattr(row, 'hash')
            features = getattr(row, 'features')
            features=features.split(',')
            factor_list=factorManager.getFactorsList(valid=False,ignore=False)
            features_set = set(features)
            factor_set = set(factor_list)

            if not features_set.issubset(factor_set):
                continue

            load_preds_data(model_hash, True)
            processes = []
    
            for cash in cash_list:
                for strategy in strategy_list:
                    cmd = f"finhack trader run --strategy={strategy} --log_level=ERROR --model_id={model_hash}  --cash={cash} --project_path={BASE_DIR}"
                    # 创建Process对象，传入函数和需要的参数，包括信号量
                    p = multiprocessing.Process(target=self.run_command_with_semaphore, args=(cmd, semaphore))
                    processes.append(p)
                    p.start()
    
            # 等待当前model的所有进程完成
            for p in processes:
                p.join()
    
            # 当前model的所有进程执行完毕，可以进行下一个model的处理
            delete_preds_data(model_hash)
    
     