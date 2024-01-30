import os
import multiprocessing
from finhack.trader.default.function import load_preds_data
from finhack.library.mydb import mydb
from finhack.trader.default.data import Data

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
        model_list = mydb.selectToDf('select * from auto_train order by score desc', 'finhack')
        
        semaphore = multiprocessing.Semaphore(int(self.args.p))  # 创建一个信号量，最大允许3个进程同时运行
        processes = []
        
        for row in model_list.itertuples():
            model_hash = getattr(row, 'hash')
            load_preds_data(model_hash, True)
            for cash in cash_list:
                cmd = 'finhack trader run --strategy=AITopNStrategy --log_level=ERROR --model_id=' + model_hash + ' --cash=' + cash
                print(cmd)
                # 创建Process对象，传入函数和需要的参数，包括信号量
                p = multiprocessing.Process(target=self.run_command_with_semaphore, args=(cmd, semaphore))
                processes.append(p)
                p.start()

        # 等待所有进程完成
        for p in processes:
            p.join()
 