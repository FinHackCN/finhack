import os
import pandas as pd
from filelock import FileLock  # 需要安装：pip install filelock
import time
from runtime.constant import *
from finhack.library.config import Config
# from finhack.factor.default.preCheck import preCheck
from finhack.factor.default.indicatorEngine import indicatorEngine
from finhack.factor.default.alphaEngine import alphaEngine
import finhack.library.log as Log

class taskRunner:
    @staticmethod
    def runTask(args):
        print(args)
        print("running task...")
        indicator_list = indicatorEngine.getIndicatorList(args.market,args.freq,args.task_list)
        indicatorEngine.computeIndicatorBatch(args.market,args.freq,indicator_list,start_date=args.start_date,end_date=args.end_date,process_num=args.process_num)
        alpha_list = alphaEngine.get_alpha_list(args.market,args.freq,args.task_list)
        alphaEngine.computeAlphaBatch(args.market,args.freq,alpha_list,start_date=args.start_date,end_date=args.end_date,process_num=args.process_num)
 
