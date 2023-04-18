from library.config import config
import pandas as pd
import traceback
import os
from library.util import util
from library.globalvar import *
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor, wait, ALL_COMPLETED

class factorRepair():
    #删除某日因子
    def deleteDate(trade_date='20000101'):
        code_factor_list=util.getFileList(CODE_FACTORS_DIR)
        single_factors_list=util.getFileList(SINGLE_FACTORS_DIR)
        date_factors_list=util.getFileList(DATE_FACTORS_DIR+trade_date)
        
        
        for date_factor in date_factors_list:
            os.remove(DATE_FACTORS_DIR+trade_date+'/'+date_factor)
        
        
        
        for code_factor in code_factor_list:
            df=pd.read_pickle(CODE_FACTORS_DIR+code_factor)
            df=df[df.trade_date!=trade_date]
            df.to_pickle(CODE_FACTORS_DIR+code_factor)

        
        
        # for single_factor in single_factors_list:
        #     df=pd.read_csv(SINGLE_FACTORS_DIR+single_factor, header=None, names=['ts_code','trade_date','alpha'],dtype={"trade_date": str})
        #     df=df[df.trade_date!=trade_date]
        #     df.to_csv(SINGLE_FACTORS_DIR+single_factor,mode='w',encoding='utf-8',header=False,index=False)


        tasklist=[]
        with ProcessPoolExecutor(max_workers=30) as pool:
            for single_factor in single_factors_list:
                mytask=pool.submit(factorRepair.deleteCsvDate,SINGLE_FACTORS_DIR+single_factor,trade_date)
                tasklist.append(mytask)
        
        wait(tasklist,return_when=ALL_COMPLETED)

        
        print('deleted all factors in '+trade_date)
        
        pass
        
        
    def deleteCsvDate(path,trade_date):
        df=pd.read_csv(path, header=None, names=['ts_code','trade_date','alpha'],dtype={"trade_date": str})
        df=df[df.trade_date!=trade_date]
        df.to_csv(path,mode='w',encoding='utf-8',header=False,index=False)        
        pass