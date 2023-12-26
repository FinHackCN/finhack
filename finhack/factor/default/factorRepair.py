import os
import traceback
import pandas as pd
from runtime.constant import *
from library.config import Config
from finhack.library.util import util
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




        
        print('deleted all factors in '+trade_date)
        
        pass
        
        
    def deleteCsvDateFactor(path,trade_date):
        df=pd.read_csv(path, header=None, names=['ts_code','trade_date','alpha'],dtype={"trade_date": str})
        df=df[df.trade_date!=trade_date]
        df.to_csv(path,mode='w',encoding='utf-8',header=False,index=False)        
        pass
    
    #写入单个
    def writeCsvDateFactor(factor,start_date='',end_date=''):
            if os.path.exists(DATE_FACTORS_DIR+trade_date+'/'+factor): 
                df=pd.read_csv(DATE_FACTORS_DIR+trade_date+'/'+factor,encoding="utf-8-sig", names=["ts_code",'trade_date','alpha'])
                if len(df)>5000:
                    return True
                else:
                    print(factor+":"+str(len(df)))
            df=pd.read_csv(SINGLE_FACTORS_DIR+factor, header=None, names=['ts_code','trade_date','alpha'],dtype={"trade_date": str})
            if start_date!='':
                df=df[df.trade_date>=start_date]
            if end_date!='':
                df=df[df.trade_date<=end_date]   
            
            trade_date_list=set(df['trade_date'].values)
            for trade_date in trade_date_list:
                if not os.path.exists(DATE_FACTORS_DIR+trade_date): 
                    try:
                        os.mkdir(DATE_FACTORS_DIR+trade_date)
                    except Exception as e:
                        print(str(e))  
                df_date=df[df.trade_date==trade_date]
                df_date=df_date.to_csv(DATE_FACTORS_DIR+trade_date+'/'+factor,mode='w',encoding='utf-8',header=False,index=False)
  
    
    
    def setDateFactors(start_date='20220101',end_date='20991231'):
        single_factors_list=util.getFileList(SINGLE_FACTORS_DIR)
        tasklist=[]
        with ProcessPoolExecutor(max_workers=30) as pool:
            for single_factor in single_factors_list:
                #factorRepair.writeCsvDateFactor(single_factor,start_date,end_date)
                mytask=pool.submit(factorRepair.writeCsvDateFactor,single_factor,start_date,end_date)
                tasklist.append(mytask)
        wait(tasklist,return_when=ALL_COMPLETED)
        print('setDateFactors done')
            
        pass