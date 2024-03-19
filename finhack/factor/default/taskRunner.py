import os
import pandas as pd

from runtime.constant import *
from finhack.library.config import Config
from finhack.factor.default.preCheck import preCheck
from finhack.factor.default.indicatorCompute import indicatorCompute
from finhack.factor.default.alphaEngine import alphaEngine
from finhack.market.astock.astock import AStock
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor, wait, ALL_COMPLETED
from finhack.factor.default.factorPkl import factorPkl


    
class taskRunner:
    def runTask(task_list):
        c_list=preCheck.checkAllFactors() #chenged factor，代码发生变化

        os.system('rm -rf '+CACHE_DIR+'/single_factors_tmp1/*')
        os.system('rm -rf '+CACHE_DIR+'/single_factors_tmp2/*')
        
        #需要刷新价格数据
        AStock.getStockDailyPrice(code_list=[],where="",startdate='',enddate='',fq='hfq',db='tushare',cache=False)
        
        #遍历任务列表
        task_list=task_list.split(',')
        if True:
            for factor_list_name in task_list:
                #factor列表
            
                if os.path.exists(CONFIG_DIR+"/factorlist/indicatorlist/"+factor_list_name):
                    
                    with open(CONFIG_DIR+"/factorlist/indicatorlist/"+factor_list_name, 'r', encoding='utf-8') as f:
                        factor_list=[_.rstrip('\n') for _ in f.readlines()]
                    #print(factor_list)
                    indicatorCompute.computeList(list_name=factor_list_name,factor_list=factor_list,c_list=c_list)
            
         
         
            #continue
            #alpha列表
            
            for factor_list_name in task_list:
                if os.path.exists(CONFIG_DIR+"/factorlist/alphalist/"+factor_list_name):
                    with open(CONFIG_DIR+"/factorlist/alphalist/"+factor_list_name, 'r', encoding='utf-8') as f:
                        factor_list=[_.rstrip('\n') for _ in f.readlines()]
                        i=0
                        with ProcessPoolExecutor(max_workers=8) as pool:
                            
                            for factor in factor_list:   
                                i=i+1
                                alpha_name=factor_list_name+'_'+str(i).zfill(3)
                                mytask=pool.submit(alphaEngine.calc,factor,pd.DataFrame(),alpha_name,False,True)
                                
                                #alphaEngine.calc(factor,pd.DataFrame(),alpha_name)
                                #exit()
        os.system('mv '+CACHE_DIR+'/single_factors_tmp2/* '+SINGLE_FACTORS_DIR)
        factorPkl.save()
        
    
    
    
    
    