import os
import pandas as pd

from runtime.constant import *
from finhack.library.config import Config
from finhack.factor.default.preCheck import preCheck
from finhack.factor.default.indicatorCompute import indicatorCompute
from finhack.factor.default.alphaEngine import alphaEngine
from finhack.market.astock.astock import AStock
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor, wait, ALL_COMPLETED




    
class taskRunner:
    def runTask(taskName='all'):
        
        c_list=preCheck.checkAllFactors() #chenged factor，代码发生变化
        if taskName=='all':
            task_list=Config.get_section_list('task')
        else:
            task_list=[taskName]

        os.system('rm -rf '+CACHE_DIR+'/single_factors_tmp1/*')
        os.system('rm -rf '+CACHE_DIR+'/single_factors_tmp2/*')
        

        
        #遍历任务列表
        for task in task_list:
            task=Config.get_config('task',task)
            factor_lists=task['list'].split(',')
            for factor_list_name in factor_lists:
                #factor列表

                if os.path.exists(CONFIG_DIR+"/factorlist/indicatorlist/"+factor_list_name):
                    
                    with open(CONFIG_DIR+"/factorlist/indicatorlist/"+factor_list_name, 'r', encoding='utf-8') as f:
                        factor_list=[_.rstrip('\n') for _ in f.readlines()]
                    indicatorCompute.computeList(list_name=factor_list_name,factor_list=factor_list,c_list=c_list)
            
         
         
            #continue
            #alpha列表
            
            for factor_list_name in factor_lists:
                if os.path.exists(CONFIG_DIR+"/factorlist/alphalist/"+factor_list_name):
                    with open(CONFIG_DIR+"/factorlist/alphalist/"+factor_list_name, 'r', encoding='utf-8') as f:
                        factor_list=[_.rstrip('\n') for _ in f.readlines()]
                        i=0
                        with ProcessPoolExecutor(max_workers=8) as pool:
                            
                            for factor in factor_list:   
                                i=i+1
                                alpha_name=factor_list_name+'_'+str(i).zfill(3)
                                mytask=pool.submit(alphaEngine.calc,factor,pd.DataFrame(),alpha_name)
                                
                                #alphaEngine.calc(factor,pd.DataFrame(),alpha_name)
                                #exit()
        os.system('mv '+CACHE_DIR+'/single_factors_tmp2/* '+SINGLE_FACTORS_DIR)
    
    
    
    
    