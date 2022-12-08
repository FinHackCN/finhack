import os
from library.config import config
from factors.preCheck import preCheck
from factors.indicatorCompute import indicatorCompute
from factors.alphaEngine import alphaEngine
from library.astock import AStock
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor, wait, ALL_COMPLETED
import pandas as pd

class taskRunner:
    def runTask(taskName='all'):
        c_list=preCheck.checkAllFactors() #chenged factor，代码发生变化
        if taskName=='all':
            task_list=config.getSectionList('task')
        else:
            task_list=[taskName]

        mypath=os.path.dirname(os.path.dirname(__file__))
        os.system('rm -rf '+mypath+'/data/single_factors_tmp1/*')
        os.system('rm -rf '+mypath+'/data/single_factors_tmp2/*')
        #遍历任务列表
        for task in task_list:
            task=config.getConfig('task',task)
            factor_lists=task['list'].split(',')
            for factor_list_name in factor_lists:
                #factor列表
                if os.path.exists(mypath+"/lists/indicatorlist/"+factor_list_name):
                    
                    with open(mypath+"/lists/indicatorlist/"+factor_list_name, 'r', encoding='utf-8') as f:
                        factor_list=[_.rstrip('\n') for _ in f.readlines()]
                    indicatorCompute.computeList(list_name=factor_list_name,factor_list=factor_list,c_list=c_list)
            os.system('mv '+mypath+'/data/single_factors_tmp2/* '+mypath+'/data/single_factors/')
         
            #alpha列表
            
            for factor_list_name in factor_lists:
                if os.path.exists(mypath+"/lists/alphalist/"+factor_list_name):
                    with open(mypath+"/lists/alphalist/"+factor_list_name, 'r', encoding='utf-8') as f:
                        factor_list=[_.rstrip('\n') for _ in f.readlines()]
                        i=0
                        
                        with ProcessPoolExecutor(max_workers=16) as pool:
                            for factor in factor_list:   
                                i=i+1
                                alpha_name=factor_list_name+'_'+str(i).zfill(3)
                                mytask=pool.submit(alphaEngine.calc,factor,pd.DataFrame(),alpha_name)
    
    
    
    
    