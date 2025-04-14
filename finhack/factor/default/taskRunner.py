import os
import pandas as pd
from filelock import FileLock  # 需要安装：pip install filelock
import time
from runtime.constant import *
from finhack.library.config import Config
# from finhack.factor.default.preCheck import preCheck
from finhack.factor.default.indicatorEngine import indicatorEngine
from finhack.factor.default.alphaEngine import alphaEngine
from finhack.market.astock.astock import AStock
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, wait, ALL_COMPLETED
from finhack.factor.default.factorPkl import factorPkl
import finhack.library.log as Log

class taskRunner:


    def get_alpha_list(market,freq,task_list):
        """
        获取所有的alpha列表
        """
        alpha_list = []
        for root, dirs, files in os.walk(CONFIG_DIR + f"/factorlist/alphalist/{market}/{freq}/"):
            for file in files:
                if file in task_list.split(','):
                    with open(os.path.join(root, file), 'r', encoding='utf-8') as f:
                        lines = [line.strip() for line in f.readlines()]
                        alpha_list.extend(lines)
        for root, dirs, files in os.walk(CONFIG_DIR + f"/factorlist/alphalist/{market}/x{freq}/"):
            for file in files:
                if file in task_list.split(','):
                    with open(os.path.join(root, file), 'r', encoding='utf-8') as f:
                        lines = [line.strip() for line in f.readlines()]
                        alpha_list.extend(lines)
        return alpha_list



    @staticmethod
    def runTask(args):
        print(args)
        print("running task...")
        indicator_list = indicatorEngine.getIndicatorList(args.market,args.freq,args.task_list)
        alpha_list = taskRunner.get_alpha_list(args.market,args.freq,args.task_list)
        indicator_relation = indicatorEngine.getAllIndicatorRelation(indicator_list, args.market, args.freq)
        print("parallel_groups:", indicator_relation['parallel_groups'])



        # """
        # 运行因子计算任务
        
        # Parameters:
        # -----------
        # task_list : str
        #     要计算的任务列表，以逗号分隔
        # force_factors : list, optional
        #     需要强制重新计算的因子列表
        # """
        # c_list = preCheck.checkAllFactors()  # chenged factor，代码发生变化

        # # 如果没有提供强制刷新的因子列表，则创建空列表
        # if force_factors is None:
        #     force_factors = []
            
        # # 记录强制刷新的因子
        # force_file = CACHE_DIR + '/force_factors.txt'
        # if len(force_factors) > 0:
        #     with open(force_file, 'w', encoding='utf-8') as f:
        #         for factor in force_factors:
        #             f.write(factor + '\n')
        #     Log.logger.info(f"将强制重新计算以下因子: {','.join(force_factors)}")
        # elif os.path.exists(force_file):
        #     os.remove(force_file)

        # os.system('rm -rf ' + CACHE_DIR + '/single_factors_tmp1/*')
        # os.system('rm -rf ' + CACHE_DIR + '/single_factors_tmp2/*')
        
        # # 需要刷新价格数据
        # AStock.getStockDailyPrice(code_list=[], where="", startdate='', enddate='', fq='hfq', db='tushare', cache=False)
        
        # # 遍历任务列表，收集所有要处理的indicator因子和alpha因子
        # task_list = task_list.split(',')
        # all_indicator_factors = []
        # all_alpha_factors = {}
        
        # # 收集所有indicator因子
        # for factor_list_name in task_list:
        #     if os.path.exists(CONFIG_DIR + "/factorlist/indicatorlist/" + factor_list_name):
        #         with open(CONFIG_DIR + "/factorlist/indicatorlist/" + factor_list_name, 'r', encoding='utf-8') as f:
        #             factor_list = [_.rstrip('\n') for _ in f.readlines()]
        #             for i in range(len(factor_list)):
        #                 if not '_' in factor_list[i]:
        #                     factor_list[i] = factor_list[i] + '_0'
        #             all_indicator_factors.extend(factor_list)
        
        # # 找出所有因子中的最早最后更新日期，作为一致的计算窗口
        # min_lastdate = taskRunner.get_min_factor_date(all_indicator_factors)
        # if min_lastdate:
        #     Log.logger.info(f"使用最早因子日期 {min_lastdate} 作为计算基准")
        
        # # 处理强制刷新的因子
        # if force_factors:
        #     for factor in force_factors:
        #         factor_path = SINGLE_FACTORS_DIR + factor + '.csv'
        #         if os.path.exists(factor_path):
        #             os.remove(factor_path)
        #             Log.logger.info(f"已删除强制重新计算的因子文件: {factor}")
        
        # # 计算indicator因子
        # for factor_list_name in task_list:
        #     # factor列表
        #     if os.path.exists(CONFIG_DIR + "/factorlist/indicatorlist/" + factor_list_name):
        #         with open(CONFIG_DIR + "/factorlist/indicatorlist/" + factor_list_name, 'r', encoding='utf-8') as f:
        #             factor_list = [_.rstrip('\n') for _ in f.readlines()]
        #         indicatorCompute.computeList(
        #             list_name=factor_list_name,
        #             factor_list=factor_list,
        #             c_list=c_list,
        #             min_lastdate=min_lastdate,
        #             force_factors=force_factors
        #         )
        
        # # 计算alpha因子
        # for factor_list_name in task_list:
        #     if os.path.exists(CONFIG_DIR + "/factorlist/alphalist/" + factor_list_name):
        #         with open(CONFIG_DIR + "/factorlist/alphalist/" + factor_list_name, 'r', encoding='utf-8') as f:
        #             factor_list = [_.rstrip('\n') for _ in f.readlines()]
        #             i = 0
        #             tasks = []
                    
        #             with ProcessPoolExecutor(max_workers=8) as pool:
        #                 for factor in factor_list:   
        #                     i = i + 1
        #                     alpha_name = factor_list_name + '_' + str(i).zfill(3)
                            
        #                     # 检查是否需要强制重新计算
        #                     force_recalc = alpha_name in force_factors
                            
        #                     mytask = pool.submit(
        #                         alphaEngine.calc,
        #                         factor,
        #                         pd.DataFrame(),
        #                         alpha_name,
        #                         False,  # check
        #                         True,   # save
        #                         False,  # ignore_notice
        #                         [],     # stock_list
        #                         not force_recalc  # diff - 如果强制重新计算，则不做差异检查
        #                     )
        #                     tasks.append(mytask)
                    
        #             # 等待所有alpha因子计算完成
        #             wait(tasks, return_when=ALL_COMPLETED)
        
        # os.system('mv ' + CACHE_DIR + '/single_factors_tmp2/* ' + SINGLE_FACTORS_DIR)
        # factorPkl.save()
    
    # @staticmethod
    # def get_min_factor_date(factor_list):
    #     """获取因子列表中的最早最后更新日期"""
    #     min_lastdate = None
    #     today = time.strftime("%Y%m%d", time.localtime())
        
    #     for factor_name in factor_list:
    #         single_factors_path = SINGLE_FACTORS_DIR + factor_name + '.csv'
    #         if os.path.isfile(single_factors_path):
    #             try:
    #                 # 尝试只读取文件的最后几行来提高效率
    #                 df = pd.read_csv(single_factors_path, header=None, 
    #                              names=['ts_code', 'trade_date', factor_name],
    #                              nrows=1000)  # 只读取最后1000行
    #                 factor_lastdate = str(df['trade_date'].max())
                    
    #                 if min_lastdate is None or factor_lastdate < min_lastdate:
    #                     min_lastdate = factor_lastdate
    #             except Exception as e:
    #                 Log.logger.error(f"读取因子 {factor_name} 日期时出错: {str(e)}")
        
    #     return min_lastdate
