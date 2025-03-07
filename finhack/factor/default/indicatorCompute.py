import os
import re
import gc
import time
import objgraph
import hashlib
import datetime
import warnings
import traceback
import numpy as np
import pandas as pd
from functools import lru_cache
from importlib import import_module
from multiprocessing import cpu_count 
import importlib
from runtime.constant import *
from finhack.library.mydb import mydb
from finhack.library.config import Config
from finhack.market.astock.astock import AStock
from filelock import FileLock 

from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor, wait, ALL_COMPLETED
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)
import finhack.library.log as Log
 


class indicatorCompute():
    def computeList(list_name='', factor_list=[], c_list=[], db='tushare', min_lastdate='', force_factors=[]):
        """
        计算因子列表
        
        Parameters:
        -----------
        list_name : str
            因子列表名称
        factor_list : list
            要计算的因子列表
        c_list : list
            已经发生变化的因子列表
        db : str
            数据库名称
        min_lastdate : str
            计算窗口基准日期，用于保证因子时间一致
        force_factors : list
            需要强制重新计算的因子列表
        """
        code_list = AStock.getStockCodeList('tushare')
        if factor_list == [] and os.path.exists(CONFIG_DIR + "/factorlist/indicatorlist/" + list_name):
            with open(CONFIG_DIR + "/factorlist/indicatorlist/" + list_name, 'r', encoding='utf-8') as f:
                factor_list = [_.rstrip('\n') for _ in f.readlines()]
        
        for i in range(len(factor_list)):
            if not '_' in factor_list[i]:
                factor_list[i] = factor_list[i] + '_0'
        
        # 检查是否有需要强制重新计算的因子
        for factor in factor_list:
            if factor in force_factors:
                factor_path = SINGLE_FACTORS_DIR + factor + '.csv'
                if os.path.exists(factor_path):
                    try:
                        os.remove(factor_path)
                        Log.logger.info(f"已删除强制重新计算的因子文件: {factor}")
                    except Exception as e:
                        Log.logger.error(f"删除因子文件 {factor} 时出错: {str(e)}")
        
        n = 30
        if cpu_count() > 2:
            n = cpu_count() - 2
        
        tasklist = []
        code_list = code_list['ts_code'].to_list()
        
        # 为了节省内存，这里拆开计算
        def split_list_n_list(origin_list, n):
            if len(origin_list) % n == 0:
                cnt = len(origin_list) // n
            else:
                cnt = len(origin_list) // n + 1
        
            for i in range(0, n):
                yield origin_list[i*cnt:(i+1)*cnt]
        
        # 创建锁管理器，防止并发写入冲突
        lock_registry = {}
        for factor in factor_list:
            single_factors_path1 = CACHE_DIR + "/single_factors_tmp1/" + factor + '.csv'
            lock_registry[factor] = FileLock(single_factors_path1 + ".lock")
        code_lists = split_list_n_list(code_list, n)
        for code_list in code_lists:
            with ProcessPoolExecutor(max_workers=n) as pool:
                for ts_code in code_list:
                    mytask = pool.submit(
                        indicatorCompute.computeListByStock,
                        ts_code, list_name, '', factor_list, c_list, True, False, True,
                        pd.DataFrame(), db, min_lastdate, lock_registry, force_factors
                    )
                    tasklist.append(mytask)
        
        # 等待所有任务完成
        wait(tasklist, return_when=ALL_COMPLETED)
        Log.logger.info(list_name + ' computed')
        
        # 移动计算好的因子文件
        os.system('mv ' + CACHE_DIR + '/single_factors_tmp1/* ' + CACHE_DIR + '/single_factors_tmp2/')
        os.system('mv ' + CACHE_DIR + '/single_factors_tmp2/* ' + SINGLE_FACTORS_DIR)
                

        
            
        #计算单支股票的一坨因子
        #pure=True时，只保留factor_list中的因子
    def computeListByStock(ts_code, list_name='all', where='', factor_list=None, c_list=[], 
                        pure=True, check=True, save=False, df_price=pd.DataFrame(), 
                        db='tushare', min_lastdate='', lock_registry=None, force_factors=[]):
        """
        计算单个股票的因子列表
        
        Parameters:
        -----------
        ts_code : str
            股票代码
        list_name : str
            因子列表名称
        where : str
            数据筛选条件
        factor_list : list
            要计算的因子列表
        c_list : list
            已经发生变化的因子列表
        pure : bool
            是否只保留factor_list中的因子
        check : bool
            是否只检查不保存
        save : bool
            是否保存结果
        df_price : DataFrame
            股票价格数据
        db : str
            数据库名称
        min_lastdate : str
            计算窗口基准日期，用于保证因子时间一致
        lock_registry : dict
            文件锁注册表，防止并发写入冲突
        force_factors : list
            需要强制重新计算的因子列表
        """
        try:
            Log.logger.info('computeListByStock---' + ts_code)
            
            # 生成唯一的hash标识
            hashstr = ts_code + '-' + list_name + '-' + where + '-' + ','.join(factor_list) + '-' + ','.join(c_list) + '-' + str(pure) + '-' + str(check)
            md5 = hashlib.md5(hashstr.encode(encoding='utf-8')).hexdigest()
            diff_date = 0  # 对比日期
            first_time = True
            
            if df_price.empty:        
                # 获取股票价格数据
                df_price = AStock.getStockDailyPriceByCode(ts_code, where=where, fq='hfq', db=db)
                df_price = df_price.reset_index(drop=True)
                if type(df_price) == bool or df_price.empty:
                    Log.logger.warning(ts_code + " empty!")
                    return False
                    
            df_all = df_price.copy()
            df_250 = df_all.tail(500)
            df_250 = df_250.reset_index(drop=True)
            diff_col = []
            
            # 检查数据有效性
            if type(df_all) == bool or df_price.empty:
                Log.logger.warning(ts_code + " empty!")
                return False
            if type(df_250) == bool or df_250.empty:
                df_250 = df_all
                return False
            # 获取交易日期和股票代码
            df_factor = pd.DataFrame()
            td_list_p = df_price['trade_date'].tolist()
            lastdate = df_price['trade_date'].max()
            code_factors_path = CODE_FACTORS_DIR + ts_code + '_' + md5
            date_factors_path = DATE_FACTORS_DIR + str(lastdate) + "/"
            
            # 创建日期因子目录
            if not os.path.exists(date_factors_path): 
                try:
                    os.makedirs(date_factors_path, exist_ok=True)
                except Exception as e:
                    Log.logger.error(str(e))
            
            # 如果提供了min_lastdate，判断是否需要重新计算
            need_recompute = False
            if min_lastdate and not check:
                if str(lastdate) <= min_lastdate:
                    Log.logger.info(f"{ts_code} 最新数据日期 {lastdate} 不超过因子组最早日期 {min_lastdate}，无需更新")
                else:
                    need_recompute = True
                    Log.logger.info(f"{ts_code} 需要更新: 当前日期 {lastdate} > 基准日期 {min_lastdate}")
            
            # 检查是否有强制重新计算的因子
            has_force_factor = False
            for factor in factor_list:
                if factor in force_factors:
                    has_force_factor = True
                    break
            
            # 如果有强制重新计算的因子，不读取缓存，强制重新计算
            if has_force_factor:
                first_time = True
                diff_date = len(td_list_p)
                Log.logger.info(f"{ts_code} 包含强制重新计算的因子，将完整重新计算")
            elif not check and os.path.exists(code_factors_path) and os.path.getsize(code_factors_path) > 0 and not need_recompute:
                # 非检查模式且缓存存在且不需要重新计算，读取缓存
                first_time = False
                try:
                    df_factor = pd.read_pickle(code_factors_path)
                    td_list_f = df_factor['trade_date'].tolist()
                    diff_date = len(td_list_p) - len(td_list_f)
                except Exception as e:
                    diff_date = len(td_list_p)
                    Log.logger.error(f"读取缓存 {code_factors_path} 时出错: {str(e)}")
            else:
                # 如果是check模式或需要重新计算，则全量交易日计算
                diff_date = len(td_list_p)
            # 去掉注释项
            for row in factor_list.copy():
                factor_name = row
                if isinstance(row, dict):
                    factor_name = row['name']
                if factor_name.startswith('#'):
                    factor_list.remove(factor_name)
            # 日期没有变化且没有强制重新计算的因子
            if diff_date == 0 and not has_force_factor:
                Log.logger.info(ts_code + "  diff_date=0")
                for row in factor_list:
                    factor_name = row
                    if isinstance(row, dict):
                        factor_name = row['name']
                    factor = factor_name.split('_')
                    if len(factor) == 1:
                        factor_name = factor_name + "_0"  
                        
                    # 列不一致
                    df_factor_colums_list = df_factor.columns.tolist()
                    if factor_name not in df_factor_colums_list:
                        diff_col.append(factor_name)
                
                # 如果没有列差异
                if diff_col == []:
                    # 需要检查单因子是否需要重写
                    for factor_name in df_factor.columns.tolist():
                        single_factors_path = SINGLE_FACTORS_DIR + factor_name + '.csv'
                        # 有这个因子的csv文件
                        if os.path.isfile(single_factors_path):
                            t = os.path.getmtime(code_factors_path) - os.path.getmtime(single_factors_path)
                            if t < 60 * 60:  # 修改时间和code_factor不超过1小时，即这个因子刚计算过
                                return True 
                                
                        if not check and save:
                            # 当日期和列都没变化的时候，把因子数据写到single_factors_path
                            single_factors_path1 = CACHE_DIR + "/single_factors_tmp1/" + factor_name + '.csv'
                            single_factors_path2 = CACHE_DIR + "/single_factors_tmp2/" + factor_name + '.csv'
                            
                            if factor_name not in ['ts_code', 'trade_date'] and not os.path.exists(single_factors_path2):
                                df = pd.DataFrame(df_factor, columns=['ts_code', 'trade_date', factor_name])
                                
                                # 使用文件锁防止并发写入冲突
                                if lock_registry and factor_name in lock_registry:
                                    with lock_registry[factor_name]:
                                        df.to_csv(single_factors_path1, mode='a', encoding='utf-8', header=False, index=False)
                                else:
                                    df.to_csv(single_factors_path1, mode='a', encoding='utf-8', header=False, index=False)
                                    
                                df_date = df[df.trade_date == lastdate]
                                df_date.to_csv(date_factors_path + factor_name + '.csv', mode='a', encoding='utf-8', header=False, index=False)
                    
                    return True
                else:
                    Log.logger.info(ts_code + "  date no change but columns changed")
            # 下面是日期发生过变化或有强制重新计算因子的情况
            for row in factor_list:
                factor_name = row
                if isinstance(row, dict):
                    factor_name = row['name']
                factor = factor_name.split('_')
                if len(factor) == 1:
                    factor_name = factor_name + "_0"  
                    
                # 检查是否是新因子（没有CSV文件）
                has_csv = False
                single_factors_path = SINGLE_FACTORS_DIR + factor_name + '.csv'
                if os.path.isfile(single_factors_path):
                    has_csv = True
                    
                # 判断是否需要完整重新计算该因子
                # 1. df_factor没有此列 - 新因子需要完整计算
                # 2. 时间差异大 - 差异日期超过阈值需要完整计算
                # 3. 没有CSV文件 - 新因子需要完整计算
                # 4. 强制重新计算列表中的因子 - 用户指定需要完整计算
                # 5. 日期比最早因子日期新 - 时间不一致需要重新计算
                need_full_calc = (
                    not factor_name in df_factor.columns or 
                    diff_date > 100 or 
                    has_csv == False or 
                    factor_name in force_factors or
                    (min_lastdate and str(lastdate) > min_lastdate)
                ) and (not factor_name in c_list)
                
                if need_full_calc:
                    # 对新因子或需要强制重新计算的因子，使用完整历史数据计算
                    df_all_tmp = indicatorCompute.computeFactorByStock(ts_code, factor_name, df_all.copy(), where=where, db='factors')
                    if type(df_all_tmp) == bool or df_all_tmp.empty:
                        df_all[factor_name] = np.nan
                    else:
                        df_all = df_all_tmp
                    
                    if factor_name in force_factors:
                        Log.logger.info(f"{ts_code} 完整重新计算因子: {factor_name}")
                else:
                    # 只计算增量数据
                    if df_250 is None or isinstance(df_250, bool):
                        return False
                    if not factor_name in df_250.columns:
                        if diff_date > 0 and factor_name in df_factor.columns:
                            df_250 = indicatorCompute.computeFactorByStock(ts_code, factor_name, df_250, where=where, db='factors')
            # 合并数据
            if first_time:
                df_factor = df_all
            else:
                # 这里有一个坑，就是如果存在2个list，那么他们的时间可能会不一致
                # 因此建议合并到一个list中，或者新增list时不要有新的行情数据出现
                df_factor = pd.concat([df_factor, df_250.tail(diff_date)], ignore_index=True)
                for f in df_all.columns:
                    df_factor[f] = df_all[f]
            # 只保存factor_list中的列
            if pure:
                df_factor = pd.DataFrame(df_factor, columns=factor_list)
                df_factor = pd.concat([df_price, df_factor], axis=1)
            
            # 清理数据
            try:
                if 'index' in df_factor:
                    del df_factor['index'] 
            except Exception as e:
                return False
            # 去除重复列并确保数据有效
            df_factor = df_factor.loc[:, ~df_factor.columns.duplicated()]
            df_factor = df_factor.dropna(subset=['ts_code', 'trade_date'])
            
            # 根据模式处理结果
            if check:
                Log.logger.info('check:' + ts_code)
                return df_factor
            elif not save:
                return df_factor
            else:
                # 保存到代码因子路径
                df_factor.to_pickle(code_factors_path)
                
                # 保存到单因子文件
                for factor_name in df_factor.columns.tolist():
                    if factor_name not in ['ts_code', 'trade_date']:
                        single_factors_path1 = CACHE_DIR + "single_factors_tmp1/" + factor_name + '.csv'
                        single_factors_path2 = CACHE_DIR + "single_factors_tmp2/" + factor_name + '.csv'
                        
                        if not os.path.exists(single_factors_path2):
                            df = pd.DataFrame(df_factor, columns=['ts_code', 'trade_date', factor_name])
                            
                            # 使用文件锁防止并发写入冲突
                            if lock_registry and factor_name in lock_registry:
                                with lock_registry[factor_name]:
                                    df.to_csv(single_factors_path1, mode='a', encoding='utf-8', header=False, index=False)
                            else:
                                df.to_csv(single_factors_path1, mode='a', encoding='utf-8', header=False, index=False)
                            
                            # 保存最新交易日的数据到日期因子目录
                            df_date = df[df.trade_date == lastdate]
                            df_date.to_csv(date_factors_path + factor_name + '.csv', mode='a', encoding='utf-8', header=False, index=False)
                            
                            del df
                            del df_date                
                
                # 清理内存
                del df_factor
                del df_all
                del df_250
                del df_price
                gc.collect()                  
                return True
                
        except Exception as e:
            Log.logger.error(ts_code + " error!")
            Log.logger.error("err exception is %s" % traceback.format_exc())
            return False
                




        

    #计算单个股票单个因子
    def computeFactorByStock(ts_code,factor_name,df_price=pd.DataFrame(),where='',db='tushare'):
        
        
        if(df_price.empty):
            df_price=AStock.getStockDailyPriceByCode(code=ts_code,where=where,db='tushare')
            #df_result=df_price.copy()
        if(df_price.empty):
            return pd.DataFrame()
        
        if factor_name in df_price.columns:
            return df_price

        indicators,func_name,code,return_fileds=indicatorCompute.getFactorInfo(factor_name)

        
        if indicators==False:
            return df_price
        
        #df_result=df_price.copy()
        pattern = re.compile(r"df\[\'(\w*?)\'\]")   # 查找数字
        flist = pattern.findall(code)
        rlist=return_fileds
        
        #计算因子引用，先计算其它因子
        flist=list(set(flist) - set(rlist))
        for f in flist:
            if not f in df_price.columns:
                #print("depend factor:"+f)
                df_price=indicatorCompute.computeFactorByStock(ts_code,f,df_price,db)
                
        factor=factor_name.split('_')
        #module = getattr(import_module('finhack.factor.default.indicators.'+indicators), indicators)
        
        df_price=df_price.copy()
        if 'level_0' in df_price.columns:
            df_price = df_price.drop(columns=['level_0'])        
        # 定义文件路径
        file_path = INDICATORS_DIR+indicators+".py"
        
        
        
        # 获取文件名和类名
        module_name = indicators
        class_name = indicators
        
        # 加载模块
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        func=getattr(module,func_name,lambda x,y:x)
        # import inspect
        
        # # 这将只工作如果 func 是一个普通的 Python 函数
        # print(inspect.getsource(func))
        # if func:
        #     try:
        #         print(inspect.getsource(func))
        #     except TypeError:
        #         print(f"The object {func_name} is not a regular Python function.")
        # else:
        #     print(f"The function {func_name} is not found in the module.")
        # print(file_path)
        # print(factor)
        # print(func_name)
        # print(f"class_name:{class_name}")

        shift="0"
        if len(factor)>1:
            shift=factor[-1]

        #给计算因子的函数传后面_分割的参数
        for i in range(1,len(factor)):
            factor[i]=int(factor[i])

        #print('111111111')
        try:
            df=func(df_price,factor)
            if df.empty:
                return False
        except Exception as e:
            Log.logger.error("err exception is %s" % traceback.format_exc())
            # print('22222222222')
            return False

        #print('333333333')

        #给返回的因子加上后缀
        suffix=factor[1:]
        
        # print(111)
        # print(suffix)
        
        suffix.pop()
        
        # print(222)
        # print(suffix)
        
        suffix='_'.join('%s' %p for p in suffix)
        
        # print(factor)
        # print(suffix)
        
        
        if suffix!="":
            suffix=suffix+'_'+shift
        else:
            suffix='_'+shift
            
        if suffix[0]!='_':
            suffix="_"+suffix
        #print(suffix)
        #exit()
    


        #如果返回多列，则需要同样进行shift计算
        if True:
            for f in rlist:
                if not f+suffix in df.columns:
                    if not f  in df.columns:
                        continue
                    if(shift!="0"):
                        df[f+suffix]=df[f].shift(int(shift))
                    else:
                        #print("suffix:"+suffix)
                        if(f not in ['open','high','low','close','pre_close','change','pct_chg']):
                            df.rename(columns={f:f+suffix},inplace=True)
                        else:
                            df[f+suffix]=df[f]
                    #print("f=%s,fs=%s" %(f,factor_name))
        #print(len(df.columns))
        
        # print("hint")
        # print(factor_name)
        # print(df)
        #exit()
        del df_price
        del func
        return df
        


    # @lru_cache(None)
    # def getFactorList():
    #     factor_list=mydb.selectToList('select * from factor_list','factors')
    #     return factor_list

    
    @lru_cache(None)
    def getFactorInfo(factor_name):
        factor=factor_name.split('_')
        factor_filed=factor[0]
        path = INDICATORS_DIR
        for subfile in os.listdir(path):
            if not '__' in subfile:
                indicators=subfile.split('.py')
                indicators=indicators[0]
                function_name=''
                code=''
                return_fileds=[]
                find=False
                with open(path+subfile) as filecontent:
                    for line in filecontent:
                        #提取当前函数名
                        if('def ' in line):
                            function_name=line.split('def ')
                            function_name=function_name[1]
                            function_name=function_name.split('(')
                            function_name=function_name[0]
                            function_name=function_name.strip()
                            code=line
                        else:
                            code=code+"\n"+line
                        left=line.split('=')
                        left=left[0]
                        
                        #pattern = re.compile(r"df\[\'(\w*?)\'\]")   # 查找数字
                        pattern = re.compile(r"df\[\'([A-Za-z0-9_\-]*?)\'\]")   # 查找数字
                        flist = pattern.findall(left)
                        return_fileds=return_fileds+flist

                        if("df['"+factor_filed+"_") in left or ("df['"+factor_filed+"']") in left:
                            find=True
                        if 'return ' in  line:
                            if find:
                                return indicators,function_name,code,return_fileds
                            else:
                                code=''
                                return_fileds=[]
                         

        print('wrong factor_name:'+factor_name)       
        return False,False,False,False




    
    #替换临时表为目标表，同时设置索引
    # def putData(table,tmptable,todb='false',db='factors'):
    #     index_list=['ts_code','end_date','trade_date']
    #     for index in index_list:
    #         sql="CREATE INDEX "+index+" ON "+tmptable+" ("+index+"(10)) "
    #         mydb.exec(sql,db)        
    #     mydb.exec('rename table '+table+' to '+table+'_old;','factors');
    #     mydb.exec('rename table '+tmptable+' to '+table+';','factors');
    #     mydb.exec("drop table if exists "+table+'_old','factors')