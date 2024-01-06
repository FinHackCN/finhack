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

from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor, wait, ALL_COMPLETED
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)
import finhack.library.log as Log
 


class indicatorCompute():
    def computeList(list_name='',factor_list=[],c_list=[],db='tushare'):
        code_list=AStock.getStockCodeList('tushare')

        if factor_list==[] and os.path.exists(CONFIG_DIR+"/factorlist/indicatorlist/"+list_name):
            with open(CONFIG_DIR+"/factorlist/indicatorlist/"+list_name, 'r', encoding='utf-8') as f:
                factor_list=[_.rstrip('\n') for _ in f.readlines()]
        
        for i in range(len(factor_list)):
            if not '_' in factor_list[i]:
                factor_list[i]=factor_list[i]+'_0'


        
        n=30
        # if cpu_count()>2:
        #     n=cpu_count()-2
        tasklist=[]
        code_list=code_list['ts_code'].to_list()
        
        #为了节省内存，这里有bug，所以要拆开计算
        def split_list_n_list(origin_list, n):
            if len(origin_list) % n == 0:
                cnt = len(origin_list) // n
            else:
                cnt = len(origin_list) // n + 1
         
            for i in range(0, n):
                yield origin_list[i*cnt:(i+1)*cnt]
 
        
        #x单进程断点调试用
        # for ts_code in code_list:
        #     if ts_code!='301187.SZ':
        #         continue
        #     indicatorCompute.computeListByStock(ts_code,list_name,'',factor_list,c_list)
        #     exit()
        # exit()
        code_lists = split_list_n_list(code_list, n)
        for code_list in code_lists:
            with ProcessPoolExecutor(max_workers=n) as pool:
                for ts_code in code_list:
                    #computeListByStock(ts_code,list_name='all',where='',factor_list=None,c_list=[],pure=True,check=True,df_price=pd.DataFrame(),db='tushare'):
    
                    mytask=pool.submit(indicatorCompute.computeListByStock,ts_code,list_name,'',factor_list,c_list,false,false)
                    tasklist.append(mytask)
        
        wait(tasklist,return_when=ALL_COMPLETED)
        Log.logger.info(list_name+' computed')
        os.system('mv '+CACHE_DIR+'/single_factors_tmp1/* '+CACHE_DIR+'/single_factors_tmp2/')
        os.system('mv '+CACHE_DIR+'/single_factors_tmp2/* '+SINGLE_FACTORS_DIR)
            

    
        
    #计算单支股票的一坨因子
    #pure=True时，只保留factor_list中的因子
    def computeListByStock(ts_code,list_name='all',where='',factor_list=None,c_list=[],pure=True,check=True,df_price=pd.DataFrame(),db='tushare'):
        try:
            Log.logger.info('computeListByStock---'+ts_code)
            
            hashstr=ts_code+'-'+list_name+'-'+where+'-'+','.join(factor_list)+'-'+','.join(c_list)+'-'+str(pure)+'-'+str(check)
            md5=hashlib.md5(hashstr.encode(encoding='utf-8')).hexdigest()
            diff_date=0  #对比日期
            first_time=True
            if(df_price.empty):        
                #TODO 未处理停牌票
                df_price=AStock.getStockDailyPriceByCode(ts_code,where=where,fq='hfq',db=db)
                df_price=df_price.reset_index(drop=True)
                if type(df_price) == bool:
                    Log.logger.warning(ts_code+"empty!")
            df_all=df_price.copy()
            df_250=df_all.tail(500)
            df_250=df_250.reset_index(drop=True)
            diff_col=[]
            
            if type(df_all) == bool or (df_price.empty):
                Log.logger.warning(ts_code+"empty!")
                return False


            if type(df_250) == bool or (df_250.empty):
                df_250=df_all
                return False


    
            df_factor=pd.DataFrame()
            td_list_p=df_price['trade_date'].tolist()
            lastdate=df_price['trade_date'].max()
            code_factors_path=CODE_FACTORS_DIR+ts_code+'_'+md5
            date_factors_path=DATE_FACTORS_DIR+lastdate+"/"
            if not os.path.exists(date_factors_path): 
                try:
                    os.mkdir(date_factors_path)
                except Exception as e:
                    Log.logger.error(str(e))
            
            
            
 
            if not check and os.path.exists(code_factors_path) and os.path.getsize(code_factors_path) > 0:
                first_time=False
                try:
                    df_factor=pd.read_pickle(code_factors_path)
                    #print('read')
                    td_list_f=df_factor['trade_date'].tolist()
                    diff_date=len(td_list_p)-len(td_list_f)
                except Exception as e:
                    diff_date=len(td_list_p)
            else:
                diff_date=len(td_list_p)


            # print(code_factors_path)
            # print(diff_date)
            # print(df_price)
            # print(df_factor)
            # exit()
            # # x=df_factor[2250:]
            # print('--------------')
            # time.sleep(10)  

            #去掉注释项
            
            

            
            for row in factor_list.copy():
                factor_name=row
                if(isinstance(row,(dict))):
                    factor_name=row['name']
                if factor_name.startswith('#'):
                    factor_list.remove(factor_name)


            
            #exit()
            
 


            #日期没有变化
            if diff_date==0:
                for row in factor_list:
                    factor_name=row
                    if(isinstance(row,(dict))):
                        factor_name=row['name']
                    factor=factor_name.split('_')
                    if len(factor)==1:
                        factor_name=factor_name+"_0"  
                        
                #列不一致
                df_factor_colums_list=df_factor.columns.tolist()
                if factor_name not in df_factor_colums_list:
                    diff_col.append(factor_name)
                if  diff_col==[]:
                    #需要检查单因子是否需要重写
                    for factor_name in df_factor.columns.tolist():
                        single_factors_path=SINGLE_FACTORS_DIR+factor_name+'.csv'
                        if os.path.isfile(single_factors_path):
                            t = os.path.getmtime(code_factors_path)-os.path.getmtime(single_factors_path)
                            if t<60*60: #修改时间和code_factor不超过1小时，即这个因子刚计算过
                                return True 
                        if not check:
                            single_factors_path1=CACHE_DIR+"/single_factors_tmp1/"+factor_name+'.csv'
                            single_factors_path2=CACHE_DIR+"/single_factors_tmp2/"+factor_name+'.csv'
                            if factor_name not in ['ts_code','trade_date'] and not os.path.exists(single_factors_path2):
                                df=pd.DataFrame(df_factor,columns=['ts_code','trade_date',factor_name])
                                df.to_csv(single_factors_path1,mode='a',encoding='utf-8',header=False,index=False)
                                df_date=df[df.trade_date==lastdate]
                                df_date.to_csv(date_factors_path+factor_name+'.csv',mode='a',encoding='utf-8',header=False,index=False)
           
                    return True
                else:
                    Log.logger.info(ts_code+"  date no change")

           
            #下面是日期发生过变化的
            #增加shift_0
            

            for row in factor_list:
                factor_name=row
                if(isinstance(row,(dict))):
                    factor_name=row['name']
                #factor_name=factor_name.strip()
                factor=factor_name.split('_')
                if len(factor)==1:
                    factor_name=factor_name+"_0"  
                    
                    
                #clist，chenged factor_list，代码发生变化
                #print(first_time)
                
                
                
                if (not factor_name in df_factor.columns or diff_date>100) and (not factor_name in c_list):
                    if not factor_name in df_all.columns:
                        df_all_tmp=indicatorCompute.computeFactorByStock(ts_code,factor_name,df_all.copy(),where=where,db='factors')
                        if type(df_all_tmp) == bool or (df_all_tmp.empty) :
                            df_all[factor_name]=np.nan
                        else:
                            df_all=df_all_tmp
                            
                #否则计算250日数据
                else:
                    if not factor_name in df_250.columns:
                        if(diff_date>0 and factor_name in df_factor.columns):
                            df_250=indicatorCompute.computeFactorByStock(ts_code,factor_name,df_250,where=where,db='factors')
                            
        

            if(first_time):
                df_factor=df_all
            else:
                df_factor=pd.concat([df_factor,df_250.tail(diff_date)],ignore_index=True)
                for f in df_all.columns:
                    df_factor[f]=df_all[f]

            #只保存factorlist中的列
            if pure==True:
                df_factor=pd.DataFrame(df_factor,columns=factor_list)
                df_factor=pd.concat([df_price,df_factor],axis=1)
            
            try:
                if 'index' in df_factor:
                    del df_factor['index'] 
            except Exception as e:
                return False
                
                
            

            # print(factor_list)
            # print(pure)
            # print(df_factor)


            df_factor=df_factor.loc[:,~df_factor.columns.duplicated()]
            
            #df_factor=df_factor.sort_values(by='trade_date')
            df_factor=df_factor.dropna(subset=['ts_code','trade_date'])
            #print(df_factor)
            
            #objgraph.show_most_common_types(limit=50)
            if check:
                Log.logger.info('check:'+ts_code)
                #print(df_factor)
                return df_factor
            else:
                df_factor.to_pickle(code_factors_path)
                #想了想，这里没法保证不同list相同因子重复写入的问题，只能先写到1里，然后移动到2里
                for factor_name in df_factor.columns.tolist():
                    #print(factor_name)
                    single_factors_path1=CACHE_DIR+"single_factors_tmp1/"+factor_name+'.csv'
                    single_factors_path2=CACHE_DIR+"single_factors_tmp2/"+factor_name+'.csv'
                    if factor_name not in ['ts_code','trade_date'] and not os.path.exists(single_factors_path2):
                        
                        # print(df_factor.columns.tolist())
                        # print(factor_name)
                        # print(df_factor.columns.duplicated().any())
                        
                        df=pd.DataFrame(df_factor,columns=['ts_code','trade_date',factor_name])
                        
                        #print('------')
                        df.to_csv(single_factors_path1,mode='a',encoding='utf-8',header=False,index=False)
                        
                        df_date=df[df.trade_date==lastdate]
                        
                        #print(date_factors_path+factor_name+"xxxxxxx")
                        
                        df_date.to_csv(date_factors_path+factor_name+'.csv',mode='a',encoding='utf-8',header=False,index=False)
                                        
                        del df
                        del df_date                
                del df_factor
                del df_all
                del df_250
                del df_price
                gc.collect()                  
                return True
        except Exception as e:
            
            Log.logger.error(ts_code+" error!")
            Log.logger.error("err exception is %s" % traceback.format_exc())
            exit()
            




        

    #计算单个股票单个因子
    def computeFactorByStock(ts_code,factor_name,df_price=pd.DataFrame(),where='',db='tushare'):
        if(df_price.empty):
            df_price=AStock.getStockDailyPriceByCode(code=ts_code,where=where,db='tushare')
            #df_result=df_price.copy()
        if(df_price.empty):
            return pd.DataFrame()
        

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

        shift="0"
        if len(factor)>1:
            shift=factor[-1]

        #给计算因子的函数传后面_分割的参数
        for i in range(1,len(factor)):
            factor[i]=int(factor[i])

        try:
            df=func(df_price,factor)
            if df.empty:
                return False
        except Exception as e:
            return False

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
        
        #print(df)
        
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