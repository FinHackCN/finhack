from library.config import config
from library.mydb import mydb
from library.astock import AStock
import pandas as pd
import traceback
import os
from importlib import import_module
import re
import datetime
from pandarallel import pandarallel
import numpy as np
from functools import lru_cache
import warnings
import hashlib
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor, wait, ALL_COMPLETED
from multiprocessing import cpu_count 
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)
import gc
import time
import objgraph
from library.globalvar import *
# class BoundThreadPoolExecutor(ProcessPoolExecutor):
#     """
#     对ThreadPoolExecutor 进行重写，给队列设置边界
#     """
#     def __init__(self, qsize: int = None, *args, **kwargs):
#         super(BoundThreadPoolExecutor, self).__init__(*args, **kwargs)
#         self._work_queue = queue.Queue(qsize)

class indicatorCompute():
    def computeList(list_name,factor_list,c_list=[],db='tushare'):
        code_list=AStock.getStockCodeList('tushare')
        #mydb.exec("drop table if exists factors_"+list_name+"_tmp",'factors')
        
        
        n=10
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
 
        
        # #单进程断点调试用
        # for ts_code in code_list:
        #     indicatorCompute.computeListByStock(ts_code,list_name,'',factor_list,c_list)
        #     #exit()
        
        code_lists = split_list_n_list(code_list, n)
        for code_list in code_lists:
            with ProcessPoolExecutor(max_workers=n) as pool:
                for ts_code in code_list:
                    mytask=pool.submit(indicatorCompute.computeListByStock,ts_code,list_name,'',factor_list,c_list)
                    tasklist.append(mytask)
        
        wait(tasklist,return_when=ALL_COMPLETED)
        print(list_name+' computed')
        os.system('mv '+CACHE_DIR+'/single_factors_tmp1/* '+CACHE_DIR+'/single_factors_tmp2/')
        os.system('mv '+CACHE_DIR+'/single_factors_tmp2/* '+SINGLE_FACTORS_DIR)
            

    
        
    #计算单支股票的一坨因子
    #pure=True时，只保留factor_list中的因子
    def computeListByStock(ts_code,list_name='all',where='',factor_list=None,c_list=[],pure=False,check=False,df_price=pd.DataFrame(),db='tushare'):
        try:
            print('computeListByStock---'+ts_code)
            
            hashstr=ts_code+'-'+list_name+'-'+where+'-'+','.join(factor_list)+'-'+','.join(c_list)+'-'+str(pure)+'-'+str(check)
            md5=hashlib.md5(hashstr.encode(encoding='utf-8')).hexdigest()
            diff_date=0  #对比日期
            first_time=True
            if(df_price.empty):        
                #TODO 未处理停牌票
                df_price=AStock.getStockDailyPriceByCode(ts_code,where=where,fq='hfq',db=db)
                df_price=df_price.reset_index(drop=True)
            df_all=df_price.copy()
            df_250=df_all.tail(500)
            df_250=df_250.reset_index(drop=True)
            diff_col=[]
            
            if(df_price.empty) :
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
                    print(str(e))
            
            
            
 
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
                    print(ts_code)

           
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
                        df_all=indicatorCompute.computeFactorByStock(ts_code,factor_name,df_all,where=where,db='factors')
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
            
            if 'index' in df_factor:
                del df_factor['index'] 
                
                
            



            #print(df_factor)



            #objgraph.show_most_common_types(limit=50)
            if check:
                print('check:'+ts_code)
                #print(df_factor)
                return df_factor
            else:
                df_factor.to_pickle(code_factors_path)
                #想了想，这里没法保证不同list相同因子重复写入的问题，只能先写到1里，然后移动到2里
                for factor_name in df_factor.columns.tolist():
                    single_factors_path1=CACHE_DIR+"single_factors_tmp1/"+factor_name+'.csv'
                    single_factors_path2=CACHE_DIR+"/data/single_factors_tmp2/"+factor_name+'.csv'
                    if factor_name not in ['ts_code','trade_date'] and not os.path.exists(single_factors_path2):
                        df=pd.DataFrame(df_factor,columns=['ts_code','trade_date',factor_name])
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
            print("err exception is %s" % traceback.format_exc())
            




        

    #计算单个股票单个因子
    def computeFactorByStock(ts_code,factor_name,df_price,where='',db='tushare'):
        if(df_price.empty):
            df_price=AStock.getStockDailyPriceByCode(ts_code,where,'tushare')
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
        module = getattr(import_module('factors.indicators.'+indicators), indicators)
        func=getattr(module,func_name,lambda x,y:x)

        shift="0"
        if len(factor)>1:
            shift=factor[-1]

        #给计算因子的函数传后面_分割的参数
        for i in range(1,len(factor)):
            factor[i]=int(factor[i])


        df=func(df_price,factor)


        #给返回的因子加上后缀
        suffix=factor[1:]
        suffix.pop(0)
        suffix='_'.join('%s' %p for p in suffix)
        
        if suffix!="":
            suffix=suffix+'_'+shift
        else:
            suffix='_'+shift
    
        #如果返回多列，则需要同样进行shift计算
        if True:
            for f in rlist:
                if not f+suffix in df.columns:
                    if not f  in df.columns:
                        continue
                    if(shift!="0"):
                        df[f+suffix]=df[f].shift(int(shift))
                    else:
                        if(f not in ['open','high','low','close','pre_close','change','pct_chg']):
                            df.rename(columns={f:f+suffix},inplace=True)
                        else:
                            df[f+suffix]=df[f]
                    #print("f=%s,fs=%s" %(f,factor_name))
        #print(len(df.columns))
        
        del df_price
        del func
        return df
        


    @lru_cache(None)
    def getFactorList():
        factor_list=mydb.selectToList('select * from factor_list','factors')
        return factor_list

    
    @lru_cache(None)
    def getFactorInfo(factor_name):
        factor=factor_name.split('_')
        factor_filed=factor[0]
        path = os.path.dirname(__file__)+"/indicators/"
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