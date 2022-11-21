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
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor, wait, ALL_COMPLETED
from multiprocessing import cpu_count 
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)
import gc
import objgraph

class BoundThreadPoolExecutor(ProcessPoolExecutor):
    """
    对ThreadPoolExecutor 进行重写，给队列设置边界
    """
    def __init__(self, qsize: int = None, *args, **kwargs):
        super(BoundThreadPoolExecutor, self).__init__(*args, **kwargs)
        self._work_queue = queue.Queue(qsize)

class indicatorCompute():
    def computeList(list_name,factor_list,c_list=[],db='tushare'):
        code_list=AStock.getStockCodeList('tushare')
        #mydb.exec("drop table if exists factors_"+list_name+"_tmp",'factors')
        
        
        n=10
        # if cpu_count()>2:
        #     n=cpu_count()-2
        tasklist=[]
        code_list=code_list['ts_code'].to_list()
        
        
        def split_list_n_list(origin_list, n):
            if len(origin_list) % n == 0:
                cnt = len(origin_list) // n
            else:
                cnt = len(origin_list) // n + 1
         
            for i in range(0, n):
                yield origin_list[i*cnt:(i+1)*cnt]
 
        
        
        
        code_lists = split_list_n_list(code_list, n)
        for code_list in code_lists:
            with ProcessPoolExecutor(max_workers=n) as pool:
                for ts_code in code_list:
                    mytask=pool.submit(indicatorCompute.computeListByStock,ts_code,list_name,'',factor_list,c_list)
                    #tasklist.append(mytask)
            wait(tasklist, return_when=ALL_COMPLETED)
            pool.shutdown()
            print(list_name+' computed')
        
        mypath=os.path.dirname(os.path.dirname(__file__))
        os.system('mv '+mypath+'/data/single_factors_tmp1/* '+mypath+'/data/single_factors_tmp2')
            
            
        # indicatorCompute.singleSave('000001.SZ',list_name,factor_list)
        # code_list.remove('000001.SZ')
            
        # with ProcessPoolExecutor(max_workers=20) as pool:
        #     mypath=os.path.dirname(os.path.dirname(__file__))
        #     for ts_code in code_list:
        #         ctors_path=mypath+"/data/code_factors/"+list_name+'_'+ts_code
        #         #print(code_factors_path)
        #         if os.path.exists(code_factors_path):
        #             mytask=pool.submit(indicatorCompute.singleSave,ts_code,list_name,factor_list)
        #             tasklist.append(mytask)
        #     wait(tasklist, return_when=ALL_COMPLETED)
        #     print(list_name+' saved')
        
        
    # def singleSave(ts_code,list_name,factor_list):
    #     mypath=os.path.dirname(os.path.dirname(__file__))
    #     code_list=AStock.getStockCodeList('tushare')
    #     print(ts_code+' saving')
        
    #     for factor_name in factor_list:
    #         factor=factor_name.split('_')
    #         if len(factor)==1:
    #             factor_name=factor_name+"_0" 
    #         code_factors_path=mypath+"/data/code_factors/"+list_name+'_'+ts_code
    #         single_factors_path=mypath+"/data/single_factors_tmp/"+ts_code+factor_name+'.pkl'
            
    #         #store = pd.HDFStore()
    #         if True:        
    #             #print(ts_code+'-------')
    #             df=pd.read_pickle(code_factors_path)
    #             df=pd.DataFrame(df,columns=['ts_code','trade_date',factor_name])
    #             df.to_pickle(single_factors_path)
    #             # # if ts_code=='000001.SZ':
    #             # #     #df.to_csv(single_factors_path,mode='w',encoding='utf-8',header=True,index=False)
    #             # #     store.put('name_of_frame', ohlcv_candle, format='t', append=True, data_columns=True)
    #             # # else:
    #             # #     df.to_csv(single_factors_path,mode='a',encoding='utf-8',header=False,index=False)
    #             # try:
    #             #     df.to_hdf(single_factors_path,key=factor_name, append=True, mode='a', format='table')
                    
    #             # except Exception as e:
    #             #     print(df)
    #             #     print(str(e))
    #             #     #exit()
    #             #     print(factor_name+' error')
 
    #             #store.put('factor', df, format='t', append=True, data_columns=True)
    #     return True
            
                        
            
            
    
        
    #计算单支股票的因子
    #pure=True时，只保留factor_list中的因子
    def computeListByStock(ts_code,list_name='all',where='',factor_list=None,c_list=[],pure=False,check=False,df_price=pd.DataFrame(),db='tushare'):
        try:
            print('computeListByStock---'+ts_code)
            diff_date=0  #对比日期
            first_time=True
            if(df_price.empty):        
                #TODO 未处理停牌票
                df_price=AStock.getStockDailyPriceByCode(ts_code,where=where,fq='hfq',db=db)
            df_all=df_price.copy()
            df_250=df_all.tail(500)
            df_250=df_250.reset_index(drop=True)
            diff_col=[]
            
            if(df_price.empty) :
                return False
    
            df_factor=pd.DataFrame()
            td_list_p=df_price['trade_date'].tolist()
            mypath=os.path.dirname(os.path.dirname(__file__))
            code_factors_path=mypath+"/data/code_factors/"+list_name+'_'+ts_code
 
            if not check and os.path.exists(code_factors_path) and os.path.getsize(code_factors_path) > 0:
                first_time=False
                try:
                    df_factor=pd.read_pickle(code_factors_path)
                    td_list_f=df_factor['trade_date'].tolist()
                    diff_date=len(td_list_p)-len(td_list_f)
                except Exception as e:
                    diff_date=len(td_list_p)
            else:
                diff_date=len(td_list_p)




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
                        single_factors_path=mypath+"/data/single_factors/"+factor_name+'.csv'
                        if os.path.isfile(single_factors_path):
                            t = os.path.getmtime(code_factors_path)-os.path.getmtime(single_factors_path)
                            if t<60*60: #修改时间和code_factor不超过1小时
                                return True 
                        
                        single_factors_path1=mypath+"/data/single_factors_tmp1/"+factor_name+'.csv'
                        single_factors_path2=mypath+"/data/single_factors_tmp2/"+factor_name+'.csv'
                        if factor_name not in ['ts_code','trade_date'] and not os.path.exists(single_factors_path2):
                            df=pd.DataFrame(df_factor,columns=['ts_code','trade_date',factor_name])
                            df.to_csv(single_factors_path1,mode='a',encoding='utf-8',header=False,index=False)
                            del df
                    return True
                else:
                    print(ts_code)

           
            #增加shift_0
            for row in factor_list:
                factor_name=row
                if(isinstance(row,(dict))):
                    factor_name=row['name']
                #factor_name=factor_name.strip()
                factor=factor_name.split('_')
                if len(factor)==1:
                    factor_name=factor_name+"_0"  
                    
                    
                
                #print(first_time)
                if (not factor_name in df_factor.columns or diff_date>100) and (not factor_name in c_list):
                    if not factor_name in df_all.columns:
                        df_all=indicatorCompute.computeFactorByStock(ts_code,factor_name,df_all,where=where,db='factors')
                #否则计算250日数据
                else:
                    if not factor_name in df_250.columns:
                        if(diff_date>0 and not factor_name in df_factor.columns):
                            df_250=indicatorCompute.computeFactorByStock(ts_code,factor_name,df_250,where=where,db='factors')
              
            
            
            if(first_time):
                df_factor=df_all
                if 'index' in df_factor:
                    del df_factor['index'] 
            else:
                df_factor=pd.concat([df_factor,df_250.tail(diff_date)],ignore_index=True)
                for f in df_all.columns:
                    df_factor[f]=df_all[f]

            #只保存factorlist中的列
            if pure==True:
                df_factor=pd.DataFrame(df_factor,columns=factor_list)
                df_factor=pd.concat([df_price,df_factor],axis=1)
            df_factor.to_pickle(code_factors_path)


            #想了想，这里没法保证不同list相同因子重复写入的问题，只能先写到1里，然后移动到2里
            for factor_name in df_factor.columns.tolist():
                single_factors_path1=mypath+"/data/single_factors_tmp1/"+factor_name+'.csv'
                single_factors_path2=mypath+"/data/single_factors_tmp2/"+factor_name+'.csv'
                if factor_name not in ['ts_code','trade_date'] and not os.path.exists(single_factors_path2):
                    df=pd.DataFrame(df_factor,columns=['ts_code','trade_date',factor_name])
                    df.to_csv(single_factors_path1,mode='a',encoding='utf-8',header=False,index=False)
                    del df
            
            

            #objgraph.show_most_common_types(limit=50)
            if check:
                print('check:'+ts_code)
                print(df_factor)
                return df_factor
            else:
                
                del df_factor
                del df_all
                del df_250
                del df_price
                gc.collect()                  
                return True
        except Exception as e:
            print("err exception is %s" % traceback.format_exc())
            





    # #计算全部股票的单个因子
    # def computeFactorAllStock(factor_name,df_price,where='',db='tushare'):
    #     indicators,func_name,code,return_fileds=indicatorCompute.getFactorInfo(factor_name)
    #     if indicators==False:
    #         return df_price
        
    #     df_result=df_price.copy()
    #     pattern = re.compile(r"df\[\'(\w*?)\'\]")   # 查找数字
    #     flist = pattern.findall(code)
    #     rlist=return_fileds
        
    #     #计算因子引用，先计算其它因子
    #     flist=list(set(flist) - set(rlist))
    #     for f in flist:
    #         if not f in df_price.columns:
    #             #print("depend factor:"+f)
    #             df_price=indicatorCompute.computeFactorByStock(ts_code,f,df_price,db)
                
    #     factor=factor_name.split('_')
    #     module = getattr(import_module('factors.indicators.'+indicators), indicators)
    #     func=getattr(module,func_name,lambda x,y:x)

    #     shift="0"
    #     if len(factor)>1:
    #         shift=factor[-1]

    #     #给计算因子的函数传后面_分割的参数
    #     for i in range(1,len(factor)):
    #         factor[i]=int(factor[i])


    #     df=func(df_price,factor)
    #     print(df)
        
        
        

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





    # def computeAlpha():
    #     pass
    
    # #计算全部因子，取消该函数
    # def computeAll(db):
    #     pandarallel.initialize(nb_workers=28,progress_bar=True,use_memory_fs=True)
    #     mydb.exec("drop table if exists factors_all_tmp",'factors')
    #     df_code= AStock.getStockCodeList(db)
    #     order = np.random.permutation(df_code.shape[0])
    #     df_code = df_code.take(order)
    #     df_all=df_code.parallel_apply(indicatorCompute.computeListByStock,axis=1)
    #     #df_all=df_code.apply(indicatorCompute.computeListByStock,axis=1)
    #     indicatorCompute.putData('factors_all','factors_all_tmp','factors')

    # #计算所有横向指标，忘了是干啥的了，估计没啥用
    # def computeTC(list_name='all',condition='',factor_list=None,pure=False,df_price=pd.DataFrame(),db='tushare'):
    #     df_price_all=AStock.getAllPrice()
    #     factor_list2=[]
    #     for row in factor_list.copy():
    #         factor_name=row            
    #         indicators,func_name,code,return_fileds=indicatorCompute.getFactorInfo(factor_name)
    #         if "#TC" in code:
    #             df_tc_all=indicatorCompute.computeFactorAllStock(factor_name,df_price_all,condition='',db='tushare')
    #         print(df_tc_all)
    #     exit()


    #刷新全部因子，根据indicators中的文件
    # def refreshAllFactorList():
    #     return_fileds=[]
    #     path = os.path.dirname(__file__)+"/indicators/"
    #     for subfile in os.listdir(path):
    #         if not '__' in subfile:
    #             indicators=subfile.split('.py')
    #             indicators=indicators[0]
    #             function_name=''
    #             code=''
                
    #             find=False
    #             with open(path+subfile) as filecontent:
    #                 for line in filecontent:
    #                     #提取当前函数名
    #                     if('def ' in line):
    #                         function_name=line.split('def ')
    #                         function_name=function_name[1]
    #                         function_name=function_name.split('(')
    #                         function_name=function_name[0]
    #                         function_name=function_name.strip()
    #                         code=line
    #                     else:
    #                         code=code+"\n"+line
    #                     left=line.split('=')
    #                     left=left[0]
                        
    #                     pattern = re.compile(r"df\[\'([A-Za-z0-9_\-]*?)\'\]")   # 查找数字
    #                     flist = pattern.findall(left)
    #                     return_fileds=return_fileds+flist
                     
    #     #print(return_fileds)   
    #     path = os.path.dirname(__file__)+"/factorlist/"
    #     with open(path+'all','w') as file_object:
    #         file_object.write("\n".join(return_fileds))        
    #     return return_fileds
        

    #遍历indicators文件，查找计算这个因子的函数名称，返回indicators,func_name,code,return_fileds
    #indicators，对应的命名空间，也就是py文件
    #func_name那么，对应的函数名
    #code 对应代码
    #return_fileds 返回字段，在code的return df[] 中遍历
    
    
    #替换临时表为目标表，同时设置索引
    # def putData(table,tmptable,todb='false',db='factors'):
    #     index_list=['ts_code','end_date','trade_date']
    #     for index in index_list:
    #         sql="CREATE INDEX "+index+" ON "+tmptable+" ("+index+"(10)) "
    #         mydb.exec(sql,db)        
    #     mydb.exec('rename table '+table+' to '+table+'_old;','factors');
    #     mydb.exec('rename table '+tmptable+' to '+table+';','factors');
    #     mydb.exec("drop table if exists "+table+'_old','factors')