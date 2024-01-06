from finhack.library.config import Config
from finhack.library.mydb import mydb
from finhack.market.astock.astock import AStock
import time
import hashlib
import pandas as pd
import os
import re
import datetime
import warnings
import numpy as np
from functools import lru_cache
from importlib import import_module
from pandarallel import pandarallel
import finhack.library.log as Log
from finhack.factor.default.factorManager import factorManager
from finhack.factor.default.alphaEngine import alphaEngine
from finhack.factor.default.indicatorCompute import indicatorCompute
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor, wait, ALL_COMPLETED
from runtime.constant import *
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

class preCheck():
    
    def beforeCheck():
        mydb.exec('update finhack.factors_list set status="checking"','finhack')
        pass
    
    def afterCheck():
        mydb.exec('update finhack.factors_list set status="deleted" where status="checking"','finhack')  
        pass
    
    #检查全部因子有效性
    def checkAllFactors():
        print("checkAllFactors---Start---")
        preCheck.beforeCheck()
        check_list=preCheck.checkIndicatorsChange()
        
        if len(check_list)>0:
            preCheck.checkIndicatorsType(check_list)
        preCheck.checkAlphas()  #当前Alphas应该不会存在未来函数，跳过检测
        preCheck.afterCheck()    
        print("checkAllFactors--End---")
        return check_list
    



    
    def checkIndicatorsChange():
        print("检测因子函数变动---Start---")
        check_list=[]
        all_factors=factorManager.getIndicatorsList()
        for factor_name in all_factors:
            find=False
            factor=factor_name.split('_')
            factor_filed=factor[0]
            return_fileds=[]
            path = INDICATORS_DIR
            for subfile in os.listdir(path):
                if find:
                    continue
                if not '__' in subfile:
                    indicators=subfile.split('.py')
                    indicators=indicators[0]
                    function_name=''
                    code=''
                    return_fileds=[]
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
                            
                            

                            pattern = re.compile(r"df\[\'([A-Za-z0-9_\-]*?)\'\]")   # 查找数字
                            flist = pattern.findall(left)
                            return_fileds=return_fileds+flist
    
                            if("df['"+factor_filed+"_") in left or ("df['"+factor_filed+"']") in left:
                                find=True
                                mydb.exec('update finhack.factors_list set status="acvivate" where factor_name="%s"' % (factor_name),'finhack')   
                            if 'return ' in  line:
                                if find:
                                    old_md5=mydb.selectToDf('select md5 from factors_list where BINARY  factor_name="%s"' % (factor_name),'finhack')
                                    now=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
                                    hashstr="%s%s%s%s%s" % (factor_name,indicators,function_name,code,",".join(return_fileds))
                                    md5=hashlib.md5(hashstr.encode(encoding='utf-8')).hexdigest()
                                    if old_md5.empty:
                                        insertsql="INSERT INTO `finhack`.`factors_list`(`factor_name`, `indicators`, `func_name`, `code`, `return_fileds`, `md5`) \
                                        VALUES ('%s', '%s', '%s', '%s', '%s', '%s')"  % (factor_name,indicators,function_name,code.replace("'" , "\\'").replace("\n" , "\\n"),",".join(return_fileds),md5)
                                        print(insertsql)
                                        mydb.exec(insertsql,'finhack')
                                        check_list.append(factor_name)
                                    elif old_md5.md5.tolist()[0]!=md5:
                                        updatesql="update finhack.factors_list set indicators='%s',func_name='%s',code='%s',return_fileds='%s',md5='%s',check_type=0,updated_at='%s',check_type=0,status='acvivate' \
                                        where factor_name='%s'"  % (indicators,function_name,code.replace("'" , "\\'").replace("\n" , "\\n"),",".join(return_fileds),md5,now,factor_name)
                                        mydb.exec(updatesql,'finhack')
                                        print(factor_name+md5)
                                        check_list.append(factor_name)
                                    break                              
        
        uncheck=mydb.selectToDf("select factor_name from finhack.factors_list where factor_name not like 'alpha%'  and check_type=0",'tushare')

        print("检测因子函数变动---End---")
        
        
        
        if uncheck.empty:
            return check_list
        else:
            return check_list+uncheck['factor_name'].tolist()
        
        
        
 

    
    
    

        
        
    #检查计算类型
    def checkIndicatorsType(check_list):
        if check_list==[]:
            return True
        ts_code='000001.sz'
        print('检查指标计算类型---Start---')
        print("check_list:"+str(check_list))
        df_all=AStock.getStockDailyPriceByCode(ts_code,where='',startdate='20150619',enddate='20200805',fq='hfq')

        
        print(df_all)
        
        
        
        
        df_250=AStock.getStockDailyPriceByCode(ts_code,where='',startdate='20180619',enddate='20200805',fq='hfq')
        df_250e=df_250.copy()
        
 
       
        daterange=df_250e['trade_date'].tolist()        

        
        #四种计算类型，直接计算全部，逐日计算全部，直接计算250日数据，逐日计算250日数据
        #首先比对直接计算250日数据和逐日计算250日数据，匹配【直接】还是【逐日】类型
        #然后比对直接计算全部数据，和直接计算250日数据，匹配是【全部】还是【250】类型
        
        
        factors250e=None
        factors250=None
        factorsAll=None
        factorsAlle=None

 
        
        e250=[]
        n=0
        for date in daterange:

            df_tmp=df_250e[(df_250e.trade_date>=daterange[0]) & (df_250e.trade_date<=date)].copy()
            factor_tmp=indicatorCompute.computeListByStock(ts_code=ts_code,df_price=df_tmp,check=True,factor_list=check_list,pure=False,db='tushare')
            if 'None' in str(type(factor_tmp)) or len(factor_tmp)==0:
                continue
            # print(factor_tmp)
            # print(1111111)
            #pd.set_option('display.max_columns',1000) 
            e250.append(factor_tmp.tail(1))
            n=n+1
            
 
 
       
        #exit()
 
        factors250e=pd.concat(e250,ignore_index=False)
        #factors250e = factors250e.drop(labels='index', axis=1)
        factors250e=factors250e.tail(250)
        factors250e=factors250e.reset_index(drop=True) 
        factors250e=factors250e.fillna(0)
        #print(factors250e)
        
        #  print(df_250)
        #  print()
       
        factors250=indicatorCompute.computeListByStock(ts_code,list_name='all',df_price=df_250,check=True,factor_list=check_list,pure=False,db='tushare')
        factors250=factors250.tail(len(factors250e))
        #factors250=factors250.drop(labels='index', axis=1)
        factors250=factors250.reset_index(drop=True) 
        factors250=factors250.fillna(0)
        #print(factors250)
        
        factorsAll=indicatorCompute.computeListByStock(ts_code,list_name='all',df_price=df_all,check=True,factor_list=check_list,pure=False,db='tushare')
        factorsAll=factorsAll.tail(len(factors250e))
        #factorsAll=factorsAll.drop(labels='index', axis=1)
        factorsAll=factorsAll.reset_index(drop=True) 
        factorsAll=factorsAll.fillna(0)
        #print(factorsAll)


        for factor in factorsAll.columns.tolist():
            ftype=""
            factor_name=factor.split('_')[0]
            if factors250[factor].tolist()==factors250e[factor].tolist():
                ftype='d' #direct
            else:
                ftype='e' #each
            if factors250[factor].tolist()==factors250e[factor].tolist():
                ftype=ftype+'s' #small
            else:
                ftype=ftype+'a' #all
                
            
            
            desc=factorsAll[factor].describe()
            if factorsAll[factor].isnull().all():
                ftype=ftype+'2' 
            elif 'unique' in desc.keys():
                if  desc['unique']==1:
                    ftype=ftype+'5'   
                else:
                    ftype=ftype+'1'
            elif desc['max']==desc['min']:
                if desc['max']==0:
                    ftype=ftype+'4' 
                else:
                    ftype=ftype+'6'
            else:
                ftype=ftype+'1'  
            ftype=ftype.replace("ds","1")
            ftype=ftype.replace("da","2")
            ftype=ftype.replace("es","3")
            ftype=ftype.replace("ea","4")
            print("------{%s,%s}-----" % (ftype,factor_name))
            updatesql="update finhack.factors_list set check_type=%s,status='acvivate' where factor_name='%s'"  % (ftype,factor_name)
            mydb.exec(updatesql,'finhack')   
                
 
        print('检查指标计算类型---End---')
        pass
    
    
    def checkConflict():
        pass
    
    
    
    
    
    #check_type=compute_type+error_type
    #compute_type ds=1,da=2,es=3,ea=4
    #error_type ok=1,nan=2,inf=3,0=4,false=5
    def getFactorCheckType(factor_name, indicators, func_name, code, return_fileds):
        old_md5=mydb.selectToDf('select md5 from factors_list where BINARY factor_name="%s"' % (factor_name),'finhack')
        now=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        hashstr="%s%s%s%s%s" % (factor_name,indicators,func_name,code,",".join(return_fileds))
        md5=hashlib.md5(hashstr.encode(encoding='utf-8')).hexdigest()
        #新增
        if old_md5.empty:
            insertsql="INSERT INTO `finhack`.`factors_list`(`factor_name`, `indicators`, `func_name`, `code`, `return_fileds`, `md5`) \
            VALUES ('%s', '%s', '%s', '%s', '%s', '%s')"  % (factor_name,indicators,func_name,code.replace("'" , "\\'").replace("\n" , "\\n"),",".join(return_fileds),md5)
            print(insertsql)
            mydb.exec(insertsql,'finhack')
            compute_type=0
            return 0
        #变化
        elif old_md5.md5.tolist()[0]!=md5:
            updatesql="update finhack.factors_list set indicators='%s',func_name='%s',code='%s',return_fileds='%s',md5='%s',updated_at='%s',check_type=0,status='acvivate' \
                where factor_name='%s'"  % (indicators,func_name,code.replace("'" , "\\'").replace("\n" , "\\n"),",".join(return_fileds),md5,now,factor_name)
            mydb.exec(updatesql,'finhack')
            print(factor_name+md5)     
            compute_type=0
            return 0
        #不变
        else:
            check_type=mydb.selectToDf('select check_type from factors_list where BINARY  factor_name="%s"' % (factor_name),'finhack')
        mydb.exec('update finhack.factors_list set status="acvivate" where factor_name="%s"' % (factor_name),'finhack')   
        return check_type['check_type'].tolist()[0]
            
        
 

    
    
    #检测某个alpha是否有效
    def checkAlpha(listname,alphaname,alpha):
        try:
            #
            ftype=preCheck.getFactorCheckType(factor_name=alphaname, indicators=listname, func_name=alphaname, code=alpha, return_fileds=[alphaname])
            if int(ftype)<10:
                print("checking "+alphaname)
                check_stock=['000001.SZ','000002.SZ','000004.SZ','000005.SZ','000006.SZ','000007.SZ','000008.SZ','000009.SZ','000010.SZ','000011.SZ']
                price_all=AStock.getStockDailyPrice(check_stock,where='',startdate='20150619',enddate='20200805',fq='hfq')
                price_all=price_all.set_index(['ts_code','trade_date'])
                price_250=AStock.getStockDailyPrice(check_stock,where='',startdate='20180619',enddate='20200805',fq='hfq')
                price_250a=price_250.set_index(['ts_code','trade_date'])
                factorsAll=alphaEngine.calc(formula=alpha,df=price_all,name=alphaname,check=True)
                print(factorsAll)
               
                if factorsAll.empty:
                    ftype=77
                elif not '000001.SZ' in factorsAll.index:
                    ftype=78
                else:
                    factorsAll=factorsAll.loc['000001.SZ']
                    factorsAll=factorsAll.tail(250)
                    factorsAll=factorsAll.reset_index(drop=True) 
                    factorsAll=factorsAll.fillna(0)                
                    factors250=alphaEngine.calc(alpha,price_250a,alphaname,True)
                    factors250=factors250.loc['000001.SZ']
                    factors250=factors250.reset_index(drop=True) 
                    factors250=factors250.tail(250)
                    factors250=factors250.fillna(0)
                    daterange=price_250a.loc['000001.SZ'].index.tolist()
                    e250=[]
                    for date in daterange:
                        #print(date+"-"+alphaname)
                        df_tmp=price_250[(price_250.trade_date>=daterange[0]) & (price_250.trade_date<=date)].copy()
                        df_tmp=df_tmp.set_index(['ts_code','trade_date'])
                        factor_tmp=alphaEngine.calc(alpha,df_tmp,alphaname,True)
                        
                        if not '000001.SZ' in factor_tmp.index:
                            #print(factor_tmp)
                            #exit()
                            pass
                        else:
                            factor_tmp=factor_tmp.loc['000001.SZ']
                            e250.append(factor_tmp.tail(1))
                    if(e250==[]):
                        print('----------------------')
                        pass
                    #print(alphaname+' status:1--------------')
                    factors250e=pd.concat(e250,ignore_index=False)
                    factors250e=factors250e.tail(250)
                    factors250e=factors250e.reset_index(drop=True) 
                    factors250e=factors250e.fillna(0)
                    if factors250.tolist()==factors250e.tolist():
                        ftype='d' #direct
                    else:
                        ftype='e' #each
                    if factors250.tolist()==factors250e.tolist():
                        ftype=ftype+'s' #small
                    else:
                        ftype=ftype+'a' #all
                        
                    
                    #print(alphaname+' status:2--------------')
                    desc=factorsAll.describe()
                    if factorsAll.isnull().all():
                        ftype=ftype+'2'  #第二位为2，全是null     
                    elif 'unique' in desc.keys():
                        if  desc['unique']==1:
                            ftype=ftype+'5'   #第二位为5，全是一个值    
                        else:
                            ftype=ftype+'1' #第二位为1，正常
                    elif desc['max']==desc['min']:
                        if desc['max']==0:
                            ftype=ftype+'4' #第二位为4，全是0   
                        else:
                            ftype=ftype+'6' #第二位为4，全是一个值
                    else:
                        ftype=ftype+'1'  #第二位为1，正常
                    #print(alphaname+' status:3--------------')
                    ftype=ftype.replace("ds","1") #可以直接计算df250
                    ftype=ftype.replace("da","2") #需要直接计算dfall
                    ftype=ftype.replace("es","3") #需要逐行计算df250
                    ftype=ftype.replace("ea","4") #需要逐行计算dfall
                print("------{%s,%s}-----" % (ftype,alphaname))
                updatesql="update finhack.factors_list set check_type=%s,status='acvivate' where factor_name='%s'"  % (ftype,alphaname)
                mydb.exec(updatesql,'finhack')   
            if ftype==41:
                pass
        except Exception as e:
            print("precheck error:",str(e))
 

            
                
                
                
                
    def checkAlphas():
        print('checkAlphas')
        alphalists=factorManager.getAlphaLists()
        for listname in alphalists:
            alphas=factorManager.getAlphaList(listname)
            i=0
            tasklist=[]
            with ProcessPoolExecutor(max_workers=24) as pool:
                for alpha in alphas:
                    i=i+1
                    alphaname=listname+'_'+str(i).zfill(3)
                    
                    #print(alphaname+":"+alpha)
                    #preCheck.checkAlpha(listname=listname,alphaname=alphaname,alpha=alpha)
                    task=pool.submit(preCheck.checkAlpha,listname,alphaname,alpha)
                    tasklist.append(task)
            wait(tasklist, return_when=ALL_COMPLETED)
            print('---------------')
                
 