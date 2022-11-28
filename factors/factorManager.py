import os
import re
import time
import sys
import datetime
import hashlib
import threading
import pandas as pd
import numpy as np
import traceback
from library.mydb import mydb
from pandarallel import pandarallel
sys.path.append("..")

from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor, wait, ALL_COMPLETED


class factorManager:
    
    def getAnalysedIndicatorsList(valid=True):
        flist=mydb.selectToDf('select * from factors_analysis where factor_name not like "alpha%"','finhack')
        flist=flist['factor_name'].tolist()
        return flist
        
    def getTopAnalysedFactorsList(valid=True,top=100):
        flist=mydb.selectToDf('select * from factors_analysis order by score desc limit '+str(top),'finhack')
        flist=flist['factor_name'].tolist()
        return flist 
    
    
    #获取因子列表
    def getFactorsList(valid=True,ignore=True):
        factorslist=[]
        result=[]
        ignore_list=[]
        path = os.path.dirname(__file__)+"/../data/single_factors"
        if ignore:
            ignore_list=['close','vol','volume','open','low','high','pct_chg','amount','pre_close','vwap','stop','lh']
        for subfile in os.listdir(path):
            if not '__' in subfile and not subfile.replace('.csv','') in ignore_list:
                factorslist.append(subfile.replace('.csv',''))
                
 
                
        if valid==True:
            flist=mydb.selectToDf('select * from factors_list where check_type=11','finhack')
 
            
            for factor in factorslist:
                if 'alpha' in factor:
                    factor_name=factor
                    pass
                else:
                    factor_name=factor.split('_')[0]
                factor_df=flist[flist.factor_name==factor_name]
                if factor_df.empty:
                    continue
                else:
                    check_type=factor_df['check_type'].values
                    if len(check_type)==0 or check_type[0]!=11:
                        continue
                result.append(factor)
            factorslist=result
        return factorslist
    
    
    
    #获取因子数据
    def getFactors(factor_list,stock_list=[],start_date='',end_date=''):
        #print(factor_list)
        mypath=os.path.dirname(os.path.dirname(__file__))
        data_path=mypath+'/data/single_factors/'
        df_factor=pd.DataFrame()
        for factor in factor_list:
            factor=factor.replace('$','')
            if os.path.isfile(data_path+factor+'.csv'):
                df=pd.read_csv(data_path+factor+'.csv',names=['ts_code','trade_date',factor], dtype={'ts_code': str,'trade_date': str, factor: np.float64},low_memory=False)
                #df=df.set_index(['ts_code','trade_date'])
                
                df=df.sort_values(['ts_code','trade_date'])
                df=df.set_index(['ts_code','trade_date'])
                #df[factor]= df[factor].astype('float16')
             
                if df_factor.empty:
                    df_factor=df
                else:
                    df_factor[factor]=df[factor]
                    del df
            else:
                print(data_path+factor+'.csv not found')
        
        
        df_factor=df_factor.reset_index() 
        
        if df_factor.empty:
            return df_factor
        
        if stock_list!=[]:
            df_list=[]
            for ts_code in stock_list:
                df_tmp=df_factor[df_factor.ts_code==ts_code]
                df_list.append(df_tmp)
            df_factor=pd.concat(df_list)
            
        if start_date!="":
            df_factor=df_factor[df_factor.trade_date>=start_date]
        
        if end_date!="":
            df_factor=df_factor[df_factor.trade_date<=end_date]
          
        df_factor=df_factor.set_index(['ts_code','trade_date'])  
        
        return df_factor    
    
    
    #获取alpha列表的列表
    def getAlphaLists():
        alphalists=[]
        path = os.path.dirname(__file__)+"/../lists/alphalist"
        for subfile in os.listdir(path):
            if not '__' in subfile:
                listname=subfile
                alphalists.append(subfile)
        return alphalists
    
    #根据alpha列表获取alpha
    def getAlphaList(listname):
        path = os.path.dirname(__file__)+"/../lists/alphalist/"+listname
        with open(path, 'r', encoding='utf-8') as f:
            return f.readlines()
            

    def getIndicatorsList():
        return_fileds=[]
        path = os.path.dirname(__file__)+"/indicators/"
        for subfile in os.listdir(path):
            if not '__' in subfile:
                indicators=subfile.split('.py')
                indicators=indicators[0]
                function_name=''
                code=''
                find=False
                with open(path+subfile) as filecontent:
                    for line in filecontent:
                        if(line.strip()[0:1]=='#'):
                            code=code+"\n"+line
                            continue
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
                        
                        for f in flist.copy():
                            #前缀是tmp_的都是临时因子，不管
                            if f[:4]=='tmp_':
                                flist.remove(f)
                        return_fileds=return_fileds+flist
                        
         
        path = os.path.dirname(__file__)+"/../lists/factorlist/"
        with open(path+'all','w') as file_object:
            file_object.write("\n".join(return_fileds))  
        
        #print("\n".join(return_fileds))  
        return return_fileds