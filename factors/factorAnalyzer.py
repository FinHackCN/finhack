from library.factor import factor
import pandas as pd
import numpy as np
import math
from library.mydb import mydb
import hashlib

class factorAnalyzer():
    def analys(factor_name,df=pd.DataFrame(),days=[1,2,3,5,8,13,21],pool='all',start_date='20000101',end_date='20100101',formula="",relace=False):
        
        
        hashstr=factor_name+'-'+(str(days))+'-'+pool+'-'+start_date+':'+end_date+'#'+formula
        md5=hashlib.md5(hashstr.encode(encoding='utf-8')).hexdigest()
        
        has=mydb.selectToDf('select hash from factors_analysis where BINARY  hash="%s"' % (md5),'finhack')
        
        #有值且不替换
        if(not has.empty and not relace):  
            return True
        
        if df.empty:
            df=factor.getFactors(factor_list=['close',factor_name])
        
        df.reset_index(inplace=True)
        df['trade_date']= df['trade_date'].astype('string')
        df=df[df.trade_date>=start_date]
        df=df[df.trade_date<end_date]
        df=df.set_index(['ts_code','trade_date'])
        
        
        
        
        IC_list=[]
        IR_list=[]

        for day in days:
            df['return']=df.groupby('ts_code')['close'].shift(-day)/df['close']
            df=df[df[factor_name].notna() & df['return'].notna()]
            IC,IR=factorAnalyzer.ICIR(df,factor_name)
            IR_list.append(IR)
            IC_list.append(IC)
            
        IRR=IR/np.std(IR_list)/7
        
        IC=np.sum(IC_list)/len(IC_list)
        IR=np.sum(IR_list)/len(IR_list)
        
        score=abs(IRR*IR*IC)
        # if(abs(IC)<0.05 or abs(IR)<0.5):
        #     score=0
        
        
        #有值且不替换
        if(not has.empty and  relace):  
            del_sql="DELETE FROM `finhack`.`factors_analysis` WHERE `hash` = '%s'" % (md5)    
            mydb.exec(del_sql,'finhack')
        insert_sql="INSERT INTO `finhack`.`factors_analysis`(`factor_name`, `days`, `pool`, `start_date`, `end_date`, `formula`, `IC`, `IR`, `IRR`, `score`, `hash`) VALUES ( '%s', '%s', '%s', '%s', '%s', '%s', %s, %s, %s, %s, '%s')" %  (factor_name,str(days),pool,start_date,end_date,formula,str(IC),str(IR),str(IRR),str(score),md5)
        mydb.exec(insert_sql,'finhack')
        #print(insert_sql)
        
        # print(IC_list)
        # print(IR_list)
        
        print("factor_name:%s,IC=%s,IR=%s,IRR=%s,score=%s" % (factor_name,str(IC),str(IR),str(IRR),str(score)))
    
    
    
    def ICIR(df,factor_name,period=120):
        grouped=df.groupby('ts_code')
        IC_all=[]
        IR_all=[]
        for code,data in grouped:
            len_data=len(data)
            if len_data<period:
                continue
            fix=int(len_data/period)
            data=data.iloc[len_data-fix*period:]
            for i in range(0,fix):
                data_tmp=data.iloc[i*period:i*period+period]
                corr=data_tmp[factor_name].corr(data_tmp['return'])
                if not math.isnan(corr) and not math.isinf(corr):
                    IC_all.append(corr)
 
        IC=np.sum(IC_all)/len(IC_all)
        IR=IC/np.std(IC_all)
 

        # print(IC)
        # print(IR)

        return IC,IR

# 0.03 可能有用 
# 0.05 好
# 0.10 很好
# 0.15 非常好
# 0.2 可能错误(未来函数)

#IC>0.05,IR>0.5