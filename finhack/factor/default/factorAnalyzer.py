import math
import hashlib
import traceback
import numpy as np
import pandas as pd
import pandas as pd
import alphalens as al
from alphalens.utils import get_clean_factor_and_forward_returns
from alphalens.tears import create_full_tear_sheet
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=RuntimeWarning)
warnings.simplefilter(action='ignore', category=UserWarning)
from finhack.library.mydb import mydb
from finhack.factor.default.factorManager import factorManager
from finhack.market.astock.astock import AStock
from scipy.stats import zscore

class factorAnalyzer():
    
    
    def all_corr():
        pass
    
    
    
    def alphalens(factor_name):
        
        
        df_industry=AStock.getStockIndustry()

        
        # df_all.index=df_all['date']
        # price.index = pd.to_datetime(price.index)
        # assets = df_all.set_index( [df_all.index,df_all['symbol']], drop=True,append=False, inplace=False)
        df=factorManager.getFactors(factor_list=['close',factor_name])
        # 假设 df 是您提供的 DataFrame，我们首先重置索引
        df = df.reset_index().merge(df_industry, on='ts_code')
        df['industry'] = df['industry'].fillna('其他')
        
        df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
        
        df[factor_name] = df.groupby(['trade_date', 'industry'])[factor_name].transform(zscore)
        
        # 确保因子值没有 NaN，如果有 NaN，可以选择填充或者去除对应的行
        df = df.dropna(subset=[factor_name,'industry'])
        
        # 重置索引，准备进行 Alphalens 分析
        df = df.set_index(['trade_date', 'ts_code'])
        
        # 创建价格 DataFrame
        prices = df['close'].unstack()
        
        # 获取行业中性化后的因子数据
        factor = df[factor_name]
        
        
        unique_industries = df['industry'].unique()
        # 创建 groupby_labels 字典，将每个行业标签映射到自己，确保没有遗漏
        groupby_labels = {ind: ind for ind in unique_industries}

        
        # 检查 groupby_labels 是否包含所有 unique_industries 中的行业
        missing_labels = [ind for ind in unique_industries if ind not in groupby_labels]
        if missing_labels:
            print(f"Missing industry labels in groupby_labels: {missing_labels}")
            # 您可以选择添加缺失的行业标签到 groupby_labels 中
            for missing in missing_labels:
                groupby_labels[missing] = '其他'  # 或者将其映射到 '其他'
        
        # 使用 Alphalens 进行因子分析
        factor_data = al.utils.get_clean_factor_and_forward_returns(
            factor=factor,
            prices=prices,
            periods=(1, 5, 10),
            groupby=df['industry'],  # 指定行业分组
            groupby_labels=groupby_labels,  # 指定行业标签
        )

        
        # 因子收益率分析
        mean_return_by_qt, std_err_by_qt = al.performance.mean_return_by_quantile(factor_data)
        #aal.plotting.plot_quantile_returns_bar(mean_return_by_qt)
        #aplt.show()
        
        # 因子信息比率
        ic_by_day = al.performance.factor_information_coefficient(factor_data)
        #al.plotting.plot_information_coefficient(ic_by_day)
        #plt.show()
        
        # 分位数平均收益率
        quantile_returns = al.performance.mean_return_by_quantile(factor_data)[0].apply(al.utils.rate_of_return, axis=0, base_period='1D')
        #al.plotting.plot_quantile_returns_violin(quantile_returns)
        #plt.show()
        
        # 分位数累积收益
        #cumulative_returns_by_qt = al.performance.cumulative_returns_by_quantile(factor_data, period=1)
        #al.plotting.plot_cumulative_returns_by_quantile(cumulative_returns_by_qt, period=1)
        #plt.show()
        
        # 分位数收益率的全面统计
        #full_tear_sheet = al.tears.create_full_tear_sheet(factor_data, long_short=True, group_neutral=False, by_group=False)
        
        # 因子自相关性分析
        autocorrelation = al.performance.factor_rank_autocorrelation(factor_data)
        #al.plotting.plot_autocorrelation(autocorrelation)
        #plt.show()
        
        # 因子收益率和分位数收益率的IC分析
        mean_monthly_ic = al.performance.mean_information_coefficient(factor_data, by_time='M')
        #al.plotting.plot_monthly_ic_heatmap(mean_monthly_ic)
        #plt.show()
        
        print("\nmean_return_by_qt")
        print(mean_return_by_qt)
        print("\nic_by_day")
        print(ic_by_day)
        print("\nquantile_returns")
        print(quantile_returns)
        print("\nautocorrelation")
        print(autocorrelation)
        print("\nmean_monthly_ic")
        print(mean_monthly_ic)
        
        # print('---')
        # print(full_tear_sheet)
        
        #al.plotting.plot_quantile_returns_bar(mean_return_by_qt)
        pass
    
    
    
    def analys(factor_name,df=pd.DataFrame(),days=[1,2,3,5,8,13,21],pool='all',start_date='20000101',end_date='20100101',formula="",relace=False,table='factors_analysis'):
        try:
        
            hashstr=factor_name+'-'+(str(days))+'-'+pool+'-'+start_date+':'+end_date+'#'+formula
            md5=hashlib.md5(hashstr.encode(encoding='utf-8')).hexdigest()
            
            has=mydb.selectToDf('select * from %s where  hash="%s"' % (table,md5),'finhack')
            
            
            #有值且不替换
            if(not has.empty and not relace):  
                return True
            
            if df.empty:
                df=factorManager.getFactors(factor_list=['close','open',factor_name])
 
            df.reset_index(inplace=True)
            df['trade_date']= df['trade_date'].astype('string')
            if start_date!='':
                df=df[df.trade_date>=start_date]
            if end_date!='':
                df=df[df.trade_date<end_date]
            df=df.set_index(['ts_code','trade_date'])
            
            
            # print(df)
            
            
            
            IC_list=[]
            IR_list=[]
            
            print("factor_name:"+factor_name)
            df = df.replace([np.inf, -np.inf], np.nan)
            df=df.dropna()
  
            desc=df[factor_name].describe()
            
            # print(df)
            # exit()
            
            if desc['mean']==desc['max']:
                if factor_name!='alpha':
                    updatesql="update finhack.factors_list set check_type=%s,status='acvivate' where factor_name='%s'"  % ('14',factor_name)
                    mydb.exec(updatesql,'finhack')  
                return False
    
            for day in days:
                df['return']=df.groupby('ts_code',group_keys=False).apply(lambda x: x['close'].shift(-1*day)/x['open'].shift(-1))
                df_tmp=df.copy().dropna()
                IC,IR=factorAnalyzer.ICIR(df_tmp,factor_name)
                IR_list.append(IR)
                IC_list.append(IC)
                
            IRR=IR/np.std(IR_list)/7
            
            IC=np.sum(IC_list)/len(IC_list)
            IR=np.sum(IR_list)/len(IR_list)
            
            score=abs(IRR*IR*IC)
            # if(abs(IC)<0.05 or abs(IR)<0.5):
            #     score=0
            
            
            
            print("factor_name:%s,IC=%s,IR=%s,IRR=%s,score=%s" % (factor_name,str(IC),str(IR),str(IRR),str(score)))
            if pd.isna(score):
                print("score na:"+factor_name)
                
                return False
            
            #有值且不替换
            if(not has.empty and  relace):  
                del_sql="DELETE FROM `finhack`.`%s` WHERE `hash` = '%s'" % (table,md5)    
                mydb.exec(del_sql,'finhack')
            insert_sql="INSERT INTO `finhack`.`%s`(`factor_name`, `days`, `pool`, `start_date`, `end_date`, `formula`, `IC`, `IR`, `IRR`, `score`, `hash`) VALUES ( '%s', '%s', '%s', '%s', '%s', '%s', %s, %s, %s, %s, '%s')" %  (table,factor_name,str(days),pool,start_date,end_date,formula,str(IC),str(IR),str(IRR),str(score),md5)
            mydb.exec(insert_sql,'finhack')
            #print(insert_sql)
            
            # print(IC_list)
            # print(IR_list)
            
            return factor_name,IC,IR,IRR,score
    
        except Exception as e:
            print(factor_name+" error:"+str(e))
            #print("err exception is %s" % traceback.format_exc())
    
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