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
import gc
import finhack.library.log as Log
class factorAnalyzer():
    
    
    def all_corr():
        pass
    
    
    
    def stock_pool(max_num=300):
        # 从数据库中获取数据
        df = mydb.selectToDf('SELECT a.ts_code, trade_date, pe, pb, ps, total_mv, industry, market FROM astock_price_daily_basic a LEFT JOIN astock_basic b ON a.ts_code=b.ts_code', 'tushare')
    
        # 确保 'trade_date' 是 datetime 类型
        df['trade_date'] = pd.to_datetime(df['trade_date'])
    
        # 将需要聚合的列转换为数值类型，并处理无法转换的数据
        agg_columns = ['pe', 'pb', 'ps', 'total_mv']
        for col in agg_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
        # 删除在转换过程中产生的NaN值所在的行，以便聚合函数可以正常工作
        df.dropna(subset=agg_columns, inplace=True)
    
        # 初始化选出的股票列表
        selected_stocks = []
    
        # 获取市场的唯一值
        markets = df['market'].unique()
    
        while len(selected_stocks) < max_num and not df.empty:
            # 计算每个市场应该选择的股票数量
            market_allocation = {market: (max_num - len(selected_stocks)) // len(markets) for market in markets}
    
            # 分配剩余的股票（如果有）
            remaining_slots = max_num - len(selected_stocks) - sum(market_allocation.values())
            for market in sorted(market_allocation, key=market_allocation.get, reverse=True):
                if remaining_slots > 0:
                    market_allocation[market] += 1
                    remaining_slots -= 1
    
            # 按市场分组并选择股票
            for market, num_stocks in market_allocation.items():
                if num_stocks <= 0:
                    continue
    
                market_df = df[df['market'] == market]
    
                # 对于每个财务指标，将市场内的股票分为三等分
                tertiles = {col: market_df[col].quantile([1/3, 2/3]).values for col in agg_columns}
    
                # 从每个三等分中选择股票
                market_selected_stocks = []
                for col in agg_columns:
                    for i, tertile in enumerate(['low', 'mid', 'high']):
                        if tertile == 'low':
                            tertile_df = market_df[market_df[col] <= tertiles[col][0]]
                        elif tertile == 'mid':
                            tertile_df = market_df[(market_df[col] > tertiles[col][0]) & (market_df[col] <= tertiles[col][1])]
                        else:
                            tertile_df = market_df[market_df[col] > tertiles[col][1]]
    
                        tertile_df = tertile_df.sort_values(by=col)
                        num_to_select_per_tertile = max(1, num_stocks // (3 * len(agg_columns)))
                        selected_tertile_stocks = tertile_df.head(num_to_select_per_tertile)['ts_code'].tolist()
                        market_selected_stocks += selected_tertile_stocks
    
                        # 从原始数据中删除已选出的股票
                        df = df[~df['ts_code'].isin(selected_tertile_stocks)]
    
                selected_stocks += market_selected_stocks
                selected_stocks=list(set(selected_stocks))
    
        # 去除可能的重复并限制数量为max_num
        selected_stocks = list(dict.fromkeys(selected_stocks))[:max_num]
    
        # 创建一个包含选出股票的新DataFrame
        selected_df = df[df['ts_code'].isin(selected_stocks)]
    
 
        return selected_stocks
        
    
    
    def alphalens(factor_name='alpha',df=pd.DataFrame(),notebook=False):
        
        df_industry=AStock.getStockIndustry()

        if df.empty:
            df=factorManager.getFactors(factor_list=['close',factor_name])
        else:
            df_close=factorManager.getFactors(factor_list=['close'])
            df_close[factor_name]=df
            df=df_close
        

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
        #Wcumulative_returns_by_qt = al.performance.cumulative_returns_by_quantile(factor_data, period=1)
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
        
        
        if notebook:
            full_tear_sheet = al.tears.create_full_tear_sheet(factor_data, long_short=True, group_neutral=False, by_group=False)
            print(full_tear_sheet)
        else:
        
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
        
        
        #al.plotting.plot_quantile_returns_bar(mean_return_by_qt)
        pass
    
    
    
    def analys(factor_name,df=pd.DataFrame(),days=[1,2,3,5,8,13,21],source='mining',start_date='20100101',end_date='20200101',formula="",replace=False,table='factors_analysis',ignore_error=False,stock_list=[]):
        try:
            hashstr=factor_name+'-'+(str(days))+'-'+source+'-'+start_date+':'+end_date+'#'+formula
            md5=hashlib.md5(hashstr.encode(encoding='utf-8')).hexdigest()
            
            has=mydb.selectToDf('select * from %s where  hash="%s"' % (table,md5),'finhack')
            
            
            #有值且不替换
            if(not has.empty and not replace):  
                return True
            
            if df.empty:
                df=factorManager.getFactors(factor_list=['close','open',factor_name])
 
            df.reset_index(inplace=True)
            df['trade_date']= df['trade_date'].astype('string')
            if start_date!='':
                df=df[df.trade_date>=start_date]
            if end_date!='':
                df=df[df.trade_date<end_date]
                
            if stock_list!=[]:
                df=df.reset_index()
                df = df[df['ts_code'].isin(stock_list)]

            df=df.set_index(['ts_code','trade_date'])
            

            
            
            IC_list=[]
            IR_list=[]
            
            df = df.replace([np.inf, -np.inf], np.nan)
            df=df.dropna()
  
            desc=df[factor_name].describe()
            
            #print(df)
            # exit()
            
            if 'mean' in desc and desc['mean']==desc['max']:
                if factor_name!='alpha':
                    updatesql="update finhack.factors_list set check_type=%s,status='acvivate' where factor_name='%s'"  % ('14',factor_name)
                    mydb.exec(updatesql,'finhack')  
                return False
            
            
            Sharpe_list=[]

            for day in days:
                df['return']=df.groupby('ts_code',group_keys=False).apply(lambda x: x['close'].shift(-1*day)/x['open'].shift(-1))
                df_tmp=df.copy().dropna()
                sharpe_ratio=factorAnalyzer.Sharpe(df_tmp,factor_name=factor_name)
                Sharpe_list.append(sharpe_ratio)
                
                IC,IR=factorAnalyzer.ICIR(df_tmp,factor_name)
        
                IR_list.append(IR)
                IC_list.append(IC)
                del df_tmp
                
            
            IC=np.sum(IC_list)/len(IC_list)
            IR=np.sum(IR_list)/len(IR_list)
            max_sharpe=np.max(Sharpe_list)
            score=abs(IC)*10+abs(IR)+abs(max_sharpe)
            

            if formula=="":
                Log.logger.info("factor_name:%s,IC=%s,IR=%s,Sharpe=%s,score=%s" % (factor_name,str(IC),str(IR),str(max_sharpe),str(score)))
            else:
                Log.logger.info("%s\nIC=%s,IR=%s,Sharpe=%s,score=%s\n" % (formula,str(IC),str(IR),str(max_sharpe),str(score)))
            if pd.isna(score):
                #print("score na:"+formula)
                return False
            
            
            if table!="":
                #有值且替换
                if(not has.empty and  replace):  
                    del_sql="DELETE FROM `finhack`.`%s` WHERE `hash` = '%s'" % (table,md5)    
                    mydb.exec(del_sql,'finhack')
                insert_sql="INSERT INTO `finhack`.`%s`(`factor_name`, `days`, `source`, `start_date`, `end_date`, `formula`, `IC`, `IR`, `Sharpe`, `score`, `hash`) VALUES ( '%s', '%s', '%s', '%s', '%s', '%s', %s, %s, %s, %s, '%s')" %  (table,factor_name,str(days),source,start_date,end_date,formula,str(IC),str(IR),str(max_sharpe),str(score),md5)
                mydb.exec(insert_sql,'finhack')
            #print(insert_sql)
            
            # print(IC_list)
            # print(IR_list)

            return factor_name,IC,IR,sharpe_ratio,score
    
        except Exception as e:

            
            if not ignore_error:
                Log.logger.info(factor_name+" error:"+str(e))
                Log.logger.info("err exception is %s" % traceback.format_exc())
    
    
    


    

    
    
    def Sharpe(df,factor_name,n=10):
        # 使用 lambda 表达式进行标准化
        df['alpha_std'] = df.groupby('trade_date')[factor_name].transform(
            lambda x: (x - x.mean()) / x.std() if x.std() != 0 else 0
        )

        def quantile_cut(group, n):
            try:
                # 尝试创建分位数
                res, bins = pd.qcut(group, q=n, retbins=True, duplicates='drop')
                # 根据实际的分位数边界数量来创建标签
                labels = range(1, len(bins))
                # 应用新的标签
                return pd.cut(group, bins=bins, labels=labels, include_lowest=True)
            except ValueError as e:
                # 如果出现错误，可以进一步处理，比如减少n的值或者其他逻辑
                print("Error in quantile_cut: ", e)
                return pd.Series([None] * len(group), index=group.index)
        
        # 应用transform方法
        df['alpha_quantile'] = df.groupby('trade_date')['alpha_std'].transform(lambda x: quantile_cut(x, n))
        

        df=df.dropna()

        quantile_return_lists = {}
        quantile_performance = {}
        #print(df)
        # 按照trade_date和alpha_quantile分组，计算平均return
        grouped = df.groupby(['trade_date', 'alpha_quantile'])

        # 遍历每个alpha_quantile分组
        for quantile in df['alpha_quantile'].unique():
            mean_df=grouped['return'].mean().fillna(1)
            quantile_return_lists[quantile] = mean_df.loc[pd.IndexSlice[:, quantile]].tolist()       
            excess_returns = [x - 1 for x in quantile_return_lists[quantile]]
            # 计算平均超额收益率
            average_excess_return = np.mean(excess_returns)
            #print(average_excess_return)
            # 计算超额收益率的标准差
            standard_deviation = np.std(excess_returns)
            # 计算夏普比率
            sharpe_ratio = average_excess_return / standard_deviation if standard_deviation != 0 else 0
            
            quantile_performance[quantile]={
                'net_value':np.cumprod(quantile_return_lists[quantile])[-1],
                'sharpe_ratio':sharpe_ratio 
            }

        # 计算总净值
        total_net_value = sum(item['net_value'] for item in quantile_performance.values())
        
        # 计算加权平均夏普比率
        weighted_sharpe_ratios = sum((item['net_value'] / total_net_value) * item['sharpe_ratio'] for item in quantile_performance.values())
        
    
        return weighted_sharpe_ratios

        
    
    
    def ICIR(df, factor_name, period=120):
        IC_all = []
        len_data = len(df)
        fix = int(len_data / period)  # 计算可以分成多少个完整的周期
        df = df.sort_index(level='trade_date')
        df_reset = df.reset_index()
        df_reset['trade_date'] = pd.to_datetime(df_reset['trade_date'], format='%Y%m%d')
        
        grouped = df_reset.groupby(pd.Grouper(key='trade_date', freq='{}D'.format(period)))
        
        for name, group in grouped:
            #print(group)
            corr = group[factor_name].corr(group['return'])
            if not math.isnan(corr) and not math.isinf(corr):
                IC_all.append(corr)

        # 如果没有有效的相关性数据，返回 NaN
        if not IC_all:
            return 0, 0
        
        # 计算 IC 和 IR
        IC = np.mean(IC_all)
        IC_std = np.std(IC_all, ddof=1)  # 使用样本标准差
        
        # 避免除以零的情况
        if IC_std == 0:
            IR = 0
        else:
            IR = IC / IC_std
        
        return IC, IR
 
 
    def rank_ICIR(df, factor_name, period=120):
        rank_IC_all = []
        len_data = len(df)
        fix = int(len_data / period)  # 计算可以分成多少个完整的周期
        df = df.sort_index(level='trade_date')
        df_reset = df.reset_index()
        df_reset['trade_date'] = pd.to_datetime(df_reset['trade_date'], format='%Y%m%d')
        
        grouped = df_reset.groupby(pd.Grouper(key='trade_date', freq='{}D'.format(period)))
        
        for name, group in grouped:
            # 对因子值和未来收益率进行排名
            factor_ranks = group[factor_name].rank(method='average')
            return_ranks = group['return'].rank(method='average')
            
            # 计算排名后的相关系数
            corr = factor_ranks.corr(return_ranks)
            if not math.isnan(corr) and not math.isinf(corr):
                rank_IC_all.append(corr)
    
        # 如果没有有效的相关性数据，返回 NaN
        if not rank_IC_all:
            return 0, 0
        
        # 计算 rank IC 和 rank IR
        rank_IC = np.mean(rank_IC_all)
        rank_IC_std = np.std(rank_IC_all, ddof=1)  # 使用样本标准差
        
        # 避免除以零的情况
        if rank_IC_std == 0:
            rank_IR = 0
        else:
            rank_IR = rank_IC / rank_IC_std
        
        return rank_IC, rank_IR

 
#IC>0.05,IR>0.5