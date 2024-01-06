import numpy as np
from finhack.market.astock.astock import AStock

class volumeprice:

    def moneyflow(df,p): 
        df_vp=AStock.alignStockFactors(df,'astock_price_moneyflow','trade_date',filed='*',conv=0,db='tushare')  
        if df_vp.empty:
            return df_vp
        df['buySmVol']=df_vp['buy_sm_vol']
        df['buySmAmount']=df_vp['buy_sm_amount']
        df['sellSmVol']=df_vp['sell_sm_vol']
        df['sellSmAmount']=df_vp['sell_sm_amount']
        df['buyMdVol']=df_vp['buy_md_vol']
        df['buyMdAmount']=df_vp['buy_md_amount']
        df['sellMdVol']=df_vp['sell_md_vol']
        df['sellMdAmount']=df_vp['sell_md_amount']
        df['buyLgVol']=df_vp['buy_lg_vol']
        df['buyLgAmount']=df_vp['buy_lg_amount']
        df['sellLgVol']=df_vp['sell_lg_vol']
        df['sellLgAmount']=df_vp['sell_lg_amount']
        df['buyElgVol']=df_vp['buy_elg_vol']
        df['buyElgAmount']=df_vp['buy_elg_amount']
        df['sellElgVol']=df_vp['sell_elg_vol']
        df['sellElgAmount']=df_vp['sell_elg_amount']
        df['netMfVol']=df_vp['net_mf_vol']
        df['netMfAmount']=df_vp['net_mf_amount']
        del df_vp
        return df

    def limit(df,p): 
        df_vp=AStock.alignStockFactors(df,'astock_price_limit_list','trade_date',filed='*',conv=2,db='tushare')  
        df['lAmp']=df_vp['amp']
        df['lFcRatio']=df_vp['fc_ratio']
        df['lFlRatio']=df_vp['fl_ratio']
        df['lFdAmount']=df_vp['fd_amount']
        df['lFirstTime']=df_vp['first_time']
        df['lLastTime']=df_vp['last_time']
        df['lOpenTimes']=df_vp['open_times']
        df['lStrth']=df_vp['strth']
        df['lLimit']=df_vp['limit']
        del df_vp
        return df
        
    # def limit_price(df,p): 
    #     df_vp=AStock.alignStockFactors(df,'astock_price_stk_limit','trade_date',filed='*',conv=2,db='tushare')  
    #     df['upLimit']=df_vp['up_limit']
    #     df['downLimit']=df_vp['down_limit']
    #     del df_vp
    #     return df
