import numpy as np
from finhack.market.astock.astock import AStock

class myfactors:
    def close(df,p):
        # print(p)
        # print("p="+','.join(p))
        # if len(p)==1:
        #     p=[p,0]
        df_c=df.copy()
        df['close']=df_c['close']
        return df

    def basic(df,p): 
        df_basic=AStock.alignStockFactors(df,'astock_price_daily_basic','trade_date',filed='turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv',conv=0,db='tushare')
        df['turnoverRate']=df_basic['turnover_rate']
        df['turnoverRatef']=df_basic['turnover_rate_f']
        df['volumeRatio']=df_basic['volume_ratio']
        df['pe']=df_basic['pe']
        df['peTtm']=df_basic['pe_ttm']
        df['pb']=df_basic['pb']
        df['ps']=df_basic['ps']
        df['psTtm']=df_basic['ps_ttm']
        df['dvRatio']=df_basic['dv_ratio']
        df['dvTtm']=df_basic['dv_ttm']
        df['totalShare']=df_basic['total_share']
        df['floatShare']=df_basic['float_share']
        df['freeShare']=df_basic['free_share']
        df['totalMv']=df_basic['total_mv']
        df['circMv']=df_basic['circ_mv']
        return df
        
        
    def xx123(df,p):
        df['xx1']=p[0]
        df['xx2']=p[1]
        df['xx3']=df['open']
        return df

    # def rim(df,p):
    #     df_rim=AStock.alignStockFactors(df,'stock_finhack_rim','date',filed='name,industry,value,value_end,value_max,vp,vep,vmp,rcount',conv=1,db='finhack')
    #     if df_rim.empty:
    #         return df
    #     df['rimn']=df_rim['name'].astype("string")
    #     df['rimi']=df_rim['industry'].astype("string")
    #     df['rimv']=df_rim['value']
    #     df['rimve']=df_rim['value_end']
    #     df['rimvm']=df_rim['value_max']
    #     df['rimvp']=df_rim['vp']
    #     df['rimvep']=df_rim['vep']
    #     df['rimvmp']=df_rim['vmp']
    #     df['rimrc']=df_rim['rcount']
    #     return df