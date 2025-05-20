from finhack.library.db import DB
from runtime.constant import *
import pandas as pd
from functools import lru_cache
import datetime
import os

class indexHelper:
    @lru_cache(None)
    def get_index_weights(idx_code="000001.SH", date="20220101"):
        idx_weight_date_cache_path=CACHE_DIR+"index/"+idx_code+'_'+date+'.pkl'
        index_cache_path=CACHE_DIR+"index/"+idx_code+'.pkl'
        if os.path.isfile(idx_weight_date_cache_path):
            df_idx_weight_date=pd.read_pickle(idx_weight_date_cache_path)
            return df_idx_weight_date
        else:
            index_cache_path=CACHE_DIR+"index/"+idx_code+'.pkl'
            if os.path.isfile(index_cache_path):
                t = datetime.datetime.now().timestamp()-os.path.getmtime(index_cache_path)
                if t<60*60:
                    df_index=DB.select_to_df("select  * from astock_index_weight where ts_code='%s' " % idx_code,'tushare')
                    df_index.to_pickle(index_cache_path)
                else:
                    df_index=pd.read_pickle(index_cache_path)
            else:
                df_index=DB.select_to_df("select  * from astock_index_weight where ts_code='%s' " % idx_code,'tushare')
                df_index.to_pickle(index_cache_path)
     
            df_index=df_index[df_index.trade_date<date]
            last_date=df_index['trade_date'].max()
            df_idx_weight_date=df_index[df_index.trade_date==last_date].copy()
            
            df_idx_weight_date['ts_code']=df_idx_weight_date['con_code']
            df_idx_weight_date.to_pickle(idx_weight_date_cache_path)
        return df_idx_weight_date

def getIdxList(idx_code, db='tushare'):
    """
    获取指数成分股列表
    
    Args:
        idx_code: 指数代码，如'000300.SH'
        db: 数据库连接名
        
    Returns:
        指数成分股列表，包含ts_code和权重
    """
    # 查询最新的指数权重数据
    df_index = DB.select_to_df("select * from astock_index_weight where ts_code='%s'" % idx_code, db)
    
    # 如果没有找到数据，尝试通过模糊匹配查找
    if df_index.empty:
        # 可能索引代码格式不同，尝试不同格式
        alt_idx_code = idx_code.replace('.SH', '.SSE').replace('.SZ', '.SZSE')
        df_index = DB.select_to_df("select * from astock_index_weight where ts_code='%s'" % alt_idx_code, db)
    
    # 返回结果，如果还是空的，那么就返回空DataFrame
    return df_index