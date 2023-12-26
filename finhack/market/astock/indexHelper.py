from library.mydb import mydb
from runtime.constant import *
import pandas as pd
from functools import lru_cache
import datetime
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
                    df_index=mydb.selectToDf("select  * from astock_index_weight where ts_code='%s' " % idx_code,'tushare')
                    df_index.to_pickle(index_cache_path)
                else:
                    df_index=pd.read_pickle(index_cache_path)
            else:
                df_index=mydb.selectToDf("select  * from astock_index_weight where ts_code='%s' " % idx_code,'tushare')
                df_index.to_pickle(index_cache_path)
     
            df_index=df_index[df_index.trade_date<date]
            last_date=df_index['trade_date'].max()
            df_idx_weight_date=df_index[df_index.trade_date==last_date].copy()
            
            df_idx_weight_date['ts_code']=df_idx_weight_date['con_code']
            df_idx_weight_date.to_pickle(idx_weight_date_cache_path)
        return df_idx_weight_date