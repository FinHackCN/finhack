from library.mydb import mydb
from library.globalvar import *
import pandas as pd
from functools import lru_cache
import datetime
class indexHelper:
    @lru_cache(None)
    def get_index_weights(idx_code="000001.SH", date="20220101"):
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
        df_index=df_index[df_index.trade_date==last_date]
        
        df_index['ts_code']=df_index['con_code']
        
        return df_index