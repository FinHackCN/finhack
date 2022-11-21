import numpy as np
from library.astock import AStock

class myfactors:
    def close(df,p):
        # print(p)
        # print("p="+','.join(p))
        # if len(p)==1:
        #     p=[p,0]
        df_c=df.copy()
        df['close']=df_c['close']
        return df

