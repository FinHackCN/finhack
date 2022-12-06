import sys
import os
import time
import random
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from runing.runing import runing

df=runing.prepare('20221205')
df=df.reset_index()
pred=runing.pred(df)

print(pred)

print(pred[pred['ts_code']=='000151.SZ'])
print(pred[pred['ts_code']=='000918.SZ'])
print(pred[pred['ts_code']=='002595.SZ'])