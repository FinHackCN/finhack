import sys
import os
import time
import random
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from runing.runing import runing

# df=runing.prepare('20221206')

# pred=runing.pred(df)

 

# print(pred[0:10])

# print(pred[pred['ts_code']=='000151.SZ'])
# print(pred[pred['ts_code']=='000918.SZ'])
# print(pred[pred['ts_code']=='002595.SZ'])



pred=runing.pred_bt(instance_id='208bee8b3bcbb99f0f242223e30d455a',trade_date='20221206')

print(pred[:10])