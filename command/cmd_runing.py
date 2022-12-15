import sys
import os
import time
import random
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from runing.runing import runing
from runing.simulate import simulate

# df=runing.prepare('20221206')

# pred=runing.pred(df)

 

# print(pred[0:10])


bt_list=simulate.getSimulateList()
simulate.loadData(bt_list)
simulate.testAll()
# print(bt_list)


# pred1=runing.pred_bt(instance_id='93d7932692aebf65e94744b7a52490a7',trade_date='20221209')
# # pred2=runing.pred_bt(instance_id='4b72e8303366e2014a10a1557638cb54',trade_date='20221209')
# # pred3=runing.pred_bt(instance_id='1fa03850202f57941ee7f6b8a814cb2d',trade_date='20221209')

# print(pred1)
 
# print(pred1[pred1['ts_code']=='002624.SZ'])
# print('---')
 
 
# print(pred2[pred2['ts_code']=='002624.SZ'])
# print('---')
 
# print(pred3[pred3['ts_code']=='002624.SZ'])
# print('---')
 
 
#000882