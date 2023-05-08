import sys
import os
import time
import random
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from astock.astock import AStock
from factors.indicatorCompute import indicatorCompute
from library.mydb import mydb
from library.config import config
from library.util import util
from library.globalvar import *
import pandas as pd
import pickle
from astock.indexHelper import indexHelper
from backtest.backtest import bt
from astock.market import market
import bottleneck as bn
from train.nntrain import nntrain
#market.load_price()
# ts_code='301138.SZ'
# market.get_price(ts_code,'20211215',client=None)
# market.get_price(ts_code,'20230414',client=None)
# exit()
pd.set_option("display.max_rows", 20)

from factors.alphaEngine import alphaEngine
#
# df=alphaEngine.calc(formula="rank($low)",name="alpha",check=True)

# print('rank_df')
# print(df)
# df=df.reset_index()
# df=df[df.ts_code=='688981.SH']
# print('rank_code_df')
# print(df)


df=alphaEngine.calc(formula="lowday($open,9)",name="alpha",check=True)
print('alpha_df')
print(df)
df=df.reset_index()
print('alpha_code_df')
df=df[df.trade_date=='20230505']
print(df)
 


#print(st_df_code)


exit()


from factors.factorRepair import factorRepair
factorRepair.deleteDate('20230418')


exit()


ts_code='301187.SZ'
print("前复权")
df=AStock.getStockDailyPriceByCode(ts_code,fq='qfq',cache=False)
print(df)
print("\n-------")
print("后复权")
df=AStock.getStockDailyPriceByCode(ts_code,fq='hfq',cache=False)
print(df)
print("\n-------")
print("不复权")
df=AStock.getStockDailyPriceByCode(ts_code,fq='no',cache=False)
print(df)

exit()
#nntrain.run(features=['alphaA_005','alphaA_016','alphaA_021'])

#'CMO_0','ROCR100_14_0','T3_14_0','TRIX_0','alpha101_026','alpha101_030','alpha101_032','alpha101_040','alpha101_041','alpha191_036','alpha191_057','alpha191_071','alpha191_109','alpha191_158','alphaA_002','alphaA_005','alphaA_015','alphaA_021','alphaA_030','dvRatio_0'
nntrain.run(features=['alphaA_021','alphaA_030'])



exit()

df_price=market.load_dividend()
x=market.get_price("000043.SZ",'20180102')
print(x)
x=market.get_price("600803.SH",'20180212')

print(x)

exit()


def traverse_datasets(hdf_file):
    print("---"+hdf_file)
    import h5py
 
    def h5py_dataset_iterator(g, prefix=''):
        for key in g.keys():
            item = g[key]
            path = '{}/{}'.format(prefix, key)
            if isinstance(item, h5py.Dataset): # test for dataset
                yield (path, item)
            elif isinstance(item, h5py.Group): # test for group (go down)
                yield from h5py_dataset_iterator(item, path)
 
    with h5py.File(hdf_file, 'r') as f:
        for (path, dset) in h5py_dataset_iterator(f):
            #print(path, dset)
            print(path)
            dataset = f[path][:]
            df = pd.DataFrame(dataset)
            print(df)
            if not df.empty:
                return
 
    return None
 
# 传入路径即可

h5list=['stocks.h5','indexes.h5','futures.h5','st_stock_days.h5','funds.h5','dividends.h5','ex_cum_factor.h5','suspended_days.h5'
,'split_factor.h5']
for file in h5list:
    traverse_datasets('/root/.rqalpha/bundle/'+file)
# traverse_datasets('/root/.rqalpha/bundle/split_factor.h5')
# traverse_datasets('/root/.rqalpha/bundle/yield_curve.h5')
# traverse_datasets('/root/.rqalpha/bundle/suspended_days.h5')



exit()





exit()

# df=AStock.getStockDailyPriceByCode('600354.SH',fq='no')
# print(df[df.trade_date=='20180326'])


# df=AStock.getStockDailyPriceByCode('600354.SH',fq='qfq')
# print(df[df.trade_date=='20180326'])

# df=AStock.getStockDailyPriceByCode('600354.SH',fq='hfq')
# print(df[df.trade_date=='20180326'])

# # x=market.get_price("600803.SH",'20180212')
# # print(x)
# exit()
# exit()


df_price=market.load_price()
 
# x=market.get_price("000043.SZ",'20180102')
# print(x)

# #16.86
# x=market.get_price("000011.SZ",'20180102')
# print(x)


# df=AStock.getStockDailyPriceByCode('000043.SZ',fq='no')
# print(df[4158:])

 
exit()



# df_pred=pd.read_pickle(PREDS_DIR+'lgb_model_3358f2ad65dab03ce1f8afad9218d91d_pred.pkl')
# df_pred=df_pred.reset_index(drop=True) 

# print(df_pred)
# exit()


# idx_weight=indexHelper.get_index_weights('000852.SH','20180102')
# idx_weight=idx_weight.set_index('ts_code',drop=True)
# print(idx_weight)

# exit();

# model_hash="d39fa9ecd75f30a7dcaa334e97ebdecd"

                        
# args={
#     "features_list":'ADX_0,ADX_14_0,ARRONUP_14_0,DX_0,FASTK_0,HTDCPERIOD_0,MACDFIX_0,MACDHISTFIX_0,MACDHIST_0,MACDX_0,MININDEX_120_0,MINUSDI_0,SLOWD_0,STDDEV_30_0,STDDEV_5_0,alphaA_002,alphaA_004,alphaA_014,alphaA_015,alphaA_019,alphaA_021,alphaA_030,totalMv_0',
#     "train":'lgb_ds',
#     "model":model_hash,
#     "loss":'ds',
#     "strategy_args":{
#         "hold_day":10,
#         "hold_n":2,
#     }
# }
# bt_instance=bt.run(
#         cash=10000000,
#         start_date='20180101',
#         strategy_name='IndexPlus3',
#         data_path="lgb_model_"+model_hash+"_pred.pkl",
#         args=args,
#         benchmark='000852.SH'
# )
# exit()

df=AStock.getStockDailyPriceByCode('000018.SZ',fq='qfq')
print(df[6612:])
exit()
import lightgbm
import numpy as np


def check_gpu_support():
    data = np.random.rand(50, 2)
    label = np.random.randint(2, size=50)
    print(label)
    train_data = lightgbm.Dataset(data, label=label)
    params = {'num_iterations': 1, 'device': 'gpu'}
    try:
        gbm = lightgbm.train(params, train_set=train_data)
        print("GPU True !!!")
    except Exception as e:
        print("GPU False !!!")


if __name__ == '__main__':
    check_gpu_support()
 

exit()
from astock.indexHelper import indexHelper

index_list=indexHelper.get_index_weights()
print(index_list)

exit()

def in_index(idx_code="000852.SH"):

    
    df_index=mydb.selectToDf("select  * from astock_index_weight where ts_code='%s' " % idx_code,'tushare')
    df_pred=pd.read_pickle(PREDS_DIR+'lgb_model_c004ee451144084e85b2d3f2a577dd57_pred.pkl')
    
    del df_index['ts_code']
    
    df_index['ts_code']=df_index['con_code']
    
    
    df_pred['weight']=0
    
    print(df_pred)
    
    
    
    
    
    for trade_date,df_idx in df_index.groupby('trade_date'):  # 遍历.DataFrameGroupBy对象
        print(trade_date)
        #print(df_idx)
        df_pred.loc[(df_pred.trade_date>trade_date) ,'weight']=0
        for c,w in zip(df_idx.ts_code,df_idx.weight):
            df_pred.loc[(df_pred.trade_date>trade_date) & (df_pred.ts_code==c) ,'weight']=w
            
    
    #df_res=pd.merge(df_pred, df_index, how='left', on='trade_date',validate="one_to_many", copy=True, indicator=False)
    
    print(df_pred)

    pass


in_index()