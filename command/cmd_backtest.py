from library.astock import AStock
from flask import request
import pandas as pd
import importlib
import os
import time
from flask import Flask
app = Flask(__name__)

class bt:
    def load_price(cache=True):
        mypath=os.path.dirname(__file__)
        cache_path=mypath+"/cache/price/bt_price"
        if os.path.isfile(cache_path):
            #print('read cache---'+code)
            t = time.time()-os.path.getmtime(cache_path)
            if t<60*60*12 and cache: #缓存时间为12小时
                df=pd.read_pickle(cache_path)
                return df        
        
        df_all=AStock.getStockDailyPrice(fq='qfq')
        df_all=df_all.reset_index()
        df_price=df_all.set_index(['trade_date','ts_code'])
        df_price=df_price[['high','low','open','close','volume']]
        df_price.to_pickle(cache_path)
        return df_price

    
    def create(
                start_date='20100101',
                end_date='20221115',
                fees=0.0003,
                min_fees=5,
                tax=0.001,
                cash=100000,
                strategy="test",
                data_path="",
                args_path="",
            ):
                
        bt_instance={
            "instance_id":0,
            "start_date":start_date,
            "end_date":end_date,
            "now_date":'',
            "data_path":data_path,
            "args_path":args_path,
            "data":None,
            "g":{},
            "positions":[ #仓位列表
                
                
            ],
            "history":[
                
            ],
            "returns":[
                
            ],
            "risk":{
                
            },
            "logs":[],
            "setting":{
                "fees":fees,
                "min_fees":min_fees,
                "tax":tax,
                "benchmark":"000001.SZ",
            },
            "cash":cash,
            "positions_value":0,
            "total_value":cash,
        }
        
        return bt_instance
    
    
    

#bt.load_price()

# test_val='123456'

# @app.route('/backtest/run', methods=['POST', 'GET'])
# def run():
#   if request.method == 'POST':
#         print(1)
#         user = request.form['nm']
#     elif request.method == 'GET':
#         start_date=request.args.get('start_date')
#     return start_date

# if __name__ == '__main__':
#   app.run(host='0.0.0.0',port=8888)

df_price=bt.load_price()

instance=bt.create()


data=pd.read_pickle("/tmp/lgb_model.pkl")
date_range=data['trade_date'].to_list()
date_range=list(set(date_range))
date_range=sorted(date_range)

data=data.set_index(['trade_date','ts_code'])


instance['data']=data
instance['date_range']=date_range
instance['df_price']=df_price

strategy_name='test'
strategy_module = importlib.import_module('.test',package='strategies')
 

 
strategy = getattr(strategy_module, 'strategy')
        # 动态加载类test_class生成类对象
        
strategy.run(instance=instance)


# for date in date_range:
#     pred=data.loc[date]
    
#     strategy.every_bar(instance,date_all,date_now)  
#     print(pred)
#     exit()

# print(date_range)

