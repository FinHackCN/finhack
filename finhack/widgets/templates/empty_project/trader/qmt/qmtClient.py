import time
import finhack.library.log as Log
from trader.qmt.dictobj import DictObj

import requests
import json

class qmtClient():
    def __init__(self, base_url='http://192.168.8.37:8000'):
        self.base_url = base_url

    def send_request(self, endpoint, params=None):
        """ Helper method to send HTTP GET requests """
        try:
            response = requests.get(f"{self.base_url}{endpoint}", params=params)
            response.raise_for_status()  # Raises a HTTPError for bad responses
            return response.json() # Returns JSON response
        except requests.RequestException as e:
            print(f"HTTP Request failed: {e}")
            return None

    def getAsset(self):
        """ Get asset information """
        return self.send_request('/get_asset')


    def assetSync(self,context):
        Log.logger.info("开始同步资产数据")
        asset= self.getAsset()
        context['portfolio']['cash']=asset['cash']
        context['portfolio']['transferable_cash']=asset['cash']-asset['frozen_cash']
        context['portfolio']['locked_cash']=asset['frozen_cash']
        context['portfolio']['total_value']=asset['total_value']
        context['portfolio']['positions_value']=asset['market_value']
        context['portfolio']['portfolio_value=0']=asset['market_value']
        Log.logger.info("结束同步资产数据")
        return asset

    def positionSync(self,context):
        Log.logger.info("开始同步持仓信息")
        positions={

        }
        pos_list= self.GetPositions()
        for pos in pos_list:
            positions[pos['stock_code']]={
                "code":pos['stock_code'],
                "amount":pos['volume'],
                "enable_amount":pos['can_use_volume'],
                "last_sale_price":pos['open_price'],
                "cost_basis":pos['avg_price'],
                "total_value":pos['market_value'],
            }
        context['portfolio']['positions']=positions
        Log.logger.info("结束同步持仓信息")
        return positions

    def sync(self,context): 
        Log.logger.info("同步实盘数据")
        self.assetSync(context)
        self.positionSync(context)

    def getPrice(self, code):
        """ Get price for a specific stock code """
        params = {'code': code}
        return self.send_request('/get_price', params=params)

    def getInfo(self, code):
        """ Get daily information for a specific stock code """
        params = {'code': code}
        info=self.send_request('/get_daily_info', params=params)
        info['downLimit']=info['down_limit']
        info['upLimit']=info['up_limit']
        return DictObj(info)

    def GetPositions(self):
        """ Get positions """
        return self.send_request('/get_positions')

    def OrderBuy(self, code, amount, price=0, strategy='strategy', remark='remark'):
        """ Send a buy order """
        params = {'code': code, 'amount': amount, 'price': price, 'strategy': strategy, 'remark': remark}
        return self.send_request('/order_buy', params=params)

    def OrderSell(self, code, amount, price=0, strategy='strategy', remark='remark'):
        """ Send a sell order """
        params = {'code': code, 'amount': amount, 'price': price, 'strategy': strategy, 'remark': remark}
        return self.send_request('/order_sell', params=params)

    def QueryOrders(self):
        """ Query all orders """
        return self.send_request('/query_orders')

    def CancelOrders(self):
        """ Cancel all orders """
        return self.send_request('/cancel_orders')

    def RetryOrders(self):
        """ Retry failed orders """
        return self.send_request('/retry_orders')

 
qclient=qmtClient()