import hashlib
import time
from .data import Data

class OrderCost:
    def __init__(self,open_tax=0, close_tax=0.001,open_commission=0.0003, close_commission=0.0003,close_today_commission=0, min_commission=5):
        self.open_tax=open_tax
        self.close_tax=close_tax
        self.open_commission=open_commission
        self.close_commission=close_commission
        self.close_today_commission=close_today_commission
        self.min_commission=min_commission
        pass



class Order():
    def __init__(self,code='',amount='',is_buy=True,side='long',action='',context=None):
        self.code=code
        self.amount=amount
        self.enable_amount=amount
        self.filled=0
        self.info=Data.get_daily_info(code=code,context=context)
        self.price=Data.get_price(code=code,context=context)
        self.order_id = self.generate_order_id()
        self.is_buy=is_buy
        self.cost=0
        self.last_sale_price=None
        #正常
        self.status=-1 if self.price==None else 1

        

    def generate_order_id(self):
        timestamp = str(int(time.time() * 1000000))
        data = f"{self.code}_{self.amount}_{timestamp}".encode('utf-8')
        hash_object = hashlib.sha256(data)
        order_id = hash_object.hexdigest()
        return order_id
        

class Position():
    def __init__(self,code,amount,enable_amount,last_sale_price):
        self.code=code
        self.amount=amount
        self.enable_amount=enable_amount
        self.last_sale_price=last_sale_price
        self.cost_basis=last_sale_price
        self.total_value=amount*last_sale_price
        self.total_cost=amount*last_sale_price
        
        
           #'position':{
                # 'xxx':sid 标的代码
                # enable_amount 可用数量
                # amount 总持仓数量
                # last_sale_price 最新价格
                # cost_basis 持仓成本价格(期货不支持)
                # today_amount 今日开仓数量(期货不支持，且仅回测有效)
                # 期货专用字段：
                # delivery_date 交割日，期货使用
                # today_short_amount 空头今仓数量
                # today_long_amount 多头今仓数量
                # long_cost_basis 多头持仓成本
                # short_cost_basis 空头持仓成本
                # margin_rate 保证金比例
                # contract_multiplier 合约乘数
                # long_amount 多头总持仓量
                # short_amount 空头总持仓量
                # long_pnl 多头浮动盈亏
                # short_pnl 空头浮动盈亏
                # long_enable_amount 多头可用数量
                # short_enable_amount 多空头可用数量
                # business_type 业务类型
           # },
        
        
        
                # instance['positions'][ts_code]['total_value']=instance['positions'][ts_code]['total_value']
                # instance['positions'][ts_code]['amount']=instance['positions'][ts_code]['amount']+amount
                # instance['positions'][ts_code]['avg_price']=(instance['positions'][ts_code]['total_value']+price+fees)/instance['positions'][ts_code]['amount']
                # instance['positions'][ts_code]['last_close']=price
        
        

        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        # slip,cost,limit

        
        
# status: 状态, 一个OrderStatus值
# add_time: 订单添加时间, [datetime.datetime]对象
# is_buy: bool值, 买还是卖，对于期货:
# 开多/平空 -> 买
# 开空/平多 -> 卖
# amount: 下单数量, 不管是买还是卖, 都是正数
# filled: 已经成交的股票数量, 正数
# security: 股票代码
# order_id: 订单ID
# price: 平均成交价格, 已经成交的股票的平均成交价格(一个订单可能分多次成交)
# avg_cost: 卖出时表示下卖单前的此股票的持仓成本, 用来计算此次卖出的收益. 买入时表示此次买入的均价(等同于price).
# side: 多/空，'long'/'short'
# action: 开/平， 'open'/'close'
# commission：交易费用（佣金、税费等）        
        


class FixedSlippage():
    def __init__(self,value):
        self.value=value
        self.type='fixed'

class PriceRelatedSlippage():
    def __init__(self,value):
        self.value=value
        self.type='pricerelated'

class StepRelatedSlippage():
    def __init__(self,value):
        self.value=value
        self.type='steprelated'


def bind_object(strategy):
    strategy.OrderCost=OrderCost
    strategy.FixedSlippage=FixedSlippage
    strategy.PriceRelatedSlippage=PriceRelatedSlippage
    strategy.StepRelatedSlippage=StepRelatedSlippage