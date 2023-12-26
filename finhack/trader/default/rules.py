import finhack.library.log as Log
import numpy as np
#涨跌停限制
from .function import *

class Rules():
    def __init__(self,order,context,log=None):
        self.order=order
        self.context=context
        self.rule_list=context.trade.rule_list
        if log==None:
            self.log=print
        else:
            self.log=log
    
    def apply(self,rule_list=None):
        if rule_list==None:
            rule_list=self.rule_list
        rule_list=rule_list.split(",")
        
        for rule in rule_list:
            method_name = f"rule_{rule}"
            if hasattr(self, method_name):
                method = getattr(self, method_name)
                result = method()
                self.log(f"{self.order.code}在{method_name}时的订单量为{self.order.amount}",'debug')  
                if result==False:
                    self.log(f"{self.order.code}未通过{method_name}规则校验，订单取消！",'warning')  
                    self.order.status=-1
                    return self.order

                    
        #最后再计算一下价格和手续费
        if self.order.is_buy==True:
            action='open'
        else:
            action='close'
        value=self.order.amount*self.order.price
        account=self.context.account
        tax=value*account[action+"_tax"]
        commission=value*account[action+"_commission"]
        if commission<account['min_commission']:
            commission=account['min_commission']
        cost=tax+commission
        self.order.value=value
        self.order.cost=cost
        self.order.last_sale_price=self.order.price
        if self.order.last_sale_price==None:
            self.order.status=-1
        return self.order

    
    
    #退市
    def rule_delist(self):
        if self.order.price!=None and '退' in self.order.info.name:
            self.order.filled=0
            self.log(f"{self.order.code}即将退市，不买入！",'warning')     
            return False
        return True
    
    #停牌
    def rule_stop(self):
        if(np.isnan(self.order.price) or int(self.order.info.stop)==1 or self.order.price==0):
            self.order.filled=0
            self.log(f"{self.order.code}停牌，无法买入！",'warning')    
            return False
        return True
            
             
    #涨跌停
    def rule_limit(self): 
        if self.order.is_buy==True:
            #这里因为没有判断不限涨跌停的情况，所以加了个price==now_price['high']，但是不严谨
            if self.order.price>=self.order.info.upLimit and self.order.price==self.order.info.high:
                self.log(f"{self.order.code}涨停，无法买入！",'warning')  
                self.order.filled=0
                return False

        else:
            if self.order.price<=self.order.info.downLimit and self.order.price==self.order.info.low:
                self.order.filled=0
                self.log(f"{self.order.code}跌停，无法卖出！",'warning')  
                return False
        return True
                
    #滑点
    def rule_slip(self):
        if self.order.is_buy==True:
            self.order.price=self.order.price*(1+self.context.trade.slip)
        else:
            self.order.price=self.order.price*(1-self.context.trade.slip)
        return True
                
                
                
    #量比           
    def rule_volume_ratio(self):
        if self.order.amount>self.order.info.volume*self.context.trade.order_volume_ratio:
            #self.order.amount=self.order.info.volume*context.trade.order_volume_ratio
            self.order.filled=self.order.info.volume*self.context.trade.order_volume_ratio
            self.order.amount=self.order.info.volume*self.context.trade.order_volume_ratio
            self.log(f"{self.order.code}超过当日最大订单数量，已经自动调整。",'warning') 
        else:
            self.order.filled=self.order.amount
        return True
    
    
    #手续费+税费
    def rule_cost(self):
        #应该是只有买入的时候才会有这个问题吧
        if self.order.is_buy==True:
            action='open'
            #这里其实应该用self.order.filled，但是怕后面有坑
            value=self.order.amount*self.order.price
            account=self.context.account
            tax=value*account[action+"_tax"]
            commission=value*account[action+"_commission"]
            if commission<account['min_commission']:
                commission=account['min_commission']
            cost=tax+commission
            if self.context.portfolio.cash-value<cost:
                if (context.portfolio.cash-cost)<0:
                    self.log(f"{self.order.code} 现金不足以支付手续费！",'warning') 
                    return False
                self.order.amount=(context.portfolio.cash-cost)/self.order.price
                self.order.filled=self.order.amount
        return True
    
    
    
    #数量
    def rule_volume_num(self):
        amount=int(self.order.amount)
        
        
        
        if self.order.is_buy==False:
            #没有这个持仓
            if self.order.code not in context.portfolio.positions:
                self.log(f"{self.order.code} 未持仓，无法卖出！",'warning') 
                return False
                
            #print(amount,context.portfolio.positions[self.order.code].amount)    
                
            #不能大于可用数量
            if amount>context.portfolio.positions[self.order.code].enable_amount:
                amount=context.portfolio.positions[self.order.code].enable_amount
            
            #如果是清仓
            if amount==context.portfolio.positions[self.order.code].amount:
                self.order.amount=amount
                self.order.filled=self.order.amount
                return True
            
        
        #非科创板
        if amount>0 and not self.order.code[:3]=='688':
            if(amount<100):
                amount=0
            if(amount % 100 !=0):
                amount=int(amount/100)*100
        elif amount>0 and self.order.code[:3]=='688':
            if(amount<200):
                amount=0
            if amount>50000:
                amount=50000
        self.order.amount=amount
        self.order.filled=self.order.amount
        return True
        
    #A股t+1策略
    def rule_t1(self):
        self.order.enable_amount=0
        return True
                
    #t0策略
    def rule_t0(self):
        self.order.enable_amount=self.order.amount
        return True             
                
               