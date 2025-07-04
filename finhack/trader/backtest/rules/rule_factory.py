"""
规则工厂类
负责创建和管理各种类型的规则
"""

from typing import Dict, Any, List, Optional
import logging

from .base_rule import BaseRule, RuleGroup, RuleType
from .common.trading_rules import *
from .common.market_rules import *
from .common.risk_rules import *
from .markets.cn_stock_rules import *
from .markets.hk_stock_rules import *
from .markets.us_stock_rules import *


class RuleFactory:
    """规则工厂类"""
    
    def __init__(self):
        self.logger = logging.getLogger("RuleFactory")
    
    def create_trading_rules(self) -> RuleGroup:
        """创建交易规则组"""
        rules = [
            # 基础交易规则
            TradingTimeRule(),
            OrderSizeRule(),
            OrderPriceRule(),
            PositionSizeRule(),
            CashRule(),
            
            # 手续费规则
            CommissionRule(),
            StampTaxRule(),
            
            # 滑点规则
            SlippageRule(),
            
            # 成交量规则
            VolumeRule(),
            
            # 最小交易单位规则
            MinTradeUnitRule(),
        ]
        
        return RuleGroup("trading_rules", rules, "基础交易规则")
    
    def create_market_rules(self) -> RuleGroup:
        """创建市场规则组"""
        rules = [
            # 涨跌停规则
            PriceLimitRule(),
            
            # 停牌规则
            SuspensionRule(),
            
            # 退市规则
            DelistingRule(),
            
            # ST标记规则
            STMarkRule(),
            
            # 新股规则
            NewStockRule(),
            
            # 交易日规则
            TradingDayRule(),
            
            # 市场开闭市规则
            MarketHoursRule(),
        ]
        
        return RuleGroup("market_rules", rules, "市场规则")
    
    def create_risk_rules(self) -> RuleGroup:
        """创建风险规则组"""
        rules = [
            # 持仓集中度规则
            PositionConcentrationRule(),
            
            # 单只股票持仓比例规则
            SingleStockPositionRule(),
            
            # 单日最大交易金额规则
            DailyTradingAmountRule(),
            
            # 最大回撤规则
            MaxDrawdownRule(),
            
            # 杠杆比例规则
            LeverageRule(),
            
            # 止损规则
            StopLossRule(),
            
            # 止盈规则
            TakeProfitRule(),
        ]
        
        return RuleGroup("risk_rules", rules, "风险控制规则")
    
    def create_cn_stock_rules(self) -> RuleGroup:
        """创建A股特定规则组"""
        rules = [
            # T+1规则
            T1Rule(),
            
            # 科创板规则
            StarMarketRule(),
            
            # 创业板规则
            GemRule(),
            
            # 北交所规则
            BseRule(),
            
            # 融资融券规则
            MarginTradingRule(),
            
            # 集合竞价规则
            CallAuctionRule(),
            
            # 大宗交易规则
            BlockTradingRule(),
        ]
        
        return RuleGroup("cn_stock_rules", rules, "A股特定规则")
    
    def create_hk_stock_rules(self) -> RuleGroup:
        """创建港股特定规则组"""
        rules = [
            # T+0规则
            T0Rule(),
            
            # 港股通规则
            StockConnectRule(),
            
            # 碎股交易规则
            OddLotRule(),
            
            # 汇率转换规则
            CurrencyConversionRule(),
            
            # 港股休市规则
            HkHolidayRule(),
        ]
        
        return RuleGroup("hk_stock_rules", rules, "港股特定规则")
    
    def create_us_stock_rules(self) -> RuleGroup:
        """创建美股特定规则组"""
        rules = [
            # T+0规则
            UsT0Rule(),
            
            # PDT规则（日内交易者规则）
            PdtRule(),
            
            # 盘前盘后交易规则
            ExtendedHoursRule(),
            
            # 美股汇率转换规则
            UsCurrencyConversionRule(),
            
            # 美股休市规则
            UsHolidayRule(),
        ]
        
        return RuleGroup("us_stock_rules", rules, "美股特定规则")
    
    def create_frequency_rules(self, frequency: str) -> RuleGroup:
        """
        创建频率特定规则组
        
        Args:
            frequency: 频率类型 ('1d', '1h', '1m', '5m', '15m', '30m')
            
        Returns:
            RuleGroup: 频率特定规则组
        """
        rules = []
        
        if frequency == '1d':
            # 日线特定规则
            rules.extend([
                DailyTradingRule(),
                EODRule(),  # 日终规则
            ])
        elif frequency in ['1h', '30m', '15m']:
            # 小时线特定规则
            rules.extend([
                IntradayTradingRule(),
                HourlyLimitRule(),
            ])
        elif frequency in ['5m', '1m']:
            # 分钟线特定规则
            rules.extend([
                MinuteTradingRule(),
                HighFrequencyRule(),
            ])
        
        return RuleGroup(f"{frequency}_rules", rules, f"{frequency}频率特定规则")
    
    def create_custom_rule(self, rule_name: str, rule_type: RuleType, 
                          apply_func: callable, config: Dict[str, Any] = None) -> BaseRule:
        """
        创建自定义规则
        
        Args:
            rule_name: 规则名称
            rule_type: 规则类型
            apply_func: 规则应用函数
            config: 规则配置
            
        Returns:
            BaseRule: 自定义规则实例
        """
        class CustomRule(BaseRule):
            def __init__(self):
                super().__init__(rule_name, rule_type, config=config)
                self.apply_func = apply_func
            
            def apply(self, context, event, data):
                return self.apply_func(context, event, data)
        
        return CustomRule()
    
    def create_conditional_rule(self, rule_name: str, base_rule: BaseRule, 
                               condition_func: callable) -> BaseRule:
        """
        创建条件规则（在特定条件下才应用的规则）
        
        Args:
            rule_name: 规则名称
            base_rule: 基础规则
            condition_func: 条件函数
            
        Returns:
            BaseRule: 条件规则实例
        """
        class ConditionalRule(BaseRule):
            def __init__(self):
                super().__init__(rule_name, base_rule.rule_type, config=base_rule.config)
                self.base_rule = base_rule
                self.condition_func = condition_func
            
            def is_applicable(self, context, event, data):
                if not super().is_applicable(context, event, data):
                    return False
                
                try:
                    return self.condition_func(context, event, data)
                except Exception as e:
                    self.logger.error(f"条件检查失败: {e}")
                    return False
            
            def apply(self, context, event, data):
                return self.base_rule.apply(context, event, data)
        
        return ConditionalRule()
    
    def create_rule_chain(self, chain_name: str, rules: List[BaseRule], 
                         operator: str = "and") -> BaseRule:
        """
        创建规则链（将多个规则组合成一个规则）
        
        Args:
            chain_name: 规则链名称
            rules: 规则列表
            operator: 操作符 ('and', 'or')
            
        Returns:
            BaseRule: 规则链实例
        """
        from .base_rule import RuleChain
        
        class ChainRule(BaseRule):
            def __init__(self):
                super().__init__(chain_name, RuleType.CUSTOM)
                self.chain = RuleChain(chain_name, rules, operator)
            
            def apply(self, context, event, data):
                return self.chain.apply(context, event, data)
        
        return ChainRule()
    
    def create_rule_from_config(self, config: Dict[str, Any]) -> BaseRule:
        """
        从配置创建规则
        
        Args:
            config: 规则配置
            
        Returns:
            BaseRule: 规则实例
        """
        rule_type = config.get('type', 'custom')
        rule_name = config.get('name', 'unknown')
        
        # 根据类型创建对应规则
        if rule_type == 'commission':
            return CommissionRule(config=config.get('config', {}))
        elif rule_type == 'slippage':
            return SlippageRule(config=config.get('config', {}))
        elif rule_type == 'price_limit':
            return PriceLimitRule(config=config.get('config', {}))
        elif rule_type == 't1':
            return T1Rule(config=config.get('config', {}))
        elif rule_type == 'stop_loss':
            return StopLossRule(config=config.get('config', {}))
        elif rule_type == 'take_profit':
            return TakeProfitRule(config=config.get('config', {}))
        # 可以继续添加更多规则类型...
        
        # 如果没有匹配的类型，返回None
        self.logger.warning(f"未知的规则类型: {rule_type}")
        return None
    
    def get_available_rule_types(self) -> List[str]:
        """获取可用的规则类型列表"""
        return [
            # 基础交易规则
            'trading_time', 'order_size', 'order_price', 'position_size', 'cash',
            'commission', 'stamp_tax', 'slippage', 'volume', 'min_trade_unit',
            
            # 市场规则
            'price_limit', 'suspension', 'delisting', 'st_mark', 'new_stock',
            'trading_day', 'market_hours',
            
            # 风险规则
            'position_concentration', 'single_stock_position', 'daily_trading_amount',
            'max_drawdown', 'leverage', 'stop_loss', 'take_profit',
            
            # A股特定规则
            't1', 'star_market', 'gem', 'bse', 'margin_trading',
            'call_auction', 'block_trading',
            
            # 港股特定规则
            't0', 'stock_connect', 'odd_lot', 'currency_conversion', 'hk_holiday',
            
            # 美股特定规则
            'us_t0', 'pdt', 'extended_hours', 'us_currency_conversion', 'us_holiday',
        ]
    
    def get_rule_template(self, rule_type: str) -> Dict[str, Any]:
        """
        获取规则模板配置
        
        Args:
            rule_type: 规则类型
            
        Returns:
            Dict[str, Any]: 规则模板
        """
        templates = {
            'commission': {
                'name': 'commission_rule',
                'type': 'commission',
                'enabled': True,
                'priority': 10,
                'config': {
                    'buy_rate': 0.0003,
                    'sell_rate': 0.0003,
                    'min_commission': 5.0
                }
            },
            'slippage': {
                'name': 'slippage_rule',
                'type': 'slippage',
                'enabled': True,
                'priority': 5,
                'config': {
                    'rate': 0.005,
                    'min_slippage': 0.01
                }
            },
            'price_limit': {
                'name': 'price_limit_rule',
                'type': 'price_limit',
                'enabled': True,
                'priority': 20,
                'config': {
                    'limit_ratio': 0.10,
                    'check_suspension': True
                }
            },
            't1': {
                'name': 't1_rule',
                'type': 't1',
                'enabled': True,
                'priority': 15,
                'config': {
                    'market': 'cn_stock'
                }
            },
            'stop_loss': {
                'name': 'stop_loss_rule',
                'type': 'stop_loss',
                'enabled': False,
                'priority': 25,
                'config': {
                    'stop_loss_ratio': 0.05,
                    'trailing_stop': False
                }
            },
        }
        
        return templates.get(rule_type, {})
    
    def create_default_rule_set(self, market: str = "cn_stock", 
                               frequency: str = "1d") -> List[RuleGroup]:
        """
        创建默认规则集
        
        Args:
            market: 市场类型
            frequency: 频率
            
        Returns:
            List[RuleGroup]: 规则组列表
        """
        rule_groups = []
        
        # 基础规则
        rule_groups.append(self.create_trading_rules())
        rule_groups.append(self.create_market_rules())
        rule_groups.append(self.create_risk_rules())
        
        # 市场特定规则
        if market == "cn_stock":
            rule_groups.append(self.create_cn_stock_rules())
        elif market == "hk_stock":
            rule_groups.append(self.create_hk_stock_rules())
        elif market == "us_stock":
            rule_groups.append(self.create_us_stock_rules())
        
        # 频率特定规则
        rule_groups.append(self.create_frequency_rules(frequency))
        
        return rule_groups
    
    def __str__(self):
        return "RuleFactory" 