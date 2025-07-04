"""
公司行为事件实现
包括分红送股、配股、停牌复牌、退市等公司行为事件
"""

from datetime import datetime
from typing import Any, Dict, Optional, List
from decimal import Decimal

from .base_event import BaseEvent, EventType


class CorporateActionEvent(BaseEvent):
    """公司行为事件基类"""
    
    def __init__(self, event_type: EventType, event_time: datetime, symbol: str,
                 market: str = "cn_stock", adapter_id: str = "default",
                 data: Optional[Dict[str, Any]] = None):
        """
        初始化公司行为事件
        
        Args:
            event_type: 事件类型
            event_time: 事件时间
            symbol: 股票代码
            market: 市场
            adapter_id: 适配器ID
            data: 事件数据
        """
        super().__init__(event_type, event_time, market, adapter_id, data)
        self.symbol = symbol
    
    def process(self, context) -> bool:
        """
        处理公司行为事件
        
        Args:
            context: 回测上下文
            
        Returns:
            bool: 是否处理成功
        """
        try:
            # 更新当前时间
            context.current_dt = self.event_time
            
            # 调用事件管理器的处理方法
            if hasattr(context, 'event_manager'):
                result = context.event_manager.process_event(self)
                if result:
                    self.processed = True
                return result
            
            # 如果没有事件管理器，使用默认处理
            return self._default_process(context)
            
        except Exception as e:
            context.logger.error(f"处理公司行为事件失败: {self.event_type.value}, 错误: {str(e)}")
            return False
    
    def _default_process(self, context) -> bool:
        """默认处理逻辑"""
        context.logger.info(f"处理公司行为事件: {self.event_type.value}, 股票: {self.symbol}")
        self.processed = True
        return True


class DividendStockSplitEvent(CorporateActionEvent):
    """分红送股事件"""
    
    def __init__(self, event_time: datetime, symbol: str, dividend_ratio: float,
                 split_ratio: float, ex_date: datetime, market: str = "cn_stock",
                 adapter_id: str = "default", **kwargs):
        """
        初始化分红送股事件
        
        Args:
            event_time: 事件时间
            symbol: 股票代码
            dividend_ratio: 分红比例（每股分红金额）
            split_ratio: 送股比例（每股送股数量）
            ex_date: 除权除息日
            market: 市场
            adapter_id: 适配器ID
            **kwargs: 其他参数
        """
        data = {
            'symbol': symbol,
            'dividend_ratio': dividend_ratio,
            'split_ratio': split_ratio,
            'ex_date': ex_date.isoformat(),
            'dividend_amount': kwargs.get('dividend_amount', 0),  # 总分红金额
            'split_amount': kwargs.get('split_amount', 0),        # 总送股数量
            'record_date': kwargs.get('record_date', ''),         # 股权登记日
            'payment_date': kwargs.get('payment_date', ''),       # 派息日
            'currency': kwargs.get('currency', 'CNY'),            # 货币
            'tax_rate': kwargs.get('tax_rate', 0.1),             # 税率
        }
        
        super().__init__(EventType.DIVIDEND_STOCK_SPLIT, event_time, symbol, market, adapter_id, data)
    
    @property
    def dividend_ratio(self) -> float:
        """分红比例"""
        return self.data.get('dividend_ratio', 0)
    
    @property
    def split_ratio(self) -> float:
        """送股比例"""
        return self.data.get('split_ratio', 0)
    
    @property
    def ex_date(self) -> datetime:
        """除权除息日"""
        return datetime.fromisoformat(self.data.get('ex_date', ''))
    
    @property
    def tax_rate(self) -> float:
        """税率"""
        return self.data.get('tax_rate', 0.1)
    
    def calculate_dividend_amount(self, shares: int) -> float:
        """计算分红金额"""
        gross_dividend = shares * self.dividend_ratio
        tax_amount = gross_dividend * self.tax_rate
        return gross_dividend - tax_amount
    
    def calculate_split_shares(self, shares: int) -> int:
        """计算送股数量"""
        return int(shares * self.split_ratio)
    
    def calculate_adjusted_price(self, price: float) -> float:
        """计算除权除息后的价格"""
        # 除权除息价格 = (收盘价 - 每股分红) / (1 + 每股送股比例)
        adjusted_price = (price - self.dividend_ratio) / (1 + self.split_ratio)
        return max(adjusted_price, 0.01)  # 确保价格不为负


class RightsIssueEvent(CorporateActionEvent):
    """配股事件"""
    
    def __init__(self, event_time: datetime, symbol: str, issue_price: float,
                 issue_ratio: float, record_date: datetime, market: str = "cn_stock",
                 adapter_id: str = "default", **kwargs):
        """
        初始化配股事件
        
        Args:
            event_time: 事件时间
            symbol: 股票代码
            issue_price: 配股价格
            issue_ratio: 配股比例（每股配股数量）
            record_date: 股权登记日
            market: 市场
            adapter_id: 适配器ID
            **kwargs: 其他参数
        """
        data = {
            'symbol': symbol,
            'issue_price': issue_price,
            'issue_ratio': issue_ratio,
            'record_date': record_date.isoformat(),
            'payment_deadline': kwargs.get('payment_deadline', ''),  # 缴款截止日
            'listing_date': kwargs.get('listing_date', ''),          # 上市日
            'currency': kwargs.get('currency', 'CNY'),               # 货币
            'min_subscription': kwargs.get('min_subscription', 0),   # 最小认购数量
            'max_subscription': kwargs.get('max_subscription', 0),   # 最大认购数量
        }
        
        super().__init__(EventType.RIGHTS_ISSUE_ADDITIONAL_ISSUANCE, event_time, symbol, market, adapter_id, data)
    
    @property
    def issue_price(self) -> float:
        """配股价格"""
        return self.data.get('issue_price', 0)
    
    @property
    def issue_ratio(self) -> float:
        """配股比例"""
        return self.data.get('issue_ratio', 0)
    
    @property
    def record_date(self) -> datetime:
        """股权登记日"""
        return datetime.fromisoformat(self.data.get('record_date', ''))
    
    def calculate_rights_shares(self, shares: int) -> int:
        """计算配股数量"""
        return int(shares * self.issue_ratio)
    
    def calculate_rights_cost(self, shares: int) -> float:
        """计算配股成本"""
        rights_shares = self.calculate_rights_shares(shares)
        return rights_shares * self.issue_price
    
    def calculate_adjusted_cost_basis(self, original_shares: int, original_cost: float, 
                                    rights_shares: int, rights_cost: float) -> float:
        """计算配股后的成本价"""
        total_cost = original_shares * original_cost + rights_cost
        total_shares = original_shares + rights_shares
        return total_cost / total_shares if total_shares > 0 else 0


class TradingSuspensionEvent(CorporateActionEvent):
    """停牌复牌事件"""
    
    def __init__(self, event_time: datetime, symbol: str, action: str, reason: str,
                 market: str = "cn_stock", adapter_id: str = "default", **kwargs):
        """
        初始化停牌复牌事件
        
        Args:
            event_time: 事件时间
            symbol: 股票代码
            action: 动作 ('suspend' 停牌, 'resume' 复牌)
            reason: 停牌原因
            market: 市场
            adapter_id: 适配器ID
            **kwargs: 其他参数
        """
        data = {
            'symbol': symbol,
            'action': action,
            'reason': reason,
            'suspension_time': kwargs.get('suspension_time', ''),    # 停牌时间
            'expected_resume_date': kwargs.get('expected_resume_date', ''),  # 预计复牌日期
            'actual_resume_date': kwargs.get('actual_resume_date', ''),     # 实际复牌日期
            'announcement_date': kwargs.get('announcement_date', ''),        # 公告日期
            'suspension_type': kwargs.get('suspension_type', 'temporary'),   # 停牌类型
        }
        
        super().__init__(EventType.TRADING_SUSPENSION_RESUMPTION, event_time, symbol, market, adapter_id, data)
    
    @property
    def action(self) -> str:
        """动作"""
        return self.data.get('action', '')
    
    @property
    def reason(self) -> str:
        """停牌原因"""
        return self.data.get('reason', '')
    
    @property
    def suspension_type(self) -> str:
        """停牌类型"""
        return self.data.get('suspension_type', 'temporary')
    
    def is_suspension(self) -> bool:
        """是否为停牌事件"""
        return self.action == 'suspend'
    
    def is_resumption(self) -> bool:
        """是否为复牌事件"""
        return self.action == 'resume'


class DelistingEvent(CorporateActionEvent):
    """退市事件"""
    
    def __init__(self, event_time: datetime, symbol: str, delisting_date: datetime,
                 delisting_reason: str, market: str = "cn_stock", 
                 adapter_id: str = "default", **kwargs):
        """
        初始化退市事件
        
        Args:
            event_time: 事件时间
            symbol: 股票代码
            delisting_date: 退市日期
            delisting_reason: 退市原因
            market: 市场
            adapter_id: 适配器ID
            **kwargs: 其他参数
        """
        data = {
            'symbol': symbol,
            'delisting_date': delisting_date.isoformat(),
            'delisting_reason': delisting_reason,
            'last_trading_date': kwargs.get('last_trading_date', ''),  # 最后交易日
            'liquidation_price': kwargs.get('liquidation_price', 0),   # 清算价格
            'delisting_type': kwargs.get('delisting_type', 'mandatory'),  # 退市类型
            'transfer_board': kwargs.get('transfer_board', ''),        # 转板信息
        }
        
        super().__init__(EventType.DELISTING, event_time, symbol, market, adapter_id, data)
    
    @property
    def delisting_date(self) -> datetime:
        """退市日期"""
        return datetime.fromisoformat(self.data.get('delisting_date', ''))
    
    @property
    def delisting_reason(self) -> str:
        """退市原因"""
        return self.data.get('delisting_reason', '')
    
    @property
    def liquidation_price(self) -> float:
        """清算价格"""
        return self.data.get('liquidation_price', 0)
    
    @property
    def delisting_type(self) -> str:
        """退市类型"""
        return self.data.get('delisting_type', 'mandatory')


class StockSplitEvent(CorporateActionEvent):
    """股票拆分事件"""
    
    def __init__(self, event_time: datetime, symbol: str, split_ratio: float,
                 ex_date: datetime, market: str = "cn_stock", adapter_id: str = "default",
                 **kwargs):
        """
        初始化股票拆分事件
        
        Args:
            event_time: 事件时间
            symbol: 股票代码
            split_ratio: 拆分比例（如2表示1拆2）
            ex_date: 除权日
            market: 市场
            adapter_id: 适配器ID
            **kwargs: 其他参数
        """
        data = {
            'symbol': symbol,
            'split_ratio': split_ratio,
            'ex_date': ex_date.isoformat(),
            'record_date': kwargs.get('record_date', ''),
            'announcement_date': kwargs.get('announcement_date', ''),
        }
        
        super().__init__(EventType.DIVIDEND_STOCK_SPLIT, event_time, symbol, market, adapter_id, data)
    
    @property
    def split_ratio(self) -> float:
        """拆分比例"""
        return self.data.get('split_ratio', 1.0)
    
    @property
    def ex_date(self) -> datetime:
        """除权日"""
        return datetime.fromisoformat(self.data.get('ex_date', ''))
    
    def calculate_split_shares(self, shares: int) -> int:
        """计算拆分后的股数"""
        return int(shares * self.split_ratio)
    
    def calculate_adjusted_price(self, price: float) -> float:
        """计算拆分后的价格"""
        return price / self.split_ratio


class BonusIssueEvent(CorporateActionEvent):
    """红股事件（纯送股，不分红）"""
    
    def __init__(self, event_time: datetime, symbol: str, bonus_ratio: float,
                 ex_date: datetime, market: str = "cn_stock", adapter_id: str = "default",
                 **kwargs):
        """
        初始化红股事件
        
        Args:
            event_time: 事件时间
            symbol: 股票代码
            bonus_ratio: 送股比例
            ex_date: 除权日
            market: 市场
            adapter_id: 适配器ID
            **kwargs: 其他参数
        """
        data = {
            'symbol': symbol,
            'bonus_ratio': bonus_ratio,
            'ex_date': ex_date.isoformat(),
            'record_date': kwargs.get('record_date', ''),
            'announcement_date': kwargs.get('announcement_date', ''),
        }
        
        super().__init__(EventType.DIVIDEND_STOCK_SPLIT, event_time, symbol, market, adapter_id, data)
    
    @property
    def bonus_ratio(self) -> float:
        """送股比例"""
        return self.data.get('bonus_ratio', 0)
    
    @property
    def ex_date(self) -> datetime:
        """除权日"""
        return datetime.fromisoformat(self.data.get('ex_date', ''))
    
    def calculate_bonus_shares(self, shares: int) -> int:
        """计算红股数量"""
        return int(shares * self.bonus_ratio)
    
    def calculate_adjusted_price(self, price: float) -> float:
        """计算除权后的价格"""
        return price / (1 + self.bonus_ratio)


class CapitalReductionEvent(CorporateActionEvent):
    """减资事件"""
    
    def __init__(self, event_time: datetime, symbol: str, reduction_ratio: float,
                 compensation_amount: float, market: str = "cn_stock", 
                 adapter_id: str = "default", **kwargs):
        """
        初始化减资事件
        
        Args:
            event_time: 事件时间
            symbol: 股票代码
            reduction_ratio: 减资比例
            compensation_amount: 每股补偿金额
            market: 市场
            adapter_id: 适配器ID
            **kwargs: 其他参数
        """
        data = {
            'symbol': symbol,
            'reduction_ratio': reduction_ratio,
            'compensation_amount': compensation_amount,
            'effective_date': kwargs.get('effective_date', ''),
            'record_date': kwargs.get('record_date', ''),
            'payment_date': kwargs.get('payment_date', ''),
        }
        
        super().__init__(EventType.DIVIDEND_STOCK_SPLIT, event_time, symbol, market, adapter_id, data)
    
    @property
    def reduction_ratio(self) -> float:
        """减资比例"""
        return self.data.get('reduction_ratio', 0)
    
    @property
    def compensation_amount(self) -> float:
        """补偿金额"""
        return self.data.get('compensation_amount', 0)
    
    def calculate_remaining_shares(self, shares: int) -> int:
        """计算减资后的剩余股数"""
        return int(shares * (1 - self.reduction_ratio))
    
    def calculate_compensation(self, shares: int) -> float:
        """计算补偿金额"""
        return shares * self.compensation_amount


class NameChangeEvent(CorporateActionEvent):
    """更名事件"""
    
    def __init__(self, event_time: datetime, symbol: str, old_name: str,
                 new_name: str, new_symbol: str = "", market: str = "cn_stock",
                 adapter_id: str = "default", **kwargs):
        """
        初始化更名事件
        
        Args:
            event_time: 事件时间
            symbol: 原股票代码
            old_name: 原股票名称
            new_name: 新股票名称
            new_symbol: 新股票代码（如果有变更）
            market: 市场
            adapter_id: 适配器ID
            **kwargs: 其他参数
        """
        data = {
            'symbol': symbol,
            'old_name': old_name,
            'new_name': new_name,
            'new_symbol': new_symbol or symbol,
            'effective_date': kwargs.get('effective_date', ''),
            'reason': kwargs.get('reason', ''),
        }
        
        super().__init__(EventType.DIVIDEND_STOCK_SPLIT, event_time, symbol, market, adapter_id, data)
    
    @property
    def old_name(self) -> str:
        """原名称"""
        return self.data.get('old_name', '')
    
    @property
    def new_name(self) -> str:
        """新名称"""
        return self.data.get('new_name', '')
    
    @property
    def new_symbol(self) -> str:
        """新代码"""
        return self.data.get('new_symbol', self.symbol) 