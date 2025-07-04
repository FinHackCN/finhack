"""
规则基础类和接口定义
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, Any, Optional, List, Union
from datetime import datetime
import logging


class RuleType(Enum):
    """规则类型枚举"""
    # 交易规则
    TRADING = "trading"
    # 市场规则
    MARKET = "market"
    # 风险规则
    RISK = "risk"
    # 数据规则
    DATA = "data"
    # 时间规则
    TIME = "time"
    # 用户自定义规则
    CUSTOM = "custom"


class RuleResult:
    """规则执行结果"""
    
    def __init__(self, 
                 passed: bool = True, 
                 message: str = "", 
                 data: Optional[Dict[str, Any]] = None,
                 modified_data: Optional[Dict[str, Any]] = None):
        """
        初始化规则结果
        
        Args:
            passed: 是否通过规则检查
            message: 结果消息
            data: 附加数据
            modified_data: 修改后的数据（如调整价格、数量等）
        """
        self.passed = passed
        self.message = message
        self.data = data or {}
        self.modified_data = modified_data or {}
        self.timestamp = datetime.now()
    
    def __bool__(self):
        return self.passed
    
    def __str__(self):
        return f"RuleResult(passed={self.passed}, message='{self.message}')"


class BaseRule(ABC):
    """规则基础类"""
    
    def __init__(self, name: str, rule_type: RuleType, enabled: bool = True, 
                 priority: int = 0, config: Optional[Dict[str, Any]] = None):
        """
        初始化规则
        
        Args:
            name: 规则名称
            rule_type: 规则类型
            enabled: 是否启用
            priority: 优先级（数字越大优先级越高）
            config: 规则配置
        """
        self.name = name
        self.rule_type = rule_type
        self.enabled = enabled
        self.priority = priority
        self.config = config or {}
        self.logger = logging.getLogger(f"Rule.{name}")
    
    @abstractmethod
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """
        应用规则
        
        Args:
            context: 回测上下文
            event: 当前事件
            data: 相关数据
            
        Returns:
            RuleResult: 规则执行结果
        """
        pass
    
    def is_applicable(self, context: Any, event: Any, data: Dict[str, Any]) -> bool:
        """
        检查规则是否适用于当前情况
        
        Args:
            context: 回测上下文
            event: 当前事件
            data: 相关数据
            
        Returns:
            bool: 是否适用
        """
        return self.enabled
    
    def get_config(self, key: str, default: Any = None) -> Any:
        """获取配置值"""
        return self.config.get(key, default)
    
    def set_config(self, key: str, value: Any):
        """设置配置值"""
        self.config[key] = value
    
    def __str__(self):
        return f"{self.__class__.__name__}(name='{self.name}', type={self.rule_type.value}, enabled={self.enabled})"


class MarketRule(BaseRule):
    """市场特定规则基类"""
    
    def __init__(self, name: str, market: str, rule_type: RuleType = RuleType.MARKET, **kwargs):
        super().__init__(name, rule_type, **kwargs)
        self.market = market
    
    def is_applicable(self, context: Any, event: Any, data: Dict[str, Any]) -> bool:
        """检查市场是否匹配"""
        if not super().is_applicable(context, event, data):
            return False
        
        # 检查市场匹配
        current_market = getattr(context, 'market', None) or data.get('market', '')
        return self.market == current_market or self.market == 'all'


class FrequencyRule(BaseRule):
    """频率特定规则基类"""
    
    def __init__(self, name: str, frequency: Union[str, List[str]], 
                 rule_type: RuleType = RuleType.TRADING, **kwargs):
        super().__init__(name, rule_type, **kwargs)
        self.frequency = frequency if isinstance(frequency, list) else [frequency]
    
    def is_applicable(self, context: Any, event: Any, data: Dict[str, Any]) -> bool:
        """检查频率是否匹配"""
        if not super().is_applicable(context, event, data):
            return False
        
        # 检查频率匹配
        current_frequency = getattr(context, 'frequency', None) or data.get('frequency', '')
        return current_frequency in self.frequency or 'all' in self.frequency


class ConditionalRule(BaseRule):
    """条件规则基类"""
    
    def __init__(self, name: str, condition_func: callable, **kwargs):
        super().__init__(name, **kwargs)
        self.condition_func = condition_func
    
    def is_applicable(self, context: Any, event: Any, data: Dict[str, Any]) -> bool:
        """检查条件是否满足"""
        if not super().is_applicable(context, event, data):
            return False
        
        try:
            return self.condition_func(context, event, data)
        except Exception as e:
            self.logger.error(f"条件检查失败: {e}")
            return False


class RuleChain:
    """规则链，用于组合多个规则"""
    
    def __init__(self, name: str, rules: List[BaseRule], operator: str = "and"):
        """
        初始化规则链
        
        Args:
            name: 规则链名称
            rules: 规则列表
            operator: 操作符，'and' 或 'or'
        """
        self.name = name
        self.rules = rules
        self.operator = operator.lower()
        self.logger = logging.getLogger(f"RuleChain.{name}")
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """
        应用规则链
        
        Args:
            context: 回测上下文
            event: 当前事件
            data: 相关数据
            
        Returns:
            RuleResult: 规则链执行结果
        """
        results = []
        messages = []
        modified_data = {}
        
        for rule in self.rules:
            if rule.is_applicable(context, event, data):
                try:
                    result = rule.apply(context, event, data)
                    results.append(result)
                    
                    if result.message:
                        messages.append(f"{rule.name}: {result.message}")
                    
                    # 合并修改后的数据
                    if result.modified_data:
                        modified_data.update(result.modified_data)
                    
                    # 根据操作符决定是否继续
                    if self.operator == "and" and not result.passed:
                        # AND操作，遇到失败就停止
                        break
                    elif self.operator == "or" and result.passed:
                        # OR操作，遇到成功就停止
                        break
                        
                except Exception as e:
                    self.logger.error(f"规则 {rule.name} 执行失败: {e}")
                    if self.operator == "and":
                        # AND操作，遇到异常就失败
                        return RuleResult(
                            passed=False, 
                            message=f"规则链 {self.name} 执行失败: {e}",
                            modified_data=modified_data
                        )
        
        # 根据操作符计算最终结果
        if self.operator == "and":
            final_passed = all(r.passed for r in results)
        else:  # or
            final_passed = any(r.passed for r in results)
        
        return RuleResult(
            passed=final_passed,
            message="; ".join(messages),
            modified_data=modified_data
        )


class RuleGroup:
    """规则组，用于管理一组相关的规则"""
    
    def __init__(self, name: str, rules: List[BaseRule], description: str = ""):
        self.name = name
        self.rules = rules
        self.description = description
        self.enabled = True
        self.logger = logging.getLogger(f"RuleGroup.{name}")
    
    def add_rule(self, rule: BaseRule):
        """添加规则"""
        self.rules.append(rule)
        self.logger.info(f"添加规则: {rule.name}")
    
    def remove_rule(self, rule_name: str):
        """移除规则"""
        self.rules = [r for r in self.rules if r.name != rule_name]
        self.logger.info(f"移除规则: {rule_name}")
    
    def get_rule(self, rule_name: str) -> Optional[BaseRule]:
        """获取规则"""
        for rule in self.rules:
            if rule.name == rule_name:
                return rule
        return None
    
    def get_applicable_rules(self, context: Any, event: Any, data: Dict[str, Any]) -> List[BaseRule]:
        """获取适用的规则"""
        if not self.enabled:
            return []
        
        applicable_rules = []
        for rule in self.rules:
            if rule.is_applicable(context, event, data):
                applicable_rules.append(rule)
        
        # 按优先级排序
        applicable_rules.sort(key=lambda x: x.priority, reverse=True)
        return applicable_rules
    
    def apply_rules(self, context: Any, event: Any, data: Dict[str, Any]) -> List[RuleResult]:
        """应用所有适用的规则"""
        results = []
        applicable_rules = self.get_applicable_rules(context, event, data)
        
        for rule in applicable_rules:
            try:
                result = rule.apply(context, event, data)
                results.append(result)
            except Exception as e:
                self.logger.error(f"规则 {rule.name} 执行失败: {e}")
                results.append(RuleResult(
                    passed=False, 
                    message=f"规则执行失败: {e}"
                ))
        
        return results
    
    def __len__(self):
        return len(self.rules)
    
    def __str__(self):
        return f"RuleGroup(name='{self.name}', rules={len(self.rules)}, enabled={self.enabled})" 