"""
规则引擎实现
负责管理和执行所有的规则
"""

from typing import Dict, Any, List, Optional, Union
from datetime import datetime
import logging
from pathlib import Path
import importlib.util

from .base_rule import BaseRule, RuleResult, RuleGroup, RuleType
from .rule_factory import RuleFactory


class RuleEngine:
    """规则引擎，负责管理和执行所有规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化规则引擎
        
        Args:
            config: 配置参数
        """
        self.config = config or {}
        self.logger = logging.getLogger("RuleEngine")
        
        # 规则存储
        self.rule_groups: Dict[str, RuleGroup] = {}
        self.rules: Dict[str, BaseRule] = {}
        
        # 规则工厂
        self.rule_factory = RuleFactory()
        
        # 执行统计
        self.execution_stats = {
            'total_executions': 0,
            'successful_executions': 0,
            'failed_executions': 0,
            'execution_times': []
        }
        
        # 初始化
        self._initialize()
    
    def _initialize(self):
        """初始化规则引擎"""
        # 加载内置规则
        self._load_builtin_rules()
        
        # 加载用户自定义规则
        self._load_user_rules()
        
        self.logger.info(f"规则引擎初始化完成，共加载 {len(self.rules)} 个规则")
    
    def _load_builtin_rules(self):
        """加载内置规则"""
        try:
            # 加载通用规则
            self.add_rule_group(self.rule_factory.create_trading_rules())
            self.add_rule_group(self.rule_factory.create_market_rules())
            self.add_rule_group(self.rule_factory.create_risk_rules())
            
            # 加载市场特定规则
            self.add_rule_group(self.rule_factory.create_cn_stock_rules())
            self.add_rule_group(self.rule_factory.create_hk_stock_rules())
            self.add_rule_group(self.rule_factory.create_us_stock_rules())
            
            self.logger.info("内置规则加载完成")
            
        except Exception as e:
            self.logger.error(f"加载内置规则失败: {e}")
    
    def _load_user_rules(self):
        """加载用户自定义规则"""
        try:
            # 从配置中获取用户规则路径
            user_rules_path = self.config.get('user_rules_path', '')
            if not user_rules_path:
                return
            
            rules_dir = Path(user_rules_path)
            if not rules_dir.exists():
                return
            
            # 加载所有 Python 文件
            for rule_file in rules_dir.glob('*.py'):
                if rule_file.stem.startswith('_'):
                    continue
                
                try:
                    # 动态导入模块
                    spec = importlib.util.spec_from_file_location(
                        rule_file.stem, rule_file
                    )
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    
                    # 查找规则类
                    for attr_name in dir(module):
                        attr = getattr(module, attr_name)
                        if (isinstance(attr, type) and 
                            issubclass(attr, BaseRule) and 
                            attr is not BaseRule):
                            
                            # 实例化规则
                            rule_instance = attr()
                            self.add_rule(rule_instance)
                            
                            self.logger.info(f"加载用户规则: {rule_instance.name}")
                            
                except Exception as e:
                    self.logger.error(f"加载用户规则文件 {rule_file} 失败: {e}")
            
            self.logger.info("用户自定义规则加载完成")
            
        except Exception as e:
            self.logger.error(f"加载用户规则失败: {e}")
    
    def add_rule_group(self, rule_group: RuleGroup):
        """添加规则组"""
        self.rule_groups[rule_group.name] = rule_group
        
        # 同时将规则添加到规则字典中
        for rule in rule_group.rules:
            self.rules[rule.name] = rule
        
        self.logger.info(f"添加规则组: {rule_group.name}, 包含 {len(rule_group.rules)} 个规则")
    
    def add_rule(self, rule: BaseRule, group_name: str = "custom"):
        """添加单个规则"""
        # 添加到规则字典
        self.rules[rule.name] = rule
        
        # 添加到规则组
        if group_name not in self.rule_groups:
            self.rule_groups[group_name] = RuleGroup(group_name, [])
        
        self.rule_groups[group_name].add_rule(rule)
        
        self.logger.info(f"添加规则: {rule.name} 到组 {group_name}")
    
    def remove_rule(self, rule_name: str):
        """移除规则"""
        if rule_name in self.rules:
            del self.rules[rule_name]
            
            # 从规则组中移除
            for group in self.rule_groups.values():
                group.remove_rule(rule_name)
            
            self.logger.info(f"移除规则: {rule_name}")
    
    def get_rule(self, rule_name: str) -> Optional[BaseRule]:
        """获取规则"""
        return self.rules.get(rule_name)
    
    def enable_rule(self, rule_name: str):
        """启用规则"""
        rule = self.get_rule(rule_name)
        if rule:
            rule.enabled = True
            self.logger.info(f"启用规则: {rule_name}")
    
    def disable_rule(self, rule_name: str):
        """禁用规则"""
        rule = self.get_rule(rule_name)
        if rule:
            rule.enabled = False
            self.logger.info(f"禁用规则: {rule_name}")
    
    def get_applicable_rules(self, context: Any, event: Any, data: Dict[str, Any], 
                           rule_types: Optional[List[RuleType]] = None) -> List[BaseRule]:
        """
        获取适用的规则
        
        Args:
            context: 回测上下文
            event: 当前事件
            data: 相关数据
            rule_types: 规则类型过滤
            
        Returns:
            List[BaseRule]: 适用的规则列表
        """
        applicable_rules = []
        
        for rule in self.rules.values():
            # 类型过滤
            if rule_types and rule.rule_type not in rule_types:
                continue
            
            # 检查规则是否适用
            if rule.is_applicable(context, event, data):
                applicable_rules.append(rule)
        
        # 按优先级排序
        applicable_rules.sort(key=lambda x: x.priority, reverse=True)
        
        return applicable_rules
    
    def apply_rules(self, context: Any, event: Any, data: Dict[str, Any],
                   rule_types: Optional[List[RuleType]] = None,
                   stop_on_failure: bool = False) -> List[RuleResult]:
        """
        应用规则
        
        Args:
            context: 回测上下文
            event: 当前事件
            data: 相关数据
            rule_types: 规则类型过滤
            stop_on_failure: 是否在失败时停止
            
        Returns:
            List[RuleResult]: 规则执行结果列表
        """
        start_time = datetime.now()
        
        try:
            # 获取适用的规则
            applicable_rules = self.get_applicable_rules(context, event, data, rule_types)
            
            results = []
            
            for rule in applicable_rules:
                try:
                    result = rule.apply(context, event, data)
                    results.append(result)
                    
                    if result.passed:
                        self.execution_stats['successful_executions'] += 1
                    else:
                        self.execution_stats['failed_executions'] += 1
                        
                        if stop_on_failure:
                            break
                    
                except Exception as e:
                    self.logger.error(f"规则 {rule.name} 执行失败: {e}")
                    
                    error_result = RuleResult(
                        passed=False,
                        message=f"规则执行异常: {e}"
                    )
                    results.append(error_result)
                    
                    self.execution_stats['failed_executions'] += 1
                    
                    if stop_on_failure:
                        break
            
            # 更新统计信息
            execution_time = (datetime.now() - start_time).total_seconds()
            self.execution_stats['total_executions'] += 1
            self.execution_stats['execution_times'].append(execution_time)
            
            return results
            
        except Exception as e:
            self.logger.error(f"规则引擎执行失败: {e}")
            return [RuleResult(passed=False, message=f"规则引擎执行失败: {e}")]
    
    def apply_rule_group(self, group_name: str, context: Any, event: Any, 
                        data: Dict[str, Any]) -> List[RuleResult]:
        """
        应用规则组
        
        Args:
            group_name: 规则组名称
            context: 回测上下文
            event: 当前事件
            data: 相关数据
            
        Returns:
            List[RuleResult]: 规则执行结果列表
        """
        if group_name not in self.rule_groups:
            return [RuleResult(passed=False, message=f"规则组 {group_name} 不存在")]
        
        rule_group = self.rule_groups[group_name]
        return rule_group.apply_rules(context, event, data)
    
    def check_order_rules(self, context: Any, event: Any, order_data: Dict[str, Any]) -> RuleResult:
        """
        检查订单规则
        
        Args:
            context: 回测上下文
            event: 当前事件
            order_data: 订单数据
            
        Returns:
            RuleResult: 综合检查结果
        """
        # 应用交易规则
        results = self.apply_rules(
            context, event, order_data,
            rule_types=[RuleType.TRADING, RuleType.RISK, RuleType.MARKET],
            stop_on_failure=True
        )
        
        # 检查是否全部通过
        all_passed = all(result.passed for result in results)
        
        # 合并消息
        messages = [result.message for result in results if result.message]
        
        # 合并修改后的数据
        modified_data = {}
        for result in results:
            if result.modified_data:
                modified_data.update(result.modified_data)
        
        return RuleResult(
            passed=all_passed,
            message="; ".join(messages),
            modified_data=modified_data
        )
    
    def check_market_rules(self, context: Any, event: Any, market_data: Dict[str, Any]) -> RuleResult:
        """
        检查市场规则
        
        Args:
            context: 回测上下文
            event: 当前事件
            market_data: 市场数据
            
        Returns:
            RuleResult: 综合检查结果
        """
        # 应用市场规则
        results = self.apply_rules(
            context, event, market_data,
            rule_types=[RuleType.MARKET, RuleType.TIME],
            stop_on_failure=False
        )
        
        # 检查是否全部通过
        all_passed = all(result.passed for result in results)
        
        # 合并消息
        messages = [result.message for result in results if result.message]
        
        return RuleResult(
            passed=all_passed,
            message="; ".join(messages)
        )
    
    def get_rule_statistics(self) -> Dict[str, Any]:
        """获取规则统计信息"""
        total_rules = len(self.rules)
        enabled_rules = sum(1 for rule in self.rules.values() if rule.enabled)
        disabled_rules = total_rules - enabled_rules
        
        # 按类型统计
        type_stats = {}
        for rule in self.rules.values():
            rule_type = rule.rule_type.value
            if rule_type not in type_stats:
                type_stats[rule_type] = {'total': 0, 'enabled': 0}
            type_stats[rule_type]['total'] += 1
            if rule.enabled:
                type_stats[rule_type]['enabled'] += 1
        
        # 执行统计
        avg_execution_time = 0
        if self.execution_stats['execution_times']:
            avg_execution_time = sum(self.execution_stats['execution_times']) / len(self.execution_stats['execution_times'])
        
        return {
            'total_rules': total_rules,
            'enabled_rules': enabled_rules,
            'disabled_rules': disabled_rules,
            'rule_groups': len(self.rule_groups),
            'type_statistics': type_stats,
            'execution_statistics': {
                'total_executions': self.execution_stats['total_executions'],
                'successful_executions': self.execution_stats['successful_executions'],
                'failed_executions': self.execution_stats['failed_executions'],
                'success_rate': (self.execution_stats['successful_executions'] / 
                               max(1, self.execution_stats['total_executions'])) * 100,
                'average_execution_time': avg_execution_time
            }
        }
    
    def list_rules(self, rule_type: Optional[RuleType] = None, 
                  enabled_only: bool = False) -> List[Dict[str, Any]]:
        """
        列出规则
        
        Args:
            rule_type: 规则类型过滤
            enabled_only: 是否只列出启用的规则
            
        Returns:
            List[Dict]: 规则信息列表
        """
        rules_info = []
        
        for rule in self.rules.values():
            # 类型过滤
            if rule_type and rule.rule_type != rule_type:
                continue
            
            # 启用状态过滤
            if enabled_only and not rule.enabled:
                continue
            
            rules_info.append({
                'name': rule.name,
                'type': rule.rule_type.value,
                'enabled': rule.enabled,
                'priority': rule.priority,
                'config': rule.config,
                'description': getattr(rule, 'description', '')
            })
        
        # 按优先级排序
        rules_info.sort(key=lambda x: x['priority'], reverse=True)
        
        return rules_info
    
    def export_config(self) -> Dict[str, Any]:
        """导出规则配置"""
        config = {
            'rule_groups': {},
            'rules': {}
        }
        
        # 导出规则组配置
        for group_name, group in self.rule_groups.items():
            config['rule_groups'][group_name] = {
                'name': group.name,
                'description': group.description,
                'enabled': group.enabled,
                'rules': [rule.name for rule in group.rules]
            }
        
        # 导出规则配置
        for rule_name, rule in self.rules.items():
            config['rules'][rule_name] = {
                'name': rule.name,
                'type': rule.rule_type.value,
                'enabled': rule.enabled,
                'priority': rule.priority,
                'config': rule.config
            }
        
        return config
    
    def load_config(self, config: Dict[str, Any]):
        """加载规则配置"""
        try:
            # 加载规则配置
            if 'rules' in config:
                for rule_name, rule_config in config['rules'].items():
                    rule = self.get_rule(rule_name)
                    if rule:
                        rule.enabled = rule_config.get('enabled', True)
                        rule.priority = rule_config.get('priority', 0)
                        rule.config.update(rule_config.get('config', {}))
            
            # 加载规则组配置
            if 'rule_groups' in config:
                for group_name, group_config in config['rule_groups'].items():
                    if group_name in self.rule_groups:
                        group = self.rule_groups[group_name]
                        group.enabled = group_config.get('enabled', True)
                        group.description = group_config.get('description', '')
            
            self.logger.info("规则配置加载完成")
            
        except Exception as e:
            self.logger.error(f"加载规则配置失败: {e}")
    
    def __str__(self):
        return f"RuleEngine(rules={len(self.rules)}, groups={len(self.rule_groups)})" 