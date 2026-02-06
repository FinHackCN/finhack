"""
事件管理器实现
负责事件配置管理、验证、优化和监控
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Set, Callable
from collections import defaultdict
from pathlib import Path

from .base_event import BaseEvent, EventType


class EventManager:
    """事件管理器，负责事件配置管理、验证和优化"""
    
    def __init__(self):
        self.logger = logging.getLogger("EventManager")
        self.context = None
        self.config = {}
        
        # 事件配置
        self.event_configs = {}
        
        # 事件验证器
        self.event_validators = {}
        
        # 事件处理器
        self.event_handlers = {}
        
        # 事件监控
        self.event_monitor = {
            'processing_times': defaultdict(list),
            'success_rates': defaultdict(float),
            'error_counts': defaultdict(int),
            'usage_counts': defaultdict(int)
        }
        
        # 事件优化建议
        self.optimization_suggestions = []
    
    def initialize(self, config: Dict[str, Any]):
        """
        初始化事件管理器
        
        Args:
            config: 配置参数
        """
        self.config = config
        
        # 加载事件配置
        self._load_event_configs()
        
        # 初始化验证器
        self._initialize_validators()
        
        # 初始化处理器
        self._initialize_handlers()
        
        self.logger.info("事件管理器初始化完成")
    
    def _load_event_configs(self):
        """加载事件配置"""
        config_path = self.config.get('event_config_path', '')
        
        if config_path and Path(config_path).exists():
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    self.event_configs = json.load(f)
                self.logger.info(f"已加载事件配置: {config_path}")
            except Exception as e:
                self.logger.error(f"加载事件配置失败: {str(e)}")
        else:
            # 使用默认配置
            self.event_configs = self._create_default_event_configs()
    
    def _create_default_event_configs(self) -> Dict[str, Any]:
        """创建默认事件配置"""
        return {
            'market_events': {
                'enabled': True,
                'priority': 'high',
                'timeout': 10.0,
                'retry_count': 3
            },
            'corporate_action_events': {
                'enabled': True,
                'priority': 'medium',
                'timeout': 30.0,
                'retry_count': 2
            },
            'trade_events': {
                'enabled': True,
                'priority': 'high',
                'timeout': 5.0,
                'retry_count': 3
            },
            'user_events': {
                'enabled': True,
                'priority': 'low',
                'timeout': 60.0,
                'retry_count': 1
            }
        }
    
    def _initialize_validators(self):
        """初始化事件验证器"""
        self.event_validators = {
            'time_validator': self._validate_event_time,
            'market_validator': self._validate_event_market,
            'data_validator': self._validate_event_data,
            'dependency_validator': self._validate_event_dependencies
        }
    
    def _initialize_handlers(self):
        """初始化事件处理器"""
        self.event_handlers = {
            EventType.DIVIDEND_STOCK_SPLIT: self._handle_dividend_stock_split,
            EventType.RIGHTS_ISSUE_ADDITIONAL_ISSUANCE: self._handle_rights_issue,
            EventType.TRADING_SUSPENSION_RESUMPTION: self._handle_trading_suspension,
            EventType.DELISTING: self._handle_delisting
        }
    
    def validate_event(self, event: BaseEvent) -> bool:
        """
        验证事件
        
        Args:
            event: 要验证的事件
            
        Returns:
            bool: 验证是否通过
        """
        try:
            # 运行所有验证器
            for validator_name, validator_func in self.event_validators.items():
                if not validator_func(event):
                    self.logger.warning(f"事件验证失败: {validator_name}, 事件: {event.event_type.value}")
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"事件验证异常: {str(e)}")
            return False
    
    def _validate_event_time(self, event: BaseEvent) -> bool:
        """验证事件时间"""
        if not isinstance(event.event_time, datetime):
            return False
        
        # 检查时间是否在合理范围内
        now = datetime.now()
        if event.event_time > now + timedelta(days=365):  # 不能超过一年
            return False
        
        return True
    
    def _validate_event_market(self, event: BaseEvent) -> bool:
        """验证事件市场"""
        valid_markets = ['cn_stock', 'hk_stock', 'us_stock', 'futures', 'options']
        return event.market in valid_markets
    
    def _validate_event_data(self, event: BaseEvent) -> bool:
        """验证事件数据"""
        if not isinstance(event.data, dict):
            return False
        
        # 根据事件类型验证数据结构
        event_type = event.event_type
        
        if event_type == EventType.DIVIDEND_STOCK_SPLIT:
            required_fields = ['symbol', 'dividend_ratio', 'split_ratio', 'ex_date']
            return all(field in event.data for field in required_fields)
        
        elif event_type == EventType.RIGHTS_ISSUE_ADDITIONAL_ISSUANCE:
            required_fields = ['symbol', 'issue_price', 'issue_ratio', 'record_date']
            return all(field in event.data for field in required_fields)
        
        elif event_type == EventType.TRADING_SUSPENSION_RESUMPTION:
            required_fields = ['symbol', 'action', 'reason']
            return all(field in event.data for field in required_fields)
        
        return True
    
    def _validate_event_dependencies(self, event: BaseEvent) -> bool:
        """验证事件依赖关系"""
        # 检查事件是否有依赖的前置事件
        dependencies = event.data.get('dependencies', [])
        
        for dep_event_type in dependencies:
            # 这里可以检查依赖事件是否已经处理完成
            # 暂时简化实现
            pass
        
        return True
    
    def process_event(self, event: BaseEvent) -> bool:
        """
        处理事件
        
        Args:
            event: 要处理的事件
            
        Returns:
            bool: 处理是否成功
        """
        start_time = datetime.now()
        
        try:
            # 验证事件
            if not self.validate_event(event):
                self.event_monitor['error_counts'][event.event_type.value] += 1
                return False
            
            # 使用特定处理器
            if event.event_type in self.event_handlers:
                handler = self.event_handlers[event.event_type]
                result = handler(event)
            else:
                # 使用默认处理器
                result = self._handle_default_event(event)
            
            # 记录处理时间
            processing_time = (datetime.now() - start_time).total_seconds()
            self.event_monitor['processing_times'][event.event_type.value].append(processing_time)
            
            # 更新使用计数
            self.event_monitor['usage_counts'][event.event_type.value] += 1
            
            if result:
                # 更新成功率
                self._update_success_rate(event.event_type.value, True)
            else:
                self.event_monitor['error_counts'][event.event_type.value] += 1
                self._update_success_rate(event.event_type.value, False)
            
            return result
            
        except Exception as e:
            self.logger.error(f"处理事件异常: {event.event_type.value}, 错误: {str(e)}")
            self.event_monitor['error_counts'][event.event_type.value] += 1
            self._update_success_rate(event.event_type.value, False)
            return False
    
    def _handle_dividend_stock_split(self, event: BaseEvent) -> bool:
        """处理分红送股事件"""
        try:
            symbol = event.data.get('symbol')
            dividend_ratio = event.data.get('dividend_ratio', 0)  # 分红比例
            split_ratio = event.data.get('split_ratio', 0)        # 送股比例
            ex_date = event.data.get('ex_date')                   # 除权日
            
            self.logger.info(f"处理分红送股事件: {symbol}, 分红比例: {dividend_ratio}, 送股比例: {split_ratio}")
            
            # 更新持仓和现金
            if self.context and hasattr(self.context, 'portfolio'):
                portfolio = self.context.portfolio
                
                # 检查是否持有该股票
                if symbol in portfolio.positions:
                    position = portfolio.positions[symbol]
                    current_shares = position.get('shares', 0)
                    
                    # 计算分红金额
                    dividend_amount = current_shares * dividend_ratio
                    
                    # 计算送股数量
                    bonus_shares = current_shares * split_ratio
                    
                    # 更新现金（加上分红）
                    portfolio.cash += dividend_amount
                    
                    # 更新持仓（加上送股）
                    portfolio.positions[symbol]['shares'] = current_shares + bonus_shares
                    
                    # 调整成本价（因为送股导致股份增加）
                    if bonus_shares > 0:
                        old_cost_basis = position.get('cost_basis', 0)
                        new_cost_basis = old_cost_basis * current_shares / (current_shares + bonus_shares)
                        portfolio.positions[symbol]['cost_basis'] = new_cost_basis
                    
                    self.logger.info(f"分红送股处理完成: {symbol}, 分红金额: {dividend_amount}, 送股数量: {bonus_shares}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"处理分红送股事件失败: {str(e)}")
            return False
    
    def _handle_rights_issue(self, event: BaseEvent) -> bool:
        """处理配股事件"""
        try:
            symbol = event.data.get('symbol')
            issue_price = event.data.get('issue_price')    # 配股价格
            issue_ratio = event.data.get('issue_ratio')    # 配股比例
            record_date = event.data.get('record_date')    # 股权登记日
            
            self.logger.info(f"处理配股事件: {symbol}, 配股价格: {issue_price}, 配股比例: {issue_ratio}")
            
            # 配股处理逻辑
            if self.context and hasattr(self.context, 'portfolio'):
                portfolio = self.context.portfolio
                
                # 检查是否持有该股票
                if symbol in portfolio.positions:
                    position = portfolio.positions[symbol]
                    current_shares = position.get('shares', 0)
                    
                    # 计算配股数量
                    rights_shares = current_shares * issue_ratio
                    
                    # 计算配股所需资金
                    rights_cost = rights_shares * issue_price
                    
                    # 检查资金是否充足
                    if portfolio.cash >= rights_cost:
                        # 执行配股
                        portfolio.cash -= rights_cost
                        portfolio.positions[symbol]['shares'] = current_shares + rights_shares
                        
                        # 调整成本价
                        old_cost_basis = position.get('cost_basis', 0)
                        total_cost = old_cost_basis * current_shares + rights_cost
                        new_cost_basis = total_cost / (current_shares + rights_shares)
                        portfolio.positions[symbol]['cost_basis'] = new_cost_basis
                        
                        self.logger.info(f"配股处理完成: {symbol}, 配股数量: {rights_shares}, 配股成本: {rights_cost}")
                    else:
                        self.logger.warning(f"配股资金不足: {symbol}, 需要: {rights_cost}, 可用: {portfolio.cash}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"处理配股事件失败: {str(e)}")
            return False
    
    def _handle_trading_suspension(self, event: BaseEvent) -> bool:
        """处理停牌复牌事件"""
        try:
            symbol = event.data.get('symbol')
            action = event.data.get('action')  # 'suspend' 或 'resume'
            reason = event.data.get('reason')
            
            self.logger.info(f"处理停牌复牌事件: {symbol}, 动作: {action}, 原因: {reason}")
            
            # 更新股票状态
            if self.context and hasattr(self.context, 'data_center'):
                data_center = self.context.data_center
                
                # 更新股票的交易状态
                if hasattr(data_center, 'update_trading_status'):
                    trading_status = 'suspended' if action == 'suspend' else 'active'
                    data_center.update_trading_status(symbol, trading_status, reason)
            
            return True
            
        except Exception as e:
            self.logger.error(f"处理停牌复牌事件失败: {str(e)}")
            return False
    
    def _handle_delisting(self, event: BaseEvent) -> bool:
        """处理退市事件"""
        try:
            symbol = event.data.get('symbol')
            delisting_date = event.data.get('delisting_date')
            
            self.logger.info(f"处理退市事件: {symbol}, 退市日期: {delisting_date}")

            # 更新股票状态为退市
            if self.context and hasattr(self.context, 'data_center'):
                data_center = self.context.data_center
                if hasattr(data_center, 'update_trading_status'):
                    delisting_reason = event.data.get('delisting_reason', '退市')
                    data_center.update_trading_status(symbol, 'delisted', delisting_reason)
                    self.logger.info(f"[退市处理] {symbol} 已标记为退市")

            # 强制清仓
            if self.context and hasattr(self.context, 'portfolio'):
                portfolio = self.context.portfolio
                
                if symbol in portfolio.positions:
                    position = portfolio.positions[symbol]
                    shares = position.get('shares', 0)
                    
                    if shares > 0:
                        # 按最后价格清仓
                        last_price = position.get('last_price', 0)
                        liquidation_value = shares * last_price
                        
                        # 更新现金和持仓
                        portfolio.cash += liquidation_value
                        del portfolio.positions[symbol]
                        
                        self.logger.info(f"退市强制清仓: {symbol}, 数量: {shares}, 价值: {liquidation_value}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"处理退市事件失败: {str(e)}")
            return False
    
    def _handle_default_event(self, event: BaseEvent) -> bool:
        """处理默认事件"""
        # 默认处理逻辑，简单记录日志
        self.logger.debug(f"处理事件: {event.event_type.value} at {event.event_time}")
        return True
    
    def _update_success_rate(self, event_type: str, success: bool):
        """更新成功率"""
        current_rate = self.event_monitor['success_rates'].get(event_type, 0.0)
        usage_count = self.event_monitor['usage_counts'].get(event_type, 0)
        
        if usage_count > 0:
            if success:
                new_rate = (current_rate * (usage_count - 1) + 1.0) / usage_count
            else:
                new_rate = (current_rate * (usage_count - 1)) / usage_count
            
            self.event_monitor['success_rates'][event_type] = new_rate
    
    def get_event_performance_report(self) -> Dict[str, Any]:
        """获取事件性能报告"""
        report = {
            'processing_times': {},
            'success_rates': dict(self.event_monitor['success_rates']),
            'error_counts': dict(self.event_monitor['error_counts']),
            'usage_counts': dict(self.event_monitor['usage_counts']),
            'optimization_suggestions': self.optimization_suggestions
        }
        
        # 计算处理时间统计
        for event_type, times in self.event_monitor['processing_times'].items():
            if times:
                report['processing_times'][event_type] = {
                    'avg': sum(times) / len(times),
                    'min': min(times),
                    'max': max(times),
                    'count': len(times)
                }
        
        return report
    
    def generate_optimization_suggestions(self):
        """生成优化建议"""
        suggestions = []
        
        # 分析处理时间
        for event_type, times in self.event_monitor['processing_times'].items():
            if times:
                avg_time = sum(times) / len(times)
                if avg_time > 1.0:  # 超过1秒
                    suggestions.append({
                        'type': 'performance',
                        'event_type': event_type,
                        'issue': f'处理时间过长: {avg_time:.2f}秒',
                        'suggestion': '考虑优化处理逻辑或增加缓存'
                    })
        
        # 分析错误率
        for event_type, error_count in self.event_monitor['error_counts'].items():
            usage_count = self.event_monitor['usage_counts'].get(event_type, 0)
            if usage_count > 0:
                error_rate = error_count / usage_count
                if error_rate > 0.1:  # 错误率超过10%
                    suggestions.append({
                        'type': 'reliability',
                        'event_type': event_type,
                        'issue': f'错误率过高: {error_rate:.1%}',
                        'suggestion': '检查事件处理逻辑和数据验证'
                    })
        
        self.optimization_suggestions = suggestions
        return suggestions
    
    def export_event_config(self, file_path: str):
        """导出事件配置"""
        try:
            config_data = {
                'event_configs': self.event_configs,
                'performance_report': self.get_event_performance_report(),
                'export_time': datetime.now().isoformat()
            }
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(config_data, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"事件配置已导出到: {file_path}")
            
        except Exception as e:
            self.logger.error(f"导出事件配置失败: {str(e)}")
    
    def import_event_config(self, file_path: str):
        """导入事件配置"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
            
            self.event_configs = config_data.get('event_configs', {})
            self.logger.info(f"事件配置已导入: {file_path}")
            
        except Exception as e:
            self.logger.error(f"导入事件配置失败: {str(e)}")
    
    def reset_monitor(self):
        """重置监控数据"""
        self.event_monitor = {
            'processing_times': defaultdict(list),
            'success_rates': defaultdict(float),
            'error_counts': defaultdict(int),
            'usage_counts': defaultdict(int)
        }
        self.optimization_suggestions = []
    
    def __str__(self):
        return f"EventManager(configs={len(self.event_configs)}, validators={len(self.event_validators)})" 