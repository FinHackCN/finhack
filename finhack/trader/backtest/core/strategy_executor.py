"""
策略执行器

负责调用策略中的各种函数，包括定时任务和事件响应
"""

import logging
import asyncio
import inspect
from typing import Dict, Any, Callable, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class StrategyExecutor:
    """策略执行器
    
    负责策略函数的调用和执行管理
    """
    
    def __init__(self):
        """初始化策略执行器"""
        self.context = None
        self.strategy_module = None
        self.scheduled_functions = {}  # 存储注册的定时函数
        self.event_handlers = {}      # 存储事件处理函数
        self.execution_stats = {     # 执行统计信息
            'total_executions': 0,
            'successful_executions': 0,
            'failed_executions': 0,
            'execution_errors': []
        }
        
        logger.info("策略执行器初始化完成")
    
    def set_context(self, context: Dict[str, Any]):
        """设置上下文
        
        Args:
            context: 回测上下文
        """
        self.context = context
        logger.debug("策略执行器已设置上下文")
    
    def set_strategy_module(self, strategy_module):
        """设置策略模块
        
        Args:
            strategy_module: 策略模块对象
        """
        self.strategy_module = strategy_module
        # 自动扫描策略模块中的事件处理函数
        self._scan_event_handlers()
        logger.debug("策略执行器已设置策略模块")
    
    def _scan_event_handlers(self):
        """扫描策略模块中的事件处理函数"""
        if not self.strategy_module:
            return
        
        # 清空现有的事件处理器
        self.event_handlers.clear()
        
        # 扫描所有函数，查找事件处理函数
        for name in dir(self.strategy_module):
            if name.startswith('_'):
                continue
                
            attr = getattr(self.strategy_module, name)
            if not callable(attr):
                continue
            
            # 检查函数签名，确定是否为事件处理函数
            sig = inspect.signature(attr)
            params = list(sig.parameters.keys())
            
            # 事件处理函数通常接受context参数，可能还接受event参数
            if 'context' in params:
                self.event_handlers[name] = attr
                logger.debug(f"发现事件处理函数: {name}")
    
    def register_scheduled_function(self, task_id: str, function: Callable):
        """注册定时函数
        
        Args:
            task_id: 任务ID
            function: 函数对象
        """
        self.scheduled_functions[task_id] = function
        logger.debug(f"注册定时函数: {task_id} -> {function.__name__}")
    
    def execute_scheduled_function(self, function_name: str, task_id: str):
        """执行定时任务函数（同步版本）
        
        Args:
            function_name: 函数名称
            task_id: 任务ID
        """
        try:
            self.execution_stats['total_executions'] += 1
            
            if task_id in self.scheduled_functions:
                func = self.scheduled_functions[task_id]
            elif hasattr(self.strategy_module, function_name):
                func = getattr(self.strategy_module, function_name)
            else:
                logger.warning(f"未找到定时函数: {function_name}")
                self.execution_stats['failed_executions'] += 1
                return
            
            logger.debug(f"执行定时函数: {function_name}")
            
            # 执行函数
            func(self.context)
            
            self.execution_stats['successful_executions'] += 1
            
        except Exception as e:
            self.execution_stats['failed_executions'] += 1
            error_info = {
                'function': function_name,
                'task_id': task_id,
                'error': str(e),
                'timestamp': datetime.now()
            }
            self.execution_stats['execution_errors'].append(error_info)
            logger.error(f"执行定时函数失败 {function_name}: {e}")
            # 不抛出异常，继续执行其他函数
    
    def execute_market_event(self, event):
        """执行市场事件响应函数（同步版本）
        
        Args:
            event: 事件对象
        """
        try:
            self.execution_stats['total_executions'] += 1
            
            # 根据事件类型查找对应的策略函数
            event_name = event.event_type.value.lower()
            
            # 优先查找专门的事件处理函数
            if event_name in self.event_handlers:
                func = self.event_handlers[event_name]
                logger.debug(f"执行专门的事件处理函数: {event_name}")
                
                # 检查函数签名
                sig = inspect.signature(func)
                params = list(sig.parameters.keys())
                
                if 'event' in params:
                    func(self.context, event)
                else:
                    func(self.context)
                
                self.execution_stats['successful_executions'] += 1
                return
            
            # 检查策略模块是否有对应的事件处理函数
            elif hasattr(self.strategy_module, event_name):
                func = getattr(self.strategy_module, event_name)
                logger.debug(f"执行市场事件函数: {event_name}")
                
                # 检查函数签名
                sig = inspect.signature(func)
                params = list(sig.parameters.keys())
                
                if 'event' in params:
                    func(self.context, event)
                else:
                    func(self.context)
                
                self.execution_stats['successful_executions'] += 1
            else:
                logger.debug(f"策略未定义事件处理函数: {event_name}")
                
        except Exception as e:
            self.execution_stats['failed_executions'] += 1
            error_info = {
                'event_type': event.event_type.value,
                'event_time': event.event_time,
                'error': str(e),
                'timestamp': datetime.now()
            }
            self.execution_stats['execution_errors'].append(error_info)
            logger.error(f"执行市场事件函数失败 {event_name}: {e}")
            # 不抛出异常，继续执行其他事件
    
    def execute_function_by_name(self, function_name: str, *args, **kwargs):
        """根据函数名执行策略函数（同步版本）
        
        Args:
            function_name: 函数名称
            *args: 位置参数
            **kwargs: 关键字参数
        """
        try:
            self.execution_stats['total_executions'] += 1
            
            if not hasattr(self.strategy_module, function_name):
                logger.warning(f"策略模块未定义函数: {function_name}")
                self.execution_stats['failed_executions'] += 1
                return None
            
            func = getattr(self.strategy_module, function_name)
            
            logger.debug(f"执行策略函数: {function_name}")
            
            # 执行函数
            result = func(*args, **kwargs)
            
            self.execution_stats['successful_executions'] += 1
            return result
                
        except Exception as e:
            self.execution_stats['failed_executions'] += 1
            error_info = {
                'function': function_name,
                'error': str(e),
                'timestamp': datetime.now()
            }
            self.execution_stats['execution_errors'].append(error_info)
            logger.error(f"执行策略函数失败 {function_name}: {e}")
            # 不抛出异常，返回None
            return None
    
    def get_available_functions(self):
        """获取策略模块中可用的函数列表
        
        Returns:
            List[str]: 函数名称列表
        """
        if not self.strategy_module:
            return []
        
        functions = []
        for name in dir(self.strategy_module):
            if not name.startswith('_'):
                attr = getattr(self.strategy_module, name)
                if callable(attr):
                    functions.append(name)
        
        return functions
    
    def get_execution_stats(self):
        """获取执行统计信息
        
        Returns:
            Dict: 执行统计信息
        """
        return self.execution_stats.copy()
    
    def reset_execution_stats(self):
        """重置执行统计信息"""
        self.execution_stats = {
            'total_executions': 0,
            'successful_executions': 0,
            'failed_executions': 0,
            'execution_errors': []
        }
        logger.info("执行统计信息已重置")
    
    def stop(self):
        """停止策略执行器"""
        self.scheduled_functions.clear()
        self.event_handlers.clear()
        logger.info("策略执行器已停止") 