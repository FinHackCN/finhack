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
        logger.debug("策略执行器已设置策略模块")
    
    def register_scheduled_function(self, task_id: str, function: Callable):
        """注册定时函数
        
        Args:
            task_id: 任务ID
            function: 函数对象
        """
        self.scheduled_functions[task_id] = function
        logger.debug(f"注册定时函数: {task_id} -> {function.__name__}")
    
    async def execute_scheduled_function(self, function_name: str, task_id: str):
        """执行定时任务函数
        
        Args:
            function_name: 函数名称
            task_id: 任务ID
        """
        try:
            if task_id in self.scheduled_functions:
                func = self.scheduled_functions[task_id]
            elif hasattr(self.strategy_module, function_name):
                func = getattr(self.strategy_module, function_name)
            else:
                logger.warning(f"未找到定时函数: {function_name}")
                return
            
            logger.debug(f"执行定时函数: {function_name}")
            
            # 检查函数是否是异步函数
            if inspect.iscoroutinefunction(func):
                await func(self.context)
            else:
                # 同步函数在异步环境中调用
                func(self.context)
                
        except Exception as e:
            logger.error(f"执行定时函数失败 {function_name}: {e}")
            raise
    
    async def execute_market_event(self, event):
        """执行市场事件响应函数
        
        Args:
            event: 事件对象
        """
        try:
            # 根据事件类型查找对应的策略函数
            event_name = event.event_type.value.lower()
            
            # 检查策略模块是否有对应的事件处理函数
            if hasattr(self.strategy_module, event_name):
                func = getattr(self.strategy_module, event_name)
                
                logger.debug(f"执行市场事件函数: {event_name}")
                
                # 检查函数是否是异步函数
                if inspect.iscoroutinefunction(func):
                    await func(self.context, event)
                else:
                    func(self.context, event)
            else:
                logger.debug(f"策略未定义事件处理函数: {event_name}")
                
        except Exception as e:
            logger.error(f"执行市场事件函数失败 {event_name}: {e}")
            raise
    
    async def execute_function_by_name(self, function_name: str, *args, **kwargs):
        """根据函数名执行策略函数
        
        Args:
            function_name: 函数名称
            *args: 位置参数
            **kwargs: 关键字参数
        """
        try:
            if not hasattr(self.strategy_module, function_name):
                logger.warning(f"策略模块未定义函数: {function_name}")
                return None
            
            func = getattr(self.strategy_module, function_name)
            
            logger.debug(f"执行策略函数: {function_name}")
            
            # 检查函数是否是异步函数
            if inspect.iscoroutinefunction(func):
                return await func(*args, **kwargs)
            else:
                return func(*args, **kwargs)
                
        except Exception as e:
            logger.error(f"执行策略函数失败 {function_name}: {e}")
            raise
    
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
    
    def stop(self):
        """停止策略执行器"""
        self.scheduled_functions.clear()
        logger.info("策略执行器已停止") 