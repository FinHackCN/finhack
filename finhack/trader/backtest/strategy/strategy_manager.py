"""
策略管理器实现
"""

import importlib
import importlib.util
import inspect
from typing import Dict, Any, Optional, List, Type
from pathlib import Path

from .base_strategy import BaseStrategy
from ..events.base_event import BaseEvent, EventType


class StrategyManager:
    """策略管理器，负责策略的加载、管理和事件分发"""
    
    def __init__(self):
        self.strategies: Dict[str, BaseStrategy] = {}
        self.active_strategy: Optional[BaseStrategy] = None
        self.context = None
        self.logger = None
        
    def initialize(self, context):
        """
        初始化策略管理器
        
        Args:
            context: 回测上下文
        """
        self.context = context
        self.logger = context.logger if context else None
        
        # 为所有策略设置上下文
        for strategy in self.strategies.values():
            strategy.initialize(context)
    
    def load_strategy_from_module(self, module_name: str, class_name: str) -> bool:
        """
        从模块加载策略
        
        Args:
            module_name: 模块名称
            class_name: 策略类名
            
        Returns:
            bool: 是否加载成功
        """
        try:
            # 动态导入模块
            module = importlib.import_module(module_name)
            
            # 获取策略类
            strategy_class = getattr(module, class_name)
            
            # 检查是否为BaseStrategy的子类
            if not issubclass(strategy_class, BaseStrategy):
                if self.logger:
                    self.logger.error(f"策略类 {class_name} 必须继承自 BaseStrategy")
                return False
            
            # 创建策略实例
            strategy = strategy_class(name=class_name)
            
            # 添加到管理器
            self.add_strategy(strategy)
            
            if self.logger:
                self.logger.info(f"成功加载策略: {class_name}")
                
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"加载策略失败: {module_name}.{class_name}, 错误: {str(e)}")
            return False
    
    def load_strategy_from_file(self, file_path: str, class_name: str) -> bool:
        """
        从文件加载策略
        
        Args:
            file_path: 策略文件路径
            class_name: 策略类名
            
        Returns:
            bool: 是否加载成功
        """
        try:
            # 转换为Path对象
            path = Path(file_path)
            
            if not path.exists():
                if self.logger:
                    self.logger.error(f"策略文件不存在: {file_path}")
                return False
            
            # 动态导入文件
            spec = importlib.util.spec_from_file_location("strategy_module", path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            # 获取策略类
            strategy_class = getattr(module, class_name)
            
            # 检查是否为BaseStrategy的子类
            if not issubclass(strategy_class, BaseStrategy):
                if self.logger:
                    self.logger.error(f"策略类 {class_name} 必须继承自 BaseStrategy")
                return False
            
            # 创建策略实例
            strategy = strategy_class(name=class_name)
            
            # 添加到管理器
            self.add_strategy(strategy)
            
            if self.logger:
                self.logger.info(f"成功加载策略: {class_name} from {file_path}")
                
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"加载策略失败: {file_path}.{class_name}, 错误: {str(e)}")
            return False
    
    def add_strategy(self, strategy: BaseStrategy):
        """
        添加策略
        
        Args:
            strategy: 策略实例
        """
        self.strategies[strategy.name] = strategy
        
        # 如果还没有激活策略，则设置为激活策略
        if not self.active_strategy:
            self.set_active_strategy(strategy.name)
    
    def remove_strategy(self, name: str) -> bool:
        """
        移除策略
        
        Args:
            name: 策略名称
            
        Returns:
            bool: 是否成功
        """
        if name in self.strategies:
            del self.strategies[name]
            
            # 如果移除的是激活策略，则重置激活策略
            if self.active_strategy and self.active_strategy.name == name:
                self.active_strategy = None
                
                # 如果还有其他策略，则激活第一个
                if self.strategies:
                    first_strategy = list(self.strategies.keys())[0]
                    self.set_active_strategy(first_strategy)
            
            return True
        
        return False
    
    def set_active_strategy(self, name: str) -> bool:
        """
        设置激活策略
        
        Args:
            name: 策略名称
            
        Returns:
            bool: 是否成功
        """
        if name in self.strategies:
            self.active_strategy = self.strategies[name]
            
            # 如果有上下文，则初始化策略
            if self.context:
                self.active_strategy.initialize(self.context)
            
            if self.logger:
                self.logger.info(f"激活策略: {name}")
                
            return True
        
        if self.logger:
            self.logger.error(f"策略不存在: {name}")
            
        return False
    
    def get_strategy(self, name: str) -> Optional[BaseStrategy]:
        """
        获取策略
        
        Args:
            name: 策略名称
            
        Returns:
            Optional[BaseStrategy]: 策略实例
        """
        return self.strategies.get(name)
    
    def get_active_strategy(self) -> Optional[BaseStrategy]:
        """获取激活策略"""
        return self.active_strategy
    
    def list_strategies(self) -> List[str]:
        """列出所有策略名称"""
        return list(self.strategies.keys())
    
    def set_strategy_params(self, strategy_name: str, params: Dict[str, Any]) -> bool:
        """
        设置策略参数
        
        Args:
            strategy_name: 策略名称
            params: 参数字典
            
        Returns:
            bool: 是否成功
        """
        if strategy_name in self.strategies:
            self.strategies[strategy_name].set_params(params)
            return True
        
        return False
    
    def dispatch_event(self, event: BaseEvent) -> bool:
        """
        分发事件到激活策略
        
        Args:
            event: 事件对象
            
        Returns:
            bool: 是否处理成功
        """
        if not self.active_strategy:
            return False
        
        try:
            # 根据事件类型调用相应的策略方法
            handler_name = f"on_{event.event_type.value.lower()}"
            
            if hasattr(self.active_strategy, handler_name):
                handler = getattr(self.active_strategy, handler_name)
                handler(self.context, event)
                return True
            else:
                # 如果没有对应的处理方法，调用通用事件处理方法
                self.active_strategy.on_event(self.context, event)
                return True
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"策略处理事件失败: {event.event_type.value}, 错误: {str(e)}")
            return False
    
    def auto_discover_strategies(self, directory: str) -> List[str]:
        """
        自动发现目录中的策略
        
        Args:
            directory: 目录路径
            
        Returns:
            List[str]: 发现的策略类名列表
        """
        discovered_strategies = []
        
        try:
            path = Path(directory)
            
            if not path.exists():
                if self.logger:
                    self.logger.warning(f"策略目录不存在: {directory}")
                return discovered_strategies
            
            # 遍历Python文件
            for py_file in path.glob("**/*.py"):
                if py_file.name.startswith("__"):
                    continue
                
                try:
                    # 动态导入文件
                    spec = importlib.util.spec_from_file_location("temp_module", py_file)
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    
                    # 查找BaseStrategy的子类
                    for name, obj in inspect.getmembers(module):
                        if (inspect.isclass(obj) and 
                            issubclass(obj, BaseStrategy) and 
                            obj != BaseStrategy):
                            
                            discovered_strategies.append(name)
                            
                            # 尝试加载策略
                            try:
                                strategy = obj(name=name)
                                self.add_strategy(strategy)
                                
                                if self.logger:
                                    self.logger.info(f"自动发现并加载策略: {name}")
                                    
                            except Exception as e:
                                if self.logger:
                                    self.logger.error(f"自动加载策略失败: {name}, 错误: {str(e)}")
                                
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"解析策略文件失败: {py_file}, 错误: {str(e)}")
                    
        except Exception as e:
            if self.logger:
                self.logger.error(f"自动发现策略失败: 错误: {str(e)}")
        
        return discovered_strategies
    
    def enable_strategy(self, name: str) -> bool:
        """
        启用策略
        
        Args:
            name: 策略名称
            
        Returns:
            bool: 是否成功
        """
        if name in self.strategies:
            self.strategies[name].enabled = True
            return True
        
        return False
    
    def disable_strategy(self, name: str) -> bool:
        """
        禁用策略
        
        Args:
            name: 策略名称
            
        Returns:
            bool: 是否成功
        """
        if name in self.strategies:
            self.strategies[name].enabled = False
            return True
        
        return False
    
    def get_strategy_info(self, name: str) -> Optional[Dict[str, Any]]:
        """
        获取策略信息
        
        Args:
            name: 策略名称
            
        Returns:
            Optional[Dict[str, Any]]: 策略信息
        """
        if name not in self.strategies:
            return None
        
        strategy = self.strategies[name]
        
        return {
            "name": strategy.name,
            "enabled": strategy.enabled,
            "params": strategy.params,
            "is_active": strategy == self.active_strategy
        }
    
    def get_all_strategies_info(self) -> Dict[str, Dict[str, Any]]:
        """获取所有策略信息"""
        info = {}
        
        for name in self.strategies:
            info[name] = self.get_strategy_info(name)
        
        return info
    
    def __str__(self) -> str:
        active_name = self.active_strategy.name if self.active_strategy else "None"
        return f"StrategyManager(strategies={len(self.strategies)}, active={active_name})"
    
    def __repr__(self) -> str:
        return self.__str__() 