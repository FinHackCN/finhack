"""
事件钩子管理器
支持动态加载和执行事件处理函数
"""

import os
import sys
import importlib.util
import inspect
from typing import Dict, Any, Optional, List, Callable
from pathlib import Path

from ..base_event import BaseEvent, EventType


class EventHooksManager:
    """事件钩子管理器"""
    
    def __init__(self):
        self.loaded_hooks: Dict[str, dict] = {}  # 已加载的hooks模块
        self.hook_cache: Dict[str, dict] = {}    # hooks函数缓存
        self.user_event_bindings: Dict[str, callable] = {}  # 用户事件绑定
        self.context = None
        self.logger = None
        
    def initialize(self, context):
        """初始化hooks管理器"""
        self.context = context
        self.logger = context.logger if context else None
        
        # 清空缓存
        self.loaded_hooks.clear()
        self.hook_cache.clear()
        
        if self.logger:
            self.logger.info("事件钩子管理器初始化完成")
    
    def process_event(self, event_name: str, event: BaseEvent) -> bool:
        """
        处理事件，按优先级顺序调用相应的hooks
        
        Args:
            event_name: 事件名称
            event: 事件对象
            
        Returns:
            bool: 是否处理成功
        """
        try:
            if self.logger:
                self.logger.debug(f"开始处理事件: {event_name}")
            
            # 获取系统组件列表
            system_components = ['data_center', 'trade_center']
            
            # 构建hooks文件路径列表
            hook_paths = self._get_hook_paths(system_components)
            
            # 按顺序处理每个hooks文件
            for hook_path, hook_type in hook_paths:
                if os.path.exists(hook_path):
                    self._execute_hook(hook_path, hook_type, event_name, event)
            
            # 处理用户自定义事件绑定
            if event_name in self.user_event_bindings:
                user_handler = self.user_event_bindings[event_name]
                if callable(user_handler):
                    if self.logger:
                        self.logger.debug(f"调用用户绑定事件处理函数: {event_name}")
                    user_handler(self.context)
            
            # 最后检查策略是否有对应的处理函数
            if hasattr(self.context, 'strategy_manager') and self.context.strategy_manager:
                strategy = self.context.strategy_manager.get_active_strategy()
                if strategy:
                    strategy_handler_name = f"on_{event_name}"
                    if hasattr(strategy, strategy_handler_name):
                        handler = getattr(strategy, strategy_handler_name)
                        if callable(handler):
                            if self.logger:
                                self.logger.debug(f"调用策略事件处理函数: {strategy_handler_name}")
                            handler(self.context, event)
            
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"处理事件hooks失败: {event_name}, 错误: {str(e)}")
            return False
    
    def _get_hook_paths(self, system_components: List[str]) -> List[tuple]:
        """
        获取hooks文件路径列表
        
        Args:
            system_components: 系统组件列表
            
        Returns:
            List[tuple]: (路径, 类型) 元组列表
        """
        hook_paths = []
        
        # 获取路径常量
        framework_dir = self._get_framework_dir()
        base_dir = self._get_base_dir()
        
        # 1. 框架默认hooks
        framework_default_path = os.path.join(
            framework_dir, 'trader', 'backtest', 'events', 'hooks', 'default_event.py'
        )
        hook_paths.append((framework_default_path, 'framework_default'))
        
        # 2. 项目默认hooks
        project_default_path = os.path.join(
            base_dir, 'trader', 'backtest', 'events', 'hooks', 'default_event.py'
        )
        hook_paths.append((project_default_path, 'project_default'))
        
        # 3. 系统组件hooks
        for component in system_components:
            component_path = os.path.join(
                base_dir, 'trader', 'backtest', 'events', 'hooks', f'{component}_event.py'
            )
            hook_paths.append((component_path, f'component_{component}'))
        
        return hook_paths
    
    def _get_framework_dir(self) -> str:
        """获取框架目录"""
        try:
            # 尝试从运行时常量获取
            if hasattr(self.context, 'framework_dir'):
                return self.context.framework_dir
            
            # 尝试从全局变量获取
            import sys
            if 'runtime.constant' in sys.modules:
                import runtime.constant as constant
                if hasattr(constant, 'FRAMEWORK_DIR'):
                    return constant.FRAMEWORK_DIR
            
            # 回退到相对路径
            current_dir = os.path.dirname(os.path.abspath(__file__))
            return os.path.join(current_dir, '../../../../../../../finhack')
            
        except Exception:
            # 最后的回退
            return os.path.dirname(os.path.abspath(__file__))
    
    def _get_base_dir(self) -> str:
        """获取项目基础目录"""
        try:
            # 尝试从运行时常量获取
            if hasattr(self.context, 'base_dir'):
                return self.context.base_dir
            
            # 尝试从全局变量获取
            import sys
            if 'runtime.constant' in sys.modules:
                import runtime.constant as constant
                if hasattr(constant, 'BASE_DIR'):
                    return constant.BASE_DIR
            
            # 回退到当前工作目录
            return os.getcwd()
            
        except Exception:
            return os.getcwd()
    
    def _execute_hook(self, hook_path: str, hook_type: str, event_name: str, event: BaseEvent):
        """
        执行单个hook文件中的事件处理函数
        
        Args:
            hook_path: hooks文件路径
            hook_type: hook类型
            event_name: 事件名称
            event: 事件对象
        """
        try:
            # 检查缓存
            cache_key = f"{hook_path}:{event_name}"
            if cache_key in self.hook_cache:
                cached_handler = self.hook_cache[cache_key]
                if cached_handler:
                    if self.logger:
                        self.logger.debug(f"调用缓存的hook函数: {hook_type}.{event_name}")
                    cached_handler(self.context)
                return
            
            # 加载hooks模块
            hook_module = self._load_hook_module(hook_path)
            
            if hook_module:
                # 查找事件处理函数
                handler_name = event_name
                if hasattr(hook_module, handler_name):
                    handler = getattr(hook_module, handler_name)
                    if callable(handler):
                        # 缓存处理函数
                        self.hook_cache[cache_key] = handler
                        
                        if self.logger:
                            self.logger.debug(f"调用hook函数: {hook_type}.{handler_name}")
                        
                        # 调用处理函数
                        handler(self.context)
                    else:
                        # 缓存None表示函数不存在
                        self.hook_cache[cache_key] = None
                else:
                    # 缓存None表示函数不存在
                    self.hook_cache[cache_key] = None
                    if self.logger:
                        self.logger.debug(f"Hook函数不存在: {hook_type}.{handler_name}")
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"执行hook失败: {hook_path}, 事件: {event_name}, 错误: {str(e)}")
    
    def _load_hook_module(self, hook_path: str):
        """
        加载hooks模块
        
        Args:
            hook_path: hooks文件路径
            
        Returns:
            module: 加载的模块对象
        """
        try:
            # 检查模块是否已加载
            if hook_path in self.loaded_hooks:
                return self.loaded_hooks[hook_path]
            
            # 检查文件是否存在
            if not os.path.exists(hook_path):
                return None
            
            # 生成模块名称
            module_name = f"hook_{hash(hook_path)}"
            
            # 加载模块
            spec = importlib.util.spec_from_file_location(module_name, hook_path)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                
                # 缓存模块
                self.loaded_hooks[hook_path] = module
                
                if self.logger:
                    self.logger.debug(f"成功加载hooks模块: {hook_path}")
                
                return module
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"加载hooks模块失败: {hook_path}, 错误: {str(e)}")
            
        return None
    
    def reload_hooks(self):
        """重新加载所有hooks"""
        self.loaded_hooks.clear()
        self.hook_cache.clear()
        
        if self.logger:
            self.logger.info("已重新加载所有hooks")
    
    def bind_user_event(self, event_name: str, handler: callable):
        """
        绑定用户自定义事件处理函数
        
        Args:
            event_name: 事件名称
            handler: 事件处理函数
        """
        self.user_event_bindings[event_name] = handler
        
        if self.logger:
            self.logger.info(f"绑定用户事件: {event_name}")
    
    def unbind_user_event(self, event_name: str):
        """
        解绑用户自定义事件处理函数
        
        Args:
            event_name: 事件名称
        """
        if event_name in self.user_event_bindings:
            del self.user_event_bindings[event_name]
            
            if self.logger:
                self.logger.info(f"解绑用户事件: {event_name}")
    
    def get_user_event_bindings(self) -> Dict[str, callable]:
        """获取用户事件绑定"""
        return self.user_event_bindings.copy()
    
    def get_hook_stats(self) -> Dict[str, Any]:
        """获取hooks统计信息"""
        return {
            'loaded_hooks_count': len(self.loaded_hooks),
            'cached_functions_count': len(self.hook_cache),
            'user_event_bindings_count': len(self.user_event_bindings),
            'loaded_hooks': list(self.loaded_hooks.keys()),
            'cached_functions': list(self.hook_cache.keys()),
            'user_event_bindings': list(self.user_event_bindings.keys())
        } 