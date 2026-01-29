"""
事件总线

负责事件的分发、排序和处理，支持多市场多频次
"""

import asyncio
import inspect
import logging
import threading
from queue import PriorityQueue
from typing import Dict, List, Callable, Any
from datetime import datetime

from .event_types import BaseEvent, EventTypeEnum, EventPriorityEnum

logger = logging.getLogger(__name__)


class EventBus:
    """事件总线
    
    负责事件的注册、分发和处理
    """
    
    def __init__(self):
        """初始化事件总线"""
        # 事件处理器映射：事件类型 -> 处理器列表
        self._handlers: Dict[EventTypeEnum, List[Callable]] = {}

        # 事件队列（按优先级和时间排序）
        self._event_queue = PriorityQueue()

        # 事件序列号计数器（确保同时间戳事件的稳定排序）
        self._event_counter = 0
        self._counter_lock = threading.Lock()

        # 处理统计
        self._processed_count = 0
        self._error_count = 0

        logger.info("事件总线初始化完成")
    
    def register_handler(self, event_type: EventTypeEnum, handler: Callable):
        """注册事件处理器
        
        Args:
            event_type: 事件类型
            handler: 处理器函数
        """
        if event_type not in self._handlers:
            self._handlers[event_type] = []
        
        self._handlers[event_type].append(handler)
        logger.debug(f"注册事件处理器: {event_type.value} -> {handler.__name__}")
    
    def unregister_handler(self, event_type: EventTypeEnum, handler: Callable):
        """取消注册事件处理器
        
        Args:
            event_type: 事件类型
            handler: 处理器函数
        """
        if event_type in self._handlers:
            if handler in self._handlers[event_type]:
                self._handlers[event_type].remove(handler)
                logger.debug(f"取消注册事件处理器: {event_type.value} -> {handler.__name__}")
    
    def publish_event(self, event: BaseEvent):
        """发布事件到队列

        Args:
            event: 事件对象
        """
        # 获取唯一序列号（线程安全）
        with self._counter_lock:
            event_seq = self._event_counter
            self._event_counter += 1

        # 使用优先级、时间戳和序列号作为排序键
        priority = event.priority.value
        timestamp = event.event_time.timestamp()

        # PriorityQueue使用元组进行排序：(优先级, 时间戳, 序列号, 事件)
        # 序列号确保即使 priority 和 timestamp 相同，也能稳定排序
        self._event_queue.put((priority, timestamp, event_seq, event))

        logger.debug(f"发布事件: {event.event_type.value} at {event.event_time}, seq={event_seq}")
    
    async def _process_event_async(self, event: BaseEvent):
        """异步事件处理方法
        
        Args:
            event: 事件对象
        """
        logger.debug(f"处理事件: {event.event_type.value} at {event.event_time}")
        
        # 查找该事件类型的处理器
        handlers = self._handlers.get(event.event_type, [])
        
        if not handlers:
            logger.warning(f"没有找到事件处理器: {event.event_type.value}")
            return
        
        # 依次调用所有处理器
        for handler in handlers:
            try:
                # 检查处理器是否为异步函数
                if inspect.iscoroutinefunction(handler):
                    # 异步处理器，等待完成
                    await handler(event)
                else:
                    # 同步处理器，直接调用
                    handler(event)
                
                logger.debug(f"事件处理器执行成功: {handler.__name__}")
                
            except Exception as e:
                logger.error(f"事件处理器 {handler.__name__} 执行失败: {e}")
                self._error_count += 1
                # 继续处理下一个处理器，不中断
                continue

    def _process_event(self, event: BaseEvent):
        """内部事件处理方法
        
        Args:
            event: 事件对象
        """
        logger.debug(f"处理事件: {event.event_type.value} at {event.event_time}")
        
        # 查找该事件类型的处理器
        handlers = self._handlers.get(event.event_type, [])
        
        if not handlers:
            logger.warning(f"没有找到事件处理器: {event.event_type.value}")
            return
        
        # 依次调用所有处理器
        for handler in handlers:
            try:
                # 检查处理器是否为异步函数
                if inspect.iscoroutinefunction(handler):
                    # 异步处理器，需要在事件循环中运行
                    try:
                        loop = asyncio.get_event_loop()
                        if loop.is_running():
                            # 如果事件循环正在运行，使用run_coroutine_threadsafe
                            future = asyncio.run_coroutine_threadsafe(handler(event), loop)
                            future.result()  # 等待完成
                        else:
                            # 如果事件循环未运行，直接运行
                            loop.run_until_complete(handler(event))
                    except RuntimeError:
                        # 如果没有事件循环，创建新的事件循环
                        asyncio.run(handler(event))
                else:
                    # 同步处理器，直接调用
                    handler(event)
                
                logger.debug(f"事件处理器执行成功: {handler.__name__}")
                
            except Exception as e:
                logger.error(f"事件处理器 {handler.__name__} 执行失败: {e}")
                self._error_count += 1
                # 继续处理下一个处理器，不中断
                continue

    async def process_next_event_async(self) -> bool:
        """异步处理下一个事件

        Returns:
            bool: 是否处理了事件
        """
        if self._event_queue.empty():
            return False

        try:
            # 解包四元组：(优先级, 时间戳, 序列号, 事件)
            _, _, _, event = self._event_queue.get_nowait()
            await self._process_event_async(event)
            self._processed_count += 1
            return True

        except Exception as e:
            logger.error(f"处理事件时发生错误: {e}")
            self._error_count += 1
            return False

    async def process_all_events_async(self):
        """异步处理所有待处理事件"""
        processed = 0
        
        while not self._event_queue.empty():
            if await self.process_next_event_async():
                processed += 1
        
        logger.debug(f"批量处理事件完成，共处理 {processed} 个事件")
    
    def process_next_event(self) -> bool:
        """处理下一个事件（同步版本）

        Returns:
            bool: 是否处理了事件
        """
        if self._event_queue.empty():
            return False

        try:
            # 解包四元组：(优先级, 时间戳, 序列号, 事件)
            _, _, _, event = self._event_queue.get_nowait()
            self._process_event(event)
            self._processed_count += 1
            return True

        except Exception as e:
            logger.error(f"处理事件时发生错误: {e}")
            self._error_count += 1
            return False
    
    def process_all_events(self):
        """处理所有待处理事件（同步版本）"""
        processed = 0
        
        while not self._event_queue.empty():
            if self.process_next_event():
                processed += 1
        
        logger.debug(f"批量处理事件完成，共处理 {processed} 个事件")
    
    def clear_queue(self):
        """清空事件队列"""
        while not self._event_queue.empty():
            self._event_queue.get_nowait()
        
        logger.debug("事件队列已清空")
    
    def get_queue_size(self) -> int:
        """获取队列大小
        
        Returns:
            int: 队列中等待处理的事件数量
        """
        return self._event_queue.qsize()
    
    def get_statistics(self) -> Dict[str, int]:
        """获取处理统计信息
        
        Returns:
            Dict[str, int]: 统计信息
        """
        return {
            'processed_count': self._processed_count,
            'error_count': self._error_count,
            'queue_size': self.get_queue_size(),
            'handlers_count': sum(len(handlers) for handlers in self._handlers.values())
        }
    
    def reset_statistics(self):
        """重置统计信息"""
        self._processed_count = 0
        self._error_count = 0
        logger.debug("事件总线统计信息已重置")
    
    def stop(self):
        """停止事件总线"""
        self.clear_queue()
        self._handlers.clear()
        self.reset_statistics()
        logger.info("事件总线已停止") 