"""
事件中心

负责事件生成、调度和分发，支持多市场多频次
"""

import logging
from datetime import datetime, date, time, timedelta
from typing import Dict, List, Any, Optional
import importlib

from ..events.event_bus import EventBus
from ..events.event_types import BaseEvent, MarketEvent, TimeEvent, TradeEvent, EventTypeEnum

logger = logging.getLogger(__name__)


class EventCenter:
    """事件中心
    
    负责事件的生成、调度和分发
    """
    
    def __init__(self, config: Dict[str, Any]):
        """初始化事件中心
        
        Args:
            config: 配置参数
        """
        self.config = config
        self.context = None
        
        # 初始化事件总线
        self.event_bus = EventBus()
        
        # 市场适配器映射
        self.market_adapters = {}
        
        # 其他组件引用
        self.trade_center = None
        self.data_center = None
        self.strategy_executor = None
        
        # 当前处理的日期和频率
        self.current_date = None
        self.current_frequency = '1d'
        
        # 预加载所有市场适配器
        self._preload_market_adapters()
        
        logger.info("事件中心初始化完成")
    
    def _preload_market_adapters(self):
        """预加载所有市场适配器"""
        try:
            from ..markets import MARKET_ADAPTERS
            from ..markets.base_market import BaseMarket
            
            for market_name, adapter_class in MARKET_ADAPTERS.items():
                try:
                    # 只跳过BaseMarket抽象类
                    if adapter_class is BaseMarket:
                        logger.warning(f"市场适配器 {market_name} 是抽象类，跳过实例化")
                        continue
                    
                    adapter = adapter_class()
                    self.market_adapters[market_name] = adapter
                    logger.info(f"成功加载市场适配器: {market_name}")
                    
                except Exception as e:
                    logger.error(f"加载市场适配器失败 {market_name}: {e}")
                    continue
            
            logger.info(f"预加载市场适配器完成，共加载 {len(self.market_adapters)} 个适配器")
            
        except Exception as e:
            logger.error(f"预加载市场适配器失败: {e}")
            raise
    
    def set_context(self, context: Dict[str, Any]):
        """设置上下文
        
        Args:
            context: 回测上下文
        """
        self.context = context
        
        # 根据配置加载市场适配器
        market_name = context.get('settings', {}).get('market', 'cn_stock')
        self._load_market_adapter(market_name)
        
        # 注册事件处理器
        self._register_event_handlers()
        
        logger.debug("事件中心已设置上下文")
    
    def set_trade_center(self, trade_center):
        """设置交易中心"""
        self.trade_center = trade_center
    
    def set_data_center(self, data_center):
        """设置数据中心"""
        self.data_center = data_center
    
    def set_strategy_executor(self, strategy_executor):
        """设置策略执行器"""
        self.strategy_executor = strategy_executor
    
    def set_scheduled_tasks(self, scheduled_tasks: List[Dict[str, Any]]):
        """设置定时任务列表
        
        Args:
            scheduled_tasks: 定时任务列表
        """
        if not self.context:
            self.context = {}
        self.context['scheduled_tasks'] = scheduled_tasks
        logger.debug(f"设置定时任务: {len(scheduled_tasks)} 个")
    
    def _load_market_adapter(self, market_name: str):
        """加载市场适配器
        
        Args:
            market_name: 市场名称
        """
        try:
            from ..markets import MARKET_ADAPTERS
            
            if market_name in MARKET_ADAPTERS:
                adapter_class = MARKET_ADAPTERS[market_name]
                
                # 跳过抽象类的实例化
                if 'Base' in adapter_class.__name__ or 'MarketAdapter' in adapter_class.__name__:
                    logger.warning(f"市场适配器 {market_name} 是抽象类，跳过实例化")
                    return
                
                self.market_adapters[market_name] = adapter_class()
                logger.info(f"成功加载市场适配器: {market_name}")
            else:
                logger.warning(f"未支持的市场类型: {market_name}，使用cn_stock适配器")
                from ..markets.cn_stock.cn_stock_adapter import CnStockMarketAdapter
                self.market_adapters[market_name] = CnStockMarketAdapter()
            
        except Exception as e:
            logger.error(f"加载市场适配器失败 {market_name}: {e}")
            raise
    
    def _register_event_handlers(self):
        """注册事件处理器"""
        # 注册所有市场事件的默认处理器
        market_events = [
            EventTypeEnum.DAY_START,
            EventTypeEnum.BEFORE_MARKET,
            EventTypeEnum.PRE_OPENING_START,
            EventTypeEnum.PRE_OPENING_END,
            EventTypeEnum.MATCHING_START,
            EventTypeEnum.OPENING_PRICE_DETERMINED,
            EventTypeEnum.MARKET_START,
            EventTypeEnum.MORNING_END,
            EventTypeEnum.AFTERNOON_START,
            EventTypeEnum.CLOSING_START,
            EventTypeEnum.CLOSING_END,
            EventTypeEnum.CLOSING_PRICE_DETERMINED,
            EventTypeEnum.MARKET_END,
            EventTypeEnum.DAILY_BAR_CLOSED,
            EventTypeEnum.AFTER_MARKET,
            EventTypeEnum.DAY_END,
        ]
        
        for event_type in market_events:
            self.event_bus.register_handler(event_type, self._handle_market_event)
        
        # 注册策略相关事件处理器
        self.event_bus.register_handler(EventTypeEnum.ON_TIME, self._handle_on_time)
        
        # 注册交易相关事件处理器
        self.event_bus.register_handler(EventTypeEnum.ORDER_SUBMISSION, self._handle_trade_event)
        self.event_bus.register_handler(EventTypeEnum.ORDER_CANCELLATION, self._handle_trade_event)
        self.event_bus.register_handler(EventTypeEnum.TRY_MATCH, self._handle_try_match)
        
        logger.debug("事件处理器注册完成")
    
    def generate_daily_events(self, trade_date: date) -> List[BaseEvent]:
        """生成指定日期的事件列表
        
        Args:
            trade_date: 交易日期
            
        Returns:
            List[BaseEvent]: 事件列表
        """
        self.current_date = trade_date
        
        if not self.context:
            logger.warning("context未设置，无法生成事件")
            return []
        
        market_name = self.context.get('settings', {}).get('market', 'cn_stock')
        frequency = self.context.get('settings', {}).get('freq', '1d')
        self.current_frequency = frequency
        
        events = []
        
        try:
            # 1. 生成静态市场事件
            market_events = self._generate_market_events(trade_date, market_name, frequency)
            events.extend(market_events)
            
            # 2. 生成动态定时任务事件
            time_events = self._generate_time_events(trade_date, frequency)
            events.extend(time_events)
            
            # 3. 生成公司行为事件
            corporate_action_events = self._generate_corporate_action_events(trade_date)
            events.extend(corporate_action_events)
            
            # 4. 按时间排序所有事件
            events.sort(key=lambda x: (x.event_time, x.priority.value))
            
            # 添加调试日志
            try_match_events = [e for e in events if e.event_type == EventTypeEnum.TRY_MATCH]
            logger.debug(f"生成 {trade_date} 的事件: {len(events)} 个，其中撮合事件: {len(try_match_events)} 个")
            
            logger.debug(f"生成 {trade_date} 的事件: {len(events)} 个")
            
        except Exception as e:
            logger.error(f"生成事件失败 {trade_date}: {e}")
            raise
        
        return events
    
    def _generate_market_events(self, trade_date: date, market_name: str, frequency: str) -> List[BaseEvent]:
        """生成市场事件
        
        Args:
            trade_date: 交易日期
            market_name: 市场名称
            frequency: 数据频率
            
        Returns:
            List[BaseEvent]: 市场事件列表
        """
        market_adapter = self.market_adapters.get(market_name)
        if not market_adapter:
            logger.warning(f"未找到市场适配器: {market_name}")
            return []
        
        # 转换date为datetime
        if isinstance(trade_date, date):
            trade_datetime = datetime.combine(trade_date, datetime.min.time())
        else:
            trade_datetime = trade_date
            
        return market_adapter.generate_daily_events(trade_datetime, frequency)
    
    def _generate_time_events(self, trade_date: date, frequency: str) -> List[BaseEvent]:
        """生成定时任务事件
        
        Args:
            trade_date: 交易日期
            frequency: 数据频率
            
        Returns:
            List[BaseEvent]: 定时任务事件列表
        """
        if not self.context:
            return []
        
        scheduled_tasks = self.context.get('scheduled_tasks', [])
        time_events = []
        
        for task in scheduled_tasks:
            task_times = self._calculate_task_times(task, trade_date, frequency)
            
            for task_time in task_times:
                event = TimeEvent(
                    event_type=EventTypeEnum.ON_TIME,
                    event_time=task_time,
                    market=self.context.get('settings', {}).get('market', 'cn_stock'),
                    frequency=frequency,
                    function_name=task['function_name'],
                    task_id=task['task_id']
                )
                time_events.append(event)
        
        return time_events
    
    def _calculate_task_times(self, task: Dict[str, Any], trade_date: date, frequency: str) -> List[datetime]:
        """计算任务执行时间
        
        Args:
            task: 任务配置
            trade_date: 交易日期
            frequency: 数据频率
            
        Returns:
            List[datetime]: 执行时间列表
        """
        rule = task.get('scheduling_rule', {})
        rule_type = rule.get('type')
        times = []
        
        if rule_type == 'daily':
            # 每日任务
            task_time_str = rule.get('time', '14:50:00')
            # 支持 HH:MM 和 HH:MM:SS 两种格式
            try:
                task_time = datetime.strptime(task_time_str, '%H:%M:%S').time()
            except ValueError:
                task_time = datetime.strptime(task_time_str, '%H:%M').time()
            times.append(datetime.combine(trade_date, task_time))
            
        elif rule_type == 'weekly':
            # 每周任务
            weekday = rule.get('weekday', 1)  # 1=周一
            task_time_str = rule.get('time', '14:50:00')
            # 支持 HH:MM 和 HH:MM:SS 两种格式
            try:
                task_time = datetime.strptime(task_time_str, '%H:%M:%S').time()
            except ValueError:
                task_time = datetime.strptime(task_time_str, '%H:%M').time()
            
            # 检查当前日期是否为指定星期几（Python中weekday()返回0-6，0=周一）
            if trade_date.weekday() == (weekday - 1):
                times.append(datetime.combine(trade_date, task_time))
                
        elif rule_type == 'interval':
            # 间隔任务
            frequency_str = rule.get('frequency', '15m')
            reference_time_str = rule.get('reference_time', '09:30:00')
            # 支持 HH:MM 和 HH:MM:SS 两种格式
            try:
                reference_time = datetime.strptime(reference_time_str, '%H:%M:%S').time()
            except ValueError:
                reference_time = datetime.strptime(reference_time_str, '%H:%M').time()
            
            # 解析频率
            if frequency_str.endswith('m'):
                interval_minutes = int(frequency_str[:-1])
                times = self._generate_interval_times(trade_date, reference_time, interval_minutes, 'minute')
            elif frequency_str.endswith('h'):
                interval_hours = int(frequency_str[:-1])
                times = self._generate_interval_times(trade_date, reference_time, interval_hours, 'hour')
        
        return times
    
    def _generate_interval_times(self, trade_date: date, reference_time: time, 
                                interval: int, unit: str) -> List[datetime]:
        """生成间隔执行时间"""
        times = []
        
        # 获取交易时间段
        market_name = self.context.get('settings', {}).get('market', 'cn_stock')
        market_adapter = self.market_adapters.get(market_name)
        
        if not market_adapter:
            return times
        
        trading_sessions = market_adapter.get_trading_sessions(trade_date, self.current_frequency)
        
        for session in trading_sessions:
            # session是tuple (start_time, end_time)
            start_time = session[0]
            end_time = session[1]

            # 如果是time对象，需要转换为datetime对象
            if isinstance(start_time, time):
                start_dt = datetime.combine(trade_date, start_time)
            else:
                start_dt = start_time

            if isinstance(end_time, time):
                end_dt = datetime.combine(trade_date, end_time)
            else:
                end_dt = end_time

            # 从参考时间开始，按间隔生成时间点
            current_time = datetime.combine(trade_date, reference_time)

            # 调整到交易时间段内
            if current_time < start_dt:
                current_time = start_dt

            while current_time <= end_dt:
                if start_dt <= current_time <= end_dt:
                    times.append(current_time)

                # 计算下一个时间点
                if unit == 'minute':
                    current_time += timedelta(minutes=interval)
                elif unit == 'hour':
                    current_time += timedelta(hours=interval)
        
        return times
    
    def _generate_corporate_action_events(self, trade_date):
        """生成公司行为事件"""
        try:
            logger.info(f"[EventCenter] _generate_corporate_action_events调用: trade_date={trade_date}")
            # 从DataCenter获取公司行为数据
            if not self.context or not hasattr(self.context, 'data_center'):
                logger.warning("[EventCenter] context或data_center不存在")
                return []

            data_center = self.context.data_center
            market = self.context.get('settings', {}).get('market', 'cn_stock')
            logger.info(f"[EventCenter] 准备调用data_center.get_corporate_actions: date={trade_date}, market={market}")

            # 获取当前日期的公司行为事件
            corporate_actions = data_center.get_corporate_actions(
                date=trade_date,
                market=market
            )
            logger.info(f"[EventCenter] data_center.get_corporate_actions返回: {len(corporate_actions)}条记录")

            events = []
            for action in corporate_actions:
                # 创建公司行为事件
                event = type('CorporateActionEvent', (), {
                    'event_type': type('EventType', (), {'value': 'CORPORATE_ACTION'})(),
                    'event_time': datetime.combine(trade_date, datetime.min.time()),
                    'symbol': action.get('symbol', ''),
                    'action_type': action.get('action_type', ''),
                    'dividend_per_share': action.get('dividend_per_share', 0),
                    'tax_rate': action.get('tax_rate', 0.10),
                    'bonus_ratio': action.get('bonus_ratio', 0),
                    'split_ratio': action.get('split_ratio', 1),
                    'rights_ratio': action.get('rights_ratio', 0),
                    'rights_price': action.get('rights_price', 0)
                })()

                events.append(event)

            if events:
                logger.info(f"[EventCenter] 生成了{len(events)}个公司行为事件")
            else:
                logger.debug(f"[EventCenter] 未生成公司行为事件")

            return events

        except Exception as e:
            logger.error(f"生成公司行为事件失败: {e}")
            return []
    
    def process_event(self, event: BaseEvent):
        """处理单个事件
        
        Args:
            event: 事件对象
        """
        try:
            # 更新当前时间到context
            if self.context:
                self.context['current_dt'] = event.event_time
            
            logger.debug(f"处理事件: {event.event_type.value} at {event.event_time}")
            
            # 发布事件到事件总线
            self.event_bus.publish_event(event)
            
            # 处理事件
            self.event_bus.process_next_event()
            
        except Exception as e:
            logger.error(f"处理事件失败 {event.event_type.value}: {e}")
            raise
    
    def _handle_strategy_event(self, event: BaseEvent):
        """处理策略相关事件"""
        if not self.strategy_executor:
            return
        
        try:
            if event.event_type == EventTypeEnum.ON_TIME:
                # 定时任务事件
                if isinstance(event, TimeEvent):
                    self.strategy_executor.execute_scheduled_function(event.function_name, event.task_id)
            else:
                # 市场事件
                self.strategy_executor.execute_market_event(event)
                
        except Exception as e:
            logger.error(f"处理策略事件失败 {event.event_type.value}: {e}")
    
    def _handle_trade_event(self, event: BaseEvent):
        """处理交易相关事件"""
        if not self.trade_center:
            return
        
        try:
            if event.event_type == EventTypeEnum.ORDER_SUBMISSION:
                self.trade_center.handle_order_submission(event)
            elif event.event_type == EventTypeEnum.ORDER_CANCELLATION:
                self.trade_center.handle_order_cancellation(event)
            elif event.event_type == EventTypeEnum.TRY_MATCH:
                self.trade_center.try_match_orders(event)
                
        except Exception as e:
            logger.error(f"处理交易事件失败 {event.event_type.value}: {e}")
    
    def _handle_before_market(self, event: BaseEvent):
        """处理盘前事件"""
        if not self.trade_center:
            return
        
        try:
            # 处理除权除息等盘前事件
            self.trade_center.handle_before_market(event)
            
        except Exception as e:
            logger.error(f"处理盘前事件失败: {e}")
    
    def _handle_after_market(self, event: BaseEvent):
        """处理盘后事件"""
        if not self.trade_center:
            return
        
        try:
            # 处理分红等盘后事件
            self.trade_center.handle_after_market(event)
            
        except Exception as e:
            logger.error(f"处理盘后事件失败: {e}")
    
    def _handle_market_end(self, event: BaseEvent):
        """处理市场收盘事件"""
        if not self.trade_center:
            return
        
        try:
            # 取消未成交订单
            self.trade_center.cancel_pending_orders(event)
            
            # 执行日终清算
            self.trade_center.daily_settlement(event)
            
        except Exception as e:
            logger.error(f"处理收盘事件失败: {e}")
    
    def publish_order_event(self, event_type: EventTypeEnum, order_data: Dict[str, Any]):
        """发布订单事件
        
        Args:
            event_type: 事件类型
            order_data: 订单数据
        """
        current_dt = self.context.get('current_dt') if self.context else datetime.now()
        market = self.context.get('settings', {}).get('market', 'cn_stock') if self.context else 'cn_stock'
        
        trade_event = TradeEvent(
            event_type=event_type,
            event_time=current_dt,
            market=market,
            frequency=self.current_frequency,
            order_id=order_data.get('order_id'),
            symbol=order_data.get('symbol')
        )
        trade_event.data = order_data
        
        self.event_bus.publish_event(trade_event)
        self.event_bus.process_next_event()
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取事件中心统计信息"""
        stats = self.event_bus.get_statistics()
        stats['current_date'] = self.current_date
        stats['current_frequency'] = self.current_frequency
        stats['loaded_markets'] = list(self.market_adapters.keys())
        return stats
    
    def stop(self):
        """停止事件中心"""
        self.event_bus.stop()
        self.market_adapters.clear()
        logger.info("事件中心已停止")
    
    def _handle_market_event(self, event: BaseEvent):
        """通用市场事件处理器
        
        Args:
            event: 市场事件
        """
        try:
            logger.debug(f"处理市场事件: {event.event_type.value} at {event.event_time}")
            
            # 根据不同的事件类型执行相应的处理
            if event.event_type == EventTypeEnum.BEFORE_MARKET:
                self._handle_before_market(event)
            elif event.event_type == EventTypeEnum.MARKET_END:
                self._handle_market_end(event)
            elif event.event_type == EventTypeEnum.AFTER_MARKET:
                self._handle_after_market(event)
            elif event.event_type == EventTypeEnum.TRY_MATCH:
                self._handle_try_match(event)
            else:
                # 其他市场事件的默认处理（主要是记录日志）
                logger.debug(f"市场事件 {event.event_type.value} 已处理")
                
        except Exception as e:
            logger.error(f"处理市场事件失败 {event.event_type.value}: {e}")
    
    async def _handle_on_time(self, event: BaseEvent):
        """处理定时任务事件
        
        Args:
            event: 定时任务事件
        """
        try:
            if not hasattr(event, 'function_name'):
                logger.warning("ON_TIME事件缺少function_name属性")
                return
            
            function_name = event.function_name
            logger.debug(f"执行定时任务: {function_name} at {event.event_time}")
            
            # 通过策略执行器执行用户定义的函数
            if self.strategy_executor:
                await self.strategy_executor.execute_function(function_name, self.context)
            else:
                logger.warning("策略执行器未设置，无法执行定时任务")
                
        except Exception as e:
            logger.error(f"执行定时任务失败 {event.function_name}: {e}")
    
    def _handle_try_match(self, event: BaseEvent):
        """处理订单撮合事件
        
        Args:
            event: 撮合事件
        """
        try:
            logger.debug(f"执行订单撮合: {event.event_time}")
            
            # 通过交易中心执行订单撮合
            if self.trade_center:
                self.trade_center.try_match_orders(event)
            else:
                logger.warning("交易中心未设置，无法执行订单撮合")
                
        except Exception as e:
            logger.error(f"订单撮合失败: {e}") 