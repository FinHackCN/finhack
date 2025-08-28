"""
定时任务调度器

负责管理用户定义的定时任务，支持多种调度规则
"""

import logging
from datetime import datetime, time, timedelta
from typing import Dict, List, Any, Callable, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class ScheduledTask:
    """定时任务定义"""
    task_id: str
    function_name: str
    scheduling_rule: Dict[str, Any]
    next_run_time: Optional[datetime] = None
    last_run_time: Optional[datetime] = None
    enabled: bool = True


class Scheduler:
    """定时任务调度器
    
    管理和调度用户定义的定时任务
    """
    
    def __init__(self, config: Dict[str, Any]):
        """初始化调度器
        
        Args:
            config: 配置参数
        """
        self.config = config
        self.tasks: Dict[str, ScheduledTask] = {}
        
        logger.info("定时任务调度器初始化完成")
    
    def add_task(self, task_config: Dict[str, Any]):
        """添加定时任务
        
        Args:
            task_config: 任务配置，包含task_id、function_name、scheduling_rule等
        """
        task_id = task_config['task_id']
        function_name = task_config['function_name']
        scheduling_rule = task_config['scheduling_rule']
        
        task = ScheduledTask(
            task_id=task_id,
            function_name=function_name,
            scheduling_rule=scheduling_rule
        )
        
        self.tasks[task_id] = task
        logger.debug(f"添加定时任务: {task_id} -> {function_name}")
    
    def remove_task(self, task_id: str):
        """移除定时任务
        
        Args:
            task_id: 任务ID
        """
        if task_id in self.tasks:
            del self.tasks[task_id]
            logger.debug(f"移除定时任务: {task_id}")
    
    def enable_task(self, task_id: str):
        """启用任务
        
        Args:
            task_id: 任务ID
        """
        if task_id in self.tasks:
            self.tasks[task_id].enabled = True
            logger.debug(f"启用任务: {task_id}")
    
    def disable_task(self, task_id: str):
        """禁用任务
        
        Args:
            task_id: 任务ID
        """
        if task_id in self.tasks:
            self.tasks[task_id].enabled = False
            logger.debug(f"禁用任务: {task_id}")
    
    def get_task(self, task_id: str) -> Optional[ScheduledTask]:
        """获取任务
        
        Args:
            task_id: 任务ID
            
        Returns:
            Optional[ScheduledTask]: 任务对象
        """
        return self.tasks.get(task_id)
    
    def list_tasks(self) -> List[ScheduledTask]:
        """列出所有任务
        
        Returns:
            List[ScheduledTask]: 任务列表
        """
        return list(self.tasks.values())
    
    def clear_tasks(self):
        """清空所有任务"""
        self.tasks.clear()
        logger.debug("已清空所有定时任务")
    
    def get_tasks_for_date(self, trade_date: datetime) -> List[ScheduledTask]:
        """获取指定日期需要执行的任务
        
        Args:
            trade_date: 交易日期
            
        Returns:
            List[ScheduledTask]: 需要执行的任务列表
        """
        applicable_tasks = []
        
        for task in self.tasks.values():
            if not task.enabled:
                continue
            
            if self._should_run_on_date(task, trade_date):
                applicable_tasks.append(task)
        
        return applicable_tasks
    
    def _should_run_on_date(self, task: ScheduledTask, trade_date: datetime) -> bool:
        """判断任务是否应该在指定日期执行
        
        Args:
            task: 任务对象
            trade_date: 交易日期
            
        Returns:
            bool: 是否应该执行
        """
        rule = task.scheduling_rule
        rule_type = rule.get('type')
        
        if rule_type == 'daily':
            # 每日任务：每个交易日都执行
            return True
            
        elif rule_type == 'weekly':
            # 每周任务：检查星期几
            weekday = rule.get('weekday', 1)  # 1=周一
            return trade_date.weekday() == (weekday - 1)  # Python中0=周一
            
        elif rule_type == 'monthly':
            # 每月任务：检查日期
            day = rule.get('day', 1)
            return trade_date.day == day
            
        elif rule_type == 'interval':
            # 间隔任务：在交易时间内执行
            return True
            
        else:
            logger.warning(f"未知的调度规则类型: {rule_type}")
            return False
    
    def calculate_next_run_times(self, task: ScheduledTask, current_date: datetime) -> List[datetime]:
        """计算任务的下次执行时间
        
        Args:
            task: 任务对象
            current_date: 当前日期
            
        Returns:
            List[datetime]: 执行时间列表
        """
        rule = task.scheduling_rule
        rule_type = rule.get('type')
        times = []
        
        if rule_type == 'daily':
            # 每日任务
            time_str = rule.get('time', '14:50:00')
            task_time = datetime.strptime(time_str, '%H:%M:%S').time()
            next_time = datetime.combine(current_date.date(), task_time)
            times.append(next_time)
            
        elif rule_type == 'weekly':
            # 每周任务
            weekday = rule.get('weekday', 1)
            time_str = rule.get('time', '14:50:00')
            task_time = datetime.strptime(time_str, '%H:%M:%S').time()
            
            # 找到下一个指定星期几
            days_ahead = (weekday - 1) - current_date.weekday()
            if days_ahead <= 0:  # 目标日期已过或就是今天
                days_ahead += 7
            
            target_date = current_date + timedelta(days=days_ahead)
            next_time = datetime.combine(target_date.date(), task_time)
            times.append(next_time)
            
        elif rule_type == 'interval':
            # 间隔任务：生成当日的所有执行时间
            frequency = rule.get('frequency', '15m')
            reference_time_str = rule.get('reference_time', '09:30:00')
            reference_time = datetime.strptime(reference_time_str, '%H:%M:%S').time()
            
            # 解析频率
            if frequency.endswith('m'):
                interval_minutes = int(frequency[:-1])
                times = self._generate_interval_times_for_day(
                    current_date, reference_time, interval_minutes, 'minute'
                )
            elif frequency.endswith('h'):
                interval_hours = int(frequency[:-1])
                times = self._generate_interval_times_for_day(
                    current_date, reference_time, interval_hours, 'hour'
                )
        
        return times
    
    def _generate_interval_times_for_day(self, trade_date: datetime, reference_time: time,
                                        interval: int, unit: str) -> List[datetime]:
        """为指定日期生成间隔执行时间
        
        Args:
            trade_date: 交易日期
            reference_time: 参考时间
            interval: 间隔数值
            unit: 时间单位 ('minute' or 'hour')
            
        Returns:
            List[datetime]: 执行时间列表
        """
        times = []
        
        # 默认交易时间段（这里简化处理，实际应该从市场适配器获取）
        trading_sessions = [
            (datetime.combine(trade_date.date(), datetime.strptime('09:30:00', '%H:%M:%S').time()),
             datetime.combine(trade_date.date(), datetime.strptime('11:30:00', '%H:%M:%S').time())),
            (datetime.combine(trade_date.date(), datetime.strptime('13:00:00', '%H:%M:%S').time()),
             datetime.combine(trade_date.date(), datetime.strptime('15:00:00', '%H:%M:%S').time()))
        ]
        
        for start_time, end_time in trading_sessions:
            # 从参考时间开始，按间隔生成时间点
            current_time = datetime.combine(trade_date.date(), reference_time)
            
            # 调整到交易时间段内
            if current_time < start_time:
                current_time = start_time
            
            while current_time <= end_time:
                if start_time <= current_time <= end_time:
                    times.append(current_time)
                
                # 计算下一个时间点
                if unit == 'minute':
                    current_time += timedelta(minutes=interval)
                elif unit == 'hour':
                    current_time += timedelta(hours=interval)
                else:
                    break
        
        return times
    
    def update_task_execution(self, task_id: str, execution_time: datetime):
        """更新任务执行记录
        
        Args:
            task_id: 任务ID
            execution_time: 执行时间
        """
        if task_id in self.tasks:
            task = self.tasks[task_id]
            task.last_run_time = execution_time
            
            # 计算下次执行时间
            next_times = self.calculate_next_run_times(task, execution_time)
            if next_times:
                task.next_run_time = min(next_times)
            
            logger.debug(f"更新任务执行记录: {task_id} at {execution_time}")
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取调度器统计信息
        
        Returns:
            Dict[str, Any]: 统计信息
        """
        total_tasks = len(self.tasks)
        enabled_tasks = sum(1 for task in self.tasks.values() if task.enabled)
        disabled_tasks = total_tasks - enabled_tasks
        
        rule_type_counts = {}
        for task in self.tasks.values():
            rule_type = task.scheduling_rule.get('type', 'unknown')
            rule_type_counts[rule_type] = rule_type_counts.get(rule_type, 0) + 1
        
        return {
            'total_tasks': total_tasks,
            'enabled_tasks': enabled_tasks,
            'disabled_tasks': disabled_tasks,
            'rule_type_counts': rule_type_counts
        }
    
    def stop(self):
        """停止调度器"""
        self.clear_tasks()
        logger.info("定时任务调度器已停止") 