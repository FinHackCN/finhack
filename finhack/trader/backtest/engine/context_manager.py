"""
上下文管理器

负责回测上下文的创建、加载、保存和恢复
支持断点续传和状态持久化
"""

import os
import pickle
import hashlib
import json
import logging
from datetime import datetime, date
from typing import Dict, Any, Optional, List
from copy import deepcopy

logger = logging.getLogger(__name__)


class DictObj:
    """字典对象转换器，支持点号访问"""
    
    def __init__(self, data: Dict[str, Any] = None):
        """初始化
        
        Args:
            data: 字典数据
        """
        if data is None:
            data = {}
        
        for key, value in data.items():
            if isinstance(value, dict):
                setattr(self, key, DictObj(value))
            elif isinstance(value, list):
                setattr(self, key, [DictObj(item) if isinstance(item, dict) else item for item in value])
            else:
                setattr(self, key, value)
    
    def __getitem__(self, key):
        return getattr(self, key)
    
    def __setitem__(self, key, value):
        setattr(self, key, value)
    
    def __contains__(self, key):
        return hasattr(self, key)
    
    def get(self, key, default=None):
        return getattr(self, key, default)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        result = {}
        for key, value in self.__dict__.items():
            if isinstance(value, DictObj):
                result[key] = value.to_dict()
            elif isinstance(value, list):
                result[key] = [item.to_dict() if isinstance(item, DictObj) else item for item in value]
            else:
                result[key] = value
        return result


class ContextManager:
    """上下文管理器
    
    负责回测上下文的生命周期管理
    """
    
    def __init__(self, config: Dict[str, Any]):
        """初始化上下文管理器
        
        Args:
            config: 配置参数
        """
        self.config = config
        
        # 运行时数据目录（从配置获取）
        base_data_dir = config.get('base_data_dir', '/data')
        self.running_dir = os.path.join(base_data_dir, 'running')
        
        # 确保目录存在
        os.makedirs(self.running_dir, exist_ok=True)
        
        logger.info("上下文管理器初始化完成")
    
    def generate_context_id(self, params: Dict[str, Any]) -> str:
        """生成上下文唯一ID
        
        Args:
            params: 上下文参数
            
        Returns:
            str: 上下文ID
        """
        # 创建参数的标准化字符串
        sorted_params = sorted(params.items())
        param_str = json.dumps(sorted_params, sort_keys=True, default=str)
        
        # 生成MD5哈希
        context_id = hashlib.md5(param_str.encode('utf-8')).hexdigest()
        
        logger.debug(f"生成上下文ID: {context_id}")
        return context_id
    
    def load_or_create_context(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """加载或创建上下文
        
        Args:
            params: 上下文参数
            
        Returns:
            Dict[str, Any]: 上下文对象
        """
        # 生成上下文ID
        context_id = self.generate_context_id(params)
        
        # 尝试加载已有上下文
        vendor = params.get('vendor', 'backtest')
        context_file = os.path.join(self.running_dir, f"{vendor}_{context_id}.pkl")
        
        if os.path.exists(context_file):
            logger.info(f"找到已有上下文文件，正在加载: {context_file}")
            try:
                context = self.load_context(context_file)
                logger.info("上下文加载成功")
                return context
            except Exception as e:
                logger.warning(f"加载上下文失败，将创建新上下文: {e}")
        
        # 创建新上下文
        logger.info("创建新的回测上下文")
        context = self.create_new_context(params, context_id)
        
        return context
    
    def create_new_context(self, params: Dict[str, Any], context_id: str) -> Dict[str, Any]:
        """创建新的上下文
        
        Args:
            params: 上下文参数
            context_id: 上下文ID
            
        Returns:
            Dict[str, Any]: 新的上下文对象
        """
        # 创建全局变量容器
        g = DictObj()
        
        # 构建完整的上下文结构
        context_data = {
            # 1. 基础信息
            'id': context_id,
            'current_dt': None,
            'previous_date': None,
            'params': params.get('params', {}),
            'benchmark': params.get('benchmark', '000001.SH'),
            
            # 2. 静态配置
            'settings': DictObj({
                'market': params.get('market', 'cn_stock'),
                'strategy_name': params.get('strategy_name', ''),
                'start_date': params.get('start_date', '2023-01-01'),
                'end_date': params.get('end_date', '2023-12-31'),
                'frequency': params.get('frequency', '1d'),
                'benchmark': params.get('benchmark', '000300.XSHG'),
                'universe': params.get('universe', []),
                
                # 交易与费用配置
                'starting_cash': params.get('starting_cash', 1000000.0),
                'order_volume_ratio': params.get('order_volume_ratio', 1.0),
                'slip_type': params.get('slip_type', 'pricerelated'),
                'slip_value': params.get('slip_value', 0.001),
                'open_tax': params.get('open_tax', 0.0),
                'close_tax': params.get('close_tax', 0.001),
                'open_commission': params.get('open_commission', 0.0003),
                'close_commission': params.get('close_commission', 0.0003),
                'close_today_commission': params.get('close_today_commission', 0.0),
                'min_commission': params.get('min_commission', 5.0),
            }),
            
            # 3. 账户状态
            'account': {
                'account_id': f'backtest_account_{context_id[:8]}',
                'platform': 'BACKTEST',
                'account_type': 'CASH',
                'currency': 'CNY',
                'total_assets': params.get('starting_cash', 1000000.0),
                'cash_available': params.get('starting_cash', 1000000.0),
                'cash_frozen': 0.0,
                'market_value': 0.0,
                'pnl_unrealized': 0.0,
                'pnl_realized': 0.0,
                'status': 'CONNECTED',
                'timestamp_updated': None,
                'margin_used': 0.0,
                'margin_free': 0.0,
                'risk_level': '0.0',
            },
            
            # 4. 投资组合
            'portfolio': DictObj({
                'positions': {},  # 持仓
                'orders': {},     # 活动订单
            }),
            
            # 5. 数据服务
            'data': DictObj({
                'calendar': [],
                'schedule_event_list': [],
                'data_source': params.get('data_source', 'file'),
                'dividend_info': {},
                'client': None,
            }),
            
            # 6. 全局变量
            'g': g,
            
            # 7. 定时任务
            'scheduled_tasks': [],
            
            # 8. 历史记录
            'logs': DictObj({
                'all_trades': [],
                'all_orders': [],
                'daily_history': [],
            }),
            
            # 9. 性能表现
            'performance': DictObj({
                'returns': [],
                'bench_returns': [],
                'turnover': [],
                'win_ratio': 0.0,
                'trade_num': 0,
                'indicators': {}
            })
        }
        
        # 转换为DictObj以支持点号访问
        context = DictObj(context_data)
        
        logger.info(f"新上下文创建完成，ID: {context_id}")
        return context.to_dict()
    
    def save_context(self, context: Dict[str, Any], force: bool = False):
        """保存上下文到文件
        
        Args:
            context: 上下文对象
            force: 是否强制保存
        """
        try:
            context_id = context.get('id')
            if not context_id:
                logger.warning("上下文缺少ID，无法保存")
                return
            
            # 对于回测，通常不需要保存状态（除非强制保存）
            vendor = 'backtest'
            if not force and vendor == 'backtest':
                logger.debug("回测模式，跳过上下文保存")
                return
            
            context_file = os.path.join(self.running_dir, f"{vendor}_{context_id}.pkl")
            
            # 准备要保存的数据（去除不需要序列化的对象）
            save_data = deepcopy(context)
            
            # 移除无法序列化的对象
            if 'data' in save_data and 'client' in save_data['data']:
                save_data['data']['client'] = None
            
            with open(context_file, 'wb') as f:
                pickle.dump(save_data, f)
            
            logger.debug(f"上下文已保存: {context_file}")
            
        except Exception as e:
            logger.error(f"保存上下文失败: {e}")
    
    def load_context(self, context_file: str) -> Dict[str, Any]:
        """从文件加载上下文
        
        Args:
            context_file: 上下文文件路径
            
        Returns:
            Dict[str, Any]: 上下文对象
        """
        try:
            with open(context_file, 'rb') as f:
                context = pickle.load(f)
            
            logger.debug(f"上下文加载成功: {context_file}")
            return context
            
        except Exception as e:
            logger.error(f"加载上下文失败: {e}")
            raise
    
    def delete_context(self, context_id: str, vendor: str = 'backtest'):
        """删除上下文文件
        
        Args:
            context_id: 上下文ID
            vendor: 供应商名称
        """
        try:
            context_file = os.path.join(self.running_dir, f"{vendor}_{context_id}.pkl")
            
            if os.path.exists(context_file):
                os.remove(context_file)
                logger.info(f"上下文文件已删除: {context_file}")
            else:
                logger.warning(f"上下文文件不存在: {context_file}")
                
        except Exception as e:
            logger.error(f"删除上下文文件失败: {e}")
    
    def list_contexts(self, vendor: str = None) -> List[Dict[str, Any]]:
        """列出所有上下文文件
        
        Args:
            vendor: 供应商名称过滤
            
        Returns:
            List[Dict[str, Any]]: 上下文文件信息列表
        """
        contexts = []
        
        try:
            for filename in os.listdir(self.running_dir):
                if filename.endswith('.pkl'):
                    if vendor and not filename.startswith(f"{vendor}_"):
                        continue
                    
                    file_path = os.path.join(self.running_dir, filename)
                    stat = os.stat(file_path)
                    
                    # 解析文件名
                    parts = filename[:-4].split('_', 1)  # 移除.pkl后缀
                    if len(parts) == 2:
                        file_vendor, context_id = parts
                    else:
                        file_vendor, context_id = 'unknown', parts[0]
                    
                    contexts.append({
                        'vendor': file_vendor,
                        'context_id': context_id,
                        'filename': filename,
                        'size': stat.st_size,
                        'modified_time': datetime.fromtimestamp(stat.st_mtime),
                        'created_time': datetime.fromtimestamp(stat.st_ctime)
                    })
            
            # 按修改时间排序
            contexts.sort(key=lambda x: x['modified_time'], reverse=True)
            
        except Exception as e:
            logger.error(f"列出上下文文件失败: {e}")
        
        return contexts
    
    def clean_old_contexts(self, days: int = 30, vendor: str = None):
        """清理旧的上下文文件
        
        Args:
            days: 保留天数
            vendor: 供应商名称过滤
        """
        try:
            cutoff_time = datetime.now().timestamp() - (days * 24 * 3600)
            deleted_count = 0
            
            for filename in os.listdir(self.running_dir):
                if filename.endswith('.pkl'):
                    if vendor and not filename.startswith(f"{vendor}_"):
                        continue
                    
                    file_path = os.path.join(self.running_dir, filename)
                    stat = os.stat(file_path)
                    
                    if stat.st_mtime < cutoff_time:
                        os.remove(file_path)
                        deleted_count += 1
                        logger.debug(f"删除旧上下文文件: {filename}")
            
            logger.info(f"清理完成，删除了 {deleted_count} 个旧上下文文件")
            
        except Exception as e:
            logger.error(f"清理旧上下文文件失败: {e}")
    
    def validate_context(self, context: Dict[str, Any]) -> tuple[bool, List[str]]:
        """验证上下文的完整性
        
        Args:
            context: 上下文对象
            
        Returns:
            tuple[bool, List[str]]: (是否有效, 错误信息列表)
        """
        errors = []
        
        # 检查必需字段
        required_fields = ['id', 'settings', 'account', 'portfolio']
        for field in required_fields:
            if field not in context:
                errors.append(f"缺少必需字段: {field}")
        
        # 检查设置
        if 'settings' in context:
            settings = context['settings']
            required_settings = ['market', 'start_date', 'end_date']
            for setting in required_settings:
                if setting not in settings:
                    errors.append(f"缺少设置字段: {setting}")
        
        # 检查账户
        if 'account' in context:
            account = context['account']
            if account.get('total_assets', 0) < 0:
                errors.append("账户总资产不能为负数")
        
        is_valid = len(errors) == 0
        return is_valid, errors 