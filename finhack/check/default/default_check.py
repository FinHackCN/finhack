"""
FinHack 数据检查模块
全面的数据质量分析和异常检测

使用方式:
    finhack check                      # 检查全部
    finhack check --target=data,cache  # 检查指定项
    finhack check --target=data        # 只检查数据库数据
    finhack check --target=cache       # 只检查缓存
    finhack check --target=factors     # 只检查因子
"""

import os
import sys
import json
import time
import pickle
import hashlib
import sqlite3
import datetime
import traceback
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict
from datetime import timedelta

import finhack.library.log as Log
from runtime.constant import *
from finhack.library.config import Config
from finhack.library.db import DB


class DefaultCheck:
    """数据质量检查器"""

    # 市场配置
    MARKETS = {
        'cn_stock': {'name': 'A股', 'tables': ['astock_'], 'cache': 'cn_stock', 'priority': 1},
        'cn_fund': {'name': '基金', 'tables': ['fund_'], 'cache': 'cn_fund', 'priority': 2},
        'cn_future': {'name': '期货', 'tables': ['futures_'], 'cache': 'cn_future', 'priority': 3},
        'cn_index': {'name': '指数', 'tables': ['astock_index_'], 'cache': 'cn_index', 'priority': 4},
        'cn_cb': {'name': '可转债', 'tables': ['cb_'], 'cache': 'cn_cb', 'priority': 5},
        'hk_stock': {'name': '港股', 'tables': ['hstock_', 'hk_'], 'cache': 'hk_stock', 'priority': 6},
        'global_crypto': {'name': '数字货币', 'tables': [], 'cache': 'global_cryptospot', 'priority': 7},
        'global_fx': {'name': '外汇', 'tables': ['fx_'], 'cache': 'global_fx', 'priority': 8},
        'econ': {'name': '宏观经济', 'tables': ['econo_'], 'cache': None, 'priority': 9},
        'other': {'name': '其他', 'tables': [], 'cache': None, 'priority': 99},
    }

    FREQUENCIES = ['1d', '1m']

    # 关键数据表配置
    KEY_TABLES = {
        # 行情数据
        'astock_price_daily': {
            'name': 'A股日线行情', 'market': 'cn_stock', 'freq': '1d', 'date_field': 'trade_date',
            'expected_records_per_day': 5000, 'critical': True, 'category': '行情数据'
        },
        'astock_price_daily_basic': {
            'name': 'A股每日指标', 'market': 'cn_stock', 'freq': '1d', 'date_field': 'trade_date',
            'expected_records_per_day': 5000, 'critical': True, 'category': '行情数据'
        },
        'astock_price_adj_factor': {
            'name': 'A股复权因子', 'market': 'cn_stock', 'freq': '1d', 'date_field': 'trade_date',
            'expected_records_per_day': 5000, 'critical': True, 'category': '行情数据'
        },
        'astock_price_weekly': {
            'name': 'A股周线行情', 'market': 'cn_stock', 'freq': '1d', 'date_field': 'trade_date',
            'expected_records_per_day': 1000, 'critical': False, 'category': '行情数据'
        },
        'astock_price_monthly': {
            'name': 'A股月线行情', 'market': 'cn_stock', 'freq': '1d', 'date_field': 'trade_date',
            'expected_records_per_day': 500, 'critical': False, 'category': '行情数据'
        },
        'astock_price_moneyflow': {
            'name': 'A股资金流向', 'market': 'cn_stock', 'freq': '1d', 'date_field': 'trade_date',
            'expected_records_per_day': 4000, 'critical': False, 'category': '行情数据'
        },
        'astock_price_stk_limit': {
            'name': 'A股涨跌停价格', 'market': 'cn_stock', 'freq': '1d', 'date_field': 'trade_date',
            'expected_records_per_day': 5000, 'critical': True, 'category': '行情数据'
        },
        'astock_price_suspend_d': {
            'name': 'A股停复牌信息', 'market': 'cn_stock', 'freq': '1d', 'date_field': 'suspend_date',
            'expected_records_per_day': 100, 'critical': False, 'category': '行情数据'
        },
        # 基础数据
        'astock_basic': {
            'name': 'A股股票列表', 'market': 'cn_stock', 'freq': '1d', 'date_field': None,
            'expected_records': 5500, 'critical': True, 'category': '基础数据'
        },
        'astock_trade_cal': {
            'name': '交易日历', 'market': 'cn_stock', 'freq': '1d', 'date_field': 'cal_date',
            'expected_records': 15000, 'critical': True, 'category': '基础数据'
        },
        'astock_index_basic': {
            'name': '指数列表', 'market': 'cn_index', 'freq': '1d', 'date_field': None,
            'expected_records': 10000, 'critical': False, 'category': '基础数据'
        },
        'astock_index_daily': {
            'name': '指数日线行情', 'market': 'cn_index', 'freq': '1d', 'date_field': 'trade_date',
            'expected_records_per_day': 5000, 'critical': True, 'category': '行情数据'
        },
        # 财务数据
        'astock_finance_income': {
            'name': '利润表', 'market': 'cn_stock', 'freq': '1d', 'date_field': 'ann_date',
            'expected_records_per_day': 200, 'critical': True, 'category': '财务数据'
        },
        'astock_finance_balancesheet': {
            'name': '资产负债表', 'market': 'cn_stock', 'freq': '1d', 'date_field': 'ann_date',
            'expected_records_per_day': 200, 'critical': True, 'category': '财务数据'
        },
        'astock_finance_cashflow': {
            'name': '现金流量表', 'market': 'cn_stock', 'freq': '1d', 'date_field': 'ann_date',
            'expected_records_per_day': 200, 'critical': True, 'category': '财务数据'
        },
        'astock_finance_indicator': {
            'name': '财务指标', 'market': 'cn_stock', 'freq': '1d', 'date_field': 'ann_date',
            'expected_records_per_day': 200, 'critical': True, 'category': '财务数据'
        },
        'astock_finance_forecast': {
            'name': '业绩预告', 'market': 'cn_stock', 'freq': '1d', 'date_field': 'ann_date',
            'expected_records_per_day': 50, 'critical': False, 'category': '财务数据'
        },
        'astock_finance_dividend': {
            'name': '分红送股', 'market': 'cn_stock', 'freq': '1d', 'date_field': 'ann_date',
            'expected_records_per_day': 20, 'critical': False, 'category': '财务数据'
        },
        # 基金数据
        'fund_basic': {
            'name': '基金列表', 'market': 'cn_fund', 'freq': '1d', 'date_field': None,
            'expected_records': 20000, 'critical': False, 'category': '基础数据'
        },
        'fund_daily': {
            'name': '基金日线行情', 'market': 'cn_fund', 'freq': '1d', 'date_field': 'trade_date',
            'expected_records_per_day': 15000, 'critical': False, 'category': '行情数据'
        },
        # 期货数据
        'futures_basic': {
            'name': '期货合约列表', 'market': 'cn_future', 'freq': '1d', 'date_field': None,
            'expected_records': 50000, 'critical': False, 'category': '基础数据'
        },
        'futures_daily': {
            'name': '期货日线行情', 'market': 'cn_future', 'freq': '1d', 'date_field': 'trade_date',
            'expected_records_per_day': 5000, 'critical': False, 'category': '行情数据'
        },
    }

    def __init__(self, args):
        self.args = args
        self.report = {
            'check_time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'check_timestamp': time.time(),
            'data': {},
            'cache': {},
            'factors': {},
            'anomalies': [],
            'health_score': 0,
            'summary': {}
        }
        # 详细的市场数据
        self.market_stats = defaultdict(lambda: self._init_market_stats())
        # 表级详细数据
        self.table_stats = {}
        # 异常列表
        self.anomalies = []
        # 文件类型详细统计
        self.file_type_stats = {}

    def _init_market_stats(self):
        """初始化市场统计数据结构 - 支持多种数据类型"""
        return {
            'database': {
                'tables': 0, 'records': 0, 'size_mb': 0,
                'min_date': None, 'max_date': None,
                'tables_detail': [],
                'by_category': {}  # 按类别: 行情数据, 财务数据, 基础数据等
            },
            'files': {
                'csv_timebased': {
                    '1d': {'files': 0, 'size_mb': 0, 'min_date': None, 'max_date': None, 'last_update': None, 'details': []},
                    '1m': {'files': 0, 'size_mb': 0, 'min_date': None, 'max_date': None, 'last_update': None, 'details': []},
                    'total': {'files': 0, 'size_mb': 0}
                },
                'csv_codebased': {
                    'total': {'files': 0, 'size_mb': 0, 'codes': 0, 'details': []}
                },
                'pkl': {
                    '1d': {'files': 0, 'size_mb': 0, 'factors': 0, 'last_update': None, 'details': []},
                    '1m': {'files': 0, 'size_mb': 0, 'factors': 0, 'last_update': None, 'details': []},
                    'total': {'files': 0, 'size_mb': 0, 'factors': 0}
                },
                'parquet': {
                    '1d': {'files': 0, 'size_mb': 0, 'last_update': None, 'details': []},
                    '1m': {'files': 0, 'size_mb': 0, 'last_update': None, 'details': []},
                    'total': {'files': 0, 'size_mb': 0}
                },
                'hdf5': {
                    'total': {'files': 0, 'size_mb': 0, 'details': []}
                },
                'feather': {
                    'total': {'files': 0, 'size_mb': 0, 'details': []}
                },
                'other': {
                    'total': {'files': 0, 'size_mb': 0, 'details': []}
                }
            },
            'cache': {
                '1d': {'files': 0, 'size_mb': 0, 'last_update': None},
                '1m': {'files': 0, 'size_mb': 0, 'last_update': None},
                'total': {'files': 0, 'size_mb': 0}
            },
            'factors': {
                '1d': {'files': 0, 'size_mb': 0, 'factors': 0},
                '1m': {'files': 0, 'size_mb': 0, 'factors': 0},
                'total': {'files': 0, 'size_mb': 0, 'factors': 0}
            },
            'health': {'score': 100, 'issues': []}
        }

    def run(self):
        """执行检查"""
        print("\n" + "="*100)
        print("  FinHack 数据质量检查工具")
        print("="*100)

        target = getattr(self.args, 'target', None)
        if target is None or target.strip() == '' or target.strip().lower() == 'all':
            targets = ['data', 'cache', 'factors']
        else:
            targets = [t.strip().lower() for t in target.split(',')]

        print(f"\n检查目标: {', '.join(targets)}")
        print(f"检查时间: {self.report['check_time']}")
        print("-"*100)

        total_steps = len(targets)
        current_step = 0

        if 'data' in targets:
            current_step += 1
            print(f"\n[{current_step}/{total_steps}] 检查数据库数据...")
            self._check_database()

        if 'cache' in targets:
            current_step += 1
            print(f"\n[{current_step}/{total_steps}] 检查缓存数据...")
            self._check_cache()

        if 'factors' in targets:
            current_step += 1
            print(f"\n[{current_step}/{total_steps}] 检查因子数据...")
            self._check_factors()

        # 从已收集的数据推断文件类型统计 (不重新扫描)
        self._infer_file_type_stats()

        # 数据质量分析
        self._analyze_data_quality()

        # 计算健康分数
        self._calculate_health_score()

        # 生成报告
        self._print_summary()

        # 保存报告
        self._save_reports()

        print("\n" + "="*100)
        print("  检查完成!")
        print("="*100)

    def _check_file_data(self):
        """检查各类文件数据 - 使用shell命令快速统计"""
        kline_dir = KLINE_DIR if 'KLINE_DIR' in dir() else os.path.join(DATA_DIR, 'market', 'kline')

        market_map = {
            'cn_stock': 'cn_stock', 'cn_fund': 'cn_fund', 'cn_future': 'cn_future',
            'cn_index': 'cn_index', 'cn_cb': 'cn_cb', 'cn_option': 'cn_option',
            'hk_stock': 'hk_stock', 'global_fx': 'global_fx',
            'global_cryptospot': 'global_cryptospot', 'global_cryptoswap': 'global_cryptoswap',
            'us_stock': 'us_stock'
        }

        # 扫描 parquet (codebased)
        codebased_dir = os.path.join(kline_dir, 'codebased')
        if os.path.exists(codebased_dir):
            print(f"  扫描 parquet 文件...")
            for market_dir in os.listdir(codebased_dir):
                market_path = os.path.join(codebased_dir, market_dir)
                if not os.path.isdir(market_path):
                    continue
                market = market_map.get(market_dir, market_dir)

                for freq_dir in os.listdir(market_path):
                    freq_path = os.path.join(market_path, freq_dir)
                    if not os.path.isdir(freq_path):
                        continue
                    freq = freq_dir if freq_dir in ['1d', '1m'] else 'unknown'

                    # 使用 find 命令快速统计
                    try:
                        result = os.popen(f'find "{freq_path}" -name "*.parquet" -type f 2>/dev/null | wc -l').read().strip()
                        file_count = int(result) if result.isdigit() else 0

                        if file_count > 0:
                            size_result = os.popen(f'du -sm "{freq_path}" 2>/dev/null | cut -f1').read().strip()
                            size_mb = float(size_result) if size_result.replace('.','').isdigit() else 0

                            stats = self.market_stats[market]
                            stats['files']['parquet'][freq]['files'] = file_count
                            stats['files']['parquet'][freq]['size_mb'] = size_mb
                            stats['files']['parquet']['total']['files'] += file_count
                            stats['files']['parquet']['total']['size_mb'] += size_mb
                    except:
                        pass

        # 扫描 csv (timebased)
        timebased_dir = os.path.join(kline_dir, 'timebased')
        if os.path.exists(timebased_dir):
            print(f"  扫描 csv 文件...")
            for market_dir in os.listdir(timebased_dir):
                market_path = os.path.join(timebased_dir, market_dir)
                if not os.path.isdir(market_path):
                    continue
                market = market_map.get(market_dir, market_dir)

                for freq_dir in os.listdir(market_path):
                    freq_path = os.path.join(market_path, freq_dir)
                    if not os.path.isdir(freq_path):
                        continue
                    freq = freq_dir if freq_dir in ['1d', '1m'] else 'unknown'

                    try:
                        result = os.popen(f'find "{freq_path}" -name "*.csv" -type f 2>/dev/null | wc -l').read().strip()
                        file_count = int(result) if result.isdigit() else 0

                        if file_count > 0:
                            size_result = os.popen(f'du -sm "{freq_path}" 2>/dev/null | cut -f1').read().strip()
                            size_mb = float(size_result) if size_result.replace('.','').isdigit() else 0

                            stats = self.market_stats[market]
                            stats['files']['csv_timebased'][freq]['files'] = file_count
                            stats['files']['csv_timebased'][freq]['size_mb'] = size_mb
                            stats['files']['csv_timebased']['total']['files'] += file_count
                            stats['files']['csv_timebased']['total']['size_mb'] += size_mb
                    except:
                        pass

    def _infer_file_type_stats(self):
        """从已收集的数据推断文件类型统计 - 同时扫描实际文件"""
        # 先执行文件扫描
        self._check_file_data()

        # 遍历所有市场，将cache和factors数据映射到PKL文件
        for market, stats in self.market_stats.items():
            # 将cache数据映射到PKL文件
            cache = stats.get('cache', {})
            for freq in ['1d', '1m']:
                cache_freq = cache.get(freq, {})
                if cache_freq.get('files', 0) > 0:
                    stats['files']['pkl'][freq]['files'] += cache_freq.get('files', 0)
                    stats['files']['pkl'][freq]['size_mb'] += cache_freq.get('size_mb', 0)

            # 将factors数据映射到PKL文件
            factors = stats.get('factors', {})
            for freq in ['1d', '1m']:
                factors_freq = factors.get(freq, {})
                if factors_freq.get('files', 0) > 0:
                    stats['files']['pkl'][freq]['files'] += factors_freq.get('files', 0)
                    stats['files']['pkl'][freq]['size_mb'] += factors_freq.get('size_mb', 0)

            # 更新PKL总计
            stats['files']['pkl']['total']['files'] = (
                stats['files']['pkl']['1d']['files'] + stats['files']['pkl']['1m']['files']
            )
            stats['files']['pkl']['total']['size_mb'] = round(
                stats['files']['pkl']['1d']['size_mb'] + stats['files']['pkl']['1m']['size_mb'], 2
            )

    def _infer_market_from_path(self, path: str) -> str:
        """从路径推断市场"""
        path_lower = path.lower()
        market_keywords = {
            'cn_stock': ['cn_stock', 'astock', 'a股'],
            'cn_fund': ['cn_fund', 'fund', '基金'],
            'cn_future': ['cn_future', 'futures', '期货'],
            'cn_index': ['cn_index', 'index', '指数'],
            'cn_cb': ['cn_cb', 'cb', '可转债'],
            'hk_stock': ['hk_stock', 'hstock', '港股'],
            'global_fx': ['global_fx', 'fx', '外汇'],
            'global_cryptospot': ['crypto', '币'],
        }
        for market, keywords in market_keywords.items():
            for kw in keywords:
                if kw in path_lower:
                    return market
        return 'other'

    def _infer_freq_from_path(self, path: str) -> str:
        """从路径推断频率"""
        path_lower = path.lower()
        if '/1m/' in path_lower or '\\1m\\' in path_lower or 'minute' in path_lower:
            return '1m'
        elif '/1d/' in path_lower or '\\1d\\' in path_lower or 'daily' in path_lower:
            return '1d'
        return 'unknown'

    def _update_market_file_stats(self, market: str, ext: str, freq: str, file_info: dict):
        """更新市场文件统计"""
        stats = self.market_stats[market]

        # 根据扩展名和路径特征分类
        if ext == '.csv':
            # 判断是时间序列还是代码序列
            if 'date' in file_info['path'].lower() or freq != 'unknown':
                category = 'csv_timebased'
                freq_key = freq if freq in ['1d', '1m'] else '1d'
                stats['files'][category][freq_key]['files'] += 1
                stats['files'][category][freq_key]['size_mb'] += file_info['size_mb']
                if len(stats['files'][category][freq_key]['details']) < 100:
                    stats['files'][category][freq_key]['details'].append(file_info)
            else:
                category = 'csv_codebased'
                stats['files'][category]['total']['files'] += 1
                stats['files'][category]['total']['size_mb'] += file_info['size_mb']
                if len(stats['files'][category]['total']['details']) < 100:
                    stats['files'][category]['total']['details'].append(file_info)

        elif ext == '.pkl':
            freq_key = freq if freq in ['1d', '1m'] else '1d'
            stats['files']['pkl'][freq_key]['files'] += 1
            stats['files']['pkl'][freq_key]['size_mb'] += file_info['size_mb']
            if len(stats['files']['pkl'][freq_key]['details']) < 100:
                stats['files']['pkl'][freq_key]['details'].append(file_info)

        elif ext == '.parquet':
            freq_key = freq if freq in ['1d', '1m'] else '1d'
            stats['files']['parquet'][freq_key]['files'] += 1
            stats['files']['parquet'][freq_key]['size_mb'] += file_info['size_mb']
            if len(stats['files']['parquet'][freq_key]['details']) < 100:
                stats['files']['parquet'][freq_key]['details'].append(file_info)

        elif ext in ['.h5', '.hdf5']:
            stats['files']['hdf5']['total']['files'] += 1
            stats['files']['hdf5']['total']['size_mb'] += file_info['size_mb']
            if len(stats['files']['hdf5']['total']['details']) < 100:
                stats['files']['hdf5']['total']['details'].append(file_info)

        elif ext == '.feather':
            stats['files']['feather']['total']['files'] += 1
            stats['files']['feather']['total']['size_mb'] += file_info['size_mb']
            if len(stats['files']['feather']['total']['details']) < 100:
                stats['files']['feather']['total']['details'].append(file_info)

        else:
            stats['files']['other']['total']['files'] += 1
            stats['files']['other']['total']['size_mb'] += file_info['size_mb']
            if len(stats['files']['other']['total']['details']) < 100:
                stats['files']['other']['total']['details'].append(file_info)

    def _check_database(self):
        """检查数据库数据"""
        db_report = {
            'status': 'unknown',
            'path': None,
            'size_mb': 0,
            'size_gb': 0,
            'tables': [],
            'statistics': {
                'total_tables': 0,
                'total_records': 0,
                'tables_with_data': 0,
                'empty_tables': 0,
                'critical_tables_ok': 0,
                'critical_tables_total': 0
            },
            'by_market': {},
            'by_category': {},
            'date_coverage': {}
        }

        try:
            db_config = Config.get_config('db', 'tushare')
            if not db_config:
                print("  [错误] 未找到数据库配置")
                db_report['status'] = 'error'
                db_report['error'] = '未找到数据库配置'
                self.report['data'] = db_report
                return

            db_path = db_config.get('path', '')
            if not os.path.isabs(db_path):
                db_path = os.path.join(BASE_DIR, db_path)

            db_report['path'] = db_path

            if not os.path.exists(db_path):
                print(f"  [错误] 数据库不存在: {db_path}")
                db_report['status'] = 'not_found'
                self.report['data'] = db_report
                return

            size_bytes = os.path.getsize(db_path)
            db_report['size_mb'] = round(size_bytes / (1024 * 1024), 2)
            db_report['size_gb'] = round(size_bytes / (1024 * 1024 * 1024), 2)

            # 测试连接
            try:
                conn = sqlite3.connect(db_path)
                conn.execute("SELECT 1")
                conn.close()
                db_report['status'] = 'ok'
                print(f"  [OK] 数据库: {db_path} ({db_report['size_gb']:.2f} GB)")
            except Exception as e:
                db_report['status'] = 'error'
                print(f"  [错误] 数据库连接失败: {e}")
                self.report['data'] = db_report
                return

            # 获取所有表
            tables = self._get_all_tables('tushare')
            db_report['statistics']['total_tables'] = len(tables)
            print(f"  发现 {len(tables)} 个数据表\n")

            # 检查每个表
            for i, table_name in enumerate(tables):
                table_info = self._check_table_detail(table_name)
                db_report['tables'].append(table_info)
                self.table_stats[table_name] = table_info

                # 更新统计
                if table_info['record_count'] > 0:
                    db_report['statistics']['tables_with_data'] += 1
                else:
                    db_report['statistics']['empty_tables'] += 1
                db_report['statistics']['total_records'] += table_info['record_count']

                # 按市场分类
                market = table_info.get('market', 'other')
                if market not in db_report['by_market']:
                    db_report['by_market'][market] = {'tables': 0, 'records': 0, 'tables_list': []}
                db_report['by_market'][market]['tables'] += 1
                db_report['by_market'][market]['records'] += table_info['record_count']
                db_report['by_market'][market]['tables_list'].append(table_name)

                # 更新市场详细统计
                self.market_stats[market]['database']['tables'] += 1
                self.market_stats[market]['database']['records'] += table_info['record_count']
                self.market_stats[market]['database']['tables_detail'].append({
                    'name': table_name,
                    'records': table_info['record_count'],
                    'min_date': table_info.get('min_date'),
                    'max_date': table_info.get('max_date'),
                    'status': table_info['status']
                })

                # 按分类统计
                category = table_info.get('category', '其他')
                if category not in db_report['by_category']:
                    db_report['by_category'][category] = {'tables': 0, 'records': 0}
                db_report['by_category'][category]['tables'] += 1
                db_report['by_category'][category]['records'] += table_info['record_count']

                # 检查关键表
                if table_name in self.KEY_TABLES:
                    config = self.KEY_TABLES[table_name]
                    db_report['statistics']['critical_tables_total'] += 1
                    if table_info['record_count'] > 0:
                        db_report['statistics']['critical_tables_ok'] += 1

                # 检测异常
                self._detect_table_anomalies(table_info)

                # 进度显示
                if (i + 1) % 20 == 0 or (i + 1) == len(tables):
                    print(f"  进度: {i+1}/{len(tables)}")

            # 更新市场数据库大小
            db_report['size_mb'] = round(size_bytes / (1024 * 1024), 2)
            for market in self.market_stats:
                self.market_stats[market]['database']['size_mb'] = round(
                    db_report['size_mb'] * self.market_stats[market]['database']['tables'] / max(len(tables), 1), 2
                )

        except Exception as e:
            db_report['status'] = 'error'
            db_report['error'] = str(e)
            traceback.print_exc()

        self.report['data'] = db_report

    def _get_all_tables(self, db_name: str) -> List[str]:
        """获取所有表名"""
        tables = []
        try:
            sql = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            result = DB.select_to_list(sql, db_name)
            tables = [row['name'] for row in result if row.get('name')]
        except Exception as e:
            print(f"  [警告] 获取表列表失败: {e}")
        return sorted(tables)

    def _check_table_detail(self, table_name: str) -> Dict[str, Any]:
        """详细检查单个表"""
        info = {
            'name': table_name,
            'record_count': 0,
            'min_date': None,
            'max_date': None,
            'date_field': None,
            'columns': 0,
            'status': 'unknown',
            'market': 'other',
            'category': '其他',
            'is_critical': False,
            'expected_records': None,
            'data_freshness_days': None,
            'completeness': None,
            'issues': []
        }

        # 获取表配置
        if table_name in self.KEY_TABLES:
            config = self.KEY_TABLES[table_name]
            info['market'] = config.get('market', 'other')
            info['category'] = config.get('category', '其他')
            info['is_critical'] = config.get('critical', False)
            info['expected_records'] = config.get('expected_records')
            info['expected_records_per_day'] = config.get('expected_records_per_day')
        else:
            # 根据表名推断市场
            for market, mconfig in self.MARKETS.items():
                for prefix in mconfig.get('tables', []):
                    if table_name.startswith(prefix):
                        info['market'] = market
                        break

        try:
            if not DB.table_exists(table_name, 'tushare'):
                info['status'] = 'not_found'
                info['issues'].append('表不存在')
                return info

            # 获取记录数
            try:
                result = DB.select_one(f"SELECT COUNT(*) as cnt FROM {table_name}", 'tushare')
                info['record_count'] = result.get('cnt', 0) if result else 0
            except Exception:
                info['record_count'] = 0

            # 获取日期范围
            if info['record_count'] > 0:
                for field in ['trade_date', 'cal_date', 'ann_date', 'end_date', 'suspend_date']:
                    try:
                        min_result = DB.select_one(f"SELECT MIN({field}) as min_date FROM {table_name}", 'tushare')
                        max_result = DB.select_one(f"SELECT MAX({field}) as max_date FROM {table_name}", 'tushare')
                        if min_result and max_result and min_result.get('min_date'):
                            info['min_date'] = min_result.get('min_date')
                            info['max_date'] = max_result.get('max_date')
                            info['date_field'] = field
                            break
                    except Exception:
                        continue

                # 计算数据新鲜度
                if info['max_date']:
                    try:
                        max_dt = datetime.datetime.strptime(str(info['max_date']), '%Y%m%d')
                        info['data_freshness_days'] = (datetime.datetime.now() - max_dt).days
                    except Exception:
                        pass

            # 获取列数
            try:
                cols = DB.select_to_list(f"PRAGMA table_info({table_name})", 'tushare')
                info['columns'] = len(cols) if cols else 0
            except Exception:
                pass

            # 评估状态
            if info['record_count'] == 0:
                info['status'] = 'empty'
                if info['is_critical']:
                    info['issues'].append('关键表为空')
            elif info['data_freshness_days'] is not None and info['data_freshness_days'] > 7:
                info['status'] = 'stale'
                info['issues'].append(f'数据已过期 {info["data_freshness_days"]} 天')
            else:
                info['status'] = 'ok'

            # 计算完整性
            if info['expected_records'] and info['expected_records'] > 0:
                info['completeness'] = round(info['record_count'] / info['expected_records'] * 100, 1)
                if info['completeness'] < 80:
                    info['issues'].append(f'数据完整性仅 {info["completeness"]}%')

        except Exception as e:
            info['status'] = 'error'
            info['issues'].append(str(e))

        return info

    def _detect_table_anomalies(self, table_info: Dict):
        """检测表数据异常"""
        table_name = table_info['name']

        # 1. 关键表为空
        if table_info['is_critical'] and table_info['record_count'] == 0:
            self._add_anomaly('critical', 'database', table_name,
                            f"关键表 {table_name} 为空", table_info)

        # 2. 数据过期
        if table_info.get('data_freshness_days') and table_info['data_freshness_days'] > 3:
            self._add_anomaly('warning', 'database', table_name,
                            f"{table_name} 数据滞后 {table_info['data_freshness_days']} 天", table_info)

        # 3. 数据不完整
        if table_info.get('completeness') and table_info['completeness'] < 50:
            self._add_anomaly('warning', 'database', table_name,
                            f"{table_name} 数据完整性仅 {table_info['completeness']}%", table_info)

    def _check_cache(self):
        """检查缓存数据"""
        cache_report = {
            'status': 'unknown',
            'path': CACHE_DIR,
            'checkpoints': [],
            'kline_cache': {},
            'factors_cache': {},
            'other_cache': {},
            'statistics': {
                'total_size_mb': 0,
                'total_size_gb': 0,
                'total_files': 0,
                'checkpoints_ok': 0,
                'checkpoints_outdated': 0
            },
            'by_market': {},
            'by_frequency': {'1d': {}, '1m': {}}
        }

        if not os.path.exists(CACHE_DIR):
            print(f"  [错误] 缓存目录不存在: {CACHE_DIR}")
            cache_report['status'] = 'not_found'
            self.report['cache'] = cache_report
            return

        print(f"  缓存目录: {CACHE_DIR}")

        # 1. 检查检查点
        print("\n  [1/4] 检查数据采集检查点...")
        if os.path.exists(CHECKPOINT_DIR):
            checkpoints = self._check_checkpoints(CHECKPOINT_DIR)
            cache_report['checkpoints'] = checkpoints
            for cp in checkpoints:
                if cp.get('status') == 'ok':
                    cache_report['statistics']['checkpoints_ok'] += 1
                elif cp.get('status') == 'outdated':
                    cache_report['statistics']['checkpoints_outdated'] += 1
                    self._add_anomaly('warning', 'checkpoint', cp.get('table', 'unknown'),
                                    f"{cp.get('table')} 检查点过期 {cp.get('days_behind')} 天", cp)
            print(f"       检查点: {cache_report['statistics']['checkpoints_ok']} 正常, "
                  f"{cache_report['statistics']['checkpoints_outdated']} 过期")

        # 2. 检查因子缓存
        print("\n  [2/4] 检查因子缓存...")
        if os.path.exists(FACTORS_CACHE_DIR):
            factors_cache = self._check_factors_cache_detail(FACTORS_CACHE_DIR)
            cache_report['factors_cache'] = factors_cache

            # 按市场统计
            for market, info in factors_cache.get('markets', {}).items():
                cache_report['by_market'][market] = {
                    'files': info.get('total_files', 0),
                    'size_mb': info.get('total_size_mb', 0)
                }

                # 更新市场统计
                if market in self.market_stats:
                    for freq, freq_info in info.get('frequencies', {}).items():
                        if freq in self.market_stats[market]['cache']:
                            self.market_stats[market]['cache'][freq]['files'] = freq_info.get('file_count', 0)
                            self.market_stats[market]['cache'][freq]['size_mb'] = freq_info.get('size_mb', 0)
                            if freq_info.get('last_update'):
                                self.market_stats[market]['cache'][freq]['last_update'] = freq_info['last_update']

            print(f"       因子缓存: {factors_cache.get('total_files', 0):,} 个文件")

        # 3. 检查K线缓存
        print("\n  [3/4] 检查K线缓存...")
        kline_cache_dir = KLINE_CACHE_DIR if 'KLINE_CACHE_DIR' in dir() else os.path.join(CACHE_DIR, 'kline')
        if os.path.exists(kline_cache_dir):
            kline_cache = self._check_kline_cache(kline_cache_dir)
            cache_report['kline_cache'] = kline_cache
            print(f"       K线缓存: {kline_cache.get('total_files', 0):,} 个文件")

        # 4. 检查其他缓存
        print("\n  [4/4] 检查其他缓存...")
        other_dirs = ['price', 'choice', 'kv', 'market', 'index', 'notice', 'runtime']
        for dir_name in other_dirs:
            dir_path = os.path.join(CACHE_DIR, dir_name)
            if os.path.exists(dir_path):
                dir_info = self._quick_scan_dir(dir_path)
                cache_report['other_cache'][dir_name] = dir_info

        # 计算总大小
        total_size = self._get_dir_size(CACHE_DIR)
        cache_report['statistics']['total_size_mb'] = round(total_size / (1024 * 1024), 2)
        cache_report['statistics']['total_size_gb'] = round(total_size / (1024 * 1024 * 1024), 2)
        cache_report['status'] = 'ok'

        print(f"\n  缓存总大小: {cache_report['statistics']['total_size_gb']:.2f} GB")

        self.report['cache'] = cache_report

    def _check_checkpoints(self, checkpoint_dir: str) -> List[Dict]:
        """检查检查点文件"""
        checkpoints = []
        for filename in os.listdir(checkpoint_dir):
            if filename.endswith('_checkpoint.json'):
                filepath = os.path.join(checkpoint_dir, filename)
                cp_info = {
                    'filename': filename,
                    'table': 'unknown',
                    'last_date': 'unknown',
                    'update_time': datetime.datetime.fromtimestamp(
                        os.path.getmtime(filepath)
                    ).strftime('%Y-%m-%d %H:%M:%S'),
                    'days_behind': 0,
                    'status': 'unknown'
                }

                try:
                    with open(filepath, 'r') as f:
                        data = json.load(f)
                        cp_info['table'] = data.get('table', 'unknown')
                        cp_info['last_date'] = data.get('last_date', 'unknown')
                        cp_info['checkpoint_update_time'] = data.get('update_time', 'unknown')

                        if cp_info['last_date'] != 'unknown':
                            try:
                                last_date = datetime.datetime.strptime(str(cp_info['last_date']), '%Y%m%d')
                                cp_info['days_behind'] = (datetime.datetime.now() - last_date).days
                                cp_info['status'] = 'outdated' if cp_info['days_behind'] > 3 else 'ok'
                            except Exception:
                                cp_info['status'] = 'unknown'
                except Exception as e:
                    cp_info['status'] = 'error'
                    cp_info['error'] = str(e)

                checkpoints.append(cp_info)

        return checkpoints

    def _check_factors_cache_detail(self, factors_dir: str) -> Dict:
        """详细检查因子缓存"""
        result = {
            'path': factors_dir,
            'markets': {},
            'total_files': 0,
            'total_size_mb': 0
        }

        for market in os.listdir(factors_dir):
            market_path = os.path.join(factors_dir, market)
            if os.path.isdir(market_path):
                market_info = {
                    'frequencies': {},
                    'total_files': 0,
                    'total_size_mb': 0
                }

                for freq in os.listdir(market_path):
                    freq_path = os.path.join(market_path, freq)
                    if os.path.isdir(freq_path):
                        file_count = 0
                        size_bytes = 0
                        last_update = None

                        for f in os.listdir(freq_path):
                            if f.endswith('.pkl'):
                                fp = os.path.join(freq_path, f)
                                file_count += 1
                                size_bytes += os.path.getsize(fp)
                                mtime = os.path.getmtime(fp)
                                if last_update is None or mtime > last_update:
                                    last_update = mtime

                        market_info['frequencies'][freq] = {
                            'file_count': file_count,
                            'size_mb': round(size_bytes / (1024 * 1024), 2),
                            'last_update': datetime.datetime.fromtimestamp(last_update).strftime('%Y-%m-%d %H:%M:%S') if last_update else None
                        }
                        market_info['total_files'] += file_count
                        market_info['total_size_mb'] += size_bytes / (1024 * 1024)

                market_info['total_size_mb'] = round(market_info['total_size_mb'], 2)
                result['markets'][market] = market_info
                result['total_files'] += market_info['total_files']
                result['total_size_mb'] += market_info['total_size_mb']

        result['total_size_mb'] = round(result['total_size_mb'], 2)
        return result

    def _check_kline_cache(self, kline_dir: str) -> Dict:
        """检查K线缓存"""
        result = {
            'path': kline_dir,
            'markets': {},
            'total_files': 0,
            'total_size_mb': 0
        }

        for item in os.listdir(kline_dir):
            item_path = os.path.join(kline_dir, item)
            if os.path.isdir(item_path):
                info = self._quick_scan_dir(item_path)
                result['markets'][item] = info
                result['total_files'] += info.get('file_count', 0)
                result['total_size_mb'] += info.get('size_mb', 0)

        result['total_size_mb'] = round(result['total_size_mb'], 2)
        return result

    def _quick_scan_dir(self, dir_path: str) -> Dict:
        """快速扫描目录"""
        result = {'path': dir_path, 'file_count': 0, 'size_mb': 0}
        try:
            for root, dirs, files in os.walk(dir_path):
                result['file_count'] += len(files)
            result['size_mb'] = round(self._get_dir_size(dir_path) / (1024 * 1024), 2)
        except Exception:
            pass
        return result

    def _check_factors(self):
        """检查因子数据"""
        factors_report = {
            'status': 'unknown',
            'path': FACTORS_DIR,
            'subdirs': {},
            'statistics': {
                'total_files': 0,
                'total_size_mb': 0,
                'total_size_gb': 0,
                'by_type': {}
            },
            'by_market': {}
        }

        if not os.path.exists(FACTORS_DIR):
            print(f"  [错误] 因子目录不存在: {FACTORS_DIR}")
            factors_report['status'] = 'not_found'
            self.report['factors'] = factors_report
            return

        print(f"  因子目录: {FACTORS_DIR}")

        subdirs = [d for d in os.listdir(FACTORS_DIR) if os.path.isdir(os.path.join(FACTORS_DIR, d))]
        print(f"  发现子目录: {', '.join(subdirs)}")

        for subdir in subdirs:
            subdir_path = os.path.join(FACTORS_DIR, subdir)
            print(f"\n  检查 {subdir}...")
            subdir_info = self._check_factor_subdir_detail(subdir_path, subdir)
            factors_report['subdirs'][subdir] = subdir_info
            factors_report['statistics']['total_files'] += subdir_info.get('file_count', 0)

            # 按市场汇总
            for market, market_info in subdir_info.get('markets', {}).items():
                if market not in factors_report['by_market']:
                    factors_report['by_market'][market] = {'files': 0, 'size_mb': 0, 'frequencies': {}}
                factors_report['by_market'][market]['files'] += market_info.get('total_files', 0)
                factors_report['by_market'][market]['size_mb'] += market_info.get('total_size_mb', 0)

                # 更新市场统计
                if market in self.market_stats:
                    self.market_stats[market]['factors']['total']['files'] += market_info.get('total_files', 0)
                    self.market_stats[market]['factors']['total']['size_mb'] += market_info.get('total_size_mb', 0)
                    for freq, freq_info in market_info.get('frequencies', {}).items():
                        if freq in self.market_stats[market]['factors']:
                            self.market_stats[market]['factors'][freq]['files'] += freq_info.get('file_count', 0)
                            self.market_stats[market]['factors'][freq]['size_mb'] += freq_info.get('size_mb', 0)

            print(f"       {subdir}: {subdir_info.get('file_count', 0):,} 个文件, {subdir_info.get('total_size_mb', 0):.2f} MB")

        total_size = self._get_dir_size(FACTORS_DIR)
        factors_report['statistics']['total_size_mb'] = round(total_size / (1024 * 1024), 2)
        factors_report['statistics']['total_size_gb'] = round(total_size / (1024 * 1024 * 1024), 2)
        factors_report['status'] = 'ok'

        print(f"\n  因子总大小: {factors_report['statistics']['total_size_gb']:.2f} GB")

        self.report['factors'] = factors_report

    def _check_factor_subdir_detail(self, dir_path: str, dir_name: str) -> Dict:
        """详细检查因子子目录"""
        result = {
            'path': dir_path,
            'name': dir_name,
            'file_count': 0,
            'total_size_mb': 0,
            'markets': {}
        }

        for market in os.listdir(dir_path):
            market_path = os.path.join(dir_path, market)
            if os.path.isdir(market_path):
                market_info = {'frequencies': {}, 'total_files': 0, 'total_size_mb': 0}

                for freq in os.listdir(market_path):
                    freq_path = os.path.join(market_path, freq)
                    if os.path.isdir(freq_path):
                        freq_info = self._count_pkl_files(freq_path)
                        market_info['frequencies'][freq] = freq_info
                        market_info['total_files'] += freq_info['file_count']
                        market_info['total_size_mb'] += freq_info['size_mb']

                market_info['total_size_mb'] = round(market_info['total_size_mb'], 2)
                result['markets'][market] = market_info
                result['file_count'] += market_info['total_files']
                result['total_size_mb'] += market_info['total_size_mb']

        result['total_size_mb'] = round(result['total_size_mb'], 2)
        return result

    def _count_pkl_files(self, dir_path: str) -> Dict:
        """统计PKL文件"""
        result = {'file_count': 0, 'size_mb': 0}
        try:
            for root, dirs, files in os.walk(dir_path):
                for f in files:
                    if f.endswith('.pkl'):
                        fp = os.path.join(root, f)
                        result['file_count'] += 1
                        result['size_mb'] += os.path.getsize(fp) / (1024 * 1024)
        except Exception:
            pass
        result['size_mb'] = round(result['size_mb'], 2)
        return result

    def _get_dir_size(self, path: str) -> int:
        """获取目录大小"""
        total = 0
        try:
            for dp, dn, fn in os.walk(path):
                for f in fn:
                    fp = os.path.join(dp, f)
                    if os.path.exists(fp):
                        total += os.path.getsize(fp)
        except Exception:
            pass
        return total

    def _add_anomaly(self, level: str, category: str, item: str, message: str, details: Any = None):
        """添加异常记录"""
        self.anomalies.append({
            'level': level,  # critical, warning, info
            'category': category,  # database, cache, checkpoint, factor
            'item': item,
            'message': message,
            'details': details,
            'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })

    def _analyze_data_quality(self):
        """分析数据质量"""
        data_report = self.report.get('data', {})

        # 检查数据连续性
        self._check_data_continuity()

        # 检查数据覆盖率
        self._check_data_coverage()

        # 检查数据一致性
        self._check_data_consistency()

        self.report['anomalies'] = self.anomalies

    def _check_data_continuity(self):
        """检查数据连续性"""
        # 检查交易日历是否有断档
        try:
            result = DB.select_one("SELECT MIN(cal_date) as min_date, MAX(cal_date) as max_date FROM astock_trade_cal WHERE is_open=1", 'tushare')
            if result:
                min_date = result.get('min_date')
                max_date = result.get('max_date')
                if min_date and max_date:
                    # 检查最新交易日是否太旧
                    try:
                        max_dt = datetime.datetime.strptime(str(max_date), '%Y%m%d')
                        days = (datetime.datetime.now() - max_dt).days
                        if days > 7:
                            self._add_anomaly('warning', 'continuity', 'astock_trade_cal',
                                            f"交易日历最新日期滞后 {days} 天", {'max_date': max_date})
                    except Exception:
                        pass
        except Exception:
            pass

        # 检查关键行情表的数据连续性
        key_tables = ['astock_price_daily', 'astock_price_daily_basic']
        for table in key_tables:
            if table in self.table_stats:
                info = self.table_stats[table]
                if info.get('min_date') and info.get('max_date'):
                    try:
                        min_dt = datetime.datetime.strptime(str(info['min_date']), '%Y%m%d')
                        max_dt = datetime.datetime.strptime(str(info['max_date']), '%Y%m%d')
                        date_range = (max_dt - min_dt).days

                        # 估算应有记录数
                        trading_days = date_range * 5 // 7  # 粗略估计交易日
                        expected = trading_days * 5000  # 约5000只股票
                        actual = info.get('record_count', 0)

                        if expected > 0 and actual > 0:
                            coverage = actual / expected
                            if coverage < 0.7:
                                self._add_anomaly('warning', 'continuity', table,
                                                f"{table} 数据覆盖率仅 {coverage*100:.1f}%",
                                                {'expected': expected, 'actual': actual})
                    except Exception:
                        pass

    def _check_data_coverage(self):
        """检查数据覆盖率"""
        # 检查股票列表覆盖
        try:
            result = DB.select_one("SELECT COUNT(*) as cnt FROM astock_basic", 'tushare')
            stock_count = result.get('cnt', 0) if result else 0
            if stock_count < 4000:
                self._add_anomaly('warning', 'coverage', 'astock_basic',
                                f"股票列表仅 {stock_count} 只，可能不完整", {'count': stock_count})
        except Exception:
            pass

    def _check_data_consistency(self):
        """检查数据一致性"""
        # 检查行情表与复权因子的一致性
        pass

    def _calculate_health_score(self):
        """计算健康分数"""
        score = 100
        issues = []

        # 数据库状态
        data_report = self.report.get('data', {})
        if data_report.get('status') != 'ok':
            score -= 30
            issues.append('数据库状态异常')

        # 关键表检查
        critical_total = data_report.get('statistics', {}).get('critical_tables_total', 0)
        critical_ok = data_report.get('statistics', {}).get('critical_tables_ok', 0)
        if critical_total > 0:
            critical_ratio = critical_ok / critical_total
            if critical_ratio < 1:
                score -= int((1 - critical_ratio) * 20)
                issues.append(f'{critical_total - critical_ok} 个关键表异常')

        # 检查点检查
        cache_report = self.report.get('cache', {})
        checkpoints_outdated = cache_report.get('statistics', {}).get('checkpoints_outdated', 0)
        if checkpoints_outdated > 0:
            score -= min(checkpoints_outdated * 2, 15)
            issues.append(f'{checkpoints_outdated} 个检查点过期')

        # 异常检查
        critical_anomalies = len([a for a in self.anomalies if a['level'] == 'critical'])
        warning_anomalies = len([a for a in self.anomalies if a['level'] == 'warning'])
        score -= critical_anomalies * 10
        score -= warning_anomalies * 3

        score = max(0, min(100, score))

        self.report['health_score'] = score
        self.report['health_issues'] = issues

        # 更新各市场健康分数
        for market in self.market_stats:
            market_issues = []
            market_score = 100

            # 检查该市场的数据库表
            db_tables = self.market_stats[market]['database']['tables_detail']
            for table in db_tables:
                if table.get('status') in ['empty', 'error']:
                    market_score -= 5
                    market_issues.append(f"{table['name']} 状态异常")

            # 检查缓存
            cache_1d = self.market_stats[market]['cache']['1d']
            cache_1m = self.market_stats[market]['cache']['1m']

            self.market_stats[market]['health']['score'] = max(0, market_score)
            self.market_stats[market]['health']['issues'] = market_issues

    def _print_summary(self):
        """打印摘要 - 按市场维度对比"""
        print("\n" + "="*120)
        print("  数据质量检查报告")
        print("="*120)

        # 健康分数
        health_score = self.report.get('health_score', 0)
        if health_score >= 80:
            health_icon = '✅'
            health_status = '良好'
        elif health_score >= 60:
            health_icon = '⚠️'
            health_status = '一般'
        else:
            health_icon = '❌'
            health_status = '需要关注'

        print(f"\n  系统健康度: {health_icon} {health_score} 分 ({health_status})")

        # 获取有数据的市场列表
        markets_with_data = self._get_markets_with_data()
        market_names = [self.MARKETS.get(m, {}).get('name', m) for m in markets_with_data]

        # 打印1d频率数据
        self._print_market_comparison_table(markets_with_data, market_names, '1d')

        # 打印1m频率数据
        self._print_market_comparison_table(markets_with_data, market_names, '1m')

        # 打印汇总数据
        self._print_market_comparison_table(markets_with_data, market_names, 'all')

        # 打印检查点状态
        self._print_checkpoints_summary()

        # 打印异常列表
        self._print_anomalies_summary()

    def _get_markets_with_data(self) -> List[str]:
        """获取有数据的市场列表"""
        markets = []
        for market in self.MARKETS.keys():
            stats = self.market_stats.get(market, {})
            db_stats = stats.get('database', {})
            cache_stats = stats.get('cache', {}).get('total', {})
            factor_stats = stats.get('factors', {}).get('total', {})

            if (db_stats.get('tables', 0) > 0 or
                cache_stats.get('files', 0) > 0 or
                factor_stats.get('files', 0) > 0):
                markets.append(market)

        # 如果没有市场有数据，返回默认市场
        if not markets:
            markets = ['cn_stock', 'cn_fund', 'cn_future', 'cn_index', 'cn_cb']

        # 按优先级排序
        markets.sort(key=lambda x: self.MARKETS.get(x, {}).get('priority', 99))
        return markets

    def _print_market_comparison_table(self, markets: List[str], market_names: List[str], freq: str):
        """打印市场对比表格"""
        freq_title = {'1d': '日频 (1d)', '1m': '分钟频 (1m)', 'all': '汇总 (all)'}[freq]

        print(f"\n{'='*120}")
        print(f"  【{freq_title}】市场数据对比")
        print(f"{'='*120}")

        # 表头
        col_width = 12
        header = f"{'指标':^14}│{'单位':^6}│" + "│".join(f"{name:^{col_width}}" for name in market_names)
        print(header)
        print("─" * len(header))

        # 数据行定义
        rows = [
            ('数据库表数', '个', 'db_tables', 'database'),
            ('数据记录数', '万', 'db_records', 'database', 10000),
            ('数据起始日', '-', 'min_date', 'database'),
            ('数据结束日', '-', 'max_date', 'database'),
            ('数据新鲜度', '天', 'freshness', 'database'),
            ('─' * 10, '─' * 4, None, None),  # 分隔线
            ('因子缓存数', '个', 'cache_files', 'cache'),
            ('缓存大小', 'MB', 'cache_size', 'cache'),
            ('─' * 10, '─' * 4, None, None),  # 分隔线
            ('因子文件数', '个', 'factor_files', 'factors'),
            ('因子大小', 'MB', 'factor_size', 'factors'),
            ('健康分数', '分', 'health_score', 'health'),
        ]

        for row in rows:
            label = row[0]
            unit = row[1]
            key = row[2] if len(row) > 2 else None
            category = row[3] if len(row) > 3 else None
            divisor = row[4] if len(row) > 4 else 1

            if key is None:  # 分隔线
                print(f"{label:^14}│{unit:^6}│" + "│".join("─" * col_width for _ in market_names))
                continue

            values = []
            for market in markets:
                stats = self.market_stats.get(market, {})

                if category == 'database':
                    cat_stats = stats.get('database', {})
                    if key == 'db_tables':
                        val = cat_stats.get('tables', 0)
                    elif key == 'db_records':
                        val = cat_stats.get('records', 0)
                        if val > 0 and divisor > 1:
                            val = round(val / divisor, 1)
                    elif key == 'min_date':
                        val = cat_stats.get('min_date', '-')
                        if val and val != '-':
                            val = str(val)[:8]
                    elif key == 'max_date':
                        val = cat_stats.get('max_date', '-')
                        if val and val != '-':
                            val = str(val)[:8]
                    elif key == 'freshness':
                        # 计算数据新鲜度
                        max_date = cat_stats.get('max_date')
                        if max_date:
                            try:
                                max_dt = datetime.datetime.strptime(str(max_date), '%Y%m%d')
                                val = (datetime.datetime.now() - max_dt).days
                            except Exception:
                                val = '-'
                        else:
                            val = '-'
                    else:
                        val = '-'

                elif category == 'cache':
                    if freq == 'all':
                        cat_stats = stats.get('cache', {}).get('total', {})
                    else:
                        cat_stats = stats.get('cache', {}).get(freq, {})

                    if key == 'cache_files':
                        val = cat_stats.get('files', 0)
                    elif key == 'cache_size':
                        val = cat_stats.get('size_mb', 0)
                        if val > 0:
                            val = round(val, 1)
                    else:
                        val = '-'

                elif category == 'factors':
                    if freq == 'all':
                        cat_stats = stats.get('factors', {}).get('total', {})
                    else:
                        cat_stats = stats.get('factors', {}).get(freq, {})

                    if key == 'factor_files':
                        val = cat_stats.get('files', 0)
                    elif key == 'factor_size':
                        val = cat_stats.get('size_mb', 0)
                        if val > 0:
                            val = round(val, 1)
                    else:
                        val = '-'

                elif category == 'health':
                    cat_stats = stats.get('health', {})
                    val = cat_stats.get('score', '-')
                else:
                    val = '-'

                # 格式化显示
                if val == 0 or val == '-' or val is None:
                    display = '-'
                elif isinstance(val, float):
                    display = f"{val:,.1f}"
                elif isinstance(val, int):
                    display = f"{val:,}"
                else:
                    display = str(val)

                values.append(display)

            row_line = f"{label:^14}│{unit:^6}│" + "│".join(f"{v:^{col_width}}" for v in values)
            print(row_line)

    def _print_checkpoints_summary(self):
        """打印检查点状态摘要"""
        cache_report = self.report.get('cache', {})
        checkpoints = cache_report.get('checkpoints', [])

        if not checkpoints:
            return

        print(f"\n{'='*120}")
        print(f"  【检查点状态】")
        print(f"{'='*120}")

        col_widths = [40, 15, 12, 10]
        header = f"{'数据表':^{col_widths[0]}}│{'最后日期':^{col_widths[1]}}│{'滞后天数':^{col_widths[2]}}│{'状态':^{col_widths[3]}}"
        print(header)
        print("─" * sum(col_widths) + "─" * (len(col_widths) + 1))

        for cp in checkpoints:
            table = cp.get('table', 'unknown')
            last_date = cp.get('last_date', 'unknown')
            days = cp.get('days_behind', '?')
            status = cp.get('status', 'unknown')
            status_icon = '✅' if status == 'ok' else '⚠️' if status == 'outdated' else '❓'

            print(f"{table:<{col_widths[0]}}│{last_date:^{col_widths[1]}}│{str(days):^{col_widths[2]}}│{status_icon:^{col_widths[3]}}")

    def _print_anomalies_summary(self):
        """打印异常摘要"""
        if not self.anomalies:
            return

        print(f"\n{'='*120}")
        print(f"  【发现异常 ({len(self.anomalies)})】")
        print(f"{'='*120}")

        # 按严重程度分组
        critical = [a for a in self.anomalies if a['level'] == 'critical']
        warning = [a for a in self.anomalies if a['level'] == 'warning']

        if critical:
            print(f"\n  🔴 严重异常 ({len(critical)}):")
            for a in critical[:5]:
                print(f"     [{a['category']}] {a['item']}: {a['message']}")
            if len(critical) > 5:
                print(f"     ... 还有 {len(critical) - 5} 个严重异常")

        if warning:
            print(f"\n  🟡 警告 ({len(warning)}):")
            for a in warning[:8]:
                print(f"     [{a['category']}] {a['item']}: {a['message']}")
            if len(warning) > 8:
                print(f"     ... 还有 {len(warning) - 8} 个警告")

    def _save_reports(self):
        """保存报告"""
        try:
            report_dir = REPORTS_DIR
            if not os.path.exists(report_dir):
                os.makedirs(report_dir, exist_ok=True)

            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            json_file = os.path.join(report_dir, f'check_report_{timestamp}.json')
            md_file = os.path.join(report_dir, f'check_report_{timestamp}.md')
            html_file = os.path.join(report_dir, f'check_report_{timestamp}.html')

            # JSON
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(self.report, f, ensure_ascii=False, indent=2, default=str)

            # Markdown
            with open(md_file, 'w', encoding='utf-8') as f:
                f.write(self._generate_markdown_report())

            # HTML
            with open(html_file, 'w', encoding='utf-8') as f:
                f.write(self._generate_html_report())

            print(f"\n  报告已保存:")
            print(f"    JSON:     {json_file}")
            print(f"    Markdown: {md_file}")
            print(f"    HTML:     {html_file}")

        except Exception as e:
            print(f"\n  [警告] 保存报告失败: {e}")

    def _generate_markdown_report(self) -> str:
        """生成Markdown报告"""
        lines = []
        lines.append("# FinHack 数据与缓存检查报告")
        lines.append("")
        lines.append(f"> 检查时间: {self.report.get('check_time', 'unknown')}")
        lines.append("")

        # 获取有数据的市场列表
        markets_with_data = self._get_markets_with_data()
        market_names = [self.MARKETS.get(m, {}).get('name', m) for m in markets_with_data]

        # 生成各频率的市场对比表
        for freq in ['1d', '1m', 'all']:
            freq_title = {'1d': '日频 (1d)', '1m': '分钟频 (1m)', 'all': '汇总 (all)'}[freq]
            lines.append(f"## {freq_title}")
            lines.append("")

            # 表头
            headers = ['指标', '单位'] + market_names
            lines.append("| " + " | ".join(headers) + " |")
            lines.append("|" + "|".join(["---"] * len(headers)) + "|")

            # 数据行
            rows = self._get_market_comparison_rows(markets_with_data, freq)
            for row in rows:
                lines.append("| " + " | ".join(row) + " |")

            lines.append("")

        # 检查点状态
        cache_report = self.report.get('cache', {})
        if cache_report.get('checkpoints'):
            lines.append("## 检查点状态")
            lines.append("")
            headers = ['数据表', '最后日期', '滞后天数', '状态']
            lines.append("| " + " | ".join(headers) + " |")
            lines.append("|" + "|".join(["---"] * len(headers)) + "|")
            for cp in cache_report['checkpoints']:
                status = '✅' if cp['status'] == 'ok' else '⚠️' if cp['status'] == 'outdated' else '❓'
                lines.append(f"| {cp['table']} | {cp['last_date']} | {cp['days_behind']} | {status} |")
            lines.append("")

        # 异常列表
        if self.anomalies:
            lines.append("## 发现问题")
            lines.append("")
            for anomaly in self.anomalies:
                level_icon = {'critical': '🔴', 'warning': '🟡', 'info': '🔵'}.get(anomaly['level'], '⚪')
                lines.append(f"- {level_icon} **[{anomaly['category']}]** {anomaly['item']}: {anomaly['message']}")
            lines.append("")

        # 建议操作
        lines.append("## 建议操作")
        lines.append("")
        if cache_report.get('statistics', {}).get('checkpoints_outdated', 0) > 0:
            lines.append("- 💡 建议运行 finhack collector run 更新数据")
        else:
            lines.append("- ✅ 数据状态良好")
        lines.append("")

        lines.append("---")
        lines.append("*报告由 FinHack 自动生成*")

        return "\n".join(lines)

    def _get_market_comparison_rows(self, markets: List[str], freq: str) -> List[List[str]]:
        """获取市场对比表格的数据行"""
        rows = []

        # 定义行配置
        row_configs = [
            ('数据库表数', '个', 'db_tables', 'database'),
            ('数据记录数', '万', 'db_records', 'database', 10000),
            ('因子缓存数', '个', 'cache_files', 'cache'),
            ('缓存大小', 'MB', 'cache_size', 'cache'),
            ('因子文件数', '个', 'factor_files', 'factors'),
            ('因子大小', 'MB', 'factor_size', 'factors'),
        ]

        for config in row_configs:
            label = config[0]
            unit = config[1]
            key = config[2]
            category = config[3]
            divisor = config[4] if len(config) > 4 else 1

            row = [label, unit]

            for market in markets:
                stats = self.market_stats.get(market, {})
                val = '-'

                if category == 'database':
                    cat_stats = stats.get('database', {})
                    if key == 'db_tables':
                        val = cat_stats.get('tables', 0)
                    elif key == 'db_records':
                        val = cat_stats.get('records', 0)
                        if val > 0 and divisor > 1:
                            val = round(val / divisor, 1)

                elif category == 'cache':
                    if freq == 'all':
                        cat_stats = stats.get('cache', {}).get('total', {})
                    else:
                        cat_stats = stats.get('cache', {}).get(freq, {})

                    if key == 'cache_files':
                        val = cat_stats.get('files', 0)
                    elif key == 'cache_size':
                        val = cat_stats.get('size_mb', 0)
                        if val > 0:
                            val = round(val, 2)

                elif category == 'factors':
                    if freq == 'all':
                        cat_stats = stats.get('factors', {}).get('total', {})
                    else:
                        cat_stats = stats.get('factors', {}).get(freq, {})

                    if key == 'factor_files':
                        val = cat_stats.get('files', 0)
                    elif key == 'factor_size':
                        val = cat_stats.get('size_mb', 0)
                        if val > 0:
                            val = round(val, 2)

                # 格式化
                if val == 0 or val == '-' or val is None:
                    row.append('-')
                elif isinstance(val, float):
                    row.append(f"{val:,.2f}")
                elif isinstance(val, int):
                    row.append(f"{val:,}")
                else:
                    row.append(str(val))

            rows.append(row)

        return rows

    def _get_cell_value_and_detail(self, market: str, freq: str, key: str, category: str, divisor: int = 1) -> tuple:
        """获取单元格值和是否有详情"""
        stats = self.market_stats.get(market, {})
        val = '-'
        has_detail = False

        if category == 'database':
            cat_stats = stats.get('database', {})
            if key == 'db_tables':
                val = cat_stats.get('tables', 0)
                has_detail = val > 0
            elif key == 'db_records':
                val = cat_stats.get('records', 0)
                if val > 0 and divisor > 1:
                    val = round(val / divisor, 1)
                has_detail = val > 0
            elif key == 'min_date':
                val = cat_stats.get('min_date', '-')
                has_detail = False
            elif key == 'max_date':
                val = cat_stats.get('max_date', '-')
                has_detail = False
            elif key == 'freshness':
                max_date = cat_stats.get('max_date')
                if max_date:
                    try:
                        max_dt = datetime.datetime.strptime(str(max_date), '%Y%m%d')
                        val = (datetime.datetime.now() - max_dt).days
                        has_detail = True
                    except Exception:
                        val = '-'

        elif category == 'cache':
            if freq == 'all':
                cat_stats = stats.get('cache', {}).get('total', {})
            else:
                cat_stats = stats.get('cache', {}).get(freq, {})

            if key == 'cache_files':
                val = cat_stats.get('files', 0)
                has_detail = val > 0
            elif key == 'cache_size':
                val = cat_stats.get('size_mb', 0)
                if val > 0:
                    val = round(val, 2)
                has_detail = val > 0
            elif key == 'cache_update':
                val = cat_stats.get('last_update', '-')
                has_detail = False

        elif category == 'factors':
            if freq == 'all':
                cat_stats = stats.get('factors', {}).get('total', {})
            else:
                cat_stats = stats.get('factors', {}).get(freq, {})

            if key == 'factor_files':
                val = cat_stats.get('files', 0)
                has_detail = val > 0
            elif key == 'factor_size':
                val = cat_stats.get('size_mb', 0)
                if val > 0:
                    val = round(val, 2)
                has_detail = val > 0

        # 格式化
        if val == 0 or val == '-' or val is None:
            return '-', False
        elif isinstance(val, float):
            return f"{val:,.2f}", has_detail
        elif isinstance(val, int):
            return f"{val:,}", has_detail
        else:
            return str(val), has_detail

    def _generate_cell_detail_html(self, market: str, freq: str, key: str, category: str) -> str:
        """生成单元格详情HTML"""
        market_name = self.MARKETS.get(market, {}).get('name', market)
        stats = self.market_stats.get(market, {})

        html = f'<div class="detail-card"><div class="detail-card-header expanded"><h4>{market_name} - 详细信息</h4><span class="toggle">▼</span></div>'
        html += '<div class="detail-card-body show">'

        if category == 'database':
            db_stats = stats.get('database', {})
            tables_detail = db_stats.get('tables_detail', [])

            # 概览信息
            html += '<div class="info-grid" style="margin-bottom:15px;">'
            html += f'<div class="info-item"><div class="label">表数量</div><div class="value">{db_stats.get("tables", 0)}</div></div>'
            html += f'<div class="info-item"><div class="label">记录总数</div><div class="value">{db_stats.get("records", 0):,}</div></div>'
            html += f'<div class="info-item"><div class="label">预估大小</div><div class="value">{db_stats.get("size_mb", 0):.1f} MB</div></div>'
            html += '</div>'

            # 表列表
            if tables_detail:
                html += f'<div class="detail-card"><div class="detail-card-header" onclick="toggleCard(this)"><h4>数据表列表 ({len(tables_detail)} 个)</h4><span class="toggle">▼</span></div>'
                html += '<div class="detail-card-body table-list">'
                html += '<table><thead><tr><th>表名</th><th>记录数</th><th>日期范围</th><th>状态</th></tr></thead><tbody>'
                for t in sorted(tables_detail, key=lambda x: -x.get('records', 0))[:50]:  # 最多显示50个
                    status = t.get('status', 'unknown')
                    status_class = 'status-ok' if status == 'ok' else 'status-warning' if status == 'stale' else 'status-error'
                    date_range = f"{t.get('min_date', '-')} ~ {t.get('max_date', '-')}" if t.get('max_date') else '-'
                    html += f'<tr><td>{t["name"]}</td><td>{t.get("records", 0):,}</td><td>{date_range}</td><td><span class="status {status_class}">{status}</span></td></tr>'
                html += '</tbody></table></div></div>'

        elif category == 'cache':
            cache_stats = stats.get('cache', {})
            if freq == 'all':
                freq_stats = cache_stats.get('total', {})
            else:
                freq_stats = cache_stats.get(freq, {})

            html += '<div class="info-grid" style="margin-bottom:15px;">'
            html += f'<div class="info-item"><div class="label">文件数量</div><div class="value">{freq_stats.get("files", 0):,}</div></div>'
            html += f'<div class="info-item"><div class="label">缓存大小</div><div class="value">{freq_stats.get("size_mb", 0):.2f} MB</div></div>'
            html += f'<div class="info-item"><div class="label">最后更新</div><div class="value">{freq_stats.get("last_update", "-")}</div></div>'
            html += '</div>'

            # 显示各频率详情
            if freq == 'all':
                for f in ['1d', '1m']:
                    f_stats = cache_stats.get(f, {})
                    if f_stats.get('files', 0) > 0:
                        html += f'<div class="detail-card"><div class="detail-card-header" onclick="toggleCard(this)"><h4>{f} 频率缓存</h4><span class="toggle">▼</span></div>'
                        html += f'<div class="detail-card-body"><div class="info-grid">'
                        html += f'<div class="info-item"><div class="label">文件数</div><div class="value">{f_stats.get("files", 0):,}</div></div>'
                        html += f'<div class="info-item"><div class="label">大小</div><div class="value">{f_stats.get("size_mb", 0):.2f} MB</div></div>'
                        html += f'<div class="info-item"><div class="label">更新时间</div><div class="value">{f_stats.get("last_update", "-")}</div></div>'
                        html += '</div></div></div>'

        elif category == 'factors':
            factor_stats = stats.get('factors', {})
            if freq == 'all':
                freq_stats = factor_stats.get('total', {})
            else:
                freq_stats = factor_stats.get(freq, {})

            html += '<div class="info-grid" style="margin-bottom:15px;">'
            html += f'<div class="info-item"><div class="label">文件数量</div><div class="value">{freq_stats.get("files", 0):,}</div></div>'
            html += f'<div class="info-item"><div class="label">数据大小</div><div class="value">{freq_stats.get("size_mb", 0):.2f} MB</div></div>'
            html += '</div>'

            if freq == 'all':
                for f in ['1d', '1m']:
                    f_stats = factor_stats.get(f, {})
                    if f_stats.get('files', 0) > 0:
                        html += f'<div class="detail-card"><div class="detail-card-header" onclick="toggleCard(this)"><h4>{f} 频率因子</h4><span class="toggle">▼</span></div>'
                        html += f'<div class="detail-card-body"><div class="info-grid">'
                        html += f'<div class="info-item"><div class="label">文件数</div><div class="value">{f_stats.get("files", 0):,}</div></div>'
                        html += f'<div class="info-item"><div class="label">大小</div><div class="value">{f_stats.get("size_mb", 0):.2f} MB</div></div>'
                        html += '</div></div></div>'

        html += '</div></div>'
        return html

    def _generate_html_report(self) -> str:
        """生成HTML报告 - 支持多级展开的交互式报告"""

        markets_with_data = self._get_markets_with_data()
        market_names = [self.MARKETS.get(m, {}).get('name', m) for m in markets_with_data]

        html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>FinHack 数据健康检查报告</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #f5f7fa; color: #333; font-size: 13px; }}
        .container {{ max-width: 1800px; margin: 0 auto; padding: 15px; }}
        h1 {{ text-align: center; color: #2c3e50; padding: 15px 0; border-bottom: 2px solid #3498db; margin-bottom: 15px; font-size: 20px; }}
        .meta {{ text-align: center; color: #7f8c8d; margin-bottom: 15px; font-size: 12px; }}

        /* 可折叠项 */
        .collapsible-header {{ cursor: pointer; user-select: none; position: relative; }}
        .collapsible-header:hover {{ background: #e8e8e8 !important; }}
        .arrow {{ display: inline-block; width: 16px; font-size: 10px; color: #666; transition: transform 0.2s; }}
        .arrow.expanded {{ transform: rotate(90deg); }}
        .collapsible-content {{ display: none; }}
        .collapsible-content.show {{ display: block; }}

        /* 数据卡片 */
        .data-card {{ background: white; border-radius: 6px; margin-bottom: 10px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); border: 1px solid #e0e0e0; }}
        .card-header {{ padding: 10px 12px; background: #fafafa; border-bottom: 1px solid #eee; display: flex; justify-content: space-between; align-items: center; }}
        .card-header:hover {{ background: #f0f0f0; }}
        .card-body {{ padding: 10px 12px; }}

        /* 标签 */
        .badge {{ display: inline-block; padding: 2px 8px; border-radius: 3px; font-size: 11px; font-weight: bold; }}
        .badge-sqlite {{ background: #0f80cc; color: white; }}
        .badge-parquet {{ background: #e040fb; color: white; }}
        .badge-csv-time {{ background: #217346; color: white; }}
        .badge-csv-code {{ background: #4caf50; color: white; }}
        .badge-pkl {{ background: #ff6f00; color: white; }}

        /* 健康指示器 */
        .health-bar {{ height: 4px; background: #eee; border-radius: 2px; overflow: hidden; margin-top: 5px; }}
        .health-bar .fill {{ height: 100%; transition: width 0.3s; }}
        .health-good {{ background: #4caf50; }}
        .health-warning {{ background: #ff9800; }}
        .health-error {{ background: #f44336; }}

        /* 年份网格 */
        .year-grid {{ display: flex; flex-wrap: wrap; gap: 3px; margin: 8px 0; }}
        .year-cell {{ width: 45px; height: 24px; display: flex; align-items: center; justify-content: center; font-size: 10px; border-radius: 3px; cursor: pointer; }}
        .year-complete {{ background: #c8e6c9; color: #2e7d32; }}
        .year-partial {{ background: #fff9c4; color: #f57f17; }}
        .year-missing {{ background: #ffcdd2; color: #c62828; }}
        .year-empty {{ background: #f5f5f5; color: #999; }}

        /* 统计信息 */
        .stat-row {{ display: flex; gap: 15px; margin: 5px 0; }}
        .stat-item {{ flex: 1; }}
        .stat-label {{ font-size: 11px; color: #666; }}
        .stat-value {{ font-size: 14px; font-weight: 600; }}

        /* 表格 */
        table {{ width: 100%; border-collapse: collapse; font-size: 12px; }}
        th, td {{ padding: 6px 8px; text-align: left; border-bottom: 1px solid #eee; }}
        th {{ background: #f5f5f5; font-weight: 600; }}

        /* 市场卡片 */
        .market-card {{ background: #fafafa; border-radius: 4px; padding: 8px; margin: 5px 0; border: 1px solid #e0e0e0; }}
        .market-header {{ display: flex; justify-content: space-between; align-items: center; padding: 5px 0; }}

        /* 详情面板 */
        .detail-panel {{ background: white; border: 1px solid #ddd; border-radius: 4px; margin-top: 8px; padding: 10px; }}

        .no-data {{ color: #ccc; font-style: italic; }}
        .clickable {{ cursor: pointer; }}
        .clickable:hover {{ text-decoration: underline; color: #1976d2; }}

        .issues {{ background: #fff3cd; border-left: 3px solid #ffc107; padding: 10px; margin: 15px 0; border-radius: 3px; }}
        footer {{ text-align: center; color: #95a5a6; padding: 15px; margin-top: 15px; border-top: 1px solid #eee; font-size: 12px; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 FinHack 数据健康检查报告</h1>
        <p class="meta">检查时间: {self.report.get('check_time', 'unknown')} | 点击任意项目可展开查看详情</p>

        <script>
        function toggle(headerEl) {{
            // 找到父容器
            var container = headerEl.closest('.collapsible-container');
            if (!container) return;

            // 找到箭头和内容
            var arrow = headerEl.querySelector('.arrow');
            var content = container.querySelector('.collapsible-content');

            if (content) {{
                var isShown = content.classList.toggle('show');
                if (arrow) {{
                    arrow.classList.toggle('expanded', isShown);
                }}
            }}
        }}
        </script>
"""

        # 1. SQLite 数据库详情
        html += self._html_sqlite_detail(markets_with_data, market_names)

        # 2. Parquet 文件详情
        html += self._html_parquet_detail(markets_with_data, market_names)

        # 3. CSV 文件详情 (区分timebased和codebased)
        html += self._html_csv_detail(markets_with_data, market_names)

        # 4. PKL 文件详情
        html += self._html_pkl_detail(markets_with_data, market_names)

        # 5. 检查点
        cache_report = self.report.get('cache', {})
        if cache_report.get('checkpoints'):
            html += self._html_checkpoints_detail(cache_report['checkpoints'])

        # 6. 异常
        if self.anomalies:
            html += f"""
        <div class="issues">
            <strong>⚠️ 发现 {len(self.anomalies)} 个问题</strong>
            <div style="max-height:150px; overflow-y:auto; margin-top:8px; font-size:12px;">
"""
            for a in self.anomalies[:30]:
                icon = {'critical': '🔴', 'warning': '🟡', 'info': '🔵'}.get(a['level'], '⚪')
                html += f"<div>{icon} [{a['category']}] {a['item']}: {a['message']}</div>"
            if len(self.anomalies) > 30:
                html += f"<div style='color:#666;'>... 还有 {len(self.anomalies) - 30} 个问题</div>"
            html += "</div></div>"

        html += """
        <footer>报告由 <strong>FinHack</strong> 自动生成</footer>
    </div>
</body>
</html>
"""
        return html

    def _html_sqlite_detail(self, markets: List[str], market_names: List[str]) -> str:
        """生成SQLite详情 - 可展开查看每个表的详细信息"""
        html = """
        <div class="data-card collapsible-container">
            <div class="card-header collapsible-header" onclick="toggle(this)">
                <span><span class="arrow expanded">▶</span><span class="badge badge-sqlite">SQLite</span> 数据库数据</span>
                <span style="font-size:12px;color:#666;">点击展开/收起</span>
            </div>
            <div class="collapsible-content show">
"""
        for market, name in zip(markets, market_names):
            db_stats = self.market_stats.get(market, {}).get('database', {})
            tables = db_stats.get('tables', 0)
            records = db_stats.get('records', 0)
            tables_detail = db_stats.get('tables_detail', [])

            if tables == 0:
                continue

            html += f"""
                <div class="market-card collapsible-container">
                    <div class="market-header collapsible-header" onclick="toggle(this)">
                        <span><span class="arrow">▶</span><strong>{name}</strong></span>
                        <span>{tables} 表 / {records:,} 条记录</span>
                    </div>
                    <div class="collapsible-content">
                        <div class="detail-panel">
                            <table>
                                <thead><tr><th>表名</th><th>记录数</th><th>日期范围</th><th>新鲜度</th><th>状态</th></tr></thead>
                                <tbody>
"""
            for t in sorted(tables_detail, key=lambda x: -x.get('records', 0)):
                date_range = f"{t.get('min_date', '-')} ~ {t.get('max_date', '-')}" if t.get('max_date') else '-'
                freshness = f"{t.get('freshness_days', '-')} 天" if t.get('freshness_days') else '-'
                status = t.get('status', 'unknown')
                status_icon = '✅' if status == 'ok' else '⚠️' if status == 'stale' else '❌'
                html += f"<tr><td>{t['name']}</td><td>{t.get('records', 0):,}</td><td>{date_range}</td><td>{freshness}</td><td>{status_icon}</td></tr>"
            html += "</tbody></table></div></div></div>"

        html += "</div></div>"
        return html

    def _html_parquet_detail(self, markets: List[str], market_names: List[str]) -> str:
        """生成Parquet详情 - 可展开查看每年的数据分布"""
        html = """
        <div class="data-card collapsible-container">
            <div class="card-header collapsible-header" onclick="toggle(this)">
                <span><span class="arrow">▶</span><span class="badge badge-parquet">Parquet</span> 文件数据 (codebased)</span>
                <span style="font-size:12px;color:#666;">按代码组织的parquet文件</span>
            </div>
            <div class="collapsible-content">
"""
        for market, name in zip(markets, market_names):
            for freq in ['1d', '1m']:
                stats = self.market_stats.get(market, {}).get('files', {}).get('parquet', {}).get(freq, {})
                files = stats.get('files', 0)
                size = stats.get('size_mb', 0)

                if files == 0:
                    continue

                # 获取年份分布（从文件名推断）
                years = self._get_parquet_years(market, freq)

                html += f"""
                <div class="market-card collapsible-container">
                    <div class="market-header collapsible-header" onclick="toggle(this)">
                        <span><span class="arrow">▶</span><strong>{name} - {freq}</strong></span>
                        <span>{files} 个文件 / {size:.1f} MB</span>
                    </div>
                    <div class="collapsible-content">
                        <div class="detail-panel">
                            <div class="stat-row">
                                <div class="stat-item"><div class="stat-label">文件数量</div><div class="stat-value">{files:,}</div></div>
                                <div class="stat-item"><div class="stat-label">数据大小</div><div class="stat-value">{size:.1f} MB</div></div>
                                <div class="stat-item"><div class="stat-label">年份范围</div><div class="stat-value">{years.get('min', '-')} ~ {years.get('max', '-')}</div></div>
                            </div>
                            <div style="margin-top:10px;"><strong>年份分布:</strong></div>
                            <div class="year-grid">
"""
                # 生成年份网格
                for year, count in sorted(years.get('years', {}).items()):
                    css_class = 'year-complete' if count > 0 else 'year-empty'
                    html += f"<div class='year-cell {css_class}' title='{year}: {count} 个文件'>{year}</div>"

                html += "</div></div></div></div>"

        html += "</div></div>"
        return html

    def _get_parquet_years(self, market: str, freq: str) -> dict:
        """获取parquet文件的年份分布"""
        years = {}
        try:
            kline_dir = KLINE_DIR if 'KLINE_DIR' in dir() else os.path.join(DATA_DIR, 'market', 'kline')
            market_dir = os.path.join(kline_dir, 'codebased', market, freq)
            if os.path.exists(market_dir):
                for f in os.listdir(market_dir):
                    if f.endswith('.parquet'):
                        # 从文件名提取年份
                        try:
                            year = f.split('_')[0][:4]
                            if year.isdigit():
                                years[year] = years.get(year, 0) + 1
                        except:
                            pass
        except:
            pass

        if years:
            return {'years': years, 'min': min(years.keys()), 'max': max(years.keys())}
        return {'years': {}, 'min': '-', 'max': '-'}

    def _html_csv_detail(self, markets: List[str], market_names: List[str]) -> str:
        """生成CSV详情 - 区分timebased和codebased"""
        html = """
        <div class="data-card collapsible-container">
            <div class="card-header collapsible-header" onclick="toggle(this)">
                <span><span class="arrow">▶</span><span class="badge badge-csv-time">CSV</span> 文件数据</span>
                <span style="font-size:12px;color:#666;">timebased + codebased</span>
            </div>
            <div class="collapsible-content">
"""

        # Time-based CSV
        html += "<div style='padding:8px 0;font-weight:600;color:#217346;'>📅 时间序列 (Time-Based)</div>"
        for market, name in zip(markets, market_names):
            for freq in ['1d', '1m']:
                stats = self.market_stats.get(market, {}).get('files', {}).get('csv_timebased', {}).get(freq, {})
                files = stats.get('files', 0)
                size = stats.get('size_mb', 0)

                if files == 0:
                    continue

                # 获取日期分布
                date_info = self._get_csv_date_range(market, freq)

                html += f"""
                <div class="market-card collapsible-container">
                    <div class="market-header collapsible-header" onclick="toggle(this)">
                        <span><span class="arrow">▶</span><strong>{name} - {freq}</strong></span>
                        <span>{files:,} 个文件 / {size:.1f} MB</span>
                    </div>
                    <div class="collapsible-content">
                        <div class="detail-panel">
                            <div class="stat-row">
                                <div class="stat-item"><div class="stat-label">文件数量</div><div class="stat-value">{files:,}</div></div>
                                <div class="stat-item"><div class="stat-label">数据大小</div><div class="stat-value">{size:.1f} MB</div></div>
                                <div class="stat-item"><div class="stat-label">数据范围</div><div class="stat-value">{date_info.get('min', '-')}</div></div>
                                <div class="stat-item"><div class="stat-label">最新日期</div><div class="stat-value">{date_info.get('max', '-')}</div></div>
                            </div>
                            <div style="margin-top:8px;">
                                <strong>健康度:</strong>
                                <div class="health-bar"><div class="fill health-{date_info.get('health', 'good')}" style="width:{date_info.get('health_pct', 100)}%"></div></div>
                                <span style="font-size:11px;color:#666;">{date_info.get('health_msg', '')}</span>
                            </div>
                        </div>
                    </div>
                </div>
"""

        # Code-based CSV (如果有)
        html += "<div style='padding:15px 0 8px;font-weight:600;color:#4caf50;'>📊 代码序列 (Code-Based)</div>"
        has_codebased = False
        for market, name in zip(markets, market_names):
            stats = self.market_stats.get(market, {}).get('files', {}).get('csv_codebased', {}).get('total', {})
            files = stats.get('files', 0)
            size = stats.get('size_mb', 0)

            if files > 0:
                has_codebased = True
                html += f"""
                <div class="market-card">
                    <div class="market-header">
                        <strong>{name}</strong>
                        <span>{files:,} 个文件 / {size:.1f} MB</span>
                    </div>
                </div>
"""
        if not has_codebased:
            html += "<div style='color:#999;padding:5px;'>暂无codebased CSV数据</div>"

        html += "</div></div>"
        return html

    def _get_csv_date_range(self, market: str, freq: str) -> dict:
        """获取CSV文件的日期范围"""
        try:
            kline_dir = KLINE_DIR if 'KLINE_DIR' in dir() else os.path.join(DATA_DIR, 'market', 'kline')
            base_dir = os.path.join(kline_dir, 'timebased', market, freq)

            if not os.path.exists(base_dir):
                return {'min': '-', 'max': '-', 'health': 'good', 'health_pct': 100, 'health_msg': '无数据'}

            # 找最新和最早的目录
            years = sorted([d for d in os.listdir(base_dir) if d.isdigit()])
            if not years:
                return {'min': '-', 'max': '-', 'health': 'good', 'health_pct': 100, 'health_msg': '无数据'}

            min_year = years[0]
            max_year = years[-1]

            # 获取最新月份
            max_year_dir = os.path.join(base_dir, max_year)
            months = sorted([d for d in os.listdir(max_year_dir) if d.isdigit()]) if os.path.exists(max_year_dir) else []
            max_month = months[-1] if months else '01'

            # 获取最新日期
            max_month_dir = os.path.join(max_year_dir, max_month)
            days = sorted([d for d in os.listdir(max_month_dir) if d.isdigit()]) if os.path.exists(max_month_dir) else []
            max_day = days[-1] if days else '01'

            max_date = f"{max_year}-{max_month}-{max_day}"

            # 计算健康度
            try:
                from datetime import datetime, timedelta
                last_date = datetime.strptime(max_date, "%Y-%m-%d")
                days_behind = (datetime.now() - last_date).days

                if days_behind <= 7:
                    health = 'good'
                    health_pct = 100
                    health_msg = f'数据最新 ({days_behind}天前)'
                elif days_behind <= 30:
                    health = 'warning'
                    health_pct = 70
                    health_msg = f'数据滞后 {days_behind} 天'
                else:
                    health = 'error'
                    health_pct = max(30, 100 - days_behind)
                    health_msg = f'⚠️ 数据严重滞后 {days_behind} 天'
            except:
                health = 'good'
                health_pct = 100
                health_msg = ''

            return {
                'min': min_year,
                'max': max_date,
                'health': health,
                'health_pct': health_pct,
                'health_msg': health_msg
            }
        except:
            return {'min': '-', 'max': '-', 'health': 'good', 'health_pct': 100, 'health_msg': ''}

    def _html_pkl_detail(self, markets: List[str], market_names: List[str]) -> str:
        """生成PKL详情"""
        html = """
        <div class="data-card collapsible-container">
            <div class="card-header collapsible-header" onclick="toggle(this)">
                <span><span class="arrow">▶</span><span class="badge badge-pkl">PKL</span> Pickle文件数据</span>
                <span style="font-size:12px;color:#666;">缓存 + 因子数据</span>
            </div>
            <div class="collapsible-content">
"""
        for market, name in zip(markets, market_names):
            for freq in ['1d', '1m']:
                stats = self.market_stats.get(market, {}).get('files', {}).get('pkl', {}).get(freq, {})
                files = stats.get('files', 0)
                size = stats.get('size_mb', 0)

                if files == 0:
                    continue

                html += f"""
                <div class="market-card collapsible-container">
                    <div class="market-header collapsible-header" onclick="toggle(this)">
                        <span><span class="arrow">▶</span><strong>{name} - {freq}</strong></span>
                        <span>{files:,} 个文件 / {size:.1f} MB</span>
                    </div>
                    <div class="collapsible-content">
                        <div class="detail-panel">
                            <div class="stat-row">
                                <div class="stat-item"><div class="stat-label">文件数量</div><div class="stat-value">{files:,}</div></div>
                                <div class="stat-item"><div class="stat-label">数据大小</div><div class="stat-value">{size:.1f} MB</div></div>
                            </div>
                        </div>
                    </div>
                </div>
"""

        html += "</div></div>"
        return html

    def _html_checkpoints_detail(self, checkpoints: List[dict]) -> str:
        """生成检查点详情"""
        html = """
        <div class="data-card collapsible-container">
            <div class="card-header collapsible-header" onclick="toggle(this)">
                <span><span class="arrow">▶</span><span class="badge badge-sqlite">检查点</span> 数据采集状态</span>
                <span style="font-size:12px;color:#666;">各表的数据更新状态</span>
            </div>
            <div class="collapsible-content">
                <table>
                    <thead><tr><th>数据表</th><th>最后日期</th><th>滞后天数</th><th>健康度</th><th>状态</th></tr></thead>
                    <tbody>
"""
        for cp in checkpoints:
            days = cp['days_behind']
            if days <= 7:
                health = '<div class="health-bar"><div class="fill health-good" style="width:100%"></div></div>'
            elif days <= 30:
                health = '<div class="health-bar"><div class="fill health-warning" style="width:60%"></div></div>'
            else:
                health = f'<div class="health-bar"><div class="fill health-error" style="width:{max(10, 100-days)}%"></div></div>'

            status = '⚠️ 过期' if cp['status'] == 'outdated' else '✅ 正常'
            html += f"<tr><td>{cp['table']}</td><td>{cp['last_date']}</td><td>{days} 天</td><td>{health}</td><td>{status}</td></tr>"

        html += "</tbody></table></div></div>"
        return html
