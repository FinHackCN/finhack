#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
通用数据库分析器
支持SQLite和MySQL等数据库的表结构和数据分析
"""

import os
import time
import pandas as pd
from typing import Dict, Any, List, Optional
from datetime import datetime

import finhack.library.log as Log
from finhack.library.config import Config
from finhack.library.db_adpter.adapter_factory import DbAdapterFactory


class DatabaseAnalyzer:
    """数据库分析器 - 支持多种数据库类型"""
    
    def __init__(self, connection_name: str):
        """
        初始化数据库分析器
        
        Args:
            connection_name: 数据库连接名称（对应db.conf中的section）
        """
        self.connection_name = connection_name
        self.adapter = DbAdapterFactory.get_adapter(connection_name)
        self.db_config = Config.get_config('db', connection_name)
        self.db_type = self.db_config.get('type', 'sqlite').lower()
        
    def get_all_tables(self) -> List[str]:
        """获取所有表名"""
        try:
            if self.db_type == 'sqlite':
                sql = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            elif self.db_type == 'mysql':
                sql = f"SELECT table_name AS name FROM information_schema.tables WHERE table_schema = '{self.db_config['db']}'"
            else:
                Log.logger.error(f"不支持的数据库类型: {self.db_type}")
                return []
            
            results = self.adapter.select_to_list(sql)
            return sorted([row['name'] for row in results])
        except Exception as e:
            Log.logger.error(f"获取表列表失败: {e}")
            return []
    
    def get_table_size_bytes(self, table_name: str) -> int:
        """获取表的大小（字节）"""
        try:
            if self.db_type == 'sqlite':
                # SQLite使用DBSTAT虚拟表或页数估算
                try:
                    sql = f"SELECT SUM(pgsize) as size FROM dbstat WHERE name='{table_name}'"
                    result = self.adapter.select_one(sql)
                    if result and result.get('size'):
                        return int(result['size'])
                except:
                    # DBSTAT不可用时，使用行数估算
                    sql = f"SELECT COUNT(*) as count FROM [{table_name}]"
                    result = self.adapter.select_one(sql)
                    return result['count'] * 100 if result else 0
            
            elif self.db_type == 'mysql':
                sql = f"""
                SELECT 
                    ROUND(((data_length + index_length) / 1024 / 1024), 2) * 1024 * 1024 as size
                FROM information_schema.tables 
                WHERE table_schema = '{self.db_config['db']}' 
                AND table_name = '{table_name}'
                """
                result = self.adapter.select_one(sql)
                return int(result['size']) if result and result.get('size') else 0
            
            return 0
        except Exception as e:
            Log.logger.warning(f"获取表 {table_name} 大小失败: {e}")
            return 0
    
    def get_table_columns(self, table_name: str) -> List[str]:
        """获取表的列名"""
        try:
            if self.db_type == 'sqlite':
                sql = f"PRAGMA table_info([{table_name}])"
                results = self.adapter.select_to_list(sql)
                return [row['name'] for row in results]
            elif self.db_type == 'mysql':
                sql = f"SHOW COLUMNS FROM {table_name}"
                results = self.adapter.select_to_list(sql)
                return [row['Field'] for row in results]
            return []
        except Exception as e:
            Log.logger.warning(f"获取表 {table_name} 列信息失败: {e}")
            return []
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """获取表的详细信息"""
        Log.logger.info(f"正在分析表: {table_name}")
        start_time = time.time()
        
        try:
            # 获取表结构
            columns = self.get_table_columns(table_name)
            
            # 获取行数
            sql = f"SELECT COUNT(*) as count FROM [{table_name}]" if self.db_type == 'sqlite' else f"SELECT COUNT(*) as count FROM {table_name}"
            row_count_result = self.adapter.select_one(sql)
            row_count = row_count_result['count'] if row_count_result else 0
            
            if row_count == 0:
                return {
                    'table_name': table_name,
                    'row_count': 0,
                    'size_bytes': 0,
                    'size_mb': 0,
                    'columns': columns,
                    'unique_ts_code': 0,
                    'unique_trade_date': 0,
                    'ts_code_x_trade_date': 0,
                    'has_ts_code': False,
                    'has_trade_date': False,
                    'analysis_time': time.time() - start_time
                }
            
            # 检查是否有特定列
            has_ts_code = 'ts_code' in columns
            has_trade_date = 'trade_date' in columns
            
            unique_ts_code = 0
            unique_trade_date = 0
            
            # 获取去重后的ts_code数量
            if has_ts_code:
                try:
                    if self.db_type == 'sqlite':
                        sql = f"SELECT COUNT(DISTINCT ts_code) as count FROM [{table_name}] WHERE ts_code IS NOT NULL"
                    else:
                        sql = f"SELECT COUNT(DISTINCT ts_code) as count FROM {table_name} WHERE ts_code IS NOT NULL"
                    result = self.adapter.select_one(sql)
                    unique_ts_code = result['count'] if result else 0
                except Exception as e:
                    Log.logger.warning(f"获取{table_name}的ts_code统计失败: {e}")
                    unique_ts_code = 0
            
            # 获取去重后的trade_date数量
            if has_trade_date:
                try:
                    if self.db_type == 'sqlite':
                        sql = f"SELECT COUNT(DISTINCT trade_date) as count FROM [{table_name}] WHERE trade_date IS NOT NULL"
                    else:
                        sql = f"SELECT COUNT(DISTINCT trade_date) as count FROM {table_name} WHERE trade_date IS NOT NULL"
                    result = self.adapter.select_one(sql)
                    unique_trade_date = result['count'] if result else 0
                except Exception as e:
                    Log.logger.warning(f"获取{table_name}的trade_date统计失败: {e}")
                    unique_trade_date = 0
            
            # 获取表大小
            size_bytes = self.get_table_size_bytes(table_name)
            size_mb = size_bytes / 1024 / 1024
            
            analysis_time = time.time() - start_time
            
            result = {
                'table_name': table_name,
                'row_count': row_count,
                'size_bytes': size_bytes,
                'size_mb': size_mb,
                'columns': columns,
                'unique_ts_code': unique_ts_code,
                'unique_trade_date': unique_trade_date,
                'ts_code_x_trade_date': unique_ts_code * unique_trade_date,
                'has_ts_code': has_ts_code,
                'has_trade_date': has_trade_date,
                'analysis_time': analysis_time
            }
            
            Log.logger.info(f"  完成分析 ({analysis_time:.2f}秒): 行数={row_count:,}, ts_code={unique_ts_code}, trade_date={unique_trade_date}")
            return result
            
        except Exception as e:
            Log.logger.error(f"分析表 {table_name} 失败: {e}")
            return {
                'table_name': table_name,
                'row_count': 0,
                'size_bytes': 0,
                'size_mb': 0,
                'columns': [],
                'unique_ts_code': 0,
                'unique_trade_date': 0,
                'ts_code_x_trade_date': 0,
                'has_ts_code': False,
                'has_trade_date': False,
                'analysis_time': time.time() - start_time,
                'error': str(e)
            }
    
    def analyze_all_tables(self) -> List[Dict[str, Any]]:
        """分析所有表"""
        tables = self.get_all_tables()
        Log.logger.info(f"发现 {len(tables)} 个表")
        
        results = []
        for i, table in enumerate(tables, 1):
            print(f"[{i}/{len(tables)}] ", end="", flush=True)
            table_info = self.get_table_info(table)
            results.append(table_info)
        
        return results
    
    def generate_report(self, results: List[Dict[str, Any]]) -> str:
        """生成分析报告"""
        report_lines = []
        report_lines.append(f"{self.db_type.upper()} 数据库分析报告")
        report_lines.append("=" * 80)
        report_lines.append(f"分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"数据库连接: {self.connection_name}")
        report_lines.append(f"数据库类型: {self.db_type.upper()}")
        
        # 显示数据库路径或连接信息
        if self.db_type == 'sqlite':
            db_path = self.db_config.get('path', '')
            if os.path.exists(db_path):
                file_size_gb = os.path.getsize(db_path) / 1024 / 1024 / 1024
                report_lines.append(f"数据库文件: {db_path}")
                report_lines.append(f"文件大小: {file_size_gb:.2f} GB")
        elif self.db_type == 'mysql':
            report_lines.append(f"主机: {self.db_config.get('host', '')}:{self.db_config.get('port', '')}")
            report_lines.append(f"数据库: {self.db_config.get('db', '')}")
        
        report_lines.append(f"总表数: {len(results)}")
        report_lines.append("")
        
        # 统计信息
        total_rows = sum(r['row_count'] for r in results)
        total_size_mb = sum(r['size_mb'] for r in results)
        tables_with_ts_code = sum(1 for r in results if r['has_ts_code'])
        tables_with_trade_date = sum(1 for r in results if r['has_trade_date'])
        
        report_lines.append("总体统计:")
        report_lines.append(f"  总行数: {total_rows:,}")
        report_lines.append(f"  总大小: {total_size_mb:.2f} MB")
        report_lines.append(f"  包含ts_code的表数: {tables_with_ts_code}")
        report_lines.append(f"  包含trade_date的表数: {tables_with_trade_date}")
        report_lines.append("")
        
        # 详细表信息
        report_lines.append("详细表信息:")
        report_lines.append("-" * 120)
        header = f"{'表名':<30} {'行数':>15} {'大小(MB)':>12} {'ts_code数':>12} {'trade_date数':>15} {'乘积':>15}"
        report_lines.append(header)
        report_lines.append("-" * 120)
        
        # 按行数排序
        sorted_results = sorted(results, key=lambda x: x['row_count'], reverse=True)
        
        for result in sorted_results:
            line = f"{result['table_name']:<30} {result['row_count']:>15,} {result['size_mb']:>12.2f} "
            
            if result['has_ts_code']:
                line += f"{result['unique_ts_code']:>12,}"
            else:
                line += f"{'N/A':>12}"
            
            if result['has_trade_date']:
                line += f"{result['unique_trade_date']:>15,}"
            else:
                line += f"{'N/A':>15}"
            
            line += f"{result['ts_code_x_trade_date']:>15,}"
            
            if 'error' in result:
                line += f" (错误: {result['error']})"
            
            report_lines.append(line)
        
        report_lines.append("-" * 120)
        
        # Top 10表格
        report_lines.append("")
        report_lines.append("Top 10 最大表 (按行数):")
        for i, result in enumerate(sorted_results[:10], 1):
            report_lines.append(f"{i:2}. {result['table_name']}: {result['row_count']:,} 行, {result['size_mb']:.2f} MB")
        
        # 显示乘积最大的表
        if any(r['has_ts_code'] and r['has_trade_date'] for r in results):
            report_lines.append("")
            report_lines.append("Top 10 最大乘积 (ts_code × trade_date):")
            ts_trade_sorted = sorted(
                [r for r in results if r['has_ts_code'] and r['has_trade_date']], 
                key=lambda x: x['ts_code_x_trade_date'], 
                reverse=True
            )
            for i, result in enumerate(ts_trade_sorted[:10], 1):
                report_lines.append(f"{i:2}. {result['table_name']}: {result['unique_ts_code']:,} × {result['unique_trade_date']:,} = {result['ts_code_x_trade_date']:,}")
        
        return "\n".join(report_lines)
    
    def save_to_csv(self, results: List[Dict[str, Any]], filename: str):
        """保存结果到CSV文件"""
        df_data = []
        for result in results:
            df_data.append({
                '表名': result['table_name'],
                '行数': result['row_count'],
                '大小(MB)': result['size_mb'],
                '列数': len(result['columns']),
                '包含ts_code': result['has_ts_code'],
                '包含trade_date': result['has_trade_date'],
                '去重ts_code数': result['unique_ts_code'] if result['has_ts_code'] else None,
                '去重trade_date数': result['unique_trade_date'] if result['has_trade_date'] else None,
                'ts_code×trade_date': result['ts_code_x_trade_date'] if (result['has_ts_code'] and result['has_trade_date']) else None,
                '分析耗时(秒)': result['analysis_time'],
                '错误信息': result.get('error', '')
            })
        
        df = pd.DataFrame(df_data)
        df.to_csv(filename, index=False, encoding='utf-8')
        Log.logger.info(f"结果已保存到: {filename}")
        print(f"结果已保存到: {filename}")
    
    def get_database_file_size(self) -> Optional[float]:
        """获取数据库文件大小（GB）"""
        if self.db_type == 'sqlite':
            db_path = self.db_config.get('path', '')
            if os.path.exists(db_path):
                return os.path.getsize(db_path) / 1024 / 1024 / 1024
        return None
