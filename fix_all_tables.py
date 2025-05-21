import os
import sys
import sqlite3
import pandas as pd
import argparse
import logging
from datetime import datetime
import time

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"fix_all_tables_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("fix_all_tables")

# 不同类型表的唯一约束定义
TABLE_CONSTRAINTS = {
    # 价格表
    'price_daily': ['ts_code', 'trade_date'],
    'price_weekly': ['ts_code', 'trade_date'],
    'price_monthly': ['ts_code', 'trade_date'],
    
    # 财务表
    'income': ['ts_code', 'end_date'],
    'balancesheet': ['ts_code', 'end_date'],
    'cashflow': ['ts_code', 'end_date'],
    'forecast': ['ts_code', 'ann_date', 'end_date'],
    'express': ['ts_code', 'end_date'],
    'fina_indicator': ['ts_code', 'end_date'],
    'fina_audit': ['ts_code', 'end_date'],
    'fina_mainbz': ['ts_code', 'end_date'],
    'dividend': ['ts_code', 'end_date', 'div_proc'],
    
    # 市场数据
    'margin': ['ts_code', 'trade_date'],
    'margin_detail': ['ts_code', 'trade_date'],
    'top_list': ['ts_code', 'trade_date'],
    'top_inst': ['ts_code', 'trade_date'],
    'pledge_stat': ['ts_code', 'end_date'],
    'pledge_detail': ['ts_code', 'ann_date'],
    'repurchase': ['ts_code', 'ann_date'],
    'concept': ['code', 'src'],
    'concept_detail': ['id', 'ts_code'],
    'share_float': ['ts_code', 'ann_date'],
    'block_trade': ['ts_code', 'trade_date', 'transaction_date'],
    'stk_holdernumber': ['ts_code', 'end_date'],
    'stk_holdertrade': ['ts_code', 'ann_date', 'holder_name'],
    'top10_holders': ['ts_code', 'end_date', 'holder_name'],
    'top10_floatholders': ['ts_code', 'end_date', 'holder_name'],
    
    # 指数数据
    'index_daily': ['ts_code', 'trade_date'],
    'index_weekly': ['ts_code', 'trade_date'],
    'index_monthly': ['ts_code', 'trade_date'],
    'index_weight': ['index_code', 'trade_date', 'con_code'],
    'index_dailybasic': ['ts_code', 'trade_date'],
    'index_member': ['index_code', 'con_code', 'in_date'],
    
    # 基金数据
    'fund_nav': ['ts_code', 'end_date'],
    'fund_div': ['ts_code', 'ann_date', 'div_proc'],
    'fund_share': ['ts_code', 'end_date'],
    
    # 可转债数据
    'cb_daily': ['ts_code', 'trade_date'],
    'cb_issue': ['ts_code', 'ann_date'],
    
    # 外汇数据
    'fx_daily': ['ts_code', 'trade_date'],
    
    # 其他数据表
    'adj_factor': ['ts_code', 'trade_date'],
    'daily_basic': ['ts_code', 'trade_date'],
    'moneyflow': ['ts_code', 'trade_date'],
    'suspend_d': ['ts_code', 'suspend_date'],
    'namechange': ['ts_code', 'start_date', 'name'],
    'hs_const': ['ts_code', 'in_date'],
    'stk_managers': ['ts_code', 'ann_date', 'name'],
    'stk_rewards': ['ts_code', 'end_date', 'name'],
    'new_share': ['ts_code', 'issue_date'],
    'trade_cal': ['exchange', 'cal_date'],
}

def get_table_structure(conn, table_name):
    """获取表结构"""
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [row[1] for row in cursor.fetchall()]
    return columns

def identify_unique_constraint(table_name, columns):
    """识别表的唯一约束组合"""
    # 尝试根据表名匹配预定义约束
    for pattern, constraint_cols in TABLE_CONSTRAINTS.items():
        if pattern in table_name:
            # 验证所有约束列是否存在
            if all(col in columns for col in constraint_cols):
                return constraint_cols
    
    # 默认策略: 如果表包含ts_code和trade_date，使用它们
    if 'ts_code' in columns and 'trade_date' in columns:
        return ['ts_code', 'trade_date']
    
    # 如果表包含ts_code和end_date，使用它们
    if 'ts_code' in columns and 'end_date' in columns:
        return ['ts_code', 'end_date']
    
    # 如果表包含ts_code和ann_date，使用它们
    if 'ts_code' in columns and 'ann_date' in columns:
        return ['ts_code', 'ann_date']
    
    # 对于基础表，使用主键/code字段
    if 'ts_code' in columns:
        return ['ts_code']
    elif 'code' in columns:
        return ['code']
    
    # 无法确定唯一约束
    return None

def check_for_duplicates(conn, table_name, constraint_cols):
    """检查表中是否有重复记录"""
    if not constraint_cols:
        return False
    
    cursor = conn.cursor()
    
    # 构建GROUP BY和HAVING子句
    group_by_cols = ', '.join(constraint_cols)
    
    sql = f"""
        SELECT {group_by_cols}, COUNT(*) 
        FROM {table_name} 
        GROUP BY {group_by_cols} 
        HAVING COUNT(*) > 1
        LIMIT 1
    """
    
    try:
        cursor.execute(sql)
        return cursor.fetchone() is not None
    except Exception as e:
        logger.error(f"检查表 {table_name} 重复记录时出错: {str(e)}")
        return False

def fix_duplicates(conn, table_name, constraint_cols):
    """修复表中的重复记录"""
    if not constraint_cols:
        return False
    
    cursor = conn.cursor()
    
    # 统计重复记录数
    group_by_cols = ', '.join(constraint_cols)
    cursor.execute(f"""
        SELECT {group_by_cols}, COUNT(*) as cnt
        FROM {table_name}
        GROUP BY {group_by_cols}
        HAVING COUNT(*) > 1
    """)
    duplicates = cursor.fetchall()
    
    if not duplicates:
        return False
    
    duplicate_groups = len(duplicates)
    total_duplicates = sum(row[-1] - 1 for row in duplicates)
    logger.warning(f"表 {table_name} 中发现 {duplicate_groups} 组重复记录，共计 {total_duplicates} 条重复数据")
    
    try:
        # 创建临时表
        cursor.execute(f"CREATE TABLE {table_name}_temp AS SELECT * FROM {table_name} WHERE 0")
        
        # 获取列名
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [row[1] for row in cursor.fetchall()]
        columns_str = ', '.join(columns)
        
        # 构建WHERE条件
        where_clause = ' AND '.join([f'a.{col} = b.{col}' for col in constraint_cols])
        
        # 插入去重后的数据
        cursor.execute(f"""
            INSERT INTO {table_name}_temp ({columns_str})
            SELECT {columns_str}
            FROM {table_name} AS a
            WHERE rowid = (
                SELECT MIN(rowid)
                FROM {table_name} AS b
                WHERE {where_clause}
            )
        """)
        conn.commit()
        
        # 检查临时表记录数
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        original_count = cursor.fetchone()[0]
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}_temp")
        new_count = cursor.fetchone()[0]
        
        logger.info(f"表 {table_name} 原有 {original_count} 条记录，去重后剩余 {new_count} 条记录")
        
        # 备份原表
        cursor.execute(f"ALTER TABLE {table_name} RENAME TO {table_name}_backup")
        cursor.execute(f"ALTER TABLE {table_name}_temp RENAME TO {table_name}")
        conn.commit()
        
        # 删除备份表
        cursor.execute(f"DROP TABLE {table_name}_backup")
        conn.commit()
        
        return True
    except Exception as e:
        logger.error(f"修复表 {table_name} 中的重复记录时出错: {str(e)}")
        conn.rollback()
        return False

def add_unique_constraint(conn, table_name, constraint_cols):
    """为表添加唯一约束"""
    if not constraint_cols:
        return False
    
    cursor = conn.cursor()
    
    try:
        # 构建约束名和列
        constraint_name = f"idx_{table_name}_{'_'.join(constraint_cols)}"
        constraint_cols_str = ', '.join(constraint_cols)
        
        # 创建唯一索引
        sql = f"CREATE UNIQUE INDEX IF NOT EXISTS {constraint_name} ON {table_name}({constraint_cols_str})"
        cursor.execute(sql)
        conn.commit()
        
        logger.info(f"成功为表 {table_name} 创建唯一约束: ({constraint_cols_str})")
        return True
    except Exception as e:
        logger.error(f"为表 {table_name} 创建唯一约束时出错: {str(e)}")
        conn.rollback()
        return False

def process_table(conn, table_name):
    """处理单个表，添加唯一约束"""
    logger.info(f"开始处理表: {table_name}")
    
    try:
        # 获取表结构
        columns = get_table_structure(conn, table_name)
        if not columns:
            logger.warning(f"无法获取表 {table_name} 的结构，跳过")
            return False
        
        # 识别唯一约束
        constraint_cols = identify_unique_constraint(table_name, columns)
        if not constraint_cols:
            logger.warning(f"无法为表 {table_name} 识别唯一约束，跳过")
            return False
        
        logger.info(f"表 {table_name} 识别的唯一约束列: {', '.join(constraint_cols)}")
        
        # 检查是否有重复记录
        has_duplicates = check_for_duplicates(conn, table_name, constraint_cols)
        
        if has_duplicates:
            logger.warning(f"表 {table_name} 存在重复记录，尝试修复")
            if not fix_duplicates(conn, table_name, constraint_cols):
                logger.error(f"修复表 {table_name} 中的重复记录失败，跳过添加唯一约束")
                return False
            logger.info(f"成功修复表 {table_name} 中的重复记录")
        else:
            logger.info(f"表 {table_name} 没有重复记录")
        
        # 添加唯一约束
        if add_unique_constraint(conn, table_name, constraint_cols):
            logger.info(f"成功处理表 {table_name}")
            return True
        else:
            logger.error(f"为表 {table_name} 添加唯一约束失败")
            return False
            
    except Exception as e:
        logger.error(f"处理表 {table_name} 时出错: {str(e)}")
        return False

def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="为所有表添加唯一约束")
    parser.add_argument("--db", type=str, help="SQLite数据库文件路径", required=True)
    parser.add_argument("--table", type=str, help="要处理的特定表名", default=None)
    args = parser.parse_args()
    
    db_path = args.db
    specific_table = args.table
    
    # 检查数据库文件是否存在
    if not os.path.exists(db_path):
        logger.error(f"数据库文件 {db_path} 不存在")
        return
    
    conn = sqlite3.connect(db_path)
    
    try:
        # 获取所有表
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        tables = [row[0] for row in cursor.fetchall()]
        
        if specific_table:
            if specific_table in tables:
                tables = [specific_table]
            else:
                logger.error(f"指定的表 {specific_table} 不存在")
                return
        
        logger.info(f"数据库中共有 {len(tables)} 个表需要处理")
        
        # 处理每个表
        success_count = 0
        for i, table in enumerate(tables):
            logger.info(f"[{i+1}/{len(tables)}] 正在处理表: {table}")
            if process_table(conn, table):
                success_count += 1
            # 每处理10个表暂停一下，避免数据库锁定
            if (i+1) % 10 == 0:
                time.sleep(1)
        
        logger.info(f"处理完成，成功处理 {success_count}/{len(tables)} 个表")
        
    except Exception as e:
        logger.error(f"处理数据库时出错: {str(e)}")
    finally:
        conn.close()

if __name__ == "__main__":
    main() 