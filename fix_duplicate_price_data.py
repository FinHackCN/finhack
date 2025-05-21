import os
import sys
import sqlite3
import pandas as pd
import argparse
import logging
from datetime import datetime

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"fix_duplicate_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("fix_duplicate")

def fix_price_table(db_path, table_name):
    """清理股票价格表中的重复数据，并添加唯一约束"""
    logger.info(f"开始修复表 {table_name} 中的重复数据")
    conn = sqlite3.connect(db_path)
    
    try:
        # 检查表是否存在
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        if not cursor.fetchone():
            logger.error(f"表 {table_name} 不存在，跳过")
            return
        
        # 获取表的总记录数
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_count = cursor.fetchone()[0]
        logger.info(f"表 {table_name} 当前有 {total_count} 条记录")
        
        # 检查是否有重复记录
        cursor.execute(f"""
            SELECT ts_code, trade_date, COUNT(*)
            FROM {table_name}
            GROUP BY ts_code, trade_date
            HAVING COUNT(*) > 1
        """)
        duplicates = cursor.fetchall()
        
        if not duplicates:
            logger.info(f"表 {table_name} 没有重复记录，无需修复")
        else:
            duplicate_count = sum(row[2] - 1 for row in duplicates)
            duplicate_groups = len(duplicates)
            logger.warning(f"发现 {duplicate_groups} 组重复记录，共 {duplicate_count} 条冗余数据")
            
            # 输出前5组重复记录的示例
            for i, (ts_code, trade_date, count) in enumerate(duplicates[:5]):
                logger.info(f"重复示例 {i+1}: {ts_code}, {trade_date}, {count} 条记录")
            
            # 创建临时表
            logger.info("创建临时表并插入去重后的数据...")
            cursor.execute(f"CREATE TABLE {table_name}_temp AS SELECT * FROM {table_name} WHERE 0")
            
            # 复制表结构
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [row[1] for row in cursor.fetchall()]
            columns_str = ", ".join(columns)
            
            # 插入去重后的数据
            cursor.execute(f"""
                INSERT INTO {table_name}_temp ({columns_str})
                SELECT {columns_str}
                FROM {table_name} AS a
                WHERE rowid = (
                    SELECT MIN(rowid)
                    FROM {table_name} AS b
                    WHERE b.ts_code = a.ts_code AND b.trade_date = a.trade_date
                )
            """)
            conn.commit()
            
            # 检查临时表记录数
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}_temp")
            new_count = cursor.fetchone()[0]
            logger.info(f"去重后的临时表有 {new_count} 条记录，减少了 {total_count - new_count} 条")
            
            # 备份原表，防止操作失败
            logger.info("备份原表...")
            cursor.execute(f"ALTER TABLE {table_name} RENAME TO {table_name}_backup")
            
            # 重命名临时表为正式表
            logger.info("重命名临时表为正式表...")
            cursor.execute(f"ALTER TABLE {table_name}_temp RENAME TO {table_name}")
            
            # 添加唯一索引
            logger.info("创建唯一索引...")
            try:
                cursor.execute(f"CREATE UNIQUE INDEX idx_{table_name}_ts_trade ON {table_name}(ts_code, trade_date)")
                conn.commit()
                logger.info("成功创建唯一索引")
            except Exception as e:
                logger.error(f"创建唯一索引失败: {str(e)}")
                conn.rollback()
                
            # 检查修复后是否还有重复
            cursor.execute(f"""
                SELECT COUNT(*)
                FROM {table_name}
                GROUP BY ts_code, trade_date
                HAVING COUNT(*) > 1
            """)
            if cursor.fetchone():
                logger.error("修复后仍有重复记录，请检查")
            else:
                logger.info("修复成功，已无重复记录")
                
                # 删除备份表
                try:
                    cursor.execute(f"DROP TABLE {table_name}_backup")
                    conn.commit()
                    logger.info("已删除备份表")
                except:
                    logger.warning("删除备份表失败，请手动检查")
    except Exception as e:
        logger.error(f"修复表 {table_name} 时出错: {str(e)}")
        conn.rollback()
    finally:
        conn.close()

def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="修复股票价格表中的重复数据")
    parser.add_argument("--db", type=str, help="SQLite数据库文件路径", required=True)
    args = parser.parse_args()
    
    db_path = args.db
    
    # 检查数据库文件是否存在
    if not os.path.exists(db_path):
        logger.error(f"数据库文件 {db_path} 不存在")
        return
    
    # 需要检查的表名列表
    tables = [
        "astock_price_daily",
        "astock_price_weekly",
        "astock_price_monthly"
    ]
    
    # 修复每个表
    for table in tables:
        fix_price_table(db_path, table)
    
    logger.info("所有表处理完成")

if __name__ == "__main__":
    main() 