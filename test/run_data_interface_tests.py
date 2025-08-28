#!/usr/bin/env python3
"""
数据接口测试运行脚本

使用方法：
1. 基础测试：python run_data_interface_tests.py
2. 包含性能测试：python run_data_interface_tests.py --performance
3. 只运行特定类别：python run_data_interface_tests.py --category kline
4. 详细输出：python run_data_interface_tests.py --verbose
"""

import argparse
import os
import sys
import time
import pytest
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def main():
    parser = argparse.ArgumentParser(description='数据接口测试套件')
    parser.add_argument('--performance', action='store_true',
                        help='运行性能基准测试（耗时较长）')
    parser.add_argument('--category', choices=['kline', 'quotes', 'factor', 'reference', 'exception', 'cache', 'benchmark'],
                        help='只运行特定类别的测试')
    parser.add_argument('--verbose', action='store_true',
                        help='详细输出')
    parser.add_argument('--quick', action='store_true',
                        help='快速测试模式（跳过大数据量测试）')
    
    args = parser.parse_args()
    
    # 构建pytest参数
    pytest_args = ['test_data_interface_comprehensive.py']
    
    if args.verbose:
        pytest_args.extend(['-v', '-s'])
    else:
        pytest_args.append('-v')
    
    # 根据类别过滤测试
    if args.category:
        category_map = {
            'kline': 'TestKlineData',
            'quotes': 'TestQuotesData', 
            'factor': 'TestFactorData',
            'reference': 'TestReferenceData',
            'exception': 'TestExceptionHandling',
            'cache': 'TestCacheManagement',
            'benchmark': 'TestBenchmarks'
        }
        
        if args.category in category_map:
            pytest_args.append(f'-k {category_map[args.category]}')
    
    # 性能测试控制
    if not args.performance:
        pytest_args.append('-k "not (large_data or concurrent)"')
    
    # 快速模式
    if args.quick:
        pytest_args.append('-k "not (large_ or concurrent or multiple_stocks)"')
    
    # 添加输出格式
    pytest_args.extend(['--tb=short'])
    
    print("="*60)
    print("FinHack 数据接口测试套件")
    print("="*60)
    print(f"测试模式: {'性能测试' if args.performance else '快速测试' if args.quick else '标准测试'}")
    if args.category:
        print(f"测试类别: {args.category}")
    print("="*60)
    
    start_time = time.time()
    
    # 运行测试
    exit_code = pytest.main(pytest_args)
    
    end_time = time.time()
    total_time = end_time - start_time
    
    print(f"\n测试完成，总耗时: {total_time:.2f}秒")
    
    if exit_code == 0:
        print("✅ 所有测试通过")
    else:
        print("❌ 部分测试失败")
    
    return exit_code

if __name__ == '__main__':
    sys.exit(main())