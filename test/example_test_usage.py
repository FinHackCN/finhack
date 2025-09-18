#!/usr/bin/env python3
"""
数据接口测试使用示例

演示如何运行特定的测试并分析结果
"""

import subprocess
import sys
import time
from pathlib import Path

def run_command(cmd, description):
    """运行命令并显示结果"""
    print(f"\n{'='*60}")
    print(f"执行: {description}")
    print(f"命令: {cmd}")
    print('='*60)
    
    start_time = time.time()
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=300)
        end_time = time.time()
        
        print(f"执行时间: {end_time - start_time:.2f}秒")
        print(f"返回码: {result.returncode}")
        
        if result.stdout:
            print(f"\n标准输出:\n{result.stdout}")
        if result.stderr:
            print(f"\n错误输出:\n{result.stderr}")
            
        return result.returncode == 0
        
    except subprocess.TimeoutExpired:
        print("命令执行超时")
        return False
    except Exception as e:
        print(f"执行错误: {e}")
        return False

def main():
    """主函数"""
    print("FinHack 数据接口测试示例")
    print("=" * 60)
    
    # 检查测试文件是否存在
    test_file = Path("test_data_interface_comprehensive.py")
    if not test_file.exists():
        print("❌ 测试文件不存在，请确保在正确的目录下运行")
        return 1
    
    examples = [
        {
            "cmd": "python run_data_interface_tests.py --quick --category kline",
            "description": "快速K线数据测试",
            "explanation": "测试基本的K线数据获取功能，跳过大数据量测试"
        },
        {
            "cmd": "python run_data_interface_tests.py --category quotes --verbose",
            "description": "详细行情数据测试",
            "explanation": "测试行情数据功能，包括智能历史数据查找"
        },
        {
            "cmd": "python run_data_interface_tests.py --category cache",
            "description": "缓存功能测试",
            "explanation": "测试数据缓存的功能和性能"
        },
        {
            "cmd": "python run_data_interface_tests.py --category exception",
            "description": "异常处理测试",
            "explanation": "测试各种异常情况下的系统行为"
        }
    ]
    
    print("可用的测试示例:")
    for i, example in enumerate(examples, 1):
        print(f"{i}. {example['description']}")
        print(f"   说明: {example['explanation']}")
        print(f"   命令: {example['cmd']}")
        print()
    
    while True:
        try:
            choice = input("请选择要运行的测试 (1-4) 或按 'q' 退出: ").strip()
            
            if choice.lower() == 'q':
                print("退出测试")
                break
            
            choice_idx = int(choice) - 1
            if 0 <= choice_idx < len(examples):
                example = examples[choice_idx]
                success = run_command(example['cmd'], example['description'])
                
                if success:
                    print(f"\n✅ {example['description']} 完成")
                else:
                    print(f"\n❌ {example['description']} 失败")
                    
                input("\n按回车键继续...")
            else:
                print("无效选择，请重试")
                
        except ValueError:
            print("请输入有效的数字")
        except KeyboardInterrupt:
            print("\n\n用户中断，退出测试")
            break
    
    return 0

if __name__ == "__main__":
    sys.exit(main())