#!/usr/bin/env python3
"""
数据接口性能分析工具

独立的性能测试和分析工具，提供详细的性能指标和建议
"""

import time
import statistics
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import sys
import os
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    from finhack.library.data import get_data_interface
    from test_config import get_test_codes, get_dynamic_config, TEST_DATE_RANGES
except ImportError as e:
    print(f"导入错误: {e}")
    print("请确保在正确的环境中运行此脚本")
    sys.exit(1)

class PerformanceAnalyzer:
    """性能分析器"""
    
    def __init__(self):
        self.data_interface = get_data_interface()
        self.results = {}
        self.test_codes = get_test_codes('cn_stock')[:5]  # 限制为5只股票
        
    def measure_performance(self, func, *args, **kwargs):
        """测量函数性能"""
        # 预热
        try:
            func(*args, **kwargs)
        except:
            pass
        
        times = []
        errors = []
        
        # 多次测量取平均值
        for _ in range(3):
            start_time = time.perf_counter()
            try:
                result = func(*args, **kwargs)
                end_time = time.perf_counter()
                times.append(end_time - start_time)
            except Exception as e:
                end_time = time.perf_counter()
                times.append(end_time - start_time)
                errors.append(str(e))
        
        return {
            'avg_time': statistics.mean(times),
            'min_time': min(times),
            'max_time': max(times),
            'std_time': statistics.stdev(times) if len(times) > 1 else 0,
            'error_count': len(errors),
            'errors': errors[:3]  # 只保留前3个错误
        }
    
    def test_kline_performance(self):
        """测试K线数据性能"""
        print("测试K线数据性能...")
        
        scenarios = [
            ('single_stock_1month', [self.test_codes[0]], '2023-01-01', '2023-01-31'),
            ('single_stock_3months', [self.test_codes[0]], '2023-01-01', '2023-03-31'),
            ('multi_stock_1month', self.test_codes[:3], '2023-01-01', '2023-01-31'),
            ('multi_stock_3months', self.test_codes[:3], '2023-01-01', '2023-03-31'),
        ]
        
        results = {}
        
        for scenario_name, codes, start_date, end_date in scenarios:
            print(f"  测试场景: {scenario_name}")
            
            # 测试无缓存性能
            self.data_interface.clear_cache()
            perf_no_cache = self.measure_performance(
                self.data_interface.get_klines,
                codes=codes,
                market='cn_stock',
                freq='1d',
                start_date=start_date,
                end_date=end_date,
                use_cache=False
            )
            
            # 测试有缓存性能
            perf_with_cache = self.measure_performance(
                self.data_interface.get_klines,
                codes=codes,
                market='cn_stock',
                freq='1d',
                start_date=start_date,
                end_date=end_date,
                use_cache=True
            )
            
            # 测试缓存命中性能
            perf_cache_hit = self.measure_performance(
                self.data_interface.get_klines,
                codes=codes,
                market='cn_stock',
                freq='1d',
                start_date=start_date,
                end_date=end_date,
                use_cache=True
            )
            
            results[scenario_name] = {
                'no_cache': perf_no_cache,
                'with_cache': perf_with_cache,
                'cache_hit': perf_cache_hit,
                'codes_count': len(codes),
                'date_range': f"{start_date} to {end_date}"
            }
        
        self.results['kline'] = results
        return results
    
    def test_quotes_performance(self):
        """测试行情数据性能"""
        print("测试行情数据性能...")
        
        test_times = [
            datetime(2023, 6, 15),  # 工作日
            datetime(2023, 6, 17),  # 周六
            datetime(2023, 5, 1),   # 节假日
        ]
        
        results = {}
        
        for i, test_time in enumerate(test_times):
            scenario_name = f"quotes_scenario_{i+1}"
            print(f"  测试场景: {scenario_name} ({test_time.strftime('%Y-%m-%d')})")
            
            perf_result = self.measure_performance(
                self.data_interface.get_quotes,
                codes=self.test_codes[:3],
                market='cn_stock',
                freq='1d',
                time=test_time
            )
            
            results[scenario_name] = {
                **perf_result,
                'query_time': test_time.strftime('%Y-%m-%d'),
                'codes_count': 3
            }
        
        self.results['quotes'] = results
        return results
    
    def test_adjustment_performance(self):
        """测试复权功能性能"""
        print("测试复权功能性能...")
        
        adj_types = ['none', 'front', 'back']
        results = {}
        
        for adj_type in adj_types:
            print(f"  测试复权类型: {adj_type}")
            
            perf_result = self.measure_performance(
                self.data_interface.get_klines,
                codes=self.test_codes[:2],
                market='cn_stock',
                freq='1d',
                start_date='2023-01-01',
                end_date='2023-01-31',
                adj_type=adj_type
            )
            
            results[adj_type] = {
                **perf_result,
                'adj_type': adj_type
            }
        
        self.results['adjustment'] = results
        return results
    
    def test_cache_performance(self):
        """测试缓存性能"""
        print("测试缓存性能...")
        
        # 清空缓存
        self.data_interface.clear_cache()
        
        test_params = {
            'codes': self.test_codes[:3],
            'market': 'cn_stock',
            'freq': '1d',
            'start_date': '2023-01-01',
            'end_date': '2023-01-31'
        }
        
        results = {}
        
        # 首次加载（缓存填充）
        print("  测试缓存填充性能...")
        perf_fill = self.measure_performance(
            self.data_interface.get_klines,
            use_cache=True,
            **test_params
        )
        
        # 缓存命中
        print("  测试缓存命中性能...")
        perf_hit = self.measure_performance(
            self.data_interface.get_klines,
            use_cache=True,
            **test_params
        )
        
        # 缓存未命中
        print("  测试缓存未命中性能...")
        self.data_interface.clear_cache()
        perf_miss = self.measure_performance(
            self.data_interface.get_klines,
            use_cache=True,
            **test_params
        )
        
        results = {
            'cache_fill': perf_fill,
            'cache_hit': perf_hit,
            'cache_miss': perf_miss,
            'speedup': perf_fill['avg_time'] / perf_hit['avg_time'] if perf_hit['avg_time'] > 0 else 0
        }
        
        self.results['cache'] = results
        return results
    
    def analyze_results(self):
        """分析测试结果"""
        print("\n" + "="*80)
        print("性能分析报告")
        print("="*80)
        
        # K线性能分析
        if 'kline' in self.results:
            print("\n📊 K线数据性能分析")
            print("-" * 40)
            
            kline_data = []
            for scenario, data in self.results['kline'].items():
                row = {
                    'scenario': scenario,
                    'codes_count': data['codes_count'],
                    'no_cache_time': data['no_cache']['avg_time'],
                    'cache_hit_time': data['cache_hit']['avg_time'],
                    'speedup': data['no_cache']['avg_time'] / data['cache_hit']['avg_time'] if data['cache_hit']['avg_time'] > 0 else 0
                }
                kline_data.append(row)
                
                print(f"场景: {scenario}")
                print(f"  股票数量: {data['codes_count']}")
                print(f"  日期范围: {data['date_range']}")
                print(f"  无缓存耗时: {data['no_cache']['avg_time']:.4f}s")
                print(f"  缓存命中耗时: {data['cache_hit']['avg_time']:.4f}s")
                print(f"  缓存加速: {row['speedup']:.2f}x")
                print()
        
        # 行情性能分析
        if 'quotes' in self.results:
            print("\n📈 行情数据性能分析")
            print("-" * 40)
            
            for scenario, data in self.results['quotes'].items():
                print(f"场景: {scenario}")
                print(f"  查询时间: {data['query_time']}")
                print(f"  平均耗时: {data['avg_time']:.4f}s")
                print(f"  错误数量: {data['error_count']}")
                print()
        
        # 复权性能分析
        if 'adjustment' in self.results:
            print("\n🔄 复权功能性能分析")
            print("-" * 40)
            
            for adj_type, data in self.results['adjustment'].items():
                print(f"复权类型: {adj_type}")
                print(f"  平均耗时: {data['avg_time']:.4f}s")
                print(f"  标准差: {data['std_time']:.4f}s")
                print()
        
        # 缓存性能分析
        if 'cache' in self.results:
            print("\n💾 缓存性能分析")
            print("-" * 40)
            
            cache_data = self.results['cache']
            print(f"缓存填充耗时: {cache_data['cache_fill']['avg_time']:.4f}s")
            print(f"缓存命中耗时: {cache_data['cache_hit']['avg_time']:.4f}s")
            print(f"缓存未命中耗时: {cache_data['cache_miss']['avg_time']:.4f}s")
            print(f"缓存加速比: {cache_data['speedup']:.2f}x")
            print()
        
        # 性能建议
        self.generate_recommendations()
    
    def generate_recommendations(self):
        """生成性能优化建议"""
        print("\n💡 性能优化建议")
        print("-" * 40)
        
        recommendations = []
        
        # 缓存相关建议
        if 'cache' in self.results:
            speedup = self.results['cache']['speedup']
            if speedup < 5:
                recommendations.append("🔧 缓存加速效果不明显，建议检查缓存配置或增加缓存大小")
            elif speedup > 50:
                recommendations.append("✅ 缓存效果良好，建议在生产环境中启用缓存")
        
        # K线性能建议
        if 'kline' in self.results:
            avg_times = [data['no_cache']['avg_time'] for data in self.results['kline'].values()]
            if max(avg_times) > 2.0:
                recommendations.append("⚠️  K线数据加载时间较长，建议优化数据存储格式或增加并行处理")
        
        # 复权性能建议
        if 'adjustment' in self.results:
            adj_times = {adj_type: data['avg_time'] for adj_type, data in self.results['adjustment'].items()}
            if adj_times.get('front', 0) > adj_times.get('none', 0) * 2:
                recommendations.append("🔄 复权计算耗时较长，建议预计算复权数据或优化复权算法")
        
        # 通用建议
        recommendations.extend([
            "📈 建议定期运行性能测试监控系统性能变化",
            "🔍 关注错误率，及时处理数据缺失或格式问题",
            "⚡ 对于高频访问的数据，建议启用缓存并合理设置过期时间",
            "📊 根据实际使用场景调整线程池大小和缓存配置"
        ])
        
        for i, rec in enumerate(recommendations, 1):
            print(f"{i}. {rec}")
        
        print()
    
    def save_results(self, filename=None):
        """保存测试结果"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"performance_results_{timestamp}.json"
        
        import json
        
        # 转换数据为可序列化格式
        serializable_results = {}
        for category, data in self.results.items():
            serializable_results[category] = {}
            for key, value in data.items():
                if isinstance(value, dict):
                    serializable_results[category][key] = {
                        k: (v if not isinstance(v, list) else str(v)) for k, v in value.items()
                    }
                else:
                    serializable_results[category][key] = value
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(serializable_results, f, indent=2, ensure_ascii=False)
        
        print(f"性能测试结果已保存到: {filename}")

def main():
    """主函数"""
    print("FinHack 数据接口性能分析工具")
    print("=" * 60)
    
    analyzer = PerformanceAnalyzer()
    
    try:
        # 运行各项性能测试
        analyzer.test_kline_performance()
        analyzer.test_quotes_performance()
        analyzer.test_adjustment_performance()
        analyzer.test_cache_performance()
        
        # 分析结果
        analyzer.analyze_results()
        
        # 保存结果
        save_choice = input("是否保存测试结果到文件? (y/n): ").strip().lower()
        if save_choice == 'y':
            analyzer.save_results()
        
        print("\n性能分析完成!")
        
    except KeyboardInterrupt:
        print("\n\n用户中断测试")
        return 1
    except Exception as e:
        print(f"\n测试过程中出现错误: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())