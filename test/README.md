# FinHack 数据接口测试套件

## 概述

这个测试套件为 FinHack 统一数据接口提供全面的功能和性能测试，涵盖 K线数据、因子数据、行情数据、参考数据等各种场景。

## 测试内容

### 功能测试
- **K线数据测试**: 单股票/多股票、不同字段组合、复权功能
- **行情数据测试**: 基本行情、智能历史查找、复权行情
- **因子数据测试**: 矩阵因子、向量因子、不同日期范围
- **参考数据测试**: 股票列表、复权因子、交易日历
- **异常处理测试**: 无效参数、错误输入的处理
- **缓存测试**: 缓存功能、缓存性能、缓存管理

### 性能测试
- **基准测试**: 大数据量加载、并发访问
- **时间统计**: 每次数据加载的详细时间统计
- **性能对比**: 缓存命中率、并发性能分析

## 文件结构

```
test/
├── test_data_interface_comprehensive.py  # 主测试文件
├── test_config.py                       # 测试配置文件
├── run_data_interface_tests.py          # 测试运行脚本
└── README.md                           # 本说明文件
```

## 配置说明

### 修改测试配置

编辑 `test_config.py` 文件，根据您的实际数据可用性调整以下参数：

```python
# 测试用的股票代码（确保这些代码在您的数据中存在）
TEST_CODES = {
    'cn_stock': [
        "000001.SZ",  # 平安银行
        "000002.SZ",  # 万科A  
        # ... 添加更多可用的股票代码
    ]
}

# 测试用的市场列表
TEST_MARKETS = [
    "cn_stock",
    # "cn_index",    # 如果有指数数据，取消注释
    # "cn_fund",     # 如果有基金数据，取消注释
]

# 测试用的因子名称（根据实际可用因子调整）
TEST_FACTORS = {
    'matrix': [
        "pe_ratio",      # 市盈率
        "pb_ratio",      # 市净率
        # ... 添加实际存在的因子
    ]
}
```

## 运行测试

### 1. 快速测试
```bash
# 运行基础功能测试
python run_data_interface_tests.py

# 或直接使用 pytest
pytest test_data_interface_comprehensive.py -v
```

### 2. 性能测试
```bash
# 包含性能基准测试（耗时较长）
python run_data_interface_tests.py --performance
```

### 3. 分类测试
```bash
# 只测试 K线数据
python run_data_interface_tests.py --category kline

# 只测试因子数据
python run_data_interface_tests.py --category factor

# 只测试异常处理
python run_data_interface_tests.py --category exception
```

### 4. 快速模式
```bash
# 跳过大数据量测试的快速模式
python run_data_interface_tests.py --quick
```

### 5. 详细输出
```bash
# 显示详细的测试信息和日志
python run_data_interface_tests.py --verbose
```

## 测试报告

测试完成后会自动生成性能报告，包含：

### 性能统计
- 各类数据操作的平均执行时间
- 最快/最慢执行时间
- 数据量与性能的关系

### 缓存分析
- 缓存命中率
- 缓存加速效果
- 缓存内存使用情况

### 错误统计
- 异常处理测试结果
- 常见错误类型分析
- 错误频率统计

## 示例输出

```
=== K线数据测试 ===
[SUCCESS] single_klines_cn_stock_2023-01-01_2023-01-31: 0.1234s, size: 23
[SUCCESS] multi_klines_cn_stock_5_stocks: 0.2567s, size: 115
[SUCCESS] klines_adj_front: 0.1456s, size: 23
[SUCCESS] klines_cache_first: 0.1234s, size: 23
[SUCCESS] klines_cache_second: 0.0012s, size: 23
Cache speedup: 102.83x

=== 行情数据测试 ===
[SUCCESS] quotes_basic_20230615: 0.0234s, size: 2
[SUCCESS] quotes_historical_weekend_morning: 0.0198s, size: 1

=== 性能基准测试 ===
[SUCCESS] large_stock_loading: 1.2345s, size: 460
[SUCCESS] concurrent_access_5_threads: 0.3456s, size: 5

=== 测试总结 ===
总测试数量: 45
平均执行时间: 0.1234s
最快执行时间: 0.0008s
最慢执行时间: 1.2345s
```

## 故障排除

### 常见问题

1. **数据不存在错误**
   - 检查 `test_config.py` 中的股票代码是否在您的数据中存在
   - 确认测试日期范围内有数据

2. **因子测试失败**
   - 检查因子名称是否正确
   - 确认因子数据文件存在

3. **市场数据错误**
   - 验证市场名称拼写
   - 检查对应市场的数据目录结构

4. **性能测试超时**
   - 可以在 `test_config.py` 中调整 `TIMEOUT_CONFIG`
   - 或使用 `--quick` 模式跳过大数据量测试

### 调试技巧

1. **启用详细日志**
   ```bash
   python run_data_interface_tests.py --verbose
   ```

2. **运行单个测试**
   ```bash
   pytest test_data_interface_comprehensive.py::TestKlineData::test_single_stock_klines_basic -v -s
   ```

3. **跳过特定测试**
   ```bash
   pytest test_data_interface_comprehensive.py -k "not large_data" -v
   ```

## 扩展测试

### 添加新的测试用例

1. 在 `test_data_interface_comprehensive.py` 中添加新的测试方法
2. 使用 `_measure_time` 方法包装数据操作以获得性能统计
3. 使用 `_log_performance` 记录测试结果

### 自定义测试配置

1. 修改 `test_config.py` 中的配置参数
2. 添加新的测试场景到相应的配置列表
3. 重新运行测试验证配置

## 最佳实践

1. **定期运行测试**: 在修改数据接口后运行测试确保功能正常
2. **监控性能**: 关注性能报告中的异常值，及时优化
3. **保持配置更新**: 根据数据变化及时更新测试配置
4. **版本控制**: 保存测试结果用于版本间的性能对比

## 依赖要求

- Python 3.8+
- pytest
- pandas
- numpy
- FinHack 框架

确保在运行测试前已正确安装所有依赖并配置好 FinHack 环境。