# 统一数据接口使用指南

## 概述

统一数据接口是FinHack框架的核心组件，提供了对项目中所有数据的统一访问能力，包括K线数据、因子数据、参考数据等。该接口具备高性能缓存、多线程处理、批量操作等优化功能。

## 设计目标

1. **统一接口**：为所有数据类型提供一致的API
2. **高性能**：通过缓存和多线程优化提升数据访问速度
3. **易用性**：简化数据获取操作，减少代码复杂度
4. **可扩展性**：支持新的数据类型和存储格式
5. **向后兼容**：保持与现有模块的兼容性

## 核心特性

### 1. 数据类型支持

- **K线数据**：支持codebased和timebased存储格式
- **因子数据**：支持matrix和vector存储格式
- **参考数据**：股票列表、复权因子、交易日历等

### 2. 高性能缓存系统

- **LRU缓存**：最近最少使用淘汰策略
- **时间过期**：自动清理过期数据
- **多级缓存**：针对不同数据类型的专用缓存
- **自动清理**：后台定期清理过期缓存

### 3. 多线程优化

- **并行加载**：多股票数据并行获取
- **批量处理**：减少I/O操作次数
- **线程池管理**：自动管理线程资源

## 基本用法

### 1. 获取数据接口实例

```python
from finhack.library.data import get_data_interface

# 方式1：使用默认项目路径（从runtime.constant获取）
data_interface = get_data_interface()

# 方式2：指定项目路径
data_interface = get_data_interface("/path/to/project")

# 方式3：自定义缓存配置
cache_config = {
    'max_size': 2000,
    'ttl_seconds': 7200,
    'cleanup_interval': 600
}
data_interface = get_data_interface("/path/to/project", cache_config)
```

### 2. K线数据获取

```python
# 获取单只股票的K线数据
klines = data_interface.get_klines(
    codes="000001.SZ",
    market="cn_stock",
    freq="1d",
    start_date="2023-01-01",
    end_date="2023-12-31",
    fields=["open", "high", "low", "close", "volume"]
)

# 获取多只股票的K线数据（支持复权）
codes = ["000001.SZ", "000002.SZ", "600000.SH"]
klines_multi = data_interface.get_klines(
    codes=codes,
    market="cn_stock",
    freq="1d",
    start_date="2023-01-01",
    end_date="2023-12-31",
    adj_type="front"  # 前复权，可选: 'none'(不复权), 'front'(前复权), 'back'(后复权)
)

# 获取指定时间点的行情快照（智能历史数据查找）
from datetime import datetime
quotes = data_interface.get_quotes(
    codes=codes,
    market="cn_stock",
    freq="1d",
    time=datetime(2023, 6, 30),  # 如果当天无数据，会自动使用最近的历史交易日数据
    adj_type="front"  # 前复权，可选: 'none', 'front', 'back'
)
```

### 3. 因子数据获取

```python
# 获取矩阵因子数据
factors = data_interface.get_factors(
    factor_names=["pe_ratio", "pb_ratio", "market_cap"],
    codes=["000001.SZ", "000002.SZ"],
    market="cn_stock",
    freq="1d",
    start_date="2023-01-01",
    end_date="2023-01-31",
    factor_type="matrix"
)

# 获取向量因子数据
vector_factors = data_interface.get_factors(
    factor_names=["momentum", "volatility"],
    market="cn_stock",
    freq="1d",
    start_date="2023-01-01",
    end_date="2023-01-31",
    factor_type="vector"
)
```

### 4. 参考数据获取

```python
from datetime import date

# 获取股票列表
stock_list = data_interface.get_stock_list("cn_stock")

# 获取复权因子
adj_factors = data_interface.get_adj_factors(
    market="cn_stock",
    codes=["000001.SZ", "000002.SZ"],
    start_date="2023-01-01",
    end_date="2023-12-31"
)

# 获取交易日历
trading_calendar = data_interface.get_trading_calendar(
    market="cn_stock",
    start_date=date(2023, 1, 1),
    end_date=date(2023, 12, 31)
)
```

### 5. 因子数据保存

```python
import pandas as pd
import numpy as np

# 创建示例因子数据
dates = pd.date_range('2023-01-01', '2023-01-10', freq='D')
codes = ['000001.SZ', '000002.SZ', '600000.SH']
index = pd.MultiIndex.from_product([dates, codes], names=['time', 'code'])

df_factors = pd.DataFrame({
    'factor1': np.random.randn(len(index)),
    'factor2': np.random.randn(len(index)),
    'factor3': np.random.randn(len(index))
}, index=index)

# 保存因子数据
success = data_interface.save_factors(
    df_factors=df_factors,
    factor_list=['factor1', 'factor2', 'factor3'],
    market='cn_stock',
    freq='1d',
    factor_type='matrix'
)
```

### 6. 复权功能详解

统一数据接口支持自动复权功能，只需在获取K线数据或行情快照时指定`adj_type`参数：

```python
# 复权类型说明：
# 'none': 不复权（原始价格）
# 'front': 前复权（以最新价格为基准，向前调整历史价格）
# 'back': 后复权（以历史价格为基准，向后调整价格）

# 获取前复权K线数据
klines_front = data_interface.get_klines(
    codes=["000001.SZ", "000002.SZ"],
    market="cn_stock",
    freq="1d",
    start_date="2023-01-01",
    end_date="2023-12-31",
    adj_type="front"  # 前复权
)

# 获取后复权K线数据
klines_back = data_interface.get_klines(
    codes=["000001.SZ", "000002.SZ"],
    market="cn_stock",
    freq="1d",
    start_date="2023-01-01",
    end_date="2023-12-31",
    adj_type="back"  # 后复权
)

# 获取前复权行情快照
quotes_adj = data_interface.get_quotes(
    codes=["000001.SZ"],
    market="cn_stock",
    freq="1d",
    time=datetime(2023, 6, 30),
    adj_type="front"  # 前复权
)

# 对比不同复权方式的价格
print("原始价格:", klines_front.loc[('2023-01-03', '000001.SZ'), 'close'])
print("前复权价格:", klines_front.loc[('2023-01-03', '000001.SZ'), 'close'])
print("后复权价格:", klines_back.loc[('2023-01-03', '000001.SZ'), 'close'])
```

### 7. 智能历史数据查找

对于`get_quotes`方法，当指定时间点没有数据时，系统会自动使用最近的历史交易日数据：

```python
# 场景1：查询周末数据，自动使用周五的数据
weekend_quotes = data_interface.get_quotes(
    codes=["000001.SZ"],
    market="cn_stock",
    freq="1d",
    time=datetime(2023, 6, 11)  # 周日，自动使用周五数据
)

# 场景2：查询节假日数据，自动使用最近交易日数据
holiday_quotes = data_interface.get_quotes(
    codes=["000001.SZ"],
    market="cn_stock", 
    freq="1d",
    time=datetime(2023, 5, 1)  # 劳动节，自动使用最近交易日数据
)

# 场景3：查询非交易时间，自动使用当日或最近交易日数据
evening_quotes = data_interface.get_quotes(
    codes=["000001.SZ"],
    market="cn_stock",
    freq="1d", 
    time=datetime(2023, 6, 15, 20, 30)  # 晚上8:30，自动使用当日数据
)
```

Note: 系统会向前查找最多90天的历史数据，以应对长假期等情况。

## 缓存管理

### 1. 查看缓存统计

```python
stats = data_interface.get_cache_stats()
print(stats)
# 输出示例:
# {
#     'kline_cache': {'size': 150, 'max_size': 1000},
#     'factor_cache': {'size': 80, 'max_size': 1000},
#     'reference_cache': {'size': 20, 'max_size': 1000},
#     ...
# }
```

### 2. 清理缓存

```python
# 清理特定类型的缓存
data_interface.clear_cache('kline')      # 清理K线缓存
data_interface.clear_cache('factor')     # 清理因子缓存
data_interface.clear_cache('reference')  # 清理参考数据缓存

# 清理所有缓存
data_interface.clear_cache()
```

### 3. 控制缓存使用

```python
# 禁用缓存
klines = data_interface.get_klines(
    codes="000001.SZ",
    market="cn_stock",
    freq="1d",
    start_date="2023-01-01",
    end_date="2023-12-31",
    use_cache=False  # 禁用缓存
)
```

## 与现有模块的集成

### 1. 回测模块（DataCenter）

原有的DataCenter类已经重构为使用统一数据接口：

```python
from finhack.trader.backtest.core.data_center import DataCenter

# DataCenter现在内部使用统一数据接口
data_center = DataCenter(project_path, market='cn_stock', freq='1d')

# 所有原有接口保持不变
quotes = data_center.get_quotes(codes=['000001.SZ'], freq='1d', time=datetime.now())
klines = data_center.get_klines(codes=['000001.SZ'], freq='1d')
factors = data_center.get_factors(['pe_ratio'], codes=['000001.SZ'])
```

### 2. 因子模块（factorManager）

factorManager类也已经重构为使用统一数据接口：

```python
from finhack.factor.default.factorManager import factorManager

# 所有方法现在使用统一数据接口
factor_info = factorManager.inspectFactor('pe_ratio', 'matrix', 'cn_stock', '1d')
factors_df = factorManager.loadFactors(['pe_ratio'], [], ['000001.SZ'], 'cn_stock', '1d')
factorManager.saveFactors(factors_df, ['pe_ratio'], 'cn_stock', '1d')
```

## 数据格式说明

### 1. K线数据格式

```python
# 返回DataFrame，MultiIndex(time, symbol)
#                          open    high     low   close    volume
# time       symbol                                               
# 2023-01-03 000001.SZ   12.34   12.56   12.20   12.45  1000000
# 2023-01-03 000002.SZ   25.67   25.89   25.30   25.78   500000
```

### 2. 行情快照格式

```python
# 返回DataFrame，index为symbol
#           open    high     low   close    volume
# symbol                                          
# 000001.SZ 12.34   12.56   12.20   12.45  1000000
# 000002.SZ 25.67   25.89   25.30   25.78   500000
```

### 3. 因子数据格式

```python
# 返回DataFrame，MultiIndex(date, symbol)
#                        pe_ratio  pb_ratio  market_cap
# date       symbol                                    
# 2023-01-03 000001.SZ      15.6       1.2    50000000
# 2023-01-03 000002.SZ      18.9       1.8    30000000
```

## 性能优化建议

### 1. 合理使用缓存

- 对于频繁访问的数据，启用缓存
- 对于一次性查询，可以禁用缓存
- 定期清理缓存避免内存过度使用

### 2. 批量操作

```python
# 推荐：批量获取多只股票
codes = ["000001.SZ", "000002.SZ", "600000.SH"]
klines = data_interface.get_klines(codes=codes, ...)

# 不推荐：逐个获取
for code in codes:
    kline = data_interface.get_klines(codes=[code], ...)
```

### 3. 合理的时间范围

```python
# 推荐：根据实际需要指定时间范围
klines = data_interface.get_klines(
    codes=codes,
    start_date="2023-01-01",
    end_date="2023-12-31"
)

# 不推荐：获取过大的时间范围
klines = data_interface.get_klines(
    codes=codes,
    start_date="2000-01-01",
    end_date="2023-12-31"
)
```

## 错误处理

### 1. 常见异常

```python
try:
    klines = data_interface.get_klines(
        codes=["INVALID.CODE"],
        market="cn_stock",
        freq="1d"
    )
except Exception as e:
    print(f"获取数据失败: {e}")
    # 返回空DataFrame而不是抛出异常
```

### 2. 数据不存在的处理

统一数据接口在数据不存在时会返回空DataFrame，而不是抛出异常：

```python
# 即使股票代码不存在，也会返回空DataFrame
klines = data_interface.get_klines(codes=["NONEXISTENT.CODE"], ...)
print(klines.empty)  # True
print(klines.shape)  # (0, n)
```

## 配置选项

### 1. 缓存配置

```python
cache_config = {
    'max_size': 1000,        # 最大缓存条目数
    'ttl_seconds': 3600,     # 缓存存活时间（秒）
    'cleanup_interval': 300  # 清理间隔（秒）
}
```

### 2. 线程池配置

线程池配置在DataInterface类内部自动管理，默认使用8个工作线程。

## 最佳实践

### 1. 资源管理

```python
# 在应用结束时关闭数据接口
data_interface.shutdown()

# 或者使用全局关闭函数
from finhack.library.data import shutdown_data_interface
shutdown_data_interface()
```

### 2. 日志记录

统一数据接口使用标准Python logging模块记录日志：

```python
import logging
logging.basicConfig(level=logging.INFO)

# 数据接口的日志信息将自动输出
data_interface = get_data_interface()
```

### 3. 监控和调试

```python
# 定期检查缓存使用情况
def monitor_cache():
    stats = data_interface.get_cache_stats()
    for cache_name, cache_stats in stats.items():
        usage = cache_stats['size'] / cache_stats['max_size']
        if usage > 0.8:
            print(f"警告: {cache_name} 缓存使用率过高 ({usage:.1%})")

# 性能分析
import time

start_time = time.time()
result = data_interface.get_klines(...)
end_time = time.time()

print(f"数据获取耗时: {end_time - start_time:.2f} 秒")
```

## 扩展和定制

### 1. 自定义缓存策略

可以通过继承LRUCache类实现自定义缓存策略：

```python
from finhack.library.data import LRUCache

class CustomCache(LRUCache):
    def __init__(self, max_size, ttl_seconds):
        super().__init__(max_size, ttl_seconds)
        # 自定义初始化逻辑
    
    def get(self, key):
        # 自定义获取逻辑
        return super().get(key)
```

### 2. 添加新的数据类型

可以通过扩展DataInterface类添加新的数据类型支持：

```python
from finhack.library.data import DataInterface

class ExtendedDataInterface(DataInterface):
    def get_custom_data(self, ...):
        # 实现自定义数据获取逻辑
        pass
```

## 迁移指南

### 从旧版DataCenter迁移

```python
# 旧版代码
from finhack.trader.backtest.core.data_center import DataCenter
data_center = DataCenter(project_path)
klines = data_center._load_symbol_klines(market, symbol, freq, start_date, end_date)

# 新版代码
from finhack.library.data import get_data_interface
data_interface = get_data_interface(project_path)
klines = data_interface.get_klines(codes=[symbol], market=market, freq=freq, 
                                   start_date=start_date, end_date=end_date)
```

### 从旧版factorManager迁移

```python
# 旧版代码
from finhack.factor.default.factorManager import factorManager
# 复杂的文件操作和路径处理

# 新版代码
from finhack.library.data import get_data_interface
data_interface = get_data_interface()
factors = data_interface.get_factors(factor_names, codes, market, freq, 
                                     start_date, end_date, factor_type)
```

## 总结

统一数据接口为FinHack框架提供了强大而灵活的数据访问能力。通过统一的API、高性能缓存和多线程优化，大大简化了数据操作，提高了开发效率和运行性能。

主要优势：
- **简化代码**：统一的API减少了学习成本
- **提升性能**：智能缓存和多线程优化
- **增强可靠性**：完善的错误处理和日志记录
- **保持兼容性**：现有模块无需修改即可受益

建议在新项目中优先使用统一数据接口，在旧项目中逐步迁移以获得更好的性能和维护性。