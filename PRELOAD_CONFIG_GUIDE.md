# 预加载历史数据配置指南

## 概述

在回测系统中，计算技术指标（如MA、MACD等）需要足够的历史数据。为了确保有足够的历史数据，系统支持配置额外预加载前几个月的数据。

## 配置项说明

### `preload_data` (bool)
- **默认值**: `true`
- **说明**: 是否启用预加载功能
  - `true`: 启用预加载，在回测开始时加载历史数据
  - `false`: 禁用预加载，使用按需加载策略（启动更快，但可能缺少历史数据）

### `preload_extra_months` (int)
- **默认值**: `3`
- **说明**: 额外预加载前几个月的数据
  - 设置为 `0`: 只预加载当前月的数据
  - 设置为 `1`: 预加载当前月 + 前1个月的数据
  - 设置为 `3`: 预加载当前月 + 前3个月的数据（默认，约90个交易日）
  - 设置为 `6`: 预加载当前月 + 前6个月的数据（约180个交易日）

## 推荐配置

根据策略使用的技术指标类型，推荐不同的配置：

### 短期策略（MA5, MA10, MA20, MACD等）
```yaml
data:
  preload_data: true
  preload_extra_months: 1  # 1个月足够（约20个交易日）
```

### 中期策略（MA60, MA90, MACD+KDJ等）
```yaml
data:
  preload_data: true
  preload_extra_months: 3  # 3个月推荐（约90个交易日）
```

### 长期策略（MA120, MA250, 布林带等）
```yaml
data:
  preload_data: true
  preload_extra_months: 6  # 6个月推荐（约180个交易日）
  # 或者设置为12个月（约250个交易日）
```

### 快速测试（不关心历史数据准确性）
```yaml
data:
  preload_data: false  # 禁用预加载，启动更快
```

## 配置方式

### 方式1：通过配置文件（推荐）

在回测配置文件中添加：

```yaml
data:
  data_source: "file"
  cache_enabled: true
  preload_data: true
  preload_extra_months: 3  # 根据策略需求调整

trade:
  market: "cn_stock"
  start_time: "2024-01-02 00:00:00"
  end_time: "2024-01-10 23:59:59"
  frequency: "1m"
  strategy: "MyStrategy"

account:
  initial_cash: 1000000.0
```

### 方式2：通过代码配置

```python
from finhack.trader.backtest.core.context import Context

# 创建配置字典
config = {
    'data': {
        'preload_data': True,
        'preload_extra_months': 6  # 预加载前6个月的数据
    }
}

# 创建上下文
context = Context(config)
```

### 方式3：直接修改配置对象

```python
from finhack.trader.backtest.core.context import Context, DataConfig

# 创建上下文
context = Context()

# 修改配置
context.data_config.preload_data = True
context.data_config.preload_extra_months = 6
```

## 使用示例

### 示例1：使用默认配置（预加载前3个月）

```python
# 默认配置：preload_extra_months = 3
# 这意味着在回测开始时，系统会自动加载：
# - 当前月的数据
# - 前1个月的数据
# - 前2个月的数据
# - 前3个月的数据
#
# 总共4个月的数据，足以计算大多数常用的技术指标
```

### 示例2：禁用预加载（快速启动）

```python
config = {
    'data': {
        'preload_data': False  # 禁用预加载
    }
}
context = Context(config)
```

### 示例3：预加载前6个月数据（长期策略）

```python
config = {
    'data': {
        'preload_data': True,
        'preload_extra_months': 6  # 预加载前6个月
    }
}
context = Context(config)
```

## 工作原理

### 1分钟频率回测

在1分钟频率的回测中：

1. **回测开始时**：
   - 系统检查 `preload_data` 配置
   - 如果启用，预加载第一个交易日及其前N个月的数据
   - 例如：回测从2024-01-02开始，配置 `preload_extra_months=3`
   - 系统会加载：2024-01月、2023-12月、2023-11月、2023-10月

2. **进入新月份时**：
   - 系统检测到进入新月份
   - 自动加载该月份及其前N个月的数据
   - 确保每个月都有足够的历史数据

### 日线频率回测

日线频率回测不受此配置影响，因为日线数据量较小，加载速度快。

## 注意事项

### 1. 启动时间 vs 数据完整性

- **启用预加载** (`preload_data: true`)：
  - ✅ 优点：有完整的历史数据，技术指标计算准确
  - ❌ 缺点：启动时间较长（需要加载更多数据）

- **禁用预加载** (`preload_data: false`)：
  - ✅ 优点：启动速度快
  - ❌ 缺点：早期交易日可能缺少历史数据，导致技术指标计算不准确

### 2. 内存占用

额外的预加载会增加内存占用：
- 1个月数据（1分钟频率）：约500-1000MB（取决于股票数量）
- 3个月数据：约1.5-3GB
- 6个月数据：约3-6GB

如果内存有限，建议：
- 减少预加载的月数
- 或者使用按需加载策略
- 或者减少股票池的大小

### 3. 磁盘I/O

预加载会一次性读取大量数据，可能会对磁盘造成压力。如果使用SSD，影响较小；如果使用HDD，可能需要等待较长时间。

## 故障排查

### 问题1：仍然看到"数据被过滤"警告

**原因**：某些股票的数据开始时间晚于回测开始时间。

**解决方案**：
- 这是正常的，这些股票可能是新上市或数据缺失
- 系统会自动排除这些股票，不影响回测运行

### 问题2：预加载耗时过长

**原因**：配置的额外月数太多，或股票池太大。

**解决方案**：
- 减少 `preload_extra_months` 的值
- 减少股票池的大小
- 或者使用更快的存储（SSD）

### 问题3：内存不足

**原因**：预加载的数据量超过了可用内存。

**解决方案**：
- 减少 `preload_extra_months` 的值
- 禁用预加载 (`preload_data: false`)
- 减少股票池的大小

## 总结

通过合理配置 `preload_data` 和 `preload_extra_months`，可以在启动速度和数据完整性之间找到平衡：

- **快速测试**：设置 `preload_data: false`
- **短期策略**：设置 `preload_extra_months: 1`
- **中期策略**：设置 `preload_extra_months: 3`（默认）
- **长期策略**：设置 `preload_extra_months: 6` 或更高
