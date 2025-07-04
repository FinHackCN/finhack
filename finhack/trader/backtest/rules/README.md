# 规则引擎系统文档

## 概述

规则引擎系统是回测框架的核心组件，提供了一套通用且灵活的规则管理机制，能够适配不同市场、不同回测频次，并支持用户自定义规则。

## 核心特性

### 🎯 通用性与灵活性
- 支持多种规则类型：交易规则、市场规则、风险规则、数据规则、时间规则
- 支持多市场：A股、港股、美股及其他市场
- 支持多频率：日线、小时线、分钟线、高频交易
- 支持用户自定义规则扩展

### 🛠️ 强大的管理功能
- 规则动态加载、启用/禁用
- 规则优先级管理
- 规则配置导入导出
- 规则预设模板管理
- 规则性能监控和优化

### 🔧 配置化设计
- 支持JSON配置文件
- 支持规则链和规则组组合
- 支持条件规则和复合规则
- 支持规则参数动态调整

## 系统架构

```
规则引擎系统
├── 基础架构
│   ├── BaseRule (规则基类)
│   ├── RuleType (规则类型枚举)
│   ├── RuleResult (规则结果类)
│   └── MarketRule (市场规则基类)
├── 核心引擎
│   ├── RuleEngine (规则引擎)
│   ├── RuleFactory (规则工厂)
│   └── RuleManager (规则管理器)
├── 通用规则
│   ├── 交易规则 (trading_rules.py)
│   ├── 市场规则 (market_rules.py)
│   └── 风险规则 (risk_rules.py)
├── 市场特定规则
│   ├── A股规则 (cn_stock_rules.py)
│   ├── 港股规则 (hk_stock_rules.py)
│   └── 美股规则 (us_stock_rules.py)
└── 示例和工具
    ├── 使用示例 (examples/)
    └── 配置模板 (templates/)
```

## 规则类型详解

### 交易规则 (Trading Rules)
- **TradingTimeRule**: 交易时间检查
- **OrderSizeRule**: 订单大小限制
- **OrderPriceRule**: 订单价格检查
- **PositionSizeRule**: 持仓大小限制
- **CashRule**: 现金充足性检查
- **CommissionRule**: 手续费计算
- **StampTaxRule**: 印花税计算
- **SlippageRule**: 滑点处理
- **VolumeRule**: 成交量限制
- **MinTradeUnitRule**: 最小交易单位

### 市场规则 (Market Rules)
- **PriceLimitRule**: 涨跌停检查
- **SuspensionRule**: 停牌检查
- **DelistingRule**: 退市检查
- **STMarkRule**: ST股票限制
- **NewStockRule**: 新股交易限制
- **TradingDayRule**: 交易日检查
- **MarketHoursRule**: 开闭市时间
- **LiquidityRule**: 流动性检查
- **VolatilityRule**: 波动率检查

### 风险规则 (Risk Rules)
- **PositionConcentrationRule**: 持仓集中度控制
- **SingleStockPositionRule**: 单股持仓限制
- **DailyTradingAmountRule**: 单日交易金额限制
- **MaxDrawdownRule**: 最大回撤控制
- **LeverageRule**: 杠杆比例控制
- **StopLossRule**: 止损规则
- **TakeProfitRule**: 止盈规则
- **VaRRule**: 风险价值控制

### A股特定规则 (CN Stock Rules)
- **T1Rule**: T+1交易限制
- **StarMarketRule**: 科创板特殊规则
- **GemRule**: 创业板规则
- **BseRule**: 北交所规则
- **MarginTradingRule**: 融资融券规则
- **CallAuctionRule**: 集合竞价规则
- **BlockTradingRule**: 大宗交易规则

### 港股特定规则 (HK Stock Rules)
- **T0Rule**: T+0交易规则
- **StockConnectRule**: 沪深港通规则
- **OddLotRule**: 碎股交易规则
- **CurrencyConversionRule**: 汇率转换规则
- **HkHolidayRule**: 港股休市规则
- **HkMarketHoursRule**: 港股交易时间规则
- **HkPriceLimitRule**: 港股价格限制规则
- **HkSettlementRule**: 港股结算规则
- **HkCommissionRule**: 港股手续费规则

### 美股特定规则 (US Stock Rules)
- **UsT0Rule**: 美股T+0交易规则（Good Faith规则）
- **PdtRule**: Pattern Day Trader规则
- **ExtendedHoursRule**: 盘前盘后交易规则
- **UsCurrencyConversionRule**: 美元汇率转换规则
- **UsHolidayRule**: 美股休市规则
- **UsMarketHoursRule**: 美股交易时间规则
- **UsCommissionRule**: 美股手续费规则
- **UsOrderTypeRule**: 美股订单类型规则
- **UsPennyStockRule**: 美股仙股规则
- **UsShortSellingRule**: 美股卖空规则

## 快速使用

### 基本使用

```python
from finhack.trader.backtest.rules import RuleManager

# 创建规则管理器
rule_manager = RuleManager()

# 获取规则引擎
rule_engine = rule_manager.get_rule_engine()

# 应用规则
results = rule_engine.apply_rules(context, event, order_data)

# 检查结果
if all(result.passed for result in results):
    # 所有规则通过，执行订单
    pass
else:
    # 有规则失败，处理错误
    pass
```

### 创建市场特定配置

```python
from finhack.trader.backtest.rules import RuleFactory

# 创建规则工厂
rule_factory = RuleFactory()

# 创建A股日线规则集
cn_rules = rule_factory.create_default_rule_set("cn_stock", "1d")

# 创建港股规则集
hk_rules = rule_factory.create_default_rule_set("hk_stock", "1d")

# 创建美股规则集
us_rules = rule_factory.create_default_rule_set("us_stock", "1d")
```

### 规则配置管理

```python
# 创建规则预设
rule_manager.create_rule_preset(
    "conservative_cn_stock",
    market="cn_stock",
    frequency="1d",
    custom_rules=["stop_loss", "take_profit"]
)

# 加载规则预设
rule_manager.load_rule_preset("conservative_cn_stock")

# 验证规则配置
validation_result = rule_manager.validate_rules()
print(f"验证结果: {validation_result['valid']}")

# 获取性能报告
performance_report = rule_manager.get_rule_performance_report()
print(f"性能报告: {performance_report}")
```

## 自定义规则

### 创建自定义规则

```python
from finhack.trader.backtest.rules import BaseRule, RuleType, RuleResult

class MyCustomRule(BaseRule):
    """自定义规则示例"""
    
    def __init__(self, config=None):
        super().__init__("my_custom_rule", RuleType.CUSTOM, config=config)
        self.description = "我的自定义规则"
        
        # 规则参数
        self.threshold = self.get_config('threshold', 0.05)
    
    def apply(self, context, event, data):
        """应用规则逻辑"""
        # 实现你的规则逻辑
        if some_condition:
            return RuleResult(passed=True, message="规则通过")
        else:
            return RuleResult(passed=False, message="规则失败")
    
    def is_applicable(self, context, event, data):
        """检查规则是否适用"""
        return self.enabled and some_condition
```

### 注册自定义规则

```python
# 创建规则实例
custom_rule = MyCustomRule({'threshold': 0.1})

# 添加到规则引擎
rule_engine.add_rule(custom_rule, "custom_group")
```

## 配置示例

### A股日线配置

```json
{
  "rules": {
    "trading_time": {
      "enabled": true,
      "priority": 10,
      "config": {
        "morning_start": [9, 30],
        "morning_end": [11, 30],
        "afternoon_start": [13, 0],
        "afternoon_end": [15, 0]
      }
    },
    "commission": {
      "enabled": true,
      "priority": 5,
      "config": {
        "commission_rate": 0.0003,
        "min_commission": 5.0,
        "max_commission": 1000.0
      }
    },
    "stop_loss": {
      "enabled": true,
      "priority": 8,
      "config": {
        "stop_loss_ratio": 0.10,
        "enable_trailing_stop": true
      }
    }
  }
}
```

### 港股配置

```json
{
  "rules": {
    "stock_connect": {
      "enabled": true,
      "priority": 10,
      "config": {
        "enable_stock_connect": true,
        "daily_quota": 52000000000,
        "min_order_value": 10000
      }
    },
    "currency_conversion": {
      "enabled": true,
      "priority": 5,
      "config": {
        "base_currency": "CNY",
        "target_currency": "HKD",
        "auto_convert": true
      }
    }
  }
}
```

## 性能优化

### 规则优先级优化
- 将执行频率高、耗时少的规则设置为高优先级
- 将可能导致订单失败的规则设置为高优先级
- 将耗时长的规则设置为低优先级

### 规则配置优化
- 根据历史统计数据调整规则参数
- 禁用不必要的规则以提高性能
- 使用规则组合减少重复计算

### 监控和分析
```python
# 获取规则性能报告
performance_report = rule_manager.get_rule_performance_report()

# 自动优化规则
optimization_result = rule_manager.auto_optimize_rules("performance")

# 导出规则文档
documentation = rule_manager.export_rules_documentation()
```

## 最佳实践

1. **规则设计原则**
   - 单一职责：每个规则只负责一个特定的检查
   - 配置化：通过配置参数控制规则行为
   - 可扩展：支持继承和自定义

2. **规则配置管理**
   - 使用预设模板简化配置
   - 定期备份和版本控制
   - 环境隔离（开发、测试、生产）

3. **性能优化**
   - 合理设置规则优先级
   - 监控规则执行性能
   - 定期优化规则配置

4. **错误处理**
   - 完善的错误日志记录
   - 规则失败的降级处理
   - 异常情况的监控告警

## 扩展开发

### 添加新的规则类型
1. 在对应的规则模块中创建新的规则类
2. 继承 `BaseRule` 或 `MarketRule`
3. 实现 `apply` 方法
4. 在 `RuleFactory` 中注册新规则

### 添加新的市场支持
1. 在 `markets/` 目录下创建新的市场规则文件
2. 实现市场特定的规则类
3. 在 `RuleFactory` 中添加创建方法
4. 更新文档和示例

### 集成外部数据源
1. 在规则中通过 `context` 访问外部数据
2. 实现数据缓存和更新机制
3. 处理数据不可用的情况

## 支持与反馈

如果你在使用过程中遇到问题或有改进建议，请：

1. 查看示例代码和文档
2. 检查日志输出
3. 提交问题或建议

---

*该文档随代码版本持续更新，请关注最新版本。* 