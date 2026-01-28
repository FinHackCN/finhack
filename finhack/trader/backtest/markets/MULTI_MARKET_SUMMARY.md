# 多市场交易规则适配器实现总结

## 概述

已为5个市场创建了完整的交易规则适配器，实现了基于时间的规则版本控制机制。

---

## 一、已实现的市场适配器

### 1.1 中国基金/ETF市场 (cn_fund)

**目录**: `/markets/cn_fund/`

| 文件 | 功能描述 |
|------|---------|
| `cn_fund_adapter.py` | 主适配器类 |
| `etf_trading_rules_versions.py` | 规则版本定义 |
| `etf_calculator.py` | 计算器模块 |
| `test_etf_adapter.py` | 单元测试 |

**核心功能**:
- 交易时段: 集合竞价09:15-09:25、连续竞价09:30-15:00、收盘集合竞价14:57-15:00、盘后15:05-15:30
- 涨跌幅: 主板10%、科创板/创业板20%、北交所30%
- 价格笼子机制: 主板2023/2/20、科创板/创业板2020/8/24
- 最小单位: 主板100股、科创板200股、创业板100股(1股递增)
- 手续费: 股票ETF万三+印花税、债券/跨境ETF免印花税

### 1.2 美股市场 (us_stock)

**目录**: `/markets/us_stock/`

| 文件 | 功能描述 |
|------|---------|
| `us_stock_adapter.py` | 主适配器类 |
| `us_trading_rules_versions.py` | 规则版本定义 |
| `us_calculator.py` | 计算器模块 |
| `test_us_stock_adapter.py` | 单元测试 |

**核心功能**:
- 交易时段: 盘前04:00-09:30、正常09:30-16:00、盘后16:00-20:00（美东时间）
- 熔断机制: S&P 500下跌7%/13%/20%触发
- 结算周期: 2024年5月前T+2，之后T+1
- PDT规则: 账户净值<$25,000时，5日最多3次日内回转
- 零碎股: 最小1股，支持0.0001股精度
- 卖空规则: Rule 201提价规则

### 1.3 港股市场 (hk_stock)

**目录**: `/markets/hk_stock/`

| 文件 | 功能描述 |
|------|---------|
| `hk_stock_adapter.py` | 主适配器类 |
| `hk_trading_rules_versions.py` | 规则版本定义 |
| `hk_calculator.py` | 计算器模块 |
| `test_hk_standalone.py` | 单元测试 |

**核心功能**:
- 交易时段: 盘前09:00-09:30、上午09:30-12:00、下午13:00-16:00、收市竞价16:00-16:10
- T+0交易，T+2结算
- VCM机制: 81只大盘股5分钟波动10%触发5分钟冷静期
- 最小单位: 根据股价动态确定（100-1000股/手）
- 印花税: 0.1%（卖出）

### 1.4 加密货币市场 (global_cryptospot)

**目录**: `/markets/global_cryptospot/`

| 文件 | 功能描述 |
|------|---------|
| `global_cryptospot_adapter.py` | 主适配器类 |
| `crypto_trading_rules_versions.py` | 规则版本定义 |
| `crypto_calculator.py` | 计算器模块 |

**核心功能**:
- 交易时间: 7x24x365永不休市
- 无涨跌幅限制
- T+0即时清算
- 资金费率: 每8小时或每1小时结算
- 强平机制: 标记价格触发
- 高杠杆: 最高125x

### 1.5 中国期货市场 (cn_future)

**目录**: `/markets/cn_future/`

| 文件 | 功能描述 |
|------|---------|
| `cn_future_adapter.py` | 主适配器类（已增强） |
| `future_trading_rules_versions.py` | 规则版本定义 |
| `future_calculator.py` | 计算器模块 |
| `test_future_adapter.py` | 单元测试 |

**核心功能**:
- 交易时间: 日盘09:00-10:15/10:30-11:30/13:30-15:00、夜盘21:00-次日凌晨
- 涨跌幅: ±5%至±15%（国债期货无限制）
- T+0交易，当日无负债结算
- 最小单位: 1手，不同品种有不同Tick Size
- 交割月限制: 临近交割月自然人禁止交易
- 不同交易所规则: 中金所、上期所、大商所、郑商所、广期所

---

## 二、统一架构设计

### 2.1 规则版本控制

所有市场都实现了基于时间的规则版本控制：

```python
# 规则版本定义示例
TRADING_RULES_VERSIONS = [
    {
        'version': 'v20200824',
        'effective_date': date(2020, 8, 24),
        'description': '创业板注册制改革',
        'rules': {
            'gem': {'daily_limit': 0.20}
        }
    },
    # ...更多版本
]
```

### 2.2 计算器模块

每个市场都有专门的计算器模块：

| 计算器类型 | cn_fund | us_stock | hk_stock | cn_future |
|----------|---------|----------|----------|-----------|
| 价格计算 | ETFPriceCalculator | USPriceCalculator | HKPriceCalculator | FuturePriceCalculator |
| 交易单位 | ETFLotSizeCalculator | USLotSizeCalculator | HKLotSizeCalculator | FutureLotSizeCalculator |
| 手续费 | ETFCommissionCalculator | USCommissionCalculator | HKCommissionCalculator | FutureCommissionCalculator |
| 特殊功能 | ETFDividendAdjuster | USCircuitBreakerCalculator<br>USPDTChecker<br>USShortSaleCalculator | HKVCMMonitor | FutureMarginCalculator<br>FutureDeliveryValidator |
| 合约相关 | - | - | - | CryptoFundingRateCalculator<br>CryptoLeverageCalculator<br>CryptoLiquidationCalculator |

### 2.3 适配器统一接口

所有适配器继承自 `BaseMarket`，实现统一接口：

```python
class BaseMarket(ABC):
    @abstractmethod
    def generate_daily_events(self, trade_date, frequency) -> List[BaseEvent]

    @abstractmethod
    def is_trading_time(self, dt, frequency) -> bool

    @abstractmethod
    def get_trading_sessions(self, trade_date, frequency) -> List[tuple]

    # 规则相关方法
    def get_price_limits(self, symbol, prev_close) -> Dict
    def get_commission_rate(self, symbol, direction) -> Dict
    def get_lot_size(self, symbol) -> int
    def set_current_date(self, date)
    def get_current_date(self) -> date
```

---

## 三、市场规则对比表

### 3.1 交易时间对比

| 市场 | 正常交易时长时间 | 盘前 | 盘后 | 时区 |
|------|----------------|------|------|------|
| cn_fund | 09:30-15:00 (4小时) | 09:15-09:25 | 15:05-15:30(部分) | Asia/Shanghai |
| us_stock | 09:30-16:00 (6.5小时) | 04:00-09:30 | 16:00-20:00 | America/New_York |
| hk_stock | 09:30-16:00 (5.5小时) | 09:00-09:30 | 16:00-16:10 | Asia/Hong_Kong |
| cryptospot | 00:00-23:59 (24小时) | - | - | UTC |
| cn_future | 日盘+夜盘 | - | - | Asia/Shanghai |

### 3.2 涨跌幅限制对比

| 市场 | 涨跌幅 | 特殊机制 |
|------|--------|---------|
| cn_fund | 主板10%，科创板/创业板20%，北交所30% | 价格笼子 |
| us_stock | 无 | 熔断机制(7%/13%/20%) |
| hk_stock | 无 | VCM机制(10%波动) |
| cryptospot | 无 | 无 |
| cn_future | ±5%至±15% | 品种合约规格定义 |

### 3.3 结算制度对比

| 市场 | 交易制度 | 结算周期 | 备注 |
|------|---------|---------|------|
| cn_fund | T+1 | T+1 | 当日买入次日可卖 |
| us_stock | T+1 | T+1(2024年5月起)<br>T+2(之前) | |
| hk_stock | T+0 | T+2 | 当日可卖，资金T+2可用 |
| cryptospot | T+0 | 即时 | 交易完成后资金立即可用 |
| cn_future | T+0 | 当日无负债结算 | 每日收盘后调整保证金 |

### 3.4 最小交易单位对比

| 市场 | 最小单位 | 特殊规则 |
|------|---------|---------|
| cn_fund | 主板100股，科创板200股，创业板100股(1股递增) | 卖出可零股 |
| us_stock | 1股 | 支持零碎股(0.0001股) |
| hk_stock | 1手(100-1000股，根据股价) | 2026年拟标准化 |
| cryptospot | 极小(如0.00001 BTC) | 取决于币种 |
| cn_future | 1手 | 有最小变动价位 |

---

## 四、使用示例

### 4.1 创建适配器

```python
from datetime import date
from finhack.trader.backtest.markets.cn_fund import CnFundMarketAdapter
from finhack.trader.backtest.markets.us_stock import USStockMarketAdapter
from finhack.trader.backtest.markets.hk_stock import HKStockMarketAdapter
from finhack.trader.backtest.markets.global_cryptospot import GlobalCryptoSpotAdapter
from finhack.trader.backtest.markets.cn_future import CnFutureMarketAdapter

# 创建适配器（可指定当前日期用于规则版本查询）
etf_adapter = CnFundMarketAdapter(current_date=date(2024, 1, 15))
us_adapter = USStockMarketAdapter(current_date=date(2024, 1, 15))
hk_adapter = HKStockMarketAdapter(current_date=date(2024, 1, 15))
crypto_adapter = GlobalCryptoSpotAdapter(current_date=date(2024, 1, 15))
future_adapter = CnFutureMarketAdapter(current_date=date(2024, 1, 15))
```

### 4.2 获取涨跌幅限制

```python
# ETF
limits = etf_adapter.get_price_limits('510300.SH', prev_close=4.5)
# {'upper_limit': 4.95, 'lower_limit': 4.05, 'limit_ratio': 0.10}

# 美股
limits = us_adapter.get_price_limits('AAPL', prev_close=150.0)
# {'upper_limit': float('inf'), 'lower_limit': 0.0, 'has_circuit_breaker': True}

# 港股
limits = hk_adapter.get_price_limits('00700.HK', prev_close=300.0)
# {'upper_limit': float('inf'), 'lower_limit': 0.0, 'vcm_enabled': True}
```

### 4.3 计算手续费

```python
# ETF - 买入1000股，价格4.5元
fee = ETFCommissionCalculator.calculate_commission('510300.SH', 1000, 4.5, 'buy')
# {'commission': 1.35, 'stamp_tax': 0.0, 'total_fee': 1.35}

# 美股 - 买入100股，价格150美元
fee = USCommissionCalculator.calculate_commission('AAPL', 100, 150, 'buy')
# {'commission': 0.0, 'sec_fee': 1.31, 'total_fee': 1.31}

# 港股 - 买入1000股，价格300港元
fee = HKCommissionCalculator.calculate_commission('00700.HK', 1000, 300, 'buy')
# {'commission': 300.0, 'stamp_tax': 0.0, 'trading_fee': 15.0, ...}
```

---

## 五、文件结构总览

```
/mnt/ssd2/finhack-dev/finhack/finhack/trader/backtest/markets/
├── base_market.py                           # 基类
├── cn_fund/                                 # 中国基金/ETF
│   ├── __init__.py
│   ├── cn_fund_adapter.py
│   ├── etf_trading_rules_versions.py
│   ├── etf_calculator.py
│   └── test_etf_adapter.py
├── us_stock/                                # 美股（新建）
│   ├── __init__.py
│   ├── us_stock_adapter.py
│   ├── us_trading_rules_versions.py
│   ├── us_calculator.py
│   └── test_us_stock_adapter.py
├── hk_stock/                                # 港股（新建）
│   ├── __init__.py
│   ├── hk_stock_adapter.py
│   ├── hk_trading_rules_versions.py
│   ├── hk_calculator.py
│   └── test_hk_standalone.py
├── global_cryptospot/                       # 加密货币（增强）
│   ├── __init__.py
│   ├── global_cryptospot_adapter.py
│   ├── crypto_trading_rules_versions.py
│   └── crypto_calculator.py
└── cn_future/                               # 中国期货（增强）
    ├── __init__.py
    ├── cn_future_adapter.py
    ├── future_trading_rules_versions.py
    ├── future_calculator.py
    └── test_future_adapter.py
```

---

## 六、关键规则变更日期

### 6.1 跨市场重要日期

| 日期 | 事件 | 影响市场 |
|------|------|---------|
| 1996-12-16 | A股实施10%涨跌停 | cn_fund |
| 2006-07-01 | 深市收盘集合竞价 | cn_fund |
| 2018-08-20 | 沪市收盘集合竞价 | cn_fund |
| 2019-07-22 | 科创板开市，20%涨跌停 | cn_fund |
| 2020-08-24 | 创业板注册制改革 | cn_fund |
| 2021-10-29 | 港股随机收市机制 | hk_stock |
| 2023-02-20 | A股主板价格笼子 | cn_fund |
| 2024-05-28 | 美股结算T+2→T+1 | us_stock |

---

## 七、测试状态

| 市场 | 测试文件 | 状态 |
|------|---------|------|
| cn_fund | test_etf_adapter.py | ✓ 全部通过 |
| us_stock | test_us_stock_adapter.py | ✓ 全部通过 |
| hk_stock | test_hk_standalone.py | ✓ 全部通过 |
| cn_future | test_future_adapter.py | ✓ 全部通过 |
| global_cryptospot | - | 待添加 |

---

## 八、后续扩展

1. **测试覆盖**: 为加密货币适配器添加完整测试
2. **文档完善**: 为各市场添加详细的使用文档
3. **注册集成**: 在markets/__init__.py中注册新适配器
4. **回测集成**: 确保与BacktestEngine的兼容性
5. **数据接口**: 与各市场数据源的对接

---

## 九、参考资料

- 上海证券交易所: http://www.sse.com.cn/
- 深圳证券交易所: http://www.szse.cn/
- 纽约证券交易所: https://www.nyse.com/
- 纳斯达克: https://www.nasdaq.com/
- 香港交易所: https://www.hkex.com.hk/
- 币安: https://www.binance.com/
- 中国金融期货交易所: http://www.cffex.com.cn/
