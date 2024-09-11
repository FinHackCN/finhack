# FinHack® 炼金术(肝不动了，长期内测)
- 如果有更新，那就是亏钱了
- 如果更新的很频繁，那就是亏了很多钱
- 啥时候不更新了，就是赚到钱了
- 啥时候删库了，就是赚到大钱了

--

bug太多修不过来了，等我慢慢完善完善再发正式版，现在大家就看看代码得了…（2024.09.11）


## 项目介绍
<div>FinHack®, an easily extensible quantitative finance framework, integrates a complete workflow for quantitative investment research in its current version, including data collection, factor computation, factor mining, factor analysis, machine learning, strategy development, and quantitative backtesting. In later stages, it will expand to include more data sources, trading instruments, analytical tools, and practical plugins, aiming to create an open, customizable, and high-level quantitative finance framework to aid Quants and researchers in related fields with their financial research work.</div>
<br/>
FinHack®，一个易于拓展的量化金融框架，它在当前版本中集成了<B>数据采集、因子计算、因子挖掘、因子分析、机器学习、策略编写、量化回测、实盘接入</B>等全流程的量化投研工作，后期它将拓展出更多的数据源、交易品种与分析工具与实用插件，力求打造一个开放的、可定制的、高水平的量化金融框架，助力广大Quant与相关学科工作者的金融研究工作。

## 项目特点
- 良好的拓展性，包括但不限于拓展自己的数据源、因子、AI模型、量化策略以及回测规则
- 可以支持公式形式计算类似Alpha101、Alpha191因子的计算引擎，并附带了这两个因子集
- 良好的环境隔离，可以通过项目目录的形式创建不同的策略集，适合多用户环境下的工作
- 回测系统中对A股规则的支持，包括涨跌停限制，T+1规则约束等，并可自定义约束规则
- 采用了动态复权的回测机制，避免了前后复权后存在较大价格误差的问题，优化了回测速度
- 内置了支持tushare的数据采集器，填写对应token后可一键采集tushare数据
- 支持多进程的回测和多进程的机器学习训练，可极限利用服务器算力

## 项目许可
- 本项目采用GNU通用公共许可证v3.0（GPL-3.0）和商业许可证双重许可。项目的默认公共许可证是GPL-3.0。如果您希望以不兼容GPL-3.0许可证的方式使用本项目，您必须购买商业许可证。
- 要获取商业许可，请通过微信(woldywei)与我们联系，了解定价和更多信息。

## 使用帮助
[快速入门](https://github.com/FinHackCN/finhack/wiki/1%E3%80%81%E5%BF%AB%E9%80%9F%E5%85%A5%E9%97%A8)
 
## 数据采集
![image](https://github.com/FinHackCN/finhack/assets/6196607/63870118-f7b0-473b-b8df-8bdbd748c018)

## 因子计算
![image](https://github.com/FinHackCN/finhack/assets/6196607/78786b5f-9520-4826-9fe1-9b1657c4d1cc)

## 因子挖掘
![image](https://github.com/FinHackCN/finhack/assets/6196607/4c99bfd8-2e90-4a2e-896c-0eb5b40146a9)

## 量化回测
![image](https://github.com/FinHackCN/finhack/assets/6196607/45210870-8167-425b-ba98-17d80d79ee7b)
![image](https://github.com/FinHackCN/finhack/assets/6196607/74e12eae-93fb-487c-a43f-92c79c8f75d6)
![image](https://github.com/FinHackCN/finhack/assets/6196607/19ce463e-9323-4f28-982b-17298c53e1d7)

## 实盘接入
![image](https://github.com/FinHackCN/finhack/assets/6196607/6bafbb9d-0798-4623-bddb-ae5d4f7e2fba)
![image](https://github.com/FinHackCN/finhack/assets/6196607/d84e4f1a-d950-49f6-afd9-c3632fe563d0)
![image](https://github.com/FinHackCN/finhack/assets/6196607/eacc7656-7161-4a81-8d1a-0a22cf85a76d)

## TODO List
- 增加更新提醒、公告通知
- 增加会员注册、数据下载
- 增加对期货、外汇、美股、比特币的数据、回测、实盘支持
- 增加多种及其学习算法的支持
- 策略市场、应用商店
