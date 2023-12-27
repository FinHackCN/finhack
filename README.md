# FinHack® 炼金术(内测中，文档完善中)
## 项目介绍
<div>FinHack®, an easily extensible quantitative finance framework, integrates a complete workflow for quantitative investment research in its current version, including data collection, factor computation, factor mining, factor analysis, machine learning, strategy development, and quantitative backtesting. In later stages, it will expand to include more data sources, trading instruments, analytical tools, and practical plugins, aiming to create an open, customizable, and high-level quantitative finance framework to aid Quants and researchers in related fields with their financial research work.</div>
<br/>
FinHack®，一个易于拓展的量化金融框架，它在当前版本中集成了<B>数据采集、因子计算、因子挖掘、因子分析、机器学习、策略编写、量化回测</B>等全流程的量化投研工作，后期它将拓展出更多的数据源、交易品种与分析工具与实用插件(如WebUI、实盘接入、插件商城等)，力求打造一个开放的、可定制的、高水平的量化金融框架，助力广大Quant与相关学科工作者的金融研究工作。

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


## 量化回测
![image](https://github.com/FinHackCN/finhack/assets/6196607/45210870-8167-425b-ba98-17d80d79ee7b)
