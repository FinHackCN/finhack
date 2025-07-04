"""
规则引擎使用示例
演示如何配置和使用规则引擎系统
"""

from ..rule_manager import RuleManager
from ..rule_engine import RuleEngine
from ..rule_factory import RuleFactory
from ..base_rule import RuleType


class RuleEngineExamples:
    """规则引擎使用示例"""
    
    def __init__(self):
        self.rule_manager = RuleManager()
        self.rule_engine = self.rule_manager.get_rule_engine()
        self.rule_factory = self.rule_manager.get_rule_factory()
    
    def create_cn_stock_1d_config(self):
        """创建A股日线回测规则配置"""
        # 创建A股日线默认规则集
        rule_groups = self.rule_factory.create_default_rule_set("cn_stock", "1d")
        
        # 创建新的规则引擎
        engine = RuleEngine()
        
        # 添加规则组
        for group in rule_groups:
            engine.add_rule_group(group)
        
        # 自定义配置
        # 1. 调整交易时间规则
        trading_time_rule = engine.get_rule("trading_time")
        if trading_time_rule:
            trading_time_rule.config.update({
                'morning_start': (9, 30),
                'morning_end': (11, 30),
                'afternoon_start': (13, 0),
                'afternoon_end': (15, 0)
            })
        
        # 2. 调整持仓限制
        position_rule = engine.get_rule("position_size")
        if position_rule:
            position_rule.config.update({
                'max_position_ratio': 0.15,  # 单只股票最大持仓15%
                'max_total_position_ratio': 0.90  # 总持仓不超过90%
            })
        
        # 3. 设置手续费
        commission_rule = engine.get_rule("commission")
        if commission_rule:
            commission_rule.config.update({
                'commission_rate': 0.0003,  # 万三手续费
                'min_commission': 5.0,  # 最低5元
                'max_commission': 1000.0  # 最高1000元
            })
        
        # 4. 设置风险控制
        stop_loss_rule = engine.get_rule("stop_loss")
        if stop_loss_rule:
            stop_loss_rule.config.update({
                'stop_loss_ratio': 0.10,  # 10%止损
                'enable_trailing_stop': True  # 启用追踪止损
            })
        
        return engine
    
    def create_hk_stock_config(self):
        """创建港股回测规则配置"""
        # 创建港股规则集
        rule_groups = self.rule_factory.create_default_rule_set("hk_stock", "1d")
        
        engine = RuleEngine()
        for group in rule_groups:
            engine.add_rule_group(group)
        
        # 港股特定配置
        # 1. 港股通配置
        stock_connect_rule = engine.get_rule("stock_connect")
        if stock_connect_rule:
            stock_connect_rule.config.update({
                'enable_stock_connect': True,
                'daily_quota': 52000000000,  # 520亿港币
                'min_order_value': 10000  # 最小1万港币
            })
        
        # 2. 汇率转换
        currency_rule = engine.get_rule("currency_conversion")
        if currency_rule:
            currency_rule.config.update({
                'base_currency': 'CNY',
                'target_currency': 'HKD',
                'auto_convert': True
            })
        
        # 3. 港股手续费
        hk_commission_rule = engine.get_rule("hk_commission")
        if hk_commission_rule:
            hk_commission_rule.config.update({
                'commission_rate': 0.0025,  # 0.25%佣金
                'min_commission': 50.0,  # 最低50港币
                'stamp_duty_rate': 0.001  # 0.1%印花税
            })
        
        return engine
    
    def create_us_stock_config(self):
        """创建美股回测规则配置"""
        # 创建美股规则集
        rule_groups = self.rule_factory.create_default_rule_set("us_stock", "1d")
        
        engine = RuleEngine()
        for group in rule_groups:
            engine.add_rule_group(group)
        
        # 美股特定配置
        # 1. PDT规则
        pdt_rule = engine.get_rule("pdt")
        if pdt_rule:
            pdt_rule.config.update({
                'enable_pdt': True,
                'min_equity': 25000,  # 最小资产25000美元
                'max_day_trades_per_week': 3  # 每周最多3次日内交易
            })
        
        # 2. 美股手续费
        us_commission_rule = engine.get_rule("us_commission")
        if us_commission_rule:
            us_commission_rule.config.update({
                'commission_per_share': 0.005,  # 每股0.5美分
                'min_commission': 1.0,  # 最低1美元
                'max_commission': 10.0  # 最高10美元
            })
        
        # 3. 仙股规则
        penny_stock_rule = engine.get_rule("us_penny_stock")
        if penny_stock_rule:
            penny_stock_rule.config.update({
                'penny_stock_threshold': 5.0,  # 5美元以下为仙股
                'allow_penny_stock': False,  # 禁止仙股交易
                'max_penny_position_ratio': 0.05  # 仙股最大持仓5%
            })
        
        return engine
    
    def create_high_frequency_config(self):
        """创建高频交易规则配置"""
        # 创建高频交易规则集
        rule_groups = self.rule_factory.create_default_rule_set("cn_stock", "1m")
        
        engine = RuleEngine()
        for group in rule_groups:
            engine.add_rule_group(group)
        
        # 高频交易特定配置
        # 1. 高频规则
        hf_rule = engine.get_rule("high_frequency")
        if hf_rule:
            hf_rule.config.update({
                'min_interval_seconds': 60,  # 最小交易间隔60秒
                'max_position_turnover': 3.0,  # 最大仓位周转率3倍
                'max_orders_per_minute': 10  # 每分钟最大订单数
            })
        
        # 2. 调整滑点
        slippage_rule = engine.get_rule("slippage")
        if slippage_rule:
            slippage_rule.config.update({
                'slippage_rate': 0.0001,  # 1BP滑点
                'min_slippage': 0.01,  # 最小滑点1分
                'max_slippage': 0.1  # 最大滑点10分
            })
        
        # 3. 严格的订单大小限制
        order_size_rule = engine.get_rule("order_size")
        if order_size_rule:
            order_size_rule.config.update({
                'max_order_ratio': 0.05,  # 单笔最大5%
                'min_order_amount': 1000,  # 最小1000元
                'max_order_amount': 100000  # 最大10万元
            })
        
        return engine
    
    def demonstrate_rule_usage(self):
        """演示规则使用"""
        print("=== 规则引擎使用示例 ===")
        
        # 1. 创建A股日线配置
        print("\n1. 创建A股日线配置")
        cn_engine = self.create_cn_stock_1d_config()
        print(f"A股规则引擎: {cn_engine}")
        
        # 2. 创建港股配置
        print("\n2. 创建港股配置")
        hk_engine = self.create_hk_stock_config()
        print(f"港股规则引擎: {hk_engine}")
        
        # 3. 创建美股配置
        print("\n3. 创建美股配置")
        us_engine = self.create_us_stock_config()
        print(f"美股规则引擎: {us_engine}")
        
        # 4. 创建高频交易配置
        print("\n4. 创建高频交易配置")
        hf_engine = self.create_high_frequency_config()
        print(f"高频规则引擎: {hf_engine}")
        
        # 5. 展示规则管理功能
        print("\n5. 规则管理功能演示")
        
        # 创建预设
        self.rule_manager.create_rule_preset(
            "cn_stock_conservative",
            market="cn_stock",
            frequency="1d",
            custom_rules=["stop_loss", "take_profit"]
        )
        
        # 列出预设
        presets = self.rule_manager.list_rule_presets()
        print(f"可用预设: {[p['name'] for p in presets]}")
        
        # 验证规则
        validation_result = self.rule_manager.validate_rules()
        print(f"规则验证结果: {validation_result['valid']}")
        
        # 性能报告
        performance_report = self.rule_manager.get_rule_performance_report()
        print(f"性能报告: {len(performance_report['execution_times'])} 个规则有执行统计")
        
        # 导出文档
        doc = self.rule_manager.export_rules_documentation()
        print(f"规则文档长度: {len(doc)} 字符")
        
        print("\n=== 示例完成 ===")


def main():
    """主函数，运行示例"""
    examples = RuleEngineExamples()
    examples.demonstrate_rule_usage()


if __name__ == "__main__":
    main() 