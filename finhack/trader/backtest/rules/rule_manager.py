"""
规则管理器
提供规则引擎的高级管理功能，包括规则的版本控制、配置管理、性能监控等
"""

from typing import Dict, Any, List, Optional, Union
from datetime import datetime
import json
from pathlib import Path
import logging
from copy import deepcopy

from .rule_engine import RuleEngine
from .rule_factory import RuleFactory
from .base_rule import BaseRule, RuleType, RuleResult, RuleGroup


class RuleManager:
    """规则管理器，提供规则引擎的高级管理功能"""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        初始化规则管理器
        
        Args:
            config_path: 配置文件路径
        """
        self.logger = logging.getLogger("RuleManager")
        self.config_path = config_path
        self.config = self._load_config()
        
        # 规则引擎
        self.rule_engine = RuleEngine(self.config)
        
        # 规则工厂
        self.rule_factory = RuleFactory()
        
        # 规则配置历史
        self.config_history = []
        
        # 性能监控
        self.performance_monitor = {
            'rule_execution_times': {},
            'rule_success_rates': {},
            'rule_usage_counts': {}
        }
    
    def _load_config(self) -> Dict[str, Any]:
        """加载配置文件"""
        if not self.config_path:
            return {}
        
        config_file = Path(self.config_path)
        if not config_file.exists():
            return {}
        
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"加载配置文件失败: {e}")
            return {}
    
    def save_config(self, config_path: Optional[str] = None):
        """保存配置文件"""
        path = config_path or self.config_path
        if not path:
            return
        
        config_file = Path(path)
        config_file.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            # 导出当前配置
            config = self.rule_engine.export_config()
            
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"配置已保存到: {path}")
            
        except Exception as e:
            self.logger.error(f"保存配置文件失败: {e}")
    
    def backup_config(self, backup_name: Optional[str] = None):
        """备份当前配置"""
        if not backup_name:
            backup_name = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        current_config = self.rule_engine.export_config()
        
        backup_entry = {
            'name': backup_name,
            'timestamp': datetime.now().isoformat(),
            'config': deepcopy(current_config)
        }
        
        self.config_history.append(backup_entry)
        
        # 保持最近的10个备份
        if len(self.config_history) > 10:
            self.config_history = self.config_history[-10:]
        
        self.logger.info(f"配置已备份为: {backup_name}")
    
    def restore_config(self, backup_name: str):
        """恢复配置"""
        backup_entry = None
        for entry in self.config_history:
            if entry['name'] == backup_name:
                backup_entry = entry
                break
        
        if not backup_entry:
            self.logger.error(f"未找到备份: {backup_name}")
            return False
        
        try:
            self.rule_engine.load_config(backup_entry['config'])
            self.logger.info(f"配置已恢复from: {backup_name}")
            return True
        except Exception as e:
            self.logger.error(f"恢复配置失败: {e}")
            return False
    
    def list_backups(self) -> List[Dict[str, Any]]:
        """列出所有备份"""
        return [
            {
                'name': entry['name'],
                'timestamp': entry['timestamp'],
                'rules_count': len(entry['config'].get('rules', {})),
                'groups_count': len(entry['config'].get('rule_groups', {}))
            }
            for entry in self.config_history
        ]
    
    def create_rule_preset(self, preset_name: str, market: str = "cn_stock", 
                          frequency: str = "1d", custom_rules: Optional[List[str]] = None):
        """
        创建规则预设
        
        Args:
            preset_name: 预设名称
            market: 市场类型
            frequency: 频率
            custom_rules: 自定义规则列表
        """
        # 创建默认规则集
        rule_groups = self.rule_factory.create_default_rule_set(market, frequency)
        
        # 创建新的规则引擎实例
        preset_engine = RuleEngine()
        
        # 添加规则组
        for group in rule_groups:
            preset_engine.add_rule_group(group)
        
        # 添加自定义规则
        if custom_rules:
            for rule_name in custom_rules:
                rule = self.rule_engine.get_rule(rule_name)
                if rule:
                    preset_engine.add_rule(rule)
        
        # 保存预设
        preset_config = preset_engine.export_config()
        preset_config['metadata'] = {
            'name': preset_name,
            'market': market,
            'frequency': frequency,
            'created_at': datetime.now().isoformat(),
            'custom_rules': custom_rules or []
        }
        
        # 保存到文件
        preset_file = Path(f"rule_presets/{preset_name}.json")
        preset_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(preset_file, 'w', encoding='utf-8') as f:
            json.dump(preset_config, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"规则预设已创建: {preset_name}")
    
    def load_rule_preset(self, preset_name: str):
        """加载规则预设"""
        preset_file = Path(f"rule_presets/{preset_name}.json")
        
        if not preset_file.exists():
            self.logger.error(f"预设文件不存在: {preset_name}")
            return False
        
        try:
            with open(preset_file, 'r', encoding='utf-8') as f:
                preset_config = json.load(f)
            
            # 备份当前配置
            self.backup_config(f"before_preset_{preset_name}")
            
            # 加载预设配置
            self.rule_engine.load_config(preset_config)
            
            self.logger.info(f"规则预设已加载: {preset_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"加载预设失败: {e}")
            return False
    
    def list_rule_presets(self) -> List[Dict[str, Any]]:
        """列出所有规则预设"""
        presets = []
        preset_dir = Path("rule_presets")
        
        if not preset_dir.exists():
            return presets
        
        for preset_file in preset_dir.glob("*.json"):
            try:
                with open(preset_file, 'r', encoding='utf-8') as f:
                    preset_config = json.load(f)
                
                metadata = preset_config.get('metadata', {})
                presets.append({
                    'name': metadata.get('name', preset_file.stem),
                    'market': metadata.get('market', 'unknown'),
                    'frequency': metadata.get('frequency', 'unknown'),
                    'created_at': metadata.get('created_at', ''),
                    'rules_count': len(preset_config.get('rules', {})),
                    'groups_count': len(preset_config.get('rule_groups', {}))
                })
                
            except Exception as e:
                self.logger.error(f"读取预设文件失败 {preset_file}: {e}")
        
        return presets
    
    def validate_rules(self) -> Dict[str, Any]:
        """验证规则配置"""
        validation_result = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'summary': {}
        }
        
        try:
            # 检查规则冲突
            self._check_rule_conflicts(validation_result)
            
            # 检查规则依赖
            self._check_rule_dependencies(validation_result)
            
            # 检查规则配置
            self._check_rule_configurations(validation_result)
            
            # 生成验证摘要
            validation_result['summary'] = {
                'total_rules': len(self.rule_engine.rules),
                'enabled_rules': len([r for r in self.rule_engine.rules.values() if r.enabled]),
                'rule_groups': len(self.rule_engine.rule_groups),
                'error_count': len(validation_result['errors']),
                'warning_count': len(validation_result['warnings'])
            }
            
            validation_result['valid'] = len(validation_result['errors']) == 0
            
        except Exception as e:
            validation_result['valid'] = False
            validation_result['errors'].append(f"验证过程异常: {e}")
        
        return validation_result
    
    def _check_rule_conflicts(self, result: Dict[str, Any]):
        """检查规则冲突"""
        # 检查同名规则
        rule_names = set()
        for rule in self.rule_engine.rules.values():
            if rule.name in rule_names:
                result['errors'].append(f"存在同名规则: {rule.name}")
            rule_names.add(rule.name)
        
        # 检查优先级冲突
        priority_groups = {}
        for rule in self.rule_engine.rules.values():
            if rule.priority not in priority_groups:
                priority_groups[rule.priority] = []
            priority_groups[rule.priority].append(rule.name)
        
        for priority, rules in priority_groups.items():
            if len(rules) > 1:
                result['warnings'].append(f"优先级 {priority} 存在多个规则: {', '.join(rules)}")
    
    def _check_rule_dependencies(self, result: Dict[str, Any]):
        """检查规则依赖"""
        # 简化实现，实际可以检查规则之间的依赖关系
        pass
    
    def _check_rule_configurations(self, result: Dict[str, Any]):
        """检查规则配置"""
        for rule in self.rule_engine.rules.values():
            # 检查必需的配置参数
            if hasattr(rule, 'required_config'):
                for param in rule.required_config:
                    if param not in rule.config:
                        result['errors'].append(f"规则 {rule.name} 缺少必需配置: {param}")
            
            # 检查配置值范围
            if hasattr(rule, 'config_validation'):
                for param, value in rule.config.items():
                    if param in rule.config_validation:
                        validator = rule.config_validation[param]
                        if not validator(value):
                            result['warnings'].append(f"规则 {rule.name} 配置 {param} 值可能不正确: {value}")
    
    def monitor_rule_performance(self, rule_name: str, execution_time: float, success: bool):
        """监控规则性能"""
        # 记录执行时间
        if rule_name not in self.performance_monitor['rule_execution_times']:
            self.performance_monitor['rule_execution_times'][rule_name] = []
        self.performance_monitor['rule_execution_times'][rule_name].append(execution_time)
        
        # 记录成功率
        if rule_name not in self.performance_monitor['rule_success_rates']:
            self.performance_monitor['rule_success_rates'][rule_name] = {'success': 0, 'total': 0}
        self.performance_monitor['rule_success_rates'][rule_name]['total'] += 1
        if success:
            self.performance_monitor['rule_success_rates'][rule_name]['success'] += 1
        
        # 记录使用次数
        if rule_name not in self.performance_monitor['rule_usage_counts']:
            self.performance_monitor['rule_usage_counts'][rule_name] = 0
        self.performance_monitor['rule_usage_counts'][rule_name] += 1
    
    def get_rule_performance_report(self) -> Dict[str, Any]:
        """获取规则性能报告"""
        report = {
            'execution_times': {},
            'success_rates': {},
            'usage_counts': self.performance_monitor['rule_usage_counts'],
            'recommendations': []
        }
        
        # 计算平均执行时间
        for rule_name, times in self.performance_monitor['rule_execution_times'].items():
            if times:
                avg_time = sum(times) / len(times)
                report['execution_times'][rule_name] = {
                    'avg': avg_time,
                    'min': min(times),
                    'max': max(times),
                    'count': len(times)
                }
                
                # 添加性能建议
                if avg_time > 0.1:  # 超过100ms
                    report['recommendations'].append(
                        f"规则 {rule_name} 平均执行时间过长 ({avg_time:.3f}s)，建议优化"
                    )
        
        # 计算成功率
        for rule_name, stats in self.performance_monitor['rule_success_rates'].items():
            if stats['total'] > 0:
                success_rate = stats['success'] / stats['total']
                report['success_rates'][rule_name] = {
                    'rate': success_rate,
                    'success': stats['success'],
                    'total': stats['total']
                }
                
                # 添加成功率建议
                if success_rate < 0.8 and stats['total'] > 10:
                    report['recommendations'].append(
                        f"规则 {rule_name} 成功率较低 ({success_rate:.1%})，建议检查配置"
                    )
        
        return report
    
    def optimize_rules(self) -> Dict[str, Any]:
        """优化规则配置"""
        optimization_result = {
            'optimized_rules': [],
            'disabled_rules': [],
            'recommendations': []
        }
        
        performance_report = self.get_rule_performance_report()
        
        # 优化建议：禁用低成功率的规则
        for rule_name, stats in performance_report['success_rates'].items():
            if stats['rate'] < 0.5 and stats['total'] > 10:
                optimization_result['recommendations'].append(
                    f"建议检查规则 {rule_name}，成功率较低: {stats['rate']:.2%}"
                )
        
        # 优化建议：调整慢规则的优先级
        for rule_name, stats in performance_report['execution_times'].items():
            if stats['avg'] > 0.1:  # 平均执行时间超过100ms
                optimization_result['recommendations'].append(
                    f"建议优化规则 {rule_name}，平均执行时间: {stats['avg']:.3f}s"
                )
        
        return optimization_result
    
    def export_rules_documentation(self) -> str:
        """导出规则文档"""
        doc = []
        doc.append("# 规则引擎文档")
        doc.append("")
        
        # 规则统计
        stats = self.rule_engine.get_rule_statistics()
        doc.append("## 规则统计")
        doc.append(f"- 总规则数: {stats['total_rules']}")
        doc.append(f"- 启用规则数: {stats['enabled_rules']}")
        doc.append(f"- 禁用规则数: {stats['disabled_rules']}")
        doc.append(f"- 规则组数: {stats['rule_groups']}")
        doc.append("")
        
        # 规则组文档
        doc.append("## 规则组")
        for group_name, group in self.rule_engine.rule_groups.items():
            doc.append(f"### {group_name}")
            doc.append(f"描述: {group.description}")
            doc.append(f"启用状态: {group.enabled}")
            doc.append(f"规则数量: {len(group.rules)}")
            doc.append("")
            
            # 规则列表
            for rule in group.rules:
                doc.append(f"- **{rule.name}**")
                doc.append(f"  - 类型: {rule.rule_type.value}")
                doc.append(f"  - 启用: {rule.enabled}")
                doc.append(f"  - 优先级: {rule.priority}")
                if hasattr(rule, 'description'):
                    doc.append(f"  - 描述: {rule.description}")
                doc.append("")
        
        return "\n".join(doc)
    
    def get_rule_engine(self) -> RuleEngine:
        """获取规则引擎实例"""
        return self.rule_engine
    
    def get_rule_factory(self) -> RuleFactory:
        """获取规则工厂实例"""
        return self.rule_factory
    
    def reset_performance_monitor(self):
        """重置性能监控数据"""
        self.performance_monitor = {
            'rule_execution_times': {},
            'rule_success_rates': {},
            'rule_usage_counts': {}
        }
        self.logger.info("性能监控数据已重置")
    
    def __str__(self):
        return f"RuleManager(rules={len(self.rule_engine.rules)}, groups={len(self.rule_engine.rule_groups)})"

    def create_rule_template(self, template_name: str, rule_names: List[str], 
                           description: str = "") -> bool:
        """创建规则模板"""
        try:
            template = {
                'name': template_name,
                'description': description,
                'rules': [],
                'created_at': datetime.now().isoformat()
            }
            
            # 收集规则配置
            for rule_name in rule_names:
                rule = self.rule_engine.get_rule(rule_name)
                if rule:
                    template['rules'].append({
                        'name': rule.name,
                        'type': rule.rule_type.value,
                        'config': rule.config.copy(),
                        'enabled': rule.enabled,
                        'priority': rule.priority
                    })
            
            # 保存模板
            template_file = Path(f"rule_templates/{template_name}.json")
            template_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(template_file, 'w', encoding='utf-8') as f:
                json.dump(template, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"规则模板已创建: {template_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"创建规则模板失败: {e}")
            return False

    def load_rule_template(self, template_name: str) -> bool:
        """加载规则模板"""
        template_file = Path(f"rule_templates/{template_name}.json")
        
        if not template_file.exists():
            self.logger.error(f"模板文件不存在: {template_name}")
            return False
        
        try:
            with open(template_file, 'r', encoding='utf-8') as f:
                template = json.load(f)
            
            # 应用模板配置
            for rule_config in template.get('rules', []):
                rule_name = rule_config['name']
                rule = self.rule_engine.get_rule(rule_name)
                if rule:
                    rule.config.update(rule_config.get('config', {}))
                    rule.enabled = rule_config.get('enabled', True)
                    rule.priority = rule_config.get('priority', 0)
            
            self.logger.info(f"规则模板已加载: {template_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"加载规则模板失败: {e}")
            return False

    def list_rule_templates(self) -> List[Dict[str, Any]]:
        """列出所有规则模板"""
        templates = []
        template_dir = Path("rule_templates")
        
        if not template_dir.exists():
            return templates
        
        for template_file in template_dir.glob("*.json"):
            try:
                with open(template_file, 'r', encoding='utf-8') as f:
                    template = json.load(f)
                
                templates.append({
                    'name': template.get('name', template_file.stem),
                    'description': template.get('description', ''),
                    'created_at': template.get('created_at', ''),
                    'rules_count': len(template.get('rules', []))
                })
                
            except Exception as e:
                self.logger.error(f"读取模板文件失败 {template_file}: {e}")
        
        return templates

    def auto_optimize_rules(self, optimization_target: str = "performance") -> Dict[str, Any]:
        """自动优化规则配置"""
        optimization_result = {
            'optimized_rules': [],
            'disabled_rules': [],
            'priority_changes': [],
            'config_changes': [],
            'summary': {}
        }
        
        try:
            performance_report = self.get_rule_performance_report()
            
            if optimization_target == "performance":
                # 优化执行性能
                for rule_name, stats in performance_report['execution_times'].items():
                    if stats['avg'] > 0.05:  # 超过50ms
                        rule = self.rule_engine.get_rule(rule_name)
                        if rule and rule.priority > 0:
                            # 降低优先级
                            old_priority = rule.priority
                            rule.priority = max(0, rule.priority - 1)
                            
                            optimization_result['priority_changes'].append({
                                'rule': rule_name,
                                'old_priority': old_priority,
                                'new_priority': rule.priority,
                                'reason': f"降低优先级以优化性能 (平均执行时间: {stats['avg']:.3f}s)"
                            })
                            
            elif optimization_target == "success_rate":
                # 优化成功率
                for rule_name, stats in performance_report['success_rates'].items():
                    if stats['rate'] < 0.5 and stats['total'] > 20:
                        rule = self.rule_engine.get_rule(rule_name)
                        if rule and rule.enabled:
                            # 禁用低成功率规则
                            rule.enabled = False
                            
                            optimization_result['disabled_rules'].append({
                                'rule': rule_name,
                                'success_rate': stats['rate'],
                                'reason': f"成功率过低 ({stats['rate']:.1%})"
                            })
            
            # 生成优化摘要
            optimization_result['summary'] = {
                'total_optimized': len(optimization_result['optimized_rules']),
                'total_disabled': len(optimization_result['disabled_rules']),
                'priority_changes': len(optimization_result['priority_changes']),
                'config_changes': len(optimization_result['config_changes'])
            }
            
            self.logger.info(f"自动优化完成: {optimization_result['summary']}")
            
        except Exception as e:
            self.logger.error(f"自动优化失败: {e}")
        
        return optimization_result

    def export_rule_config_comparison(self, other_config_path: str) -> str:
        """导出规则配置对比"""
        try:
            # 加载对比配置
            with open(other_config_path, 'r', encoding='utf-8') as f:
                other_config = json.load(f)
            
            current_config = self.rule_engine.export_config()
            
            comparison = []
            comparison.append("# 规则配置对比")
            comparison.append("")
            
            # 对比规则
            current_rules = current_config.get('rules', {})
            other_rules = other_config.get('rules', {})
            
            all_rules = set(current_rules.keys()) | set(other_rules.keys())
            
            for rule_name in sorted(all_rules):
                comparison.append(f"## {rule_name}")
                
                if rule_name in current_rules and rule_name in other_rules:
                    # 对比规则配置
                    current_rule = current_rules[rule_name]
                    other_rule = other_rules[rule_name]
                    
                    if current_rule != other_rule:
                        comparison.append("**配置差异:**")
                        comparison.append("| 项目 | 当前配置 | 对比配置 |")
                        comparison.append("|-----|---------|---------|")
                        
                        for key in set(current_rule.keys()) | set(other_rule.keys()):
                            current_value = current_rule.get(key, 'N/A')
                            other_value = other_rule.get(key, 'N/A')
                            
                            if current_value != other_value:
                                comparison.append(f"| {key} | {current_value} | {other_value} |")
                    else:
                        comparison.append("配置相同")
                
                elif rule_name in current_rules:
                    comparison.append("**仅在当前配置中存在**")
                else:
                    comparison.append("**仅在对比配置中存在**")
                
                comparison.append("")
            
            return "\n".join(comparison)
            
        except Exception as e:
            self.logger.error(f"导出配置对比失败: {e}")
            return f"导出失败: {e}" 