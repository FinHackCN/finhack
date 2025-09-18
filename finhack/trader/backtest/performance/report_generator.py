"""
报告生成器实现
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path
import pandas as pd


class ReportGenerator:
    """报告生成器，生成HTML和文本格式的回测报告"""
    
    def __init__(self):
        self.report_template = self._get_html_template()
    
    def generate_html_report(self, analysis_result: Dict[str, Any], 
                           output_path: str = "backtest_report.html") -> str:
        """
        生成HTML格式的回测报告
        
        Args:
            analysis_result: 分析结果
            output_path: 输出路径
            
        Returns:
            str: 报告文件路径
        """
        
        # 准备报告数据
        report_data = self._prepare_report_data(analysis_result)
        
        # 生成HTML内容
        html_content = self._generate_html_content(report_data)
        
        # 写入文件
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        return output_path
    
    def generate_text_report(self, analysis_result: Dict[str, Any], 
                           output_path: str = "backtest_report.txt") -> str:
        """
        生成文本格式的回测报告
        
        Args:
            analysis_result: 分析结果
            output_path: 输出路径
            
        Returns:
            str: 报告文件路径
        """
        
        # 准备报告数据
        report_data = self._prepare_report_data(analysis_result)
        
        # 生成文本内容
        text_content = self._generate_text_content(report_data)
        
        # 写入文件
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(text_content)
        
        return output_path
    
    def generate_json_report(self, analysis_result: Dict[str, Any], 
                           output_path: str = "backtest_report.json") -> str:
        """
        生成JSON格式的回测报告
        
        Args:
            analysis_result: 分析结果
            output_path: 输出路径
            
        Returns:
            str: 报告文件路径
        """
        
        # 准备报告数据
        report_data = self._prepare_report_data(analysis_result)
        
        # 写入JSON文件
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, ensure_ascii=False, indent=2, 
                     default=self._json_serializer)
        
        return output_path
    
    def _prepare_report_data(self, analysis_result: Dict[str, Any]) -> Dict[str, Any]:
        """准备报告数据"""
        
        summary = analysis_result.get('summary', {})
        performance_metrics = analysis_result.get('performance_metrics', {})
        
        # 基本信息
        report_data = {
            'basic_info': {
                '策略名称': summary.get('strategy', 'Unknown'),
                '回测开始日期': summary.get('start_date', 'Unknown'),
                '回测结束日期': summary.get('end_date', 'Unknown'),
                '初始资金': f"{summary.get('initial_cash', 0):,.2f}",
                '最终资金': f"{summary.get('final_value', 0):,.2f}",
                '交易次数': summary.get('total_trades', 0),
                '报告生成时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            },
            'performance_metrics': {
                '总收益率': f"{performance_metrics.get('total_return', 0):.2%}",
                '年化收益率': f"{performance_metrics.get('annualized_return', 0):.2%}",
                '年化波动率': f"{performance_metrics.get('volatility', 0):.2%}",
                '夏普比率': f"{performance_metrics.get('sharpe_ratio', 0):.4f}",
                '索提诺比率': f"{performance_metrics.get('sortino_ratio', 0):.4f}",
                '卡玛比率': f"{performance_metrics.get('calmar_ratio', 0):.4f}",
                '最大回撤': f"{performance_metrics.get('max_drawdown', 0):.2%}",
                '胜率': f"{performance_metrics.get('win_rate', 0):.2%}",
                '盈亏比': f"{performance_metrics.get('profit_loss_ratio', 0):.4f}",
                '凯利比例': f"{performance_metrics.get('kelly_criterion', 0):.4f}",
                'VaR(95%)': f"{performance_metrics.get('var_95', 0):.2%}",
                'CVaR(95%)': f"{performance_metrics.get('cvar_95', 0):.2%}"
            },
            'risk_metrics': {
                '95%置信水平VaR': f"{performance_metrics.get('var_95', 0):.2%}",
                '95%置信水平CVaR': f"{performance_metrics.get('cvar_95', 0):.2%}",
                '99%置信水平VaR': f"{performance_metrics.get('var_99', 0):.2%}",
                '99%置信水平CVaR': f"{performance_metrics.get('cvar_99', 0):.2%}"
            }
        }
        
        # 如果有基准比较数据
        if 'beta' in performance_metrics:
            report_data['benchmark_comparison'] = {
                'Beta系数': f"{performance_metrics.get('beta', 0):.4f}",
                'Alpha': f"{performance_metrics.get('alpha', 0):.2%}",
                '信息比率': f"{performance_metrics.get('information_ratio', 0):.4f}",
                '基准总收益率': f"{performance_metrics.get('benchmark_total_return', 0):.2%}",
                '基准年化收益率': f"{performance_metrics.get('benchmark_annualized_return', 0):.2%}",
                '基准波动率': f"{performance_metrics.get('benchmark_volatility', 0):.2%}",
                '超额收益': f"{performance_metrics.get('excess_return', 0):.2%}"
            }
        
        # 统计信息
        statistics = performance_metrics.get('statistics', {})
        if statistics:
            report_data['statistics'] = {
                '总周期数': statistics.get('total_periods', 0),
                '盈利周期数': statistics.get('positive_periods', 0),
                '亏损周期数': statistics.get('negative_periods', 0),
                '平盘周期数': statistics.get('zero_periods', 0),
                '最佳周期收益': f"{statistics.get('best_period', 0):.2%}",
                '最差周期收益': f"{statistics.get('worst_period', 0):.2%}",
                '平均周期收益': f"{statistics.get('mean_return', 0):.2%}",
                '中位数周期收益': f"{statistics.get('median_return', 0):.2%}"
            }
        
        # 最大回撤详情
        max_drawdown_period = performance_metrics.get('max_drawdown_period', {})
        if max_drawdown_period:
            report_data['max_drawdown_details'] = {
                '回撤开始位置': max_drawdown_period.get('peak_index', 0),
                '回撤结束位置': max_drawdown_period.get('trough_index', 0),
                '回撤持续期间': max_drawdown_period.get('duration', 0)
            }
        
        # 交易明细
        trades = analysis_result.get('trades', [])
        if trades:
            report_data['trade_summary'] = {
                '总交易次数': len(trades),
                '盈利交易次数': len([t for t in trades if t.get('profit', 0) > 0]),
                '亏损交易次数': len([t for t in trades if t.get('profit', 0) < 0]),
                '平盘交易次数': len([t for t in trades if t.get('profit', 0) == 0])
            }
        
        # 持仓信息
        positions = analysis_result.get('positions', {})
        if positions:
            report_data['position_summary'] = {
                '当前持仓数量': len(positions),
                '持仓代码': list(positions.keys())
            }
        
        return report_data
    
    def _generate_html_content(self, report_data: Dict[str, Any]) -> str:
        """生成HTML内容"""
        
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>回测报告 - {report_data['basic_info']['策略名称']}</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background-color: white;
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #2c3e50;
            text-align: center;
            border-bottom: 2px solid #3498db;
            padding-bottom: 10px;
        }}
        h2 {{
            color: #34495e;
            border-left: 4px solid #3498db;
            padding-left: 10px;
            margin-top: 30px;
        }}
        .section {{
            margin-bottom: 30px;
        }}
        .metrics-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }}
        .metric-card {{
            background-color: #f8f9fa;
            padding: 15px;
            border-radius: 8px;
            border: 1px solid #e9ecef;
        }}
        .metric-name {{
            font-weight: bold;
            color: #495057;
            margin-bottom: 5px;
        }}
        .metric-value {{
            font-size: 1.2em;
            color: #2c3e50;
        }}
        .positive {{
            color: #27ae60;
        }}
        .negative {{
            color: #e74c3c;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 10px;
        }}
        th, td {{
            border: 1px solid #ddd;
            padding: 8px;
            text-align: left;
        }}
        th {{
            background-color: #f2f2f2;
            font-weight: bold;
        }}
        .footer {{
            text-align: center;
            margin-top: 30px;
            padding-top: 20px;
            border-top: 1px solid #ddd;
            color: #666;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>回测报告</h1>
        
        <div class="section">
            <h2>基本信息</h2>
            <div class="metrics-grid">
"""
        
        # 基本信息
        for key, value in report_data['basic_info'].items():
            html += f"""
                <div class="metric-card">
                    <div class="metric-name">{key}</div>
                    <div class="metric-value">{value}</div>
                </div>
"""
        
        html += """
            </div>
        </div>
        
        <div class="section">
            <h2>绩效指标</h2>
            <div class="metrics-grid">
"""
        
        # 绩效指标
        for key, value in report_data['performance_metrics'].items():
            css_class = ""
            if "收益" in key or "比率" in key:
                try:
                    numeric_value = float(value.replace('%', ''))
                    css_class = "positive" if numeric_value > 0 else "negative"
                except:
                    pass
            
            html += f"""
                <div class="metric-card">
                    <div class="metric-name">{key}</div>
                    <div class="metric-value {css_class}">{value}</div>
                </div>
"""
        
        html += """
            </div>
        </div>
"""
        
        # 风险指标
        if 'risk_metrics' in report_data:
            html += """
        <div class="section">
            <h2>风险指标</h2>
            <div class="metrics-grid">
"""
            for key, value in report_data['risk_metrics'].items():
                html += f"""
                <div class="metric-card">
                    <div class="metric-name">{key}</div>
                    <div class="metric-value negative">{value}</div>
                </div>
"""
            html += """
            </div>
        </div>
"""
        
        # 基准比较
        if 'benchmark_comparison' in report_data:
            html += """
        <div class="section">
            <h2>基准比较</h2>
            <div class="metrics-grid">
"""
            for key, value in report_data['benchmark_comparison'].items():
                html += f"""
                <div class="metric-card">
                    <div class="metric-name">{key}</div>
                    <div class="metric-value">{value}</div>
                </div>
"""
            html += """
            </div>
        </div>
"""
        
        # 统计信息
        if 'statistics' in report_data:
            html += """
        <div class="section">
            <h2>统计信息</h2>
            <div class="metrics-grid">
"""
            for key, value in report_data['statistics'].items():
                html += f"""
                <div class="metric-card">
                    <div class="metric-name">{key}</div>
                    <div class="metric-value">{value}</div>
                </div>
"""
            html += """
            </div>
        </div>
"""
        
        # 最大回撤详情
        if 'max_drawdown_details' in report_data:
            html += """
        <div class="section">
            <h2>最大回撤详情</h2>
            <div class="metrics-grid">
"""
            for key, value in report_data['max_drawdown_details'].items():
                html += f"""
                <div class="metric-card">
                    <div class="metric-name">{key}</div>
                    <div class="metric-value">{value}</div>
                </div>
"""
            html += """
            </div>
        </div>
"""
        
        # 交易汇总
        if 'trade_summary' in report_data:
            html += """
        <div class="section">
            <h2>交易汇总</h2>
            <div class="metrics-grid">
"""
            for key, value in report_data['trade_summary'].items():
                html += f"""
                <div class="metric-card">
                    <div class="metric-name">{key}</div>
                    <div class="metric-value">{value}</div>
                </div>
"""
            html += """
            </div>
        </div>
"""
        
        # 持仓汇总
        if 'position_summary' in report_data:
            html += """
        <div class="section">
            <h2>持仓汇总</h2>
            <div class="metrics-grid">
"""
            for key, value in report_data['position_summary'].items():
                if key == '持仓代码':
                    value = ', '.join(value) if isinstance(value, list) else str(value)
                html += f"""
                <div class="metric-card">
                    <div class="metric-name">{key}</div>
                    <div class="metric-value">{value}</div>
                </div>
"""
            html += """
            </div>
        </div>
"""
        
        html += """
        <div class="footer">
            <p>报告由 FinHack 回测系统生成</p>
        </div>
    </div>
</body>
</html>
"""
        
        return html
    
    def _generate_text_content(self, report_data: Dict[str, Any]) -> str:
        """生成文本内容"""
        
        text = f"""
========================================
           回测报告
========================================

基本信息：
{'-' * 40}
"""
        
        # 基本信息
        for key, value in report_data['basic_info'].items():
            text += f"{key:15}: {value}\n"
        
        text += f"""
绩效指标：
{'-' * 40}
"""
        
        # 绩效指标
        for key, value in report_data['performance_metrics'].items():
            text += f"{key:15}: {value}\n"
        
        # 风险指标
        if 'risk_metrics' in report_data:
            text += f"""
风险指标：
{'-' * 40}
"""
            for key, value in report_data['risk_metrics'].items():
                text += f"{key:15}: {value}\n"
        
        # 基准比较
        if 'benchmark_comparison' in report_data:
            text += f"""
基准比较：
{'-' * 40}
"""
            for key, value in report_data['benchmark_comparison'].items():
                text += f"{key:15}: {value}\n"
        
        # 统计信息
        if 'statistics' in report_data:
            text += f"""
统计信息：
{'-' * 40}
"""
            for key, value in report_data['statistics'].items():
                text += f"{key:15}: {value}\n"
        
        # 最大回撤详情
        if 'max_drawdown_details' in report_data:
            text += f"""
最大回撤详情：
{'-' * 40}
"""
            for key, value in report_data['max_drawdown_details'].items():
                text += f"{key:15}: {value}\n"
        
        # 交易汇总
        if 'trade_summary' in report_data:
            text += f"""
交易汇总：
{'-' * 40}
"""
            for key, value in report_data['trade_summary'].items():
                text += f"{key:15}: {value}\n"
        
        # 持仓汇总
        if 'position_summary' in report_data:
            text += f"""
持仓汇总：
{'-' * 40}
"""
            for key, value in report_data['position_summary'].items():
                if key == '持仓代码':
                    value = ', '.join(value) if isinstance(value, list) else str(value)
                text += f"{key:15}: {value}\n"
        
        text += f"""
========================================
报告由 FinHack 回测系统生成
========================================
"""
        
        return text
    
    def _get_html_template(self) -> str:
        """获取HTML模板"""
        # 这里可以加载外部模板文件
        return ""
    
    def _json_serializer(self, obj):
        """JSON序列化器"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        return str(obj)
    
    def create_comparison_report(self, results: List[Dict[str, Any]], 
                               names: List[str], 
                               output_path: str = "comparison_report.html") -> str:
        """
        创建多策略比较报告
        
        Args:
            results: 多个分析结果
            names: 策略名称列表
            output_path: 输出路径
            
        Returns:
            str: 报告文件路径
        """
        
        # 准备比较数据
        comparison_data = []
        
        for i, result in enumerate(results):
            name = names[i] if i < len(names) else f"策略{i+1}"
            data = self._prepare_report_data(result)
            data['strategy_name'] = name
            comparison_data.append(data)
        
        # 生成比较HTML
        html_content = self._generate_comparison_html(comparison_data)
        
        # 写入文件
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        return output_path
    
    def _generate_comparison_html(self, comparison_data: List[Dict[str, Any]]) -> str:
        """生成比较HTML"""
        
        html = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>策略比较报告</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .container { max-width: 1200px; margin: 0 auto; }
        h1 { color: #2c3e50; text-align: center; }
        table { width: 100%; border-collapse: collapse; margin-top: 10px; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: center; }
        th { background-color: #f2f2f2; }
        .positive { color: #27ae60; }
        .negative { color: #e74c3c; }
    </style>
</head>
<body>
    <div class="container">
        <h1>策略比较报告</h1>
        
        <h2>绩效指标对比</h2>
        <table>
            <tr>
                <th>指标</th>
"""
        
        # 添加策略名称到表头
        for data in comparison_data:
            html += f"<th>{data['strategy_name']}</th>"
        
        html += "</tr>"
        
        # 获取所有指标
        if comparison_data:
            metrics = comparison_data[0]['performance_metrics'].keys()
            
            for metric in metrics:
                html += f"<tr><td>{metric}</td>"
                for data in comparison_data:
                    value = data['performance_metrics'].get(metric, 'N/A')
                    html += f"<td>{value}</td>"
                html += "</tr>"
        
        html += """
        </table>
    </div>
</body>
</html>
"""
        
        return html 