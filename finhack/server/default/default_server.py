from runtime.constant import *
import runtime.global_var as global_var
from finhack.core.loader.class_loader import ClassLoader
import threading
import json
import os
import importlib
import finhack.library.log as Log
import runtime.global_var as global_var
from finhack.library.db import DB
from finhack.trader.default.default_trader import DefaultTrader
from flask import Flask, send_from_directory,render_template,request
import re
class DefaultServer:
    def __init__(self, args):
        # BaseLoader 实例化时传 args（与 DefaultTrader 一致），存为 self.args 供 run() 用
        self.args = args

    def run(self, args=None):
        app = Flask(__name__,
                    template_folder=REPORTS_DIR,
                    static_folder=REPORTS_DIR+'static/')

        root_directory = REPORTS_DIR

        # 挂载 REST API（finhack dashboard 用：/api/markets /api/factors /api/run/* 等）
        try:
            from finhack.server.default.api import api_bp
            app.register_blueprint(api_bp)
            Log.logger.info("API blueprint mounted at /api")
        except Exception as e:
            Log.logger.warning(f"API blueprint 挂载失败（dashboard 将不可用）: {e}")

        @app.route('/dashboard')
        def dashboard():
            """finhack 量化全流程 Dashboard（Vue3 + ECharts SPA）"""
            return send_from_directory(root_directory, 'dashboard.html')

        # @app.route('/<path:path>')
        # def static_proxy(path):
        #     # send_static_file 会猜测正确的 MIME 类型
        #     return send_from_directory(root_directory, path)

        @app.template_filter('to_json')
        def to_json_filter(s):
            # 将DictObj字符串转换为字典，然后转换为JSON字符串
            # 这里假设DictObj的格式总是像DictObj(key="value", ...)这样
            # 你可能需要根据实际情况调整正则表达式
            dict_str = re.sub(r"DictObj\((.*?)\)", r"{\1}", s)
            dict_str = re.sub(r"(\w+)=('[^']*'|\"[^\"]*\")", r'"\1": \2', dict_str)
            dict_str = dict_str.replace("'", '"')
            return dict_str

        # 确保将这个过滤器添加到Jinja的环境中
        app.jinja_env.filters['to_json'] = to_json_filter


        @app.route('/btlog')
        def btlog():
            # 原 view 无 return（Flask 返回 None → TypeError 500）。返回最近一份回测日志尾部。
            import glob as _g
            from runtime.constant import DATA_DIR
            logs = sorted(_g.glob(os.path.join(DATA_DIR, 'logs', 'trader', '*.log'),
                                  key=os.path.getmtime, reverse=True))
            if not logs:
                return '<pre>no trader logs</pre>'
            with open(logs[0], 'r', encoding='utf-8', errors='replace') as f:
                tail = f.readlines()[-500:]
            return '<pre>' + ''.join(tail) + '</pre>'

        @app.route('/detail')
        def detail():
            # 获取查询参数id的值
            id = request.args.get('id')
            
            if id:
                # 假设您有一个函数get_detail_by_id来根据id获取详细信息
                context=DefaultTrader.get(id)
                detail=context
                
                
                p_df=context.performance.returns+1
                i_df=context.performance.bench_returns+1
                e_df=(p_df-i_df)+1
                try:
                    p_dates = p_df.index.strftime('%d/%m/%Y').tolist()
                except Exception as e:
                    p_dates = p_df
                p_values = (p_df.values.cumprod()-1).tolist()
                try:
                    i_dates = i_df.index.strftime('%d/%m/%Y').tolist()
                except Exception as e:
                    i_dates = i_df.index.strftime('%d/%m/%Y').tolist()
                i_values = (i_df.values.cumprod()-1).tolist()
                e_values = (e_df.values.cumprod()-1).tolist()   
                
                chart={
                    'dates':p_dates,
                    'i_values':i_values,
                    'p_values':p_values,
                    'e_values':e_values
                }

                # 渲染模板并传递详细信息
                return render_template('detail.html',detail=detail,chart=chart)
            else:
                # 如果没有id参数，则可以重定向到其他页面或返回错误信息
                return "ID is required", 400
            

        @app.route('/<path:path>')
        def static_proxy(path):
            # send_static_file 会猜测正确的 MIME 类型
            return send_from_directory(root_directory, path)

        @app.route('/')
        def redirect_to_index():
            # 原 render_template('index.html')：demo_project 无 templates 目录 → TemplateNotFound 500。
            # dashboard 是静态单页（root_directory/dashboard.html），根路径直接送它。
            idx = os.path.join(root_directory, 'index.html')
            if os.path.isfile(idx):
                return send_from_directory(root_directory, 'index.html')
            return send_from_directory(root_directory, 'dashboard.html')

        @app.route('/legacy_index')
        def legacy_index():
            strategy = request.args.get('strategy')
            # where=' where 1=1 and created_at > (NOW() - INTERVAL 1 DAY)'
            # if strategy:
            #     where=where+f" and strategy='{strategy}'"
            #     bt_list=DB.select_to_list(f"SELECT id, instance_id, features_list, train, model, strategy, start_date, end_date, init_cash, params, total_value, alpha, beta, annual_return, cagr, annual_volatility, info_ratio, downside_risk, R2, sharpe, sortino, calmar, omega, max_down, SQN, created_at, filter, win, server, trade_num, runtime, starttime, endtime,  roto, simulate, benchmark, strategy_code FROM `finhack`.`backtest` {where} order by sharpe desc LIMIT 100",'finhack')
            # else:
            #     bt_list=DB.select_to_list(f"SELECT id, instance_id, features_list, train, model, strategy, start_date, end_date, init_cash, params, total_value, alpha, beta, annual_return, cagr, annual_volatility, info_ratio, downside_risk, R2, sharpe, sortino, calmar, omega, max_down, SQN, created_at, filter, win, server, trade_num, runtime, starttime, endtime,  roto, simulate, benchmark, strategy_code FROM `finhack`.`backtest` {where} order by sharpe desc LIMIT 100",'finhack')   
            

            sql=""
            path=f"{BASE_DIR}/data/config/sqllist/server/bt_list.sql"
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as file:
                    sql= file.read()
            else:
                sql="""SELECT 
            id, a.instance_id, features_list, train, model, strategy, a.start_date, a.end_date, init_cash, params, a.total_value, a.alpha, a.beta, annual_return, cagr, annual_volatility, info_ratio, a.downside_risk, R2, a.sharpe,b.sharpe as sharpe2, a.sortino, calmar, omega, max_down, SQN, created_at, filter, win, server, trade_num, runtime, starttime, endtime,  roto, simulate, a.benchmark, strategy_code 
            FROM `finhack`.`backtest` a
            RIGHT JOIN rqalpha b on a.instance_id =b.instance_id
            ORDER BY b.sharpe desc limit 100"""


            bt_list=DB.select_to_list(sql,'finhack')

            return render_template('index.html', data=bt_list)

        # 不再需要检查 __name__ == '__main__'，因为这个方法将被直接调用
        # host 默认 0.0.0.0（外部可访问），可通过 host= 参数覆盖；port 默认 5000
        app.run(debug=False,
                host=getattr(self.args, 'host', '0.0.0.0') or '0.0.0.0',
                port=int(getattr(self.args, 'port', 5000) or 5000)
            )



