from runtime.constant import *
import runtime.global_var as global_var
from finhack.library.class_loader import ClassLoader
import threading
import json
import os
import importlib
import finhack.library.log as Log
import runtime.global_var as global_var
from finhack.library.mydb import mydb
from finhack.trader.default.default_trader import DefaultTrader
from flask import Flask, send_from_directory,render_template,request
import re
class DefaultServer:
    def run(self):
        app = Flask(__name__,
                    template_folder=REPORTS_DIR,
                    static_folder=REPORTS_DIR+'static/')

        root_directory = REPORTS_DIR

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

        @app.route('/detail')
        def detail():
            # 获取查询参数id的值
            id = request.args.get('id')
            
            if id:
                # 假设您有一个函数get_detail_by_id来根据id获取详细信息
                context=DefaultTrader.get(id)
                # 渲染模板并传递详细信息
                return render_template('detail.html', detail=context)
            else:
                # 如果没有id参数，则可以重定向到其他页面或返回错误信息
                return "ID is required", 400
            

        @app.route('/<path:path>')
        def static_proxy(path):
            # send_static_file 会猜测正确的 MIME 类型
            return send_from_directory(root_directory, path)

        @app.route('/')
        def redirect_to_index():
            strategy = request.args.get('strategy')
            if strategy:
                bt_list=mydb.selectToList(f"SELECT id, instance_id, features_list, train, model, strategy, start_date, end_date, init_cash, args, total_value, alpha, beta, annual_return, cagr, annual_volatility, info_ratio, downside_risk, R2, sharpe, sortino, calmar, omega, max_down, SQN, created_at, filter, win, server, trade_num, runtime, starttime, endtime,  roto, simulate, benchmark, strategy_code FROM `finhack`.`backtest` where strategy='{strategy}' order by sharpe desc LIMIT 100",'finhack')
            else:
                bt_list=mydb.selectToList("SELECT id, instance_id, features_list, train, model, strategy, start_date, end_date, init_cash, args, total_value, alpha, beta, annual_return, cagr, annual_volatility, info_ratio, downside_risk, R2, sharpe, sortino, calmar, omega, max_down, SQN, created_at, filter, win, server, trade_num, runtime, starttime, endtime,  roto, simulate, benchmark, strategy_code FROM `finhack`.`backtest`  order by sharpe desc LIMIT 100",'finhack')   
            
            return render_template('index.html', data=bt_list)

        # 不再需要检查 __name__ == '__main__'，因为这个方法将被直接调用
        app.run(debug=True,
                port=int(self.args.port)
            )



