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


        @app.route('/btlog')
        def btlog():
            id = request.args.get('id')
            

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
            strategy = request.args.get('strategy')
            # where=' where 1=1 and created_at > (NOW() - INTERVAL 1 DAY)'
            # if strategy:
            #     where=where+f" and strategy='{strategy}'"
            #     bt_list=mydb.selectToList(f"SELECT id, instance_id, features_list, train, model, strategy, start_date, end_date, init_cash, params, total_value, alpha, beta, annual_return, cagr, annual_volatility, info_ratio, downside_risk, R2, sharpe, sortino, calmar, omega, max_down, SQN, created_at, filter, win, server, trade_num, runtime, starttime, endtime,  roto, simulate, benchmark, strategy_code FROM `finhack`.`backtest` {where} order by sharpe desc LIMIT 100",'finhack')
            # else:
            #     bt_list=mydb.selectToList(f"SELECT id, instance_id, features_list, train, model, strategy, start_date, end_date, init_cash, params, total_value, alpha, beta, annual_return, cagr, annual_volatility, info_ratio, downside_risk, R2, sharpe, sortino, calmar, omega, max_down, SQN, created_at, filter, win, server, trade_num, runtime, starttime, endtime,  roto, simulate, benchmark, strategy_code FROM `finhack`.`backtest` {where} order by sharpe desc LIMIT 100",'finhack')   
            
            sql="""SELECT 
            id, a.instance_id, features_list, train, model, strategy, a.start_date, a.end_date, init_cash, params, a.total_value, a.alpha, a.beta, annual_return, cagr, annual_volatility, info_ratio, a.downside_risk, R2, a.sharpe,b.sharpe as sharpe2, a.sortino, calmar, omega, max_down, SQN, created_at, filter, win, server, trade_num, runtime, starttime, endtime,  roto, simulate, a.benchmark, strategy_code 
            FROM `finhack`.`backtest` a
            RIGHT JOIN rqalpha b on a.instance_id =b.instance_id
            ORDER BY b.sharpe desc limit 100"""
            bt_list=mydb.selectToList(sql,'finhack')
            return render_template('index.html', data=bt_list)

        # 不再需要检查 __name__ == '__main__'，因为这个方法将被直接调用
        app.run(debug=False,
                port=int(self.args.port)
            )



