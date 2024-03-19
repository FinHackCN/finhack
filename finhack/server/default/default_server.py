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
from flask import Flask, send_from_directory,render_template

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


        @app.route('/<path:path>')
        def static_proxy(path):
            # send_static_file 会猜测正确的 MIME 类型
            return send_from_directory(root_directory, path)

        @app.route('/')
        def redirect_to_index():
            bt_list=mydb.selectToList("SELECT id, instance_id, features_list, train, model, strategy, start_date, end_date, init_cash, args, total_value, alpha, beta, annual_return, cagr, annual_volatility, info_ratio, downside_risk, R2, sharpe, sortino, calmar, omega, max_down, SQN, created_at, filter, win, server, trade_num, runtime, starttime, endtime,  roto, simulate, benchmark, strategy_code FROM `finhack`.`backtest`  order by sharpe desc LIMIT 100",'finhack')   
            
            return render_template('index.html', data=bt_list)

        # 不再需要检查 __name__ == '__main__'，因为这个方法将被直接调用
        app.run(debug=True,
                port=int(self.args.port)
            )



