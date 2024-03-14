from runtime.constant import *
import runtime.global_var as global_var
from finhack.library.class_loader import ClassLoader
import threading

import os
import importlib
import finhack.library.log as Log
import runtime.global_var as global_var
from finhack.library.mydb import mydb
from flask import Flask, send_from_directory

class DefaultServer:
    def run(self):
        app = Flask(__name__)

        root_directory = REPORTS_DIR

        @app.route('/<path:path>')
        def static_proxy(path):
            # send_static_file 会猜测正确的 MIME 类型
            return send_from_directory(root_directory, path)

        @app.route('/')
        def redirect_to_index():
            return send_from_directory(root_directory, 'index.html')

        # 不再需要检查 __name__ == '__main__'，因为这个方法将被直接调用
        app.run(debug=True,port=int(self.args.port))