from finhack.core.loader.base_loader import BaseLoader


class ServerLoader(BaseLoader):
    """finhack server run port=5055 —— 启动 Flask server + dashboard。
    BaseLoader 会按 finhack.server.default.default_server 实例化 DefaultServer(self.args)。"""
    def run(self):
        server = self.klass
        server.args = self.args
        server.run(self.args)
