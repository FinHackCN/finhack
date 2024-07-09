from finhack.core.loader.base_loader import BaseLoader

class TraderLoader(BaseLoader):
    def run(self):
        # print(self.module_path)
        # print(self.user_module_path)
        # print(self.klass)
        trader=self.klass
        trader.args=self.args
        trader.run()
        
        pass
        