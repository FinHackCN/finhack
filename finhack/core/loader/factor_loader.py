from finhack.core.loader.base_loader import BaseLoader

class FactorLoader(BaseLoader):
    def run(self):
        # print(self.module_path)
        # print(self.user_module_path)
        # print(self.klass)
        factor=self.klass
        factor.run()
        
        pass
