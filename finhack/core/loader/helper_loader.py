from finhack.core.loader.base_loader import BaseLoader

class HelperLoader(BaseLoader):
    def run(self):
        # print(self.module_path)
        # print(self.user_module_path)
        # print(self.klass)
        helper=self.klass
        helper.run()
        
        pass
        