from finhack.core.loader.base_loader import BaseLoader

class CollectorLoader(BaseLoader):
    def run(self):
        # print(self.module_path)
        # print(self.user_module_path)
        # print(self.klass)
        collector=self.klass
        collector.run()
        
        pass
        