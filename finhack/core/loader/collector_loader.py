from finhack.core.loader.base_loader import BaseLoader

class CollectorLoader(BaseLoader):
    def run(self):
        # print(self.module_path)
        # print(self.user_module_path)
        # print(self.klass)
        collector=self.klass
        collector.run()
        
        pass
    
    def fix(self):
        """执行数据修复检测"""
        collector = self.klass
        if hasattr(collector, 'fix'):
            result = collector.fix()
            if result:
                print("数据修复检测成功完成")
            else:
                print("数据修复检测过程中出现错误")
        else:
            print("该collector不支持fix功能")
    
    def count(self):
        """执行数据库统计分析"""
        collector = self.klass
        if hasattr(collector, 'count'):
            collector.count()
        else:
            print("该collector不支持count功能")
        