from finhack.core.loader.base_loader import BaseLoader

class CheckLoader(BaseLoader):
    """
    检查模块加载器
    支持 finhack check --target=data,cache 命令
    """

    def run(self):
        """执行检查任务"""
        checker = self.klass
        checker.args = self.args
        checker.run()
