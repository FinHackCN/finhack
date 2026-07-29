from finhack.core.loader.base_loader import BaseLoader


class TrainerLoader(BaseLoader):
    def run(self):
        trainer = self.klass
        trainer.args = self.args
        trainer.run()
