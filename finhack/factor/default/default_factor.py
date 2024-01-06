from runtime.constant import *
from finhack.library.config import Config
from finhack.factor.default.preCheck import preCheck
from finhack.factor.default.indicatorCompute import indicatorCompute
from finhack.factor.default.alphaEngine import alphaEngine
from finhack.market.astock.astock import AStock
from finhack.factor.default.taskRunner import taskRunner
class DefaultFactor:
    def __init__(self):
        pass

    def run(self):
        pass
        taskRunner.runTask()
        
    def test(self):
        print(self.args)