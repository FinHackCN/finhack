from backtest.function import buy
from strategies.basicStrategy import basicStrategy

class strategy(basicStrategy):
    def run(instance):
        print(instance)
        pass
    
    def every_bar(instance,i):
        print("every_bar")
        print(instance)
        instance['n']=instance['n']+1
        buy()
        strategy.xxx()
        return instance
        

    def market_open():
        pass
    
    
    
    def market_close():
        pass