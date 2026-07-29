import random
from runtime.constant import *
from finhack.library.config import Config
# 单股因子计算(compute)待迁移：indicatorEngine 仅提供批量接口 computeIndicator，详见 compute()
from finhack.factor.default.indicatorEngine import indicatorEngine
from finhack.factor.default.alphaEngine import alphaEngine
from finhack.factor.default.taskRunner import taskRunner
from finhack.factor.default.factorManager import factorManager
from finhack.factor.default.factorAnalyzer import factorAnalyzer
from finhack.factor.default.factorMining import factorMining
from finhack.library.db import DB
import finhack.library.log as Log


class DefaultFactor:
    def __init__(self, args):
        self.args = args

    def run(self):
        taskRunner.runTask(self.args)

    def test(self):
        print(self.args)

    def compute(self):
        # 旧版 indicatorCompute.computeFactorByStock 在当前版未迁移（indicatorEngine 仅提供批量接口）。
        # 如需单股计算：indicatorEngine.computeIndicator(market, freq, [factor], code_list=[code])
        raise NotImplementedError("单股因子计算(compute)待迁移到 indicatorEngine")

    def todb(self):
        factor = self.args.factor

    def list(self):
        market = getattr(self.args, 'market', 'cn_stock')
        freq = getattr(self.args, 'freq', '1d')
        print(factorManager.list_factors(market=market, freq=freq))

    def show(self):
        market = getattr(self.args, 'market', 'cn_stock')
        freq = getattr(self.args, 'freq', '1d')
        factor_name = self.args.factor
        factor = factorManager.loadFactors(matrix_list=[factor_name], market=market, freq=freq)
        print(factor)
        if not factor.empty:
            print(factor.describe())

    def analys(self):
        market = getattr(self.args, 'market', 'cn_stock')
        freq = getattr(self.args, 'freq', '1d')
        factor_name = self.args.factor
        stock300 = self.args.stock300.split(',') if getattr(self.args, 'stock300', '') else []
        factorAnalyzer.analys(factor_name=factor_name, source='default', replace=True,
                              code_list=stock300, market=market, freq=freq)
        factorAnalyzer.alphalens(factor_name=factor_name, market=market, freq=freq, code_list=stock300)

    def analys_all(self):
        market = getattr(self.args, 'market', 'cn_stock')
        freq = getattr(self.args, 'freq', '1d')
        factor_list = factorManager.list_factors(market=market, freq=freq)
        stock300 = self.args.stock300.split(',') if getattr(self.args, 'stock300', '') else []
        for factor in factor_list:
            factorAnalyzer.analys(factor_name=factor, source='default', replace=False,
                                  code_list=stock300, market=market, freq=freq, ignore_error=True)

    def mining(self):
        method = self.args.method
        market = getattr(self.args, 'market', 'cn_stock')
        freq = getattr(self.args, 'freq', '1d')
        start_date = getattr(self.args, 'start_date', '20180101')
        end_date = getattr(self.args, 'end_date', '20221231')
        stock25 = self.args.stock25.split(',')
        stock300 = self.args.stock300.split(',')

        if method == "gplearn":
            while True:
                min_n = int(self.args.min_n)
                max_n = int(self.args.max_n)
                # 已分析因子：MySQL factors_analysis；不可达则用 list_factors
                try:
                    df_a = DB.select_to_df(
                        'select factor_name from factors_analysis where factor_name not like "alpha%"', 'finhack')
                    flist = df_a['factor_name'].tolist() if not df_a.empty else []
                except Exception:
                    flist = []
                if not flist:
                    flist = factorManager.list_factors(market=market, freq=freq)
                random.shuffle(flist)
                n = random.randint(min_n, max_n)
                factor_list = flist[:n] + ['open', 'close']

                df25 = factorManager.loadFactors(matrix_list=factor_list, code_list=stock25,
                                                 market=market, freq=freq, start_date=start_date, end_date=end_date)
                if df25 is None or df25.empty:
                    print("mining: 训练数据为空，跳过本轮"); continue
                df25 = df25.reset_index()
                df25['Y'] = df25.groupby('code', group_keys=False).apply(
                    lambda x: x['close'].shift(-10) / x['open'].shift(-1))
                df25 = df25.dropna(subset=['Y'])

                df300 = factorManager.loadFactors(matrix_list=factor_list, code_list=stock300,
                                                  market=market, freq=freq, start_date=start_date, end_date=end_date)
                if df300 is None or df300.empty:
                    continue
                df300 = df300.reset_index()
                df300['Y'] = df300.groupby('code', group_keys=False).apply(
                    lambda x: x['close'].shift(-10) / x['open'].shift(-1))

                label = df25['Y']
                train = df25.drop(columns=['time', 'code', 'Y'])
                df_tmp = df25.set_index(['time', 'code'])
                df_check = df300.set_index(['time', 'code'])
                factorMining.gplearn(train, label, df_tmp, df_check, source='gplearn',
                                      market=market, freq=freq, start_date=start_date, end_date=end_date)

        elif method.lower() in ("gpt", "chatgpt"):
            factorMining.gpt(self.args.prompt, self.args.model, stock300,
                             market=market, freq=freq, start_date=start_date, end_date=end_date)
        elif method.lower() == "kimi":
            factorMining.kimi(self.args.prompt, self.args.model, stock300,
                              market=market, freq=freq, start_date=start_date, end_date=end_date)

    def calc(self):
        formula = self.args.formula
        print(formula)
        market = getattr(self.args, 'market', 'cn_stock')
        freq = getattr(self.args, 'freq', '1d')
        df_alpha = alphaEngine.calc(formula=formula, name="alpha", market=market, freq=freq)
        print(df_alpha)
        factorAnalyzer.alphalens(factor_name='alpha', df=df_alpha, market=market, freq=freq)

    def newlist(self):
        pass
        # finhack factor newlist --listname=woldy
