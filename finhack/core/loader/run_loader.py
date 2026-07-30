from finhack.core.loader.base_loader import BaseLoader


class RunLoader(BaseLoader):
    """统一流程编排 loader：finhack run --steps=factor,analyze,train,trader market=.. freq=.."""
    def __init__(self, args):
        # override BaseLoader.__init__：run 是编排（不经 finhack.<module> 模块实现），只存 args
        self.args = args
        self.klass = self   # core.do_action 调 loader.klass.<action>()，这里 action='run' → self.run()

    def run(self):
        from finhack.api.functional import run_pipeline
        a = self.args
        steps = tuple(s.strip() for s in a.steps.split(',')) if getattr(a, 'steps', '') else ('factor', 'analyze', 'train', 'trader')
        tl = getattr(a, 'task_list', '')
        ml = getattr(a, 'matrix_list', '')
        cl = getattr(a, 'code_list', '')
        run_pipeline(
            steps=steps,
            market=getattr(a, 'market', 'cn_stock'),
            freq=getattr(a, 'freq', '1d'),
            start_date=getattr(a, 'start_date', ''),
            end_date=getattr(a, 'end_date', ''),
            valid_date=getattr(a, 'valid_date', ''),
            factor_task=tl.split(',') if tl else None,
            factor_list=ml.split(',') if ml else None,
            strategy=getattr(a, 'strategy', 'ma_cross'),
            code_list=cl.split(',') if cl else None)
