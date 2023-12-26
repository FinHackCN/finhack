from finhack.core.classes.dictobj import DictObj




g=DictObj()

 
context_attr= {
            'id':'',
            'universe':[],
            'previous_date':None,
            'current_dt':None,
            'args':None,
            'trade':DictObj({
                'market':'',
                'start_time':'',
                'end_time':'',
                'benchmark':'000001',
                'log_type':'',
                'record_type':'',
                'strategy':'',
                'order_volume_ratio':1,
                'slip':0,
                'sliptype':'pricerelated',
                'rule_list':''
            }),
        
            'account':DictObj({
                'username':'',
                'password':'',
                'account_id':'',
                'open_tax':0,
                'close_tax':0.001,
                'open_commission':0.0003,
                'close_commission':0.0003,
                'close_today_commission':0, 
                'min_commission':5
                
            }),
            
            'portfolio':DictObj({
                'inout_cash':0,
                'cash':0,
                'transferable_cash':0,
                'locked_cash':0,
                'margin':0,
                'total_value':0,
                'previous_value':0,
                'returns':0,
                'starting_cash':0,
                'positions_value':0,
                'portfolio_value':0,
                'locked_cash_by_purchase':0,
                'locked_cash_by_redeem':0,
                'locked_amound_by_redeen':0,
                'positions':{
                    
                }
            }),
            
            'data':DictObj({
                'calendar':[],
                'event_list':[],
                'data_source':'file',
                'daily_info':None,
                'dividend':{},
                'quote':None,
                'client':None
            }),
            
            'logs':DictObj({
                'trade_list':[],
                'order_list':[],
                'position_list':[],
                'return_list':[],
                'trade_returns':[],
                'history':{}
            }),
            'performance':DictObj({
                'returns':[],
                'bench_returns':[],
                'turnover':[],
                'win':0,
                'trade_num':0,
                'indicators':{}
            })

        }
context=DictObj(context_attr)




           #'position':{
                # 'xxx':sid 标的代码
                # enable_amount 可用数量
                # amount 总持仓数量
                # last_sale_price 最新价格
                # cost_basis 持仓成本价格(期货不支持)
                # today_amount 今日开仓数量(期货不支持，且仅回测有效)
                # 期货专用字段：
                # delivery_date 交割日，期货使用
                # today_short_amount 空头今仓数量
                # today_long_amount 多头今仓数量
                # long_cost_basis 多头持仓成本
                # short_cost_basis 空头持仓成本
                # margin_rate 保证金比例
                # contract_multiplier 合约乘数
                # long_amount 多头总持仓量
                # short_amount 空头总持仓量
                # long_pnl 多头浮动盈亏
                # short_pnl 空头浮动盈亏
                # long_enable_amount 多头可用数量
                # short_enable_amount 多空头可用数量
                # business_type 业务类型
           # },