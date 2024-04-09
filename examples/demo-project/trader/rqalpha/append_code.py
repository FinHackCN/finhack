def init(context):
    context.g=DictObj()
    initialize(context)


def on_trade_handler(context,event):
    trade = event.trade
    order = event.order
    account = event.account
    
    code=order.order_book_id
    # print("after trade")
    # print(get_position(code, POSITION_DIRECTION.LONG).quantity)
    # print(get_position(code, POSITION_DIRECTION.LONG).market_value)   
    
    code=code.replace('XSHG','SH') 
    code=code.replace('XSHE','SZ')
    now_date=context.now.strftime("%Y%m%d")
    vol=str(order.filled_quantity)
    price=str(order.avg_price)
    side=""
    if order.side==SIDE.BUY:
      side="买入"
    else:
      side="卖出"
    buy_str="%s %s %s%s股，当前价格%s [trade-rqalpha]" %(now_date,code,side,vol,price)
    print(buy_str)


def check_stock_code(code,rule_list):
    code=replace_stock_code(code)

    if "BJ" in code or 'T' in code:
        return False
    
    if 'mainboard' in rule_list.split(','):
        if not code.startswith(('600', '601', '603','000','002')):
            return False

    return code


def replace_stock_code(code):
    if 'XSHG' not in code and 'XSHE' not in code:
        code=code.replace('SH','XSHG') 
        code=code.replace('SZ','XSHE')
    return code


def get_position_keys():
    position_keys=[]
    positions=get_positions()
    for pos in positions:
        position_keys.append(pos.order_book_id)
    return position_keys



def get_price(code,context):
    code=replace_stock_code(code)
    snapshot=current_snapshot(code)
    return snapshot['last']