#主板
def MainBoard(stock):
    #print(stock)
    if stock==None:
        return False
    else:
        if '688' in stock['ts_code'][:3]:
            return False
        elif  '300' in stock['ts_code'][:3]:
            return False
        elif  'BJ' in stock['ts_code']:
            return False
        else:
            return True