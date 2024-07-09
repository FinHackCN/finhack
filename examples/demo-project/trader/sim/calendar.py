from finhack.library.mydb import mydb
class Calendar:
    def get_calendar(start_time,end_time,market):
        start_time=start_time.replace('-','')[0:8]
        end_time=end_time.replace('-','')[0:8]
        calendar={}

        if market=='astock':
            calendar=Calendar.get_astock_calendar(start_time,end_time)
        calendar = [f"{date[:4]}-{date[4:6]}-{date[6:]}" for date in calendar]
        return calendar

    
    #获取A股交易日历
    def get_astock_calendar(start_time,end_time):
        cal=mydb.selectToDf(f"select cal_date from astock_trade_cal where is_open=1 \
        and exchange='SSE'  and cal_date>={start_time} and cal_date<={end_time} \
        order by cal_date asc",'tushare')
        return cal['cal_date'].tolist()