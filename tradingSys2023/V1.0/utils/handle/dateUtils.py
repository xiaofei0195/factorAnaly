import calendar
import chinese_calendar
from datetime import datetime, timedelta, date

time_format_day = "%Y-%m-%d"


# 获取两个日期之间的 datetime 日期列表
def return_range_day_datetime(start_dt, end_dt):
    dates = []
    dt = datetime.strptime(start_dt, time_format_day)

    newDate = start_dt[:]

    while newDate <= end_dt:
        # 当日为非交易日，跳过
        today = strToDate(newDate)
        year = today.year
        month = today.month
        max_day = calendar.monthrange(year, month)[1]
        work_days = chinese_calendar.get_workdays(datetime(year, month, 1),
                                                  datetime(year, month, max_day))

        tradingDayDate = datetime.strptime(newDate, '%Y-%m-%d').date()
        # tradingDayDate = datetime.date(*map(int, newDate.split('-')))
        if tradingDayDate not in work_days:
            a = 1
            # print(newDate + "为非交易日，跳过")
        else:
            dates.append(newDate)

        dt = dt + timedelta(1)
        newDate = dt.strftime(time_format_day)
    return dates


def getTradingDayList(tradingDay, shiftDays):
    # 前任意天
    pre_date = (datetime.strptime(tradingDay, time_format_day) - timedelta(days=shiftDays)).strftime(time_format_day)
    dates = return_range_day_datetime(pre_date, tradingDay)
    return dates


def strToDate(date_string):
    return datetime.strptime(date_string, time_format_day)


# 字符串日期加减
def dateStrShift(date_String, num):
    dateFormat = strToDate(date_String)
    shiftDateStr = (dateFormat + timedelta(days=num)).strftime("%Y-%m-%d")
    return shiftDateStr

# dayRange = getTradingDayList('2023-04-17', 5)
# dateStrShift('2023-04-15', -1)
