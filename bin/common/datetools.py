from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

UTC_TZ = ZoneInfo("UTC")
MOSCOW_TZ = ZoneInfo("Europe/Moscow")

def minusMonth(d: date) -> date:
    prevMonthEnd = d.replace(day=1) - timedelta(days=1)
    prevMonthDays = prevMonthEnd.day
    return d - timedelta(days=prevMonthDays)

def dateToDt(d: date, tz: ZoneInfo = None)  -> datetime:
    if tz is None:
        tz = UTC_TZ
    return datetime.combine(d, datetime.min.time(), tzinfo=tz)

def toIsoDate(dt: str, fmt:str) -> str:
    return datetime.strptime(dt, fmt).strftime("%Y-%m-%d")
