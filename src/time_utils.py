from datetime import datetime, timezone
from src.config import TZ

def to_israel(s):
    if not s: return None
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(TZ)

def fmt_time(s):
    dt = to_israel(s)
    return dt.strftime("%H:%M:%S") if dt else ""

def fmt_date(s):
    dt = to_israel(s)
    return dt.strftime("%d/%m/%Y") if dt else ""

def day_he(s):
    days = ["שני","שלישי","רביעי","חמישי","שישי","שבת","ראשון"]
    dt = to_israel(s)
    return days[dt.weekday()] if dt else ""

def round_hour(s):
    dt = to_israel(s)
    return f"{dt.hour:02d}:00" if dt else ""

def dur_min(a, b):
    if not a or not b: return ""
    m = round((datetime.fromisoformat(b) - datetime.fromisoformat(a)).total_seconds() / 60)
    return m if 0 < m < 600 else ""

