from calendar import monthrange
from datetime import datetime, timedelta

def daysdelta(d1, d2):
    d1 = datetime.strptime(d1, "%Y-%m-%d")
    d2 = datetime.strptime(d2, "%Y-%m-%d")
    return abs((d2 - d1).days)


def monthsdelta(d1, d2):
    delta = 0
    if d1 != "" and d1 is not None and d2 != "" and d2 is not None:
        d1 = datetime.strptime(d1, "%Y-%m-%d")
        d2 = datetime.strptime(d2, "%Y-%m-%d")
        while True:
            mdays = monthrange(d1.year, d1.month)[1]
            d1 += timedelta(days=mdays)
            if d1 <= d2:
                delta += 1
            else:
                break
        delta = delta + 1
    return delta
