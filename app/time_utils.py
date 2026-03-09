from __future__ import annotations

import time


def now_ts() -> int:
    return int(time.time())


def now_date_local() -> str:
    return time.strftime("%Y-%m-%d", time.localtime(time.time()))


def now_datetime_local() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
