from __future__ import annotations

import datetime as _dt
from typing import Any, Dict

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


_US_MARKET_HOLIDAYS = {
    "2026-01-01",
    "2026-01-19",
    "2026-02-16",
    "2026-04-03",
    "2026-05-25",
    "2026-07-03",
    "2026-09-07",
    "2026-11-26",
    "2026-12-25",
    "2027-01-01",
    "2027-01-18",
    "2027-02-15",
    "2027-03-26",
    "2027-05-31",
    "2027-07-05",
    "2027-09-06",
    "2027-11-25",
    "2027-12-24",
}


def _now_ny() -> _dt.datetime:
    if ZoneInfo is None:
        return _dt.datetime.now()
    return _dt.datetime.now(ZoneInfo("America/New_York"))


def _next_stock_open(now: _dt.datetime) -> _dt.datetime:
    day = now
    for _ in range(14):
        open_t = day.replace(hour=9, minute=30, second=0, microsecond=0)
        dkey = day.date().isoformat()
        if day.weekday() < 5 and dkey not in _US_MARKET_HOLIDAYS and open_t > now:
            return open_t
        day = (day + _dt.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return (now + _dt.timedelta(days=1)).replace(hour=9, minute=30, second=0, microsecond=0)


def stock_market_awareness(now_ny: _dt.datetime | None = None) -> Dict[str, Any]:
    now = now_ny or _now_ny()
    dkey = now.date().isoformat()
    is_weekend = now.weekday() >= 5
    is_holiday = dkey in _US_MARKET_HOLIDAYS
    open_t = now.replace(hour=9, minute=30, second=0, microsecond=0)
    close_t = now.replace(hour=16, minute=0, second=0, microsecond=0)
    is_open = (not is_weekend) and (not is_holiday) and (open_t <= now <= close_t)
    next_open = _next_stock_open(now)
    next_close = close_t if is_open else None
    countdown_s = 0
    status = "closed"
    if is_holiday:
        note = f"US holiday ({dkey})"
        countdown_s = int(max(0.0, (next_open - now).total_seconds()))
    elif is_weekend:
        note = "Weekend (market closed)"
        countdown_s = int(max(0.0, (next_open - now).total_seconds()))
    elif is_open:
        mins_to_close = int(max(0.0, (close_t - now).total_seconds()) // 60)
        note = f"Open | closes in {mins_to_close}m"
        status = "open"
        countdown_s = int(max(0.0, (close_t - now).total_seconds()))
    elif now < open_t:
        mins_to_open = int(max(0.0, (open_t - now).total_seconds()) // 60)
        note = f"Pre-market | opens in {mins_to_open}m"
        countdown_s = int(max(0.0, (open_t - now).total_seconds()))
        next_open = open_t
    else:
        note = "After-hours"
        countdown_s = int(max(0.0, (next_open - now).total_seconds()))
    return {
        "is_open": bool(is_open),
        "is_holiday": bool(is_holiday),
        "status": status,
        "note": note,
        "countdown_s": int(max(0, countdown_s)),
        "next_open_ts": int(next_open.timestamp()) if next_open else 0,
        "next_close_ts": int(next_close.timestamp()) if next_close else 0,
    }


def forex_session_bias(now_ny: _dt.datetime | None = None) -> Dict[str, Any]:
    now = now_ny or _now_ny()
    if now.weekday() >= 5:
        sunday_open = now
        while sunday_open.weekday() != 6:
            sunday_open = (sunday_open + _dt.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        sunday_open = sunday_open.replace(hour=17, minute=0, second=0, microsecond=0)
        if sunday_open <= now:
            sunday_open = sunday_open + _dt.timedelta(days=7)
        return {
            "session": "Weekend",
            "bias": "FLAT",
            "volatility": "LOW",
            "note": "Forex weekend closure",
            "next_session": "Sydney/Asia",
            "session_eta_s": int(max(0.0, (sunday_open - now).total_seconds())),
        }
    h = now.hour + (now.minute / 60.0)
    if 0 <= h < 7:
        eta = int(max(0.0, ((7.0 - h) * 3600.0)))
        return {
            "session": "Asia",
            "bias": "RANGE",
            "volatility": "LOW",
            "note": "Asia session typically lower volatility",
            "next_session": "London",
            "session_eta_s": eta,
        }
    if 7 <= h < 12:
        eta = int(max(0.0, ((12.0 - h) * 3600.0)))
        return {
            "session": "London",
            "bias": "TREND",
            "volatility": "MED",
            "note": "London open often increases momentum",
            "next_session": "London/NY",
            "session_eta_s": eta,
        }
    if 12 <= h < 16:
        eta = int(max(0.0, ((16.0 - h) * 3600.0)))
        return {
            "session": "London/NY",
            "bias": "TREND",
            "volatility": "HIGH",
            "note": "Session overlap usually highest liquidity",
            "next_session": "NY Late",
            "session_eta_s": eta,
        }
    eta = int(max(0.0, ((24.0 - h) * 3600.0)))
    return {
        "session": "NY Late",
        "bias": "MEAN-REV",
        "volatility": "MED",
        "note": "Late NY often cools trend speed",
        "next_session": "Asia",
        "session_eta_s": eta,
    }


def broker_maintenance_awareness(now_ny: _dt.datetime | None = None) -> Dict[str, Any]:
    now = now_ny or _now_ny()
    # Conservative awareness notes (informational only).
    alpaca = "Normal"
    alpaca_level = "ok"
    if 22 <= now.hour <= 23:
        alpaca = "Potential nightly maintenance window (informational)"
        alpaca_level = "warn"
    if now.weekday() >= 5:
        alpaca = "Weekend (equity markets closed)"
        alpaca_level = "info"

    oanda = "Normal"
    oanda_level = "ok"
    if (now.weekday() == 4 and now.hour >= 17) or (now.weekday() == 5) or (now.weekday() == 6 and now.hour < 17):
        oanda = "Weekend FX closure window"
        oanda_level = "info"
    return {
        "alpaca": alpaca,
        "alpaca_level": alpaca_level,
        "oanda": oanda,
        "oanda_level": oanda_level,
    }


def build_awareness_payload() -> Dict[str, Any]:
    now = _now_ny()
    return {
        "ts": int(now.timestamp()),
        "stocks": stock_market_awareness(now),
        "forex": forex_session_bias(now),
        "brokers": broker_maintenance_awareness(now),
    }
