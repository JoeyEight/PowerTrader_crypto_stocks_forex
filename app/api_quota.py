from __future__ import annotations

from typing import Any, Dict, Iterable


def _component_for_row(row: Dict[str, Any]) -> str:
    details = row.get("details", {}) if isinstance(row.get("details", {}), dict) else {}
    txt = " ".join(
        [
            str(row.get("event", "") or ""),
            str(row.get("msg", "") or ""),
            str(details.get("market", "") or ""),
            str(details.get("component", "") or ""),
        ]
    ).lower()
    if "alpaca" in txt or "stocks" in txt:
        return "alpaca"
    if "oanda" in txt or "forex" in txt:
        return "oanda"
    if "kucoin" in txt or "crypto" in txt:
        return "kucoin"
    return "other"


def _is_quota_event(row: Dict[str, Any]) -> bool:
    txt = " ".join(
        [
            str(row.get("event", "") or ""),
            str(row.get("msg", "") or ""),
        ]
    ).lower()
    keys = (" 429", "http 429", "rate limit", "too many requests", "retry-after", "retry after", "quota")
    return any(k in txt for k in keys)


def summarize_quota_events(
    rows: Iterable[Dict[str, Any]],
    now_ts: float,
    warn_15m: int = 4,
    crit_15m: int = 10,
) -> Dict[str, Any]:
    now = float(now_ts)
    out: Dict[str, Any] = {
        "window_seconds": 900,
        "by_component": {},
        "total_15m": 0,
        "status": "ok",
    }
    comps: Dict[str, Dict[str, Any]] = {
        "alpaca": {"count_15m": 0, "count_60m": 0, "last_ts": 0},
        "oanda": {"count_15m": 0, "count_60m": 0, "last_ts": 0},
        "kucoin": {"count_15m": 0, "count_60m": 0, "last_ts": 0},
        "other": {"count_15m": 0, "count_60m": 0, "last_ts": 0},
    }

    for row in rows:
        if not isinstance(row, dict):
            continue
        if not _is_quota_event(row):
            continue
        try:
            ts = float(row.get("ts", 0.0) or 0.0)
        except Exception:
            ts = 0.0
        if ts <= 0.0:
            continue
        age = now - ts
        if age > 3600.0:
            continue
        comp = _component_for_row(row)
        bucket = comps.get(comp, comps["other"])
        if age <= 900.0:
            bucket["count_15m"] = int(bucket.get("count_15m", 0) or 0) + 1
            out["total_15m"] = int(out.get("total_15m", 0) or 0) + 1
        bucket["count_60m"] = int(bucket.get("count_60m", 0) or 0) + 1
        bucket["last_ts"] = max(int(bucket.get("last_ts", 0) or 0), int(ts))
        comps[comp] = bucket

    for name, row in comps.items():
        c15 = int(row.get("count_15m", 0) or 0)
        if c15 >= int(crit_15m):
            row["status"] = "critical"
        elif c15 >= int(warn_15m):
            row["status"] = "warning"
        else:
            row["status"] = "ok"
        comps[name] = row

    total15 = int(out.get("total_15m", 0) or 0)
    if total15 >= int(crit_15m):
        out["status"] = "critical"
    elif total15 >= int(warn_15m):
        out["status"] = "warning"
    out["by_component"] = comps
    return out
