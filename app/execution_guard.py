from __future__ import annotations

from typing import Any, Dict


def _market_row(state: Dict[str, Any], market: str) -> Dict[str, Any]:
    markets = state.get("markets", {}) if isinstance(state.get("markets", {}), dict) else {}
    row = markets.get(market, {})
    if not isinstance(row, dict):
        row = {}
    row.setdefault("failure_streak", 0)
    row.setdefault("disabled_until", 0)
    row.setdefault("last_failure_ts", 0)
    row.setdefault("last_success_ts", 0)
    row.setdefault("last_reason", "")
    markets[market] = row
    state["markets"] = markets
    return row


def update_market_guard(
    state: Dict[str, Any],
    market: str,
    failed: bool,
    now_ts: int,
    threshold: int,
    cooldown_s: int,
    reason: str = "",
) -> Dict[str, Any]:
    out = dict(state or {})
    row = _market_row(out, str(market or "").strip().lower() or "unknown")
    now = int(now_ts)
    thr = max(1, int(threshold))
    cool = max(30, int(cooldown_s))

    if failed:
        row["failure_streak"] = int(row.get("failure_streak", 0) or 0) + 1
        row["last_failure_ts"] = now
        if reason:
            row["last_reason"] = str(reason).strip()[:240]
        if int(row.get("failure_streak", 0) or 0) >= thr:
            row["disabled_until"] = max(int(row.get("disabled_until", 0) or 0), now + cool)
            row["failure_streak"] = 0
    else:
        row["last_success_ts"] = now
        row["failure_streak"] = 0
        if int(row.get("disabled_until", 0) or 0) <= now:
            row["disabled_until"] = 0

    out["ts"] = now
    return out


def market_guard_status(state: Dict[str, Any], market: str, now_ts: int) -> Dict[str, Any]:
    s = dict(state or {})
    row = _market_row(s, str(market or "").strip().lower() or "unknown")
    now = int(now_ts)
    until = int(row.get("disabled_until", 0) or 0)
    rem = max(0, until - now)
    return {
        "active": bool(rem > 0),
        "remaining_s": int(rem),
        "disabled_until": int(until),
        "failure_streak": int(row.get("failure_streak", 0) or 0),
        "last_reason": str(row.get("last_reason", "") or ""),
        "last_failure_ts": int(row.get("last_failure_ts", 0) or 0),
        "last_success_ts": int(row.get("last_success_ts", 0) or 0),
    }
