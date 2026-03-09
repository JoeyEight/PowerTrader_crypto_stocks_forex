from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, Iterable, List, Tuple


def _safe_read_jsonl(path: str, max_lines: int = 5000) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = [ln.strip() for ln in f if ln.strip()]
    except Exception:
        return out
    for ln in lines[-max(1, int(max_lines)):]:
        try:
            row = json.loads(ln)
        except Exception:
            continue
        if isinstance(row, dict):
            out.append(row)
    return out


def _f(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _day_bucket(ts: int) -> str:
    try:
        return time.strftime("%Y-%m-%d", time.localtime(int(ts)))
    except Exception:
        return ""


def _event_outcome(row: Dict[str, Any]) -> Tuple[int, float]:
    evt = str(row.get("event", "") or "").strip().lower()
    ok = bool(row.get("ok", False))
    pnl = _f(row.get("pnl_usd", 0.0), 0.0)
    if evt in {"entry", "exit"} and ok:
        return 1, pnl
    if evt in {"entry_fail", "exit_fail", "shadow_live_divergence"}:
        return 0, pnl
    if evt in {"entry", "exit"} and (not ok):
        return 0, pnl
    return -1, pnl


def _aggregate_rows(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    total = 0
    wins = 0
    pnl_total = 0.0
    for row in rows:
        if not isinstance(row, dict):
            continue
        outcome, pnl = _event_outcome(row)
        if outcome < 0:
            continue
        total += 1
        if outcome > 0:
            wins += 1
        pnl_total += float(pnl)
    win_rate = (100.0 * wins / max(1, total)) if total > 0 else 0.0
    return {
        "samples": int(total),
        "wins": int(wins),
        "win_rate_pct": round(float(win_rate), 4),
        "pnl_usd": round(float(pnl_total), 6),
    }


def build_market_walkforward_report(hub_dir: str, market: str, train_days: int = 7, test_days: int = 1) -> Dict[str, Any]:
    m = str(market or "").strip().lower()
    if m not in {"stocks", "forex"}:
        return {"market": m, "state": "ERROR", "msg": "unsupported market"}

    path = os.path.join(hub_dir, m, "execution_audit.jsonl")
    rows = _safe_read_jsonl(path, max_lines=8000)

    by_day: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        ts = int(float(row.get("ts", 0) or 0))
        if ts <= 0:
            continue
        day = _day_bucket(ts)
        if not day:
            continue
        by_day.setdefault(day, []).append(row)

    ordered_days = sorted(by_day.keys())
    windows: List[Dict[str, Any]] = []
    train_n = max(2, int(train_days))
    test_n = max(1, int(test_days))

    for i in range(train_n, len(ordered_days)):
        train_start = max(0, i - train_n)
        train_slice = ordered_days[train_start:i]
        test_slice = ordered_days[i : i + test_n]
        if not test_slice:
            continue
        train_rows: List[Dict[str, Any]] = []
        test_rows: List[Dict[str, Any]] = []
        for d in train_slice:
            train_rows.extend(by_day.get(d, []))
        for d in test_slice:
            test_rows.extend(by_day.get(d, []))
        train_stats = _aggregate_rows(train_rows)
        test_stats = _aggregate_rows(test_rows)
        windows.append(
            {
                "train_days": train_slice,
                "test_days": test_slice,
                "train": train_stats,
                "test": test_stats,
                "delta_win_rate_pct": round(
                    float(test_stats.get("win_rate_pct", 0.0) or 0.0) - float(train_stats.get("win_rate_pct", 0.0) or 0.0),
                    4,
                ),
            }
        )

    all_stats = _aggregate_rows(rows)
    latest = windows[-1] if windows else {}
    stability = "insufficient"
    if latest:
        delta_win_rate = float(latest.get("delta_win_rate_pct", 0.0) or 0.0)
        stability = "stable" if abs(delta_win_rate) <= 12.0 else ("drifting" if abs(delta_win_rate) <= 22.0 else "unstable")

    return {
        "ts": int(time.time()),
        "market": m,
        "state": "READY",
        "days_covered": int(len(ordered_days)),
        "events_considered": int(all_stats.get("samples", 0) or 0),
        "aggregate": all_stats,
        "latest_window": latest,
        "stability": stability,
        "windows": windows[-16:],
    }


def build_walkforward_report(hub_dir: str) -> Dict[str, Any]:
    return {
        "ts": int(time.time()),
        "stocks": build_market_walkforward_report(hub_dir, "stocks"),
        "forex": build_market_walkforward_report(hub_dir, "forex"),
    }
