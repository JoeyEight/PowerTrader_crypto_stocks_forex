from __future__ import annotations

import json
import os
import statistics
import time
from typing import Any, Dict, Iterable, List, Tuple


def _safe_read_json(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


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


def _i(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return int(default)


def _percentile(values: List[float], q: float) -> float:
    if not values:
        return 0.0
    arr = sorted(float(v) for v in values)
    idx = int(round((len(arr) - 1) * max(0.0, min(1.0, float(q)))))
    idx = max(0, min(idx, len(arr) - 1))
    return float(arr[idx])


def build_incident_trend(incident_rows: Iterable[Dict[str, Any]], now_ts_value: float | None = None) -> Dict[str, Any]:
    now_ts = int(now_ts_value if now_ts_value is not None else time.time())
    horizons = {"1h": 3600, "24h": 86400, "7d": 7 * 86400}
    counts = {k: 0 for k in horizons}
    severities = {"error_1h": 0, "warning_1h": 0}
    for row in incident_rows:
        ts = _i(row.get("ts", 0), 0)
        if ts <= 0 or ts > now_ts:
            continue
        age = now_ts - ts
        for label, sec in horizons.items():
            if age <= sec:
                counts[label] = int(counts[label]) + 1
        if age <= 3600:
            sev = str(row.get("severity", "") or "").strip().lower()
            if sev in {"error", "critical", "high"}:
                severities["error_1h"] = int(severities["error_1h"]) + 1
            elif sev in {"warning", "warn"}:
                severities["warning_1h"] = int(severities["warning_1h"]) + 1

    vals = [int(counts["1h"]), int(counts["24h"]), int(counts["7d"])]
    max_v = max(vals) if vals else 1
    bar = []
    for v in vals:
        width = int(round((20.0 * float(v)) / float(max_v))) if max_v > 0 else 0
        width = max(0, min(20, width))
        bar.append("#" * width + "-" * (20 - width))
    sparkline = f"1h[{bar[0]}] 24h[{bar[1]}] 7d[{bar[2]}]"
    return {
        "counts": {"1h": int(counts["1h"]), "24h": int(counts["24h"]), "7d": int(counts["7d"])},
        "severity_1h": severities,
        "sparkline": sparkline,
        "ts": int(now_ts),
    }


def _unrealized_crypto_usd(hub_dir: str) -> float:
    data = _safe_read_json(os.path.join(hub_dir, "trader_data.json"))
    positions = data.get("positions", {}) if isinstance(data.get("positions", {}), dict) else {}
    total = 0.0
    for row in positions.values():
        if not isinstance(row, dict):
            continue
        # Primary path: explicit unrealized USD.
        u = _f(row.get("unrealized_usd", row.get("unrealized", 0.0)), 0.0)
        if u == 0.0:
            # Fallback: estimate from value and pnl pct if available.
            value = _f(row.get("value_usd", 0.0), 0.0)
            pnl_pct = _f(row.get("pnl_percent", row.get("pnl_pct", 0.0)), 0.0)
            if value > 0.0 and abs(pnl_pct) > 0.0:
                u = (value / max(0.01, 100.0 + pnl_pct)) * pnl_pct
        total += float(u)
    return float(total)


def _unrealized_market_usd(rows: List[Dict[str, Any]], market: str) -> float:
    total = 0.0
    for row in rows:
        if not isinstance(row, dict):
            continue
        if market == "stocks":
            total += _f(row.get("unrealized_pl", 0.0), 0.0)
        else:
            long_leg = row.get("long", {}) if isinstance(row.get("long", {}), dict) else {}
            short_leg = row.get("short", {}) if isinstance(row.get("short", {}), dict) else {}
            total += _f(long_leg.get("unrealizedPL", 0.0), 0.0)
            total += _f(short_leg.get("unrealizedPL", 0.0), 0.0)
    return float(total)


def _collect_trade_fees_and_slippage(audit_rows: Iterable[Dict[str, Any]]) -> Tuple[float, List[float]]:
    fees = 0.0
    spreads: List[float] = []
    for row in audit_rows:
        if not isinstance(row, dict):
            continue
        payload = row.get("payload", {}) if isinstance(row.get("payload", {}), dict) else {}
        fee_row = 0.0
        for key in ("fees_usd", "fee_usd", "fee", "fees"):
            if key in row:
                fee_row += _f(row.get(key), 0.0)
            if key in payload:
                fee_row += _f(payload.get(key), 0.0)
        fees += float(fee_row)
        spr = _f(row.get("spread_bps", 0.0), 0.0)
        if spr > 0.0:
            spreads.append(float(spr))
    return float(fees), spreads


def build_pnl_decomposition(hub_dir: str) -> Dict[str, Any]:
    pnl = _safe_read_json(os.path.join(hub_dir, "pnl_ledger.json"))
    realized = _f(pnl.get("total_realized_profit_usd", 0.0), 0.0)

    stocks_status = _safe_read_json(os.path.join(hub_dir, "stocks", "alpaca_status.json"))
    forex_status = _safe_read_json(os.path.join(hub_dir, "forex", "oanda_status.json"))
    stocks_positions = list(stocks_status.get("raw_positions", []) or []) if isinstance(stocks_status.get("raw_positions", []), list) else []
    forex_positions = list(forex_status.get("raw_positions", []) or []) if isinstance(forex_status.get("raw_positions", []), list) else []

    unrealized = (
        _unrealized_crypto_usd(hub_dir)
        + _unrealized_market_usd(stocks_positions, "stocks")
        + _unrealized_market_usd(forex_positions, "forex")
    )

    trade_hist = _safe_read_jsonl(os.path.join(hub_dir, "trade_history.jsonl"), max_lines=5000)
    stock_audit = _safe_read_jsonl(os.path.join(hub_dir, "stocks", "execution_audit.jsonl"), max_lines=5000)
    forex_audit = _safe_read_jsonl(os.path.join(hub_dir, "forex", "execution_audit.jsonl"), max_lines=5000)

    fees_crypto, slip_crypto = _collect_trade_fees_and_slippage(trade_hist)
    fees_stock, slip_stock = _collect_trade_fees_and_slippage(stock_audit)
    fees_forex, slip_forex = _collect_trade_fees_and_slippage(forex_audit)
    slippage_series = list(slip_crypto) + list(slip_stock) + list(slip_forex)
    avg_slippage = round((sum(slippage_series) / max(1, len(slippage_series))), 4) if slippage_series else 0.0

    return {
        "ts": int(time.time()),
        "realized_usd": round(float(realized), 6),
        "unrealized_usd": round(float(unrealized), 6),
        "fees_usd": round(float(fees_crypto + fees_stock + fees_forex), 6),
        "slippage_bps_avg": float(avg_slippage),
        "slippage_bps_p95": round(_percentile(slippage_series, 0.95), 4),
        "slippage_samples": int(len(slippage_series)),
    }


def _history_values(account_history_rows: Iterable[Dict[str, Any]]) -> List[Tuple[int, float]]:
    vals: List[Tuple[int, float]] = []
    for row in account_history_rows:
        if not isinstance(row, dict):
            continue
        ts = _i(row.get("ts", row.get("timestamp", 0)), 0)
        if ts <= 0:
            continue
        value = None
        for key in ("account_value", "total_account_value", "value", "equity"):
            if key in row:
                v = _f(row.get(key), 0.0)
                if v > 0.0:
                    value = v
                    break
        if value is None:
            continue
        vals.append((int(ts), float(value)))
    vals.sort(key=lambda x: x[0])
    return vals


def detect_equity_anomaly(
    account_history_rows: Iterable[Dict[str, Any]],
    now_ts_value: float | None = None,
    lookback_points: int = 180,
    min_samples: int = 20,
    spike_pct: float = 3.0,
) -> Dict[str, Any]:
    now_ts = int(now_ts_value if now_ts_value is not None else time.time())
    vals = _history_values(account_history_rows)
    if len(vals) < max(3, int(min_samples)):
        return {"state": "insufficient_data", "active": False, "samples": int(len(vals)), "ts": int(now_ts)}

    window = vals[-max(int(lookback_points), int(min_samples)) :]
    series = [v for _, v in window]
    last = float(series[-1])
    prev = float(series[-2]) if len(series) >= 2 else float(last)
    baseline = list(series[:-1])
    med = float(statistics.median(baseline)) if baseline else float(last)
    delta_prev_pct = ((last - prev) / max(1e-9, prev)) * 100.0
    delta_med_pct = ((last - med) / max(1e-9, med)) * 100.0
    mad = statistics.median([abs(x - med) for x in baseline]) if baseline else 0.0
    robust_z = 0.0
    if mad > 0.0:
        robust_z = ((last - med) / max(1e-9, mad * 1.4826))
    else:
        # Flat baselines are common during API outages/restarts. Fall back to stdev and
        # a tiny relative denominator so real jumps are still detectable.
        stdev = statistics.pstdev(baseline) if len(baseline) >= 2 else 0.0
        denom = stdev if stdev > 0.0 else max(1e-9, abs(med) * 0.001)
        robust_z = ((last - med) / max(1e-9, denom))
    z_ok = abs(robust_z) >= 3.5
    if (not z_ok) and mad <= 0.0 and abs(delta_med_pct) >= float(spike_pct) * 1.2:
        z_ok = True
    active = bool((abs(delta_prev_pct) >= float(spike_pct)) and (abs(delta_med_pct) >= float(spike_pct)) and z_ok)
    return {
        "state": "ok",
        "active": bool(active),
        "direction": ("up" if delta_prev_pct > 0.0 else ("down" if delta_prev_pct < 0.0 else "flat")),
        "last_value": round(last, 6),
        "delta_prev_pct": round(delta_prev_pct, 6),
        "delta_median_pct": round(delta_med_pct, 6),
        "robust_z": round(float(robust_z), 6),
        "samples": int(len(series)),
        "thresholds": {"spike_pct": float(spike_pct), "robust_z": 3.5},
        "ts": int(now_ts),
    }


def detect_stale_history(
    account_history_rows: Iterable[Dict[str, Any]],
    now_ts_value: float | None = None,
    stale_after_s: int = 600,
) -> Dict[str, Any]:
    now_ts = int(now_ts_value if now_ts_value is not None else time.time())
    vals = _history_values(account_history_rows)
    last_ts = int(vals[-1][0]) if vals else 0
    age_s = (now_ts - last_ts) if last_ts > 0 else -1
    active = bool((last_ts <= 0) or (age_s >= max(30, int(stale_after_s))))
    return {
        "active": bool(active),
        "last_ts": int(last_ts),
        "age_s": int(age_s),
        "stale_after_s": int(max(30, int(stale_after_s))),
        "state": ("stale" if active else "fresh"),
        "ts": int(now_ts),
    }


def build_broker_latency_histogram(
    runtime_event_rows: Iterable[Dict[str, Any]],
    market_audit_rows: Dict[str, List[Dict[str, Any]]] | None = None,
    now_ts_value: float | None = None,
) -> Dict[str, Any]:
    now_ts = int(now_ts_value if now_ts_value is not None else time.time())
    waits: List[float] = []
    for row in runtime_event_rows:
        if not isinstance(row, dict):
            continue
        evt = str(row.get("event", "") or "").strip().lower()
        if evt != "broker_retry_after_wait":
            continue
        details = row.get("details", {}) if isinstance(row.get("details", {}), dict) else {}
        wait_s = _f(details.get("wait_s", 0.0), 0.0)
        if wait_s <= 0.0:
            wait_s = _f(row.get("wait_s", 0.0), 0.0)
        if wait_s > 0.0:
            waits.append(wait_s)

    rows_map = market_audit_rows if isinstance(market_audit_rows, dict) else {}
    for rows in rows_map.values():
        for row in list(rows or []):
            if not isinstance(row, dict):
                continue
            wait_s = _f(row.get("retry_after_wait_s", 0.0), 0.0)
            if wait_s > 0.0:
                waits.append(wait_s)

    buckets = {
        "lt_1s": 0,
        "1_3s": 0,
        "3_10s": 0,
        "10_30s": 0,
        "30_120s": 0,
        "gte_120s": 0,
    }
    for w in waits:
        if w < 1.0:
            buckets["lt_1s"] += 1
        elif w < 3.0:
            buckets["1_3s"] += 1
        elif w < 10.0:
            buckets["3_10s"] += 1
        elif w < 30.0:
            buckets["10_30s"] += 1
        elif w < 120.0:
            buckets["30_120s"] += 1
        else:
            buckets["gte_120s"] += 1
    return {
        "ts": int(now_ts),
        "samples": int(len(waits)),
        "avg_s": round((sum(waits) / max(1, len(waits))), 4) if waits else 0.0,
        "p50_s": round(_percentile(waits, 0.50), 4),
        "p95_s": round(_percentile(waits, 0.95), 4),
        "max_s": round((max(waits) if waits else 0.0), 4),
        "buckets": buckets,
    }
