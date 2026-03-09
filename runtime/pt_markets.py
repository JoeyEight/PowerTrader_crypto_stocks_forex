from __future__ import annotations

import json
import os
import random
import signal
import statistics
import time
from typing import Any, Dict

from app.api_endpoint_validation import validate_alpaca_endpoints, validate_oanda_endpoints
from app.confidence_calibration import build_confidence_calibration_payload
from app.credential_utils import get_alpaca_creds, get_oanda_creds
from app.execution_guard import market_guard_status, update_market_guard
from app.market_trends import build_trends_payload
from app.path_utils import read_settings_file, resolve_runtime_paths, resolve_settings_path
from app.regime_classifier import build_all_market_regimes
from app.runtime_logging import append_jsonl, atomic_write_json, runtime_event
from app.settings_utils import sanitize_settings
from app.shadow_scorecard import build_shadow_scorecards
from app.time_utils import now_date_local
from app.walkforward_report import build_walkforward_report
from brokers.broker_alpaca import AlpacaBrokerClient
from brokers.broker_oanda import OandaBrokerClient
from engines.forex_thinker import run_scan as run_forex_scan
from engines.forex_trader import run_step as run_forex_trader_step
from engines.stock_thinker import run_scan as run_stock_scan
from engines.stock_trader import run_step as run_stock_trader_step

BASE_DIR, _SETTINGS_PATH, HUB_DATA_DIR, _BOOT_SETTINGS = resolve_runtime_paths(__file__, "pt_markets")
STOP_FLAG_PATH = os.path.join(HUB_DATA_DIR, "stop_trading.flag")
INCIDENTS_PATH = os.path.join(HUB_DATA_DIR, "incidents.jsonl")
RUNTIME_EVENTS_PATH = os.path.join(HUB_DATA_DIR, "runtime_events.jsonl")
SLA_METRICS_PATH = os.path.join(HUB_DATA_DIR, "market_sla_metrics.json")
SCAN_DRIFT_PATH = os.path.join(HUB_DATA_DIR, "scan_drift_alerts.json")
CADENCE_DRIFT_PATH = os.path.join(HUB_DATA_DIR, "scanner_cadence_drift.json")
MARKET_TRENDS_PATH = os.path.join(HUB_DATA_DIR, "market_trends.json")
MARKET_REGIMES_PATH = os.path.join(HUB_DATA_DIR, "market_regimes.json")
WALKFORWARD_PATH = os.path.join(HUB_DATA_DIR, "walkforward_report.json")
CONFIDENCE_CALIBRATION_PATH = os.path.join(HUB_DATA_DIR, "confidence_calibration.json")
SHADOW_SCORECARDS_PATH = os.path.join(HUB_DATA_DIR, "shadow_deployment_scorecards.json")
EXEC_GUARD_PATH = os.path.join(HUB_DATA_DIR, "broker_execution_guard.json")
MARKET_LOOP_STATUS_PATH = os.path.join(HUB_DATA_DIR, "market_loop_status.json")
INCIDENT_COOLDOWN_S = 120.0
_LAST_INCIDENT_AT: Dict[str, float] = {}


def _atomic_write_json(path: str, payload: Dict[str, Any]) -> None:
    atomic_write_json(path, payload)


def _safe_read_json(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _p95(values: list[float]) -> float:
    if not values:
        return 0.0
    arr = sorted(float(x) for x in values)
    idx = int(round((len(arr) - 1) * 0.95))
    idx = max(0, min(idx, len(arr) - 1))
    return float(arr[idx])


def _update_sla_metrics(bucket: str, ok: bool, elapsed_ms: float, extra: Dict[str, Any] | None = None) -> None:
    data = _safe_read_json(SLA_METRICS_PATH)
    if not isinstance(data, dict):
        data = {}
    metrics = data.get("metrics", {})
    if not isinstance(metrics, dict):
        metrics = {}
    row = metrics.get(bucket, {})
    if not isinstance(row, dict):
        row = {}

    count = int(row.get("count", 0) or 0) + 1
    ok_count = int(row.get("ok_count", 0) or 0) + (1 if ok else 0)
    err_count = int(row.get("err_count", 0) or 0) + (0 if ok else 1)
    total_ms = float(row.get("total_ms", 0.0) or 0.0) + float(elapsed_ms)

    recent = row.get("recent_ms", [])
    if not isinstance(recent, list):
        recent = []
    recent = [float(x) for x in recent[-199:]] + [float(elapsed_ms)]

    out_row = {
        "count": int(count),
        "ok_count": int(ok_count),
        "err_count": int(err_count),
        "error_rate_pct": round((100.0 * float(err_count) / float(max(1, count))), 3),
        "avg_ms": round(float(total_ms) / float(max(1, count)), 3),
        "last_ms": round(float(elapsed_ms), 3),
        "p95_ms": round(_p95(recent), 3),
        "recent_ms": recent,
        "updated_ts": int(time.time()),
        "extra": dict(extra or {}),
        "status": "ok" if ok else "error",
    }
    metrics[bucket] = out_row
    payload = {"ts": int(time.time()), "metrics": metrics}
    _atomic_write_json(SLA_METRICS_PATH, payload)


def _update_scan_reject_drift(market: str, reject_rate_pct: float, settings: Dict[str, Any], state: str) -> Dict[str, Any]:
    now = int(time.time())
    data = _safe_read_json(SCAN_DRIFT_PATH)
    if not isinstance(data, dict):
        data = {}
    markets = data.get("markets", {})
    if not isinstance(markets, dict):
        markets = {}
    row = markets.get(market, {})
    if not isinstance(row, dict):
        row = {}
    history = row.get("history", [])
    if not isinstance(history, list):
        history = []

    rr = max(0.0, min(100.0, float(reject_rate_pct or 0.0)))
    history = [float(x) for x in history[-119:]]
    history.append(rr)

    min_samples = max(3, int(float(settings.get("runtime_alert_reject_spike_min_samples", 6) or 6)))
    min_rate = max(0.0, min(100.0, float(settings.get("runtime_alert_reject_spike_min_rate_pct", 25.0) or 25.0)))
    delta_thr = max(0.0, min(100.0, float(settings.get("runtime_alert_reject_spike_delta_pct", 25.0) or 25.0)))
    ratio_thr = max(1.0, float(settings.get("runtime_alert_reject_spike_ratio", 2.0) or 2.0))

    prev = history[:-1]
    baseline = float(statistics.median(prev[-max(min_samples, 12):])) if len(prev) >= min_samples else 0.0
    delta = rr - baseline
    ratio = (rr / baseline) if baseline > 0.0 else (99.0 if rr > 0.0 else 1.0)
    spike = bool(
        str(state or "").upper() == "READY"
        and len(prev) >= min_samples
        and rr >= min_rate
        and delta >= delta_thr
        and ratio >= ratio_thr
    )

    last_alert_ts = int(row.get("last_alert_ts", 0) or 0)
    cooldown_s = 300
    triggered = False
    active = []
    old_active = data.get("active", [])
    if isinstance(old_active, list):
        active = [a for a in old_active if isinstance(a, dict)]

    if spike and ((now - last_alert_ts) >= cooldown_s):
        triggered = True
        alert = {
            "ts": int(now),
            "market": str(market),
            "reject_rate_pct": round(rr, 3),
            "baseline_pct": round(baseline, 3),
            "delta_pct": round(delta, 3),
            "ratio": round(ratio, 3),
            "min_rate_pct": round(min_rate, 3),
            "delta_threshold_pct": round(delta_thr, 3),
            "ratio_threshold": round(ratio_thr, 3),
        }
        active = [a for a in active if str(a.get("market", "") or "").strip().lower() != str(market).lower()]
        active.append(alert)
        row["last_alert_ts"] = int(now)
        _incident(
            "warning",
            "scanner_reject_spike",
            f"{market} reject spike {rr:.1f}% (baseline {baseline:.1f}%, delta {delta:.1f}%)",
            {"market": market, "reject_rate_pct": rr, "baseline_pct": baseline, "delta_pct": delta, "ratio": ratio},
            cooldown_key=f"scanner_reject_spike:{market}",
        )

    # Auto-clear stale active alerts after 30 minutes.
    cutoff = now - 1800
    active = [a for a in active if int(a.get("ts", 0) or 0) >= cutoff]

    row["history"] = history
    row["updated_ts"] = int(now)
    row["baseline_pct"] = round(baseline, 3)
    row["last_reject_rate_pct"] = round(rr, 3)
    row["last_delta_pct"] = round(delta, 3)
    row["last_ratio"] = round(ratio, 3)
    markets[market] = row
    payload = {
        "ts": int(now),
        "markets": markets,
        "active": active,
    }
    _atomic_write_json(SCAN_DRIFT_PATH, payload)
    return {
        "triggered": bool(triggered),
        "market": str(market),
        "reject_rate_pct": round(rr, 3),
        "baseline_pct": round(baseline, 3),
        "delta_pct": round(delta, 3),
        "ratio": round(ratio, 3),
        "active_count": int(len(active)),
    }


def _update_scan_cadence_drift(
    market: str,
    now_ts: int,
    expected_interval_s: float,
    settings: Dict[str, Any],
    state: str,
) -> Dict[str, Any]:
    now = int(now_ts or time.time())
    expected = max(1.0, float(expected_interval_s or 1.0))
    data = _safe_read_json(CADENCE_DRIFT_PATH)
    if not isinstance(data, dict):
        data = {}
    markets = data.get("markets", {})
    if not isinstance(markets, dict):
        markets = {}
    row = markets.get(market, {})
    if not isinstance(row, dict):
        row = {}

    history = row.get("history_s", [])
    if not isinstance(history, list):
        history = []
    history = [float(x) for x in history[-119:]]

    last_scan_ts = int(row.get("last_scan_ts", 0) or 0)
    observed_s = float(now - last_scan_ts) if last_scan_ts > 0 else 0.0
    late_pct = 0.0
    if observed_s > 0.0:
        late_pct = max(0.0, ((observed_s - expected) / expected) * 100.0)

    if observed_s > 0.0:
        history.append(observed_s)
    min_samples = max(2, int(float(settings.get("runtime_alert_cadence_min_samples", 3) or 3)))
    warn_pct = max(10.0, float(settings.get("runtime_alert_cadence_late_warn_pct", 80.0) or 80.0))
    crit_pct = max(warn_pct, float(settings.get("runtime_alert_cadence_late_crit_pct", 180.0) or 180.0))
    cooldown_s = max(30, int(float(settings.get("runtime_alert_cadence_cooldown_s", 300) or 300)))

    level = "ok"
    if late_pct >= crit_pct:
        level = "critical"
    elif late_pct >= warn_pct:
        level = "warning"
    late = level in {"warning", "critical"}

    active = data.get("active", [])
    if not isinstance(active, list):
        active = []
    active = [a for a in active if isinstance(a, dict)]
    last_alert_ts = int(row.get("last_alert_ts", 0) or 0)
    triggered = False
    if late and str(state or "").upper() == "READY" and len(history) >= min_samples and (now - last_alert_ts) >= cooldown_s:
        triggered = True
        alert = {
            "ts": int(now),
            "market": str(market),
            "level": str(level),
            "observed_s": round(observed_s, 3),
            "expected_s": round(expected, 3),
            "late_pct": round(late_pct, 3),
        }
        active = [a for a in active if str(a.get("market", "") or "").strip().lower() != str(market).lower()]
        active.append(alert)
        row["last_alert_ts"] = int(now)
        sev = "error" if level == "critical" else "warning"
        _incident(
            sev,
            "scanner_cadence_drift",
            f"{market} scan cadence drift: observed {observed_s:.1f}s vs expected {expected:.1f}s ({late_pct:.1f}% late)",
            {"market": market, "level": level, "observed_s": observed_s, "expected_s": expected, "late_pct": late_pct},
            cooldown_key=f"scanner_cadence_drift:{market}",
        )
    if not late:
        active = [a for a in active if str(a.get("market", "") or "").strip().lower() != str(market).lower()]

    cutoff = now - 7200
    active = [a for a in active if int(a.get("ts", 0) or 0) >= cutoff]

    row["updated_ts"] = int(now)
    row["last_scan_ts"] = int(now)
    row["expected_s"] = round(expected, 3)
    row["observed_s"] = round(observed_s, 3)
    row["late_pct"] = round(late_pct, 3)
    row["level"] = str(level)
    row["history_s"] = history
    markets[str(market)] = row
    payload = {"ts": int(now), "markets": markets, "active": active}
    _atomic_write_json(CADENCE_DRIFT_PATH, payload)
    return {
        "market": str(market),
        "observed_s": round(observed_s, 3),
        "expected_s": round(expected, 3),
        "late_pct": round(late_pct, 3),
        "level": str(level),
        "late": bool(late),
        "triggered": bool(triggered),
        "active_count": int(len(active)),
    }


def _incident(severity: str, event: str, msg: str, details: Dict[str, Any] | None = None, cooldown_key: str = "") -> None:
    now = time.time()
    key = str(cooldown_key or f"{event}:{msg[:80]}").strip() or event
    last = float(_LAST_INCIDENT_AT.get(key, 0.0) or 0.0)
    if (now - last) < INCIDENT_COOLDOWN_S:
        return
    _LAST_INCIDENT_AT[key] = now
    append_jsonl(
        INCIDENTS_PATH,
        {
            "ts": int(now),
            "date": now_date_local(),
            "severity": str(severity or "info").lower(),
            "event": str(event or "markets_event"),
            "msg": str(msg or "").strip(),
            "details": dict(details or {}),
        },
    )
    runtime_event(
        RUNTIME_EVENTS_PATH,
        component="markets",
        event=str(event or "markets_event"),
        level=str(severity or "info"),
        msg=str(msg or ""),
        details=dict(details or {}),
    )


def _is_missing_value(v: Any) -> bool:
    s = str(v or "").strip().lower()
    return s in {"", "n/a", "pending account link", "none", "null"}


def _broker_failure_signal(msg: str, state: str) -> bool:
    st = str(state or "").upper().strip()
    txt = str(msg or "").lower()
    if st == "READY":
        return False
    bad = ("http ", "network error", "timeout", "rate limit", "too many requests", "retry-after", "dns", "connection")
    return any(k in txt for k in bad)


def _guard_load() -> Dict[str, Any]:
    data = _safe_read_json(EXEC_GUARD_PATH)
    return data if isinstance(data, dict) else {}


def _guard_save(data: Dict[str, Any]) -> None:
    _atomic_write_json(EXEC_GUARD_PATH, data if isinstance(data, dict) else {})


def _record_guard_result(settings: Dict[str, Any], market: str, failed: bool, reason: str = "") -> Dict[str, Any]:
    now = int(time.time())
    try:
        threshold = max(2, int(float(settings.get("broker_failure_disable_threshold", 4) or 4)))
    except Exception:
        threshold = 4
    try:
        cooldown_s = max(60, int(float(settings.get("broker_failure_disable_cooldown_s", 900) or 900)))
    except Exception:
        cooldown_s = 900
    state = _guard_load()
    before = market_guard_status(state, market, now)
    state = update_market_guard(
        state,
        market=market,
        failed=bool(failed),
        now_ts=now,
        threshold=threshold,
        cooldown_s=cooldown_s,
        reason=reason,
    )
    after = market_guard_status(state, market, now)
    _guard_save(state)
    if (not before.get("active", False)) and bool(after.get("active", False)):
        _incident(
            "warning",
            "execution_temporarily_disabled",
            f"{market} execution disabled for {int(after.get('remaining_s', 0))}s after repeated broker failures",
            {"market": market, "remaining_s": int(after.get("remaining_s", 0)), "reason": str(after.get("last_reason", "") or "")},
            cooldown_key=f"exec_guard_on:{market}",
        )
    return after


def _merge_with_last_good(path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    cur = payload if isinstance(payload, dict) else {}
    prev = _safe_read_json(path)
    if not isinstance(prev, dict):
        prev = {}
    if not prev:
        return cur

    cur_state = str(cur.get("state", "") or "").upper().strip()
    cur_buying_power = cur.get("buying_power", "")
    prev_buying_power = prev.get("buying_power", "")
    cur_ready = (cur_state == "READY") and (not _is_missing_value(cur_buying_power))
    prev_ready = str(prev.get("state", "") or "").upper().strip() == "READY" and (not _is_missing_value(prev_buying_power))
    if cur_ready:
        return cur
    if not prev_ready:
        return cur

    # Keep last known-good account metrics when refresh is degraded.
    merged = dict(prev)
    merged.update(cur)
    for k in ("buying_power", "open_positions", "realized_pnl", "positions_preview", "raw_positions", "equity"):
        if _is_missing_value(cur.get(k, "")) or k not in cur:
            merged[k] = prev.get(k)
    merged["state"] = "READY"
    base_msg = str(cur.get("msg", "") or "").strip()
    if base_msg:
        merged["msg"] = f"{base_msg} | using last good snapshot"
    else:
        merged["msg"] = str(prev.get("msg", "") or "using last good snapshot")
    return merged


def _payload_age_s(payload: Dict[str, Any], now_ts: int | None = None) -> float:
    if not isinstance(payload, dict):
        return float("inf")
    now = int(now_ts if now_ts is not None else time.time())
    ts = int(float(payload.get("ts", payload.get("updated_at", 0)) or 0))
    if ts <= 0:
        return float("inf")
    return max(0.0, float(now - ts))


def _cached_status_fallback(path: str, max_age_s: float, now_ts: int | None = None) -> Dict[str, Any]:
    cached = _safe_read_json(path)
    age_s = _payload_age_s(cached, now_ts=now_ts)
    if (not cached) or (age_s > float(max_age_s)):
        return {}
    out = dict(cached)
    out["fallback_cached"] = True
    out["fallback_age_s"] = int(round(age_s))
    return out


def _load_settings() -> Dict[str, Any]:
    settings_path = resolve_settings_path(BASE_DIR) or _SETTINGS_PATH or os.path.join(BASE_DIR, "gui_settings.json")
    data = read_settings_file(settings_path, module_name="pt_markets") or {}
    return sanitize_settings(data if isinstance(data, dict) else {})


def _jittered_interval(base_s: float, jitter_pct: float) -> float:
    base = max(1.0, float(base_s))
    pct = max(0.0, min(0.5, float(jitter_pct)))
    if pct <= 0.0:
        return base
    span = base * pct
    return max(1.0, base + random.uniform(-span, span))


def _write_loop_status(payload: Dict[str, Any]) -> None:
    try:
        _atomic_write_json(MARKET_LOOP_STATUS_PATH, payload if isinstance(payload, dict) else {})
    except Exception:
        pass


def _write_snapshots(settings: Dict[str, Any]) -> Dict[str, Any]:
    stocks_dir = os.path.join(HUB_DATA_DIR, "stocks")
    forex_dir = os.path.join(HUB_DATA_DIR, "forex")
    os.makedirs(stocks_dir, exist_ok=True)
    os.makedirs(forex_dir, exist_ok=True)
    out: Dict[str, Any] = {
        "stocks_ok": False,
        "forex_ok": False,
        "stocks_elapsed_ms": 0.0,
        "forex_elapsed_ms": 0.0,
        "stocks_state": "",
        "forex_state": "",
    }
    now_ts = int(time.time())
    try:
        snapshot_fallback_age_s = max(30.0, float(settings.get("market_fallback_snapshot_max_age_s", 1800.0) or 1800.0))
    except Exception:
        snapshot_fallback_age_s = 1800.0
    stocks_path = os.path.join(stocks_dir, "alpaca_status.json")
    forex_path = os.path.join(forex_dir, "oanda_status.json")

    alpaca_key, alpaca_secret = get_alpaca_creds(settings, base_dir=BASE_DIR)
    oanda_account, oanda_token = get_oanda_creds(settings, base_dir=BASE_DIR)
    alpaca_endpoint = validate_alpaca_endpoints(
        settings.get("alpaca_base_url", "https://paper-api.alpaca.markets"),
        settings.get("alpaca_data_url", "https://data.alpaca.markets"),
        paper_mode=bool(settings.get("alpaca_paper_mode", True)),
    )
    oanda_endpoint = validate_oanda_endpoints(
        settings.get("oanda_rest_url", "https://api-fxpractice.oanda.com"),
        settings.get("oanda_stream_url", ""),
        practice_mode=bool(settings.get("oanda_practice_mode", True)),
    )
    out["endpoint_validation"] = {
        "alpaca_valid": bool(alpaca_endpoint.get("valid", False)),
        "oanda_valid": bool(oanda_endpoint.get("valid", False)),
        "alpaca_issues": int(len(list(alpaca_endpoint.get("issues", []) or []))),
        "oanda_issues": int(len(list(oanda_endpoint.get("issues", []) or []))),
    }
    for row in list(alpaca_endpoint.get("issues", []) or []):
        if not isinstance(row, dict):
            continue
        lvl = str(row.get("level", "warning") or "warning").strip().lower()
        msg = str(row.get("message", "alpaca endpoint warning") or "alpaca endpoint warning")
        _incident(
            "error" if lvl == "critical" else "warning",
            str(row.get("code", "alpaca_endpoint_warning") or "alpaca_endpoint_warning"),
            msg,
            {"service": "alpaca", "details": row.get("details", {}) if isinstance(row.get("details", {}), dict) else {}},
            cooldown_key=f"endpoint:alpaca:{str(row.get('code', '') or '')}",
        )
    for row in list(oanda_endpoint.get("issues", []) or []):
        if not isinstance(row, dict):
            continue
        lvl = str(row.get("level", "warning") or "warning").strip().lower()
        msg = str(row.get("message", "oanda endpoint warning") or "oanda endpoint warning")
        _incident(
            "error" if lvl == "critical" else "warning",
            str(row.get("code", "oanda_endpoint_warning") or "oanda_endpoint_warning"),
            msg,
            {"service": "oanda", "details": row.get("details", {}) if isinstance(row.get("details", {}), dict) else {}},
            cooldown_key=f"endpoint:oanda:{str(row.get('code', '') or '')}",
        )
    alpaca = AlpacaBrokerClient(
        api_key_id=alpaca_key,
        secret_key=alpaca_secret,
        base_url=str(alpaca_endpoint.get("normalized_base_url", "") or "https://paper-api.alpaca.markets"),
        data_url=str(alpaca_endpoint.get("normalized_data_url", "") or "https://data.alpaca.markets"),
    )
    oanda = OandaBrokerClient(
        account_id=oanda_account,
        api_token=oanda_token,
        rest_url=str(oanda_endpoint.get("normalized_rest_url", "") or "https://api-fxpractice.oanda.com"),
    )

    try:
        t0 = time.perf_counter()
        s = alpaca.fetch_snapshot()
        s["ts"] = int(time.time())
        s = _merge_with_last_good(stocks_path, s)
        _atomic_write_json(stocks_path, s)
        out["stocks_ok"] = True
        out["stocks_state"] = str(s.get("state", "") or "")
        out["stocks_elapsed_ms"] = round((time.perf_counter() - t0) * 1000.0, 3)
        _incident("info", "stocks_snapshot_ok", "stocks snapshot updated", {"state": s.get("state", "")}, cooldown_key="stocks_snapshot_ok")
        _update_sla_metrics("stocks_snapshot", ok=True, elapsed_ms=(time.perf_counter() - t0) * 1000.0, extra={"state": s.get("state", "")})
        _record_guard_result(
            settings,
            market="stocks",
            failed=_broker_failure_signal(str(s.get("msg", "") or ""), str(s.get("state", "") or "")),
            reason=str(s.get("msg", "") or ""),
        )
    except Exception as exc:
        print(f"[MARKETS] stocks snapshot failed: {type(exc).__name__}: {exc}")
        out["stocks_ok"] = False
        out["stocks_state"] = "ERROR"
        _incident("error", "stocks_snapshot_failed", f"{type(exc).__name__}: {exc}", {"market": "stocks"}, cooldown_key="stocks_snapshot_failed")
        _update_sla_metrics("stocks_snapshot", ok=False, elapsed_ms=0.0, extra={"error": f"{type(exc).__name__}: {exc}"})
        _record_guard_result(settings, market="stocks", failed=True, reason=f"{type(exc).__name__}: {exc}")
        cached = _cached_status_fallback(stocks_path, snapshot_fallback_age_s, now_ts=now_ts)
        if cached:
            cached["state"] = str(cached.get("state", "READY") or "READY")
            cached["msg"] = f"{type(exc).__name__}: {exc} | using cached snapshot ({int(cached.get('fallback_age_s', 0))}s old)"
            _atomic_write_json(stocks_path, cached)
            out["stocks_ok"] = True
            out["stocks_state"] = str(cached.get("state", "READY") or "READY")
            out["stocks_elapsed_ms"] = 0.0
            out["stocks_fallback_cached"] = True
            _incident(
                "warning",
                "stocks_snapshot_fallback_cached",
                str(cached.get("msg", "") or "stocks snapshot fallback cached"),
                {"market": "stocks", "fallback_age_s": int(cached.get("fallback_age_s", 0) or 0)},
                cooldown_key="stocks_snapshot_fallback_cached",
            )
            _update_sla_metrics(
                "stocks_snapshot",
                ok=True,
                elapsed_ms=0.0,
                extra={"state": "CACHED_FALLBACK", "fallback_age_s": int(cached.get("fallback_age_s", 0) or 0)},
            )
    try:
        t0 = time.perf_counter()
        f = oanda.fetch_snapshot()
        f["ts"] = int(time.time())
        f = _merge_with_last_good(forex_path, f)
        _atomic_write_json(forex_path, f)
        out["forex_ok"] = True
        out["forex_state"] = str(f.get("state", "") or "")
        out["forex_elapsed_ms"] = round((time.perf_counter() - t0) * 1000.0, 3)
        _incident("info", "forex_snapshot_ok", "forex snapshot updated", {"state": f.get("state", "")}, cooldown_key="forex_snapshot_ok")
        _update_sla_metrics("forex_snapshot", ok=True, elapsed_ms=(time.perf_counter() - t0) * 1000.0, extra={"state": f.get("state", "")})
        _record_guard_result(
            settings,
            market="forex",
            failed=_broker_failure_signal(str(f.get("msg", "") or ""), str(f.get("state", "") or "")),
            reason=str(f.get("msg", "") or ""),
        )
    except Exception as exc:
        print(f"[MARKETS] forex snapshot failed: {type(exc).__name__}: {exc}")
        out["forex_ok"] = False
        out["forex_state"] = "ERROR"
        _incident("error", "forex_snapshot_failed", f"{type(exc).__name__}: {exc}", {"market": "forex"}, cooldown_key="forex_snapshot_failed")
        _update_sla_metrics("forex_snapshot", ok=False, elapsed_ms=0.0, extra={"error": f"{type(exc).__name__}: {exc}"})
        _record_guard_result(settings, market="forex", failed=True, reason=f"{type(exc).__name__}: {exc}")
        cached = _cached_status_fallback(forex_path, snapshot_fallback_age_s, now_ts=now_ts)
        if cached:
            cached["state"] = str(cached.get("state", "READY") or "READY")
            cached["msg"] = f"{type(exc).__name__}: {exc} | using cached snapshot ({int(cached.get('fallback_age_s', 0))}s old)"
            _atomic_write_json(forex_path, cached)
            out["forex_ok"] = True
            out["forex_state"] = str(cached.get("state", "READY") or "READY")
            out["forex_elapsed_ms"] = 0.0
            out["forex_fallback_cached"] = True
            _incident(
                "warning",
                "forex_snapshot_fallback_cached",
                str(cached.get("msg", "") or "forex snapshot fallback cached"),
                {"market": "forex", "fallback_age_s": int(cached.get("fallback_age_s", 0) or 0)},
                cooldown_key="forex_snapshot_fallback_cached",
            )
            _update_sla_metrics(
                "forex_snapshot",
                ok=True,
                elapsed_ms=0.0,
                extra={"state": "CACHED_FALLBACK", "fallback_age_s": int(cached.get("fallback_age_s", 0) or 0)},
            )
    return out


def _run_stocks(settings: Dict[str, Any]) -> Dict[str, Any]:
    stocks_dir = os.path.join(HUB_DATA_DIR, "stocks")
    os.makedirs(stocks_dir, exist_ok=True)
    thinker_status_path = os.path.join(stocks_dir, "stock_thinker_status.json")
    try:
        scan_fallback_age_s = max(60.0, float(settings.get("market_fallback_scan_max_age_s", 7200.0) or 7200.0))
    except Exception:
        scan_fallback_age_s = 7200.0
    out: Dict[str, Any] = {
        "scan_ok": False,
        "scan_state": "",
        "scan_ms": 0.0,
        "step_ok": False,
        "step_state": "",
        "step_ms": 0.0,
        "guard_active": False,
        "cadence": {},
    }
    try:
        t0 = time.perf_counter()
        thinker = run_stock_scan(settings, HUB_DATA_DIR)
        thinker["ts"] = int(time.time())
        _atomic_write_json(thinker_status_path, thinker)
        t_state = str(thinker.get("state", "") or "").upper()
        out["scan_ok"] = t_state != "ERROR"
        out["scan_state"] = t_state
        out["scan_ms"] = round((time.perf_counter() - t0) * 1000.0, 3)
        if t_state == "ERROR":
            _incident("error", "stocks_thinker_error", str(thinker.get("msg", "") or "stocks thinker error"), {"market": "stocks"}, cooldown_key="stocks_thinker_error")
            _update_sla_metrics("stocks_scan", ok=False, elapsed_ms=(time.perf_counter() - t0) * 1000.0, extra={"state": t_state, "msg": str(thinker.get("msg", ""))[:180]})
        else:
            _incident("info", "stocks_thinker_ok", "stocks thinker updated", {"state": t_state}, cooldown_key="stocks_thinker_ok")
            _update_sla_metrics("stocks_scan", ok=True, elapsed_ms=(time.perf_counter() - t0) * 1000.0, extra={"state": t_state})
        rs = thinker.get("reject_summary", {}) if isinstance(thinker.get("reject_summary", {}), dict) else {}
        _update_scan_reject_drift("stocks", float(rs.get("reject_rate_pct", 0.0) or 0.0), settings, t_state)
        out["cadence"] = _update_scan_cadence_drift(
            "stocks",
            int(time.time()),
            float(settings.get("market_bg_stocks_interval_s", 15.0) or 15.0),
            settings,
            t_state,
        )
    except Exception as exc:
        print(f"[MARKETS] stocks thinker failed: {type(exc).__name__}: {exc}")
        cached = _cached_status_fallback(thinker_status_path, scan_fallback_age_s)
        if cached:
            cached["state"] = str(cached.get("state", "READY") or "READY")
            cached["msg"] = f"{type(exc).__name__}: {exc} | using cached thinker ({int(cached.get('fallback_age_s', 0))}s old)"
            _atomic_write_json(thinker_status_path, cached)
            t_state = str(cached.get("state", "READY") or "READY").upper()
            out["scan_ok"] = t_state != "ERROR"
            out["scan_state"] = t_state
            out["scan_ms"] = 0.0
            out["scan_fallback_cached"] = True
            rs = cached.get("reject_summary", {}) if isinstance(cached.get("reject_summary", {}), dict) else {}
            _update_scan_reject_drift("stocks", float(rs.get("reject_rate_pct", 0.0) or 0.0), settings, t_state)
            out["cadence"] = _update_scan_cadence_drift(
                "stocks",
                int(time.time()),
                float(settings.get("market_bg_stocks_interval_s", 15.0) or 15.0),
                settings,
                t_state,
            )
            _incident(
                "warning",
                "stocks_thinker_fallback_cached",
                str(cached.get("msg", "") or "stocks thinker fallback cached"),
                {"market": "stocks", "fallback_age_s": int(cached.get("fallback_age_s", 0) or 0)},
                cooldown_key="stocks_thinker_fallback_cached",
            )
            _update_sla_metrics(
                "stocks_scan",
                ok=True,
                elapsed_ms=0.0,
                extra={"state": "CACHED_FALLBACK", "fallback_age_s": int(cached.get("fallback_age_s", 0) or 0)},
            )
        else:
            out["scan_ok"] = False
            out["scan_state"] = "ERROR"
            _incident("error", "stocks_thinker_failed", f"{type(exc).__name__}: {exc}", {"market": "stocks"}, cooldown_key="stocks_thinker_failed")
            _update_sla_metrics("stocks_scan", ok=False, elapsed_ms=0.0, extra={"error": f"{type(exc).__name__}: {exc}"})
            out["cadence"] = _update_scan_cadence_drift(
                "stocks",
                int(time.time()),
                float(settings.get("market_bg_stocks_interval_s", 15.0) or 15.0),
                settings,
                "ERROR",
            )
    guard = market_guard_status(_guard_load(), "stocks", int(time.time()))
    if bool(guard.get("active", False)):
        out["guard_active"] = True
        out["step_ok"] = True
        out["step_state"] = "GUARD_PAUSED"
        remaining = int(guard.get("remaining_s", 0) or 0)
        status_payload = {
            "state": "READY",
            "trader_state": "Execution paused",
            "msg": f"Execution temporarily disabled ({remaining}s remaining) due to broker instability",
            "ts": int(time.time()),
            "guard_active": True,
            "guard_remaining_s": remaining,
            "guard_reason": str(guard.get("last_reason", "") or ""),
        }
        _atomic_write_json(os.path.join(stocks_dir, "stock_trader_status.json"), status_payload)
        _update_sla_metrics("stocks_trader_step", ok=True, elapsed_ms=0.0, extra={"state": "GUARD_PAUSED"})
        return out
    try:
        t0 = time.perf_counter()
        trader = run_stock_trader_step(settings, HUB_DATA_DIR)
        trader["ts"] = int(time.time())
        _atomic_write_json(os.path.join(stocks_dir, "stock_trader_status.json"), trader)
        tr_state = str(trader.get("state", "") or "").upper()
        out["step_ok"] = tr_state != "ERROR"
        out["step_state"] = tr_state
        out["step_ms"] = round((time.perf_counter() - t0) * 1000.0, 3)
        if tr_state == "ERROR":
            _incident("error", "stocks_trader_error", str(trader.get("msg", "") or "stocks trader error"), {"market": "stocks"}, cooldown_key="stocks_trader_error")
            _update_sla_metrics("stocks_trader_step", ok=False, elapsed_ms=(time.perf_counter() - t0) * 1000.0, extra={"state": tr_state, "msg": str(trader.get("msg", ""))[:180]})
        else:
            _incident("info", "stocks_trader_ok", "stocks trader updated", {"state": tr_state}, cooldown_key="stocks_trader_ok")
            _update_sla_metrics("stocks_trader_step", ok=True, elapsed_ms=(time.perf_counter() - t0) * 1000.0, extra={"state": tr_state})
    except Exception as exc:
        print(f"[MARKETS] stocks trader failed: {type(exc).__name__}: {exc}")
        out["step_ok"] = False
        out["step_state"] = "ERROR"
        _incident("error", "stocks_trader_failed", f"{type(exc).__name__}: {exc}", {"market": "stocks"}, cooldown_key="stocks_trader_failed")
        _update_sla_metrics("stocks_trader_step", ok=False, elapsed_ms=0.0, extra={"error": f"{type(exc).__name__}: {exc}"})
    return out


def _run_forex(settings: Dict[str, Any]) -> Dict[str, Any]:
    forex_dir = os.path.join(HUB_DATA_DIR, "forex")
    os.makedirs(forex_dir, exist_ok=True)
    thinker_status_path = os.path.join(forex_dir, "forex_thinker_status.json")
    try:
        scan_fallback_age_s = max(60.0, float(settings.get("market_fallback_scan_max_age_s", 7200.0) or 7200.0))
    except Exception:
        scan_fallback_age_s = 7200.0
    out: Dict[str, Any] = {
        "scan_ok": False,
        "scan_state": "",
        "scan_ms": 0.0,
        "step_ok": False,
        "step_state": "",
        "step_ms": 0.0,
        "guard_active": False,
        "cadence": {},
    }
    try:
        t0 = time.perf_counter()
        thinker = run_forex_scan(settings, HUB_DATA_DIR)
        thinker["ts"] = int(time.time())
        _atomic_write_json(thinker_status_path, thinker)
        t_state = str(thinker.get("state", "") or "").upper()
        out["scan_ok"] = t_state != "ERROR"
        out["scan_state"] = t_state
        out["scan_ms"] = round((time.perf_counter() - t0) * 1000.0, 3)
        if t_state == "ERROR":
            _incident("error", "forex_thinker_error", str(thinker.get("msg", "") or "forex thinker error"), {"market": "forex"}, cooldown_key="forex_thinker_error")
            _update_sla_metrics("forex_scan", ok=False, elapsed_ms=(time.perf_counter() - t0) * 1000.0, extra={"state": t_state, "msg": str(thinker.get("msg", ""))[:180]})
        else:
            _incident("info", "forex_thinker_ok", "forex thinker updated", {"state": t_state}, cooldown_key="forex_thinker_ok")
            _update_sla_metrics("forex_scan", ok=True, elapsed_ms=(time.perf_counter() - t0) * 1000.0, extra={"state": t_state})
        rs = thinker.get("reject_summary", {}) if isinstance(thinker.get("reject_summary", {}), dict) else {}
        _update_scan_reject_drift("forex", float(rs.get("reject_rate_pct", 0.0) or 0.0), settings, t_state)
        out["cadence"] = _update_scan_cadence_drift(
            "forex",
            int(time.time()),
            float(settings.get("market_bg_forex_interval_s", 10.0) or 10.0),
            settings,
            t_state,
        )
    except Exception as exc:
        print(f"[MARKETS] forex thinker failed: {type(exc).__name__}: {exc}")
        cached = _cached_status_fallback(thinker_status_path, scan_fallback_age_s)
        if cached:
            cached["state"] = str(cached.get("state", "READY") or "READY")
            cached["msg"] = f"{type(exc).__name__}: {exc} | using cached thinker ({int(cached.get('fallback_age_s', 0))}s old)"
            _atomic_write_json(thinker_status_path, cached)
            t_state = str(cached.get("state", "READY") or "READY").upper()
            out["scan_ok"] = t_state != "ERROR"
            out["scan_state"] = t_state
            out["scan_ms"] = 0.0
            out["scan_fallback_cached"] = True
            rs = cached.get("reject_summary", {}) if isinstance(cached.get("reject_summary", {}), dict) else {}
            _update_scan_reject_drift("forex", float(rs.get("reject_rate_pct", 0.0) or 0.0), settings, t_state)
            out["cadence"] = _update_scan_cadence_drift(
                "forex",
                int(time.time()),
                float(settings.get("market_bg_forex_interval_s", 10.0) or 10.0),
                settings,
                t_state,
            )
            _incident(
                "warning",
                "forex_thinker_fallback_cached",
                str(cached.get("msg", "") or "forex thinker fallback cached"),
                {"market": "forex", "fallback_age_s": int(cached.get("fallback_age_s", 0) or 0)},
                cooldown_key="forex_thinker_fallback_cached",
            )
            _update_sla_metrics(
                "forex_scan",
                ok=True,
                elapsed_ms=0.0,
                extra={"state": "CACHED_FALLBACK", "fallback_age_s": int(cached.get("fallback_age_s", 0) or 0)},
            )
        else:
            out["scan_ok"] = False
            out["scan_state"] = "ERROR"
            _incident("error", "forex_thinker_failed", f"{type(exc).__name__}: {exc}", {"market": "forex"}, cooldown_key="forex_thinker_failed")
            _update_sla_metrics("forex_scan", ok=False, elapsed_ms=0.0, extra={"error": f"{type(exc).__name__}: {exc}"})
            out["cadence"] = _update_scan_cadence_drift(
                "forex",
                int(time.time()),
                float(settings.get("market_bg_forex_interval_s", 10.0) or 10.0),
                settings,
                "ERROR",
            )
    guard = market_guard_status(_guard_load(), "forex", int(time.time()))
    if bool(guard.get("active", False)):
        out["guard_active"] = True
        out["step_ok"] = True
        out["step_state"] = "GUARD_PAUSED"
        remaining = int(guard.get("remaining_s", 0) or 0)
        status_payload = {
            "state": "READY",
            "trader_state": "Execution paused",
            "msg": f"Execution temporarily disabled ({remaining}s remaining) due to broker instability",
            "ts": int(time.time()),
            "guard_active": True,
            "guard_remaining_s": remaining,
            "guard_reason": str(guard.get("last_reason", "") or ""),
        }
        _atomic_write_json(os.path.join(forex_dir, "forex_trader_status.json"), status_payload)
        _update_sla_metrics("forex_trader_step", ok=True, elapsed_ms=0.0, extra={"state": "GUARD_PAUSED"})
        return out
    try:
        t0 = time.perf_counter()
        trader = run_forex_trader_step(settings, HUB_DATA_DIR)
        trader["ts"] = int(time.time())
        _atomic_write_json(os.path.join(forex_dir, "forex_trader_status.json"), trader)
        tr_state = str(trader.get("state", "") or "").upper()
        out["step_ok"] = tr_state != "ERROR"
        out["step_state"] = tr_state
        out["step_ms"] = round((time.perf_counter() - t0) * 1000.0, 3)
        if tr_state == "ERROR":
            _incident("error", "forex_trader_error", str(trader.get("msg", "") or "forex trader error"), {"market": "forex"}, cooldown_key="forex_trader_error")
            _update_sla_metrics("forex_trader_step", ok=False, elapsed_ms=(time.perf_counter() - t0) * 1000.0, extra={"state": tr_state, "msg": str(trader.get("msg", ""))[:180]})
        else:
            _incident("info", "forex_trader_ok", "forex trader updated", {"state": tr_state}, cooldown_key="forex_trader_ok")
            _update_sla_metrics("forex_trader_step", ok=True, elapsed_ms=(time.perf_counter() - t0) * 1000.0, extra={"state": tr_state})
    except Exception as exc:
        print(f"[MARKETS] forex trader failed: {type(exc).__name__}: {exc}")
        out["step_ok"] = False
        out["step_state"] = "ERROR"
        _incident("error", "forex_trader_failed", f"{type(exc).__name__}: {exc}", {"market": "forex"}, cooldown_key="forex_trader_failed")
        _update_sla_metrics("forex_trader_step", ok=False, elapsed_ms=0.0, extra={"error": f"{type(exc).__name__}: {exc}"})
    return out


def _write_market_trends() -> None:
    try:
        payload = build_trends_payload(HUB_DATA_DIR)
        _atomic_write_json(MARKET_TRENDS_PATH, payload)
    except Exception as exc:
        _incident("warning", "market_trends_update_failed", f"{type(exc).__name__}: {exc}", {"component": "trends"}, cooldown_key="market_trends_update_failed")
    try:
        regimes = build_all_market_regimes(HUB_DATA_DIR)
        _atomic_write_json(MARKET_REGIMES_PATH, regimes)
    except Exception as exc:
        _incident(
            "warning",
            "market_regimes_update_failed",
            f"{type(exc).__name__}: {exc}",
            {"component": "regimes"},
            cooldown_key="market_regimes_update_failed",
        )


def _write_market_intelligence(settings: Dict[str, Any]) -> None:
    try:
        walk = build_walkforward_report(HUB_DATA_DIR)
        _atomic_write_json(WALKFORWARD_PATH, walk)
    except Exception as exc:
        _incident(
            "warning",
            "walkforward_report_update_failed",
            f"{type(exc).__name__}: {exc}",
            {"component": "walkforward"},
            cooldown_key="walkforward_report_update_failed",
        )
    try:
        calibration = build_confidence_calibration_payload(HUB_DATA_DIR, settings)
        _atomic_write_json(CONFIDENCE_CALIBRATION_PATH, calibration)
    except Exception as exc:
        _incident(
            "warning",
            "confidence_calibration_update_failed",
            f"{type(exc).__name__}: {exc}",
            {"component": "confidence_calibration"},
            cooldown_key="confidence_calibration_update_failed",
        )
    try:
        scorecards = build_shadow_scorecards(HUB_DATA_DIR)
        _atomic_write_json(SHADOW_SCORECARDS_PATH, scorecards)
    except Exception as exc:
        _incident(
            "warning",
            "shadow_scorecard_update_failed",
            f"{type(exc).__name__}: {exc}",
            {"component": "shadow_scorecards"},
            cooldown_key="shadow_scorecard_update_failed",
        )


def main() -> int:
    running = {"ok": True}

    def _stop(_signum: int, _frame: Any) -> None:
        running["ok"] = False

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    settings = _load_settings()
    now = time.time()
    next_snap = now
    next_stock = now
    next_fx = now
    next_intel = now
    last_settings_load = now
    loop_status: Dict[str, Any] = {
        "ts": int(now),
        "started_ts": int(now),
        "next_snapshot_ts": int(next_snap),
        "next_stocks_scan_ts": int(next_stock),
        "next_forex_scan_ts": int(next_fx),
        "stocks_last_scan_ts": 0,
        "stocks_last_step_ts": 0,
        "forex_last_scan_ts": 0,
        "forex_last_step_ts": 0,
        "snapshots": {},
        "stocks_cycle": {},
        "forex_cycle": {},
    }
    _write_loop_status(loop_status)

    while running["ok"]:
        if os.path.exists(STOP_FLAG_PATH):
            break
        now = time.time()
        try:
            reload_every = max(1.0, float(settings.get("market_settings_reload_interval_s", 8.0) or 8.0))
        except Exception:
            reload_every = 8.0
        if (now - last_settings_load) >= reload_every:
            settings = _load_settings()
            last_settings_load = now
        try:
            snap_every = max(5.0, float(settings.get("market_bg_snapshot_interval_s", 15.0) or 15.0))
            stock_every = max(8.0, float(settings.get("market_bg_stocks_interval_s", 18.0) or 18.0))
            fx_every = max(6.0, float(settings.get("market_bg_forex_interval_s", 12.0) or 12.0))
            jitter_pct = max(0.0, min(0.5, float(settings.get("market_loop_jitter_pct", 0.10) or 0.10)))
            intelligence_every = max(30.0, float(settings.get("market_intelligence_interval_s", 180.0) or 180.0))
        except Exception:
            snap_every, stock_every, fx_every = 15.0, 18.0, 12.0
            jitter_pct = 0.10
            intelligence_every = 180.0
        loop_status["settings_reload_s"] = float(reload_every)
        loop_status["jitter_pct"] = float(jitter_pct)
        loop_status["intervals"] = {
            "snapshot_s": float(snap_every),
            "stocks_s": float(stock_every),
            "forex_s": float(fx_every),
        }

        if now >= next_snap:
            snap_meta = _write_snapshots(settings)
            loop_status["snapshots"] = dict(snap_meta or {})
            loop_status["last_snapshot_ts"] = int(now)
            next_snap = now + _jittered_interval(snap_every, jitter_pct)
        if now >= next_stock:
            stock_meta = _run_stocks(settings)
            loop_status["stocks_cycle"] = dict(stock_meta or {})
            if str((stock_meta or {}).get("scan_state", "") or "").strip():
                loop_status["stocks_last_scan_ts"] = int(now)
            if str((stock_meta or {}).get("step_state", "") or "").strip():
                loop_status["stocks_last_step_ts"] = int(now)
            next_stock = now + _jittered_interval(stock_every, jitter_pct)
        if now >= next_fx:
            fx_meta = _run_forex(settings)
            loop_status["forex_cycle"] = dict(fx_meta or {})
            if str((fx_meta or {}).get("scan_state", "") or "").strip():
                loop_status["forex_last_scan_ts"] = int(now)
            if str((fx_meta or {}).get("step_state", "") or "").strip():
                loop_status["forex_last_step_ts"] = int(now)
            next_fx = now + _jittered_interval(fx_every, jitter_pct)
            _write_market_trends()
        if now >= next_intel:
            _write_market_intelligence(settings)
            next_intel = now + _jittered_interval(intelligence_every, jitter_pct)

        loop_status["ts"] = int(now)
        loop_status["next_snapshot_ts"] = int(next_snap)
        loop_status["next_stocks_scan_ts"] = int(next_stock)
        loop_status["next_forex_scan_ts"] = int(next_fx)
        loop_status["next_intelligence_ts"] = int(next_intel)
        _write_loop_status(loop_status)
        time.sleep(1.0)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
