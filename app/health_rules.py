from __future__ import annotations

from typing import Any, Dict, List

_QUICKFIX_MAP: Dict[str, str] = {
    "startup_checks_failed": "Open Runtime Checks and resolve missing scripts/permissions first.",
    "startup_warnings": "Review startup warnings; fix credential or path hygiene before live execution.",
    "scan_reject_pressure": "Lower score/quality gates slightly and verify data source freshness.",
    "error_incidents": "Open incidents and logs; address top recurring runtime error first.",
    "api_unstable": "Reduce scan/step frequency and keep paper/practice mode until stable.",
    "scanner_reject_spike": "Check scanner diagnostics dominant reject reason and tune that gate.",
    "cadence_drift_pressure": "Reduce scanner cadence pressure or network latency; align loop intervals with broker/data capacity.",
    "market_loop_stale": "Check runtime/markets process health and loop heartbeat freshness before trusting scanner status.",
    "exposure_concentration": "Lower per-asset exposure caps or rotate into broader symbol set.",
    "execution_temporarily_disabled": "Wait for cooldown, then verify broker connectivity and quotas before resuming.",
    "key_rotation_due": "Rotate API keys/secrets and update credentials before switching to live mode.",
    "drawdown_guard_triggered": "Review drawdown guard payload and account-value history before restarting trading.",
    "stop_flag_active": "Clear stop flag only after root cause is resolved and checks are green.",
}


def evaluate_runtime_alerts(runtime_state: Dict[str, Any], settings: Dict[str, Any]) -> Dict[str, Any]:
    scan_health = runtime_state.get("scan_health", {}) if isinstance(runtime_state.get("scan_health", {}), dict) else {}
    stocks = scan_health.get("stocks", {}) if isinstance(scan_health.get("stocks", {}), dict) else {}
    forex = scan_health.get("forex", {}) if isinstance(scan_health.get("forex", {}), dict) else {}
    checks = runtime_state.get("checks", {}) if isinstance(runtime_state.get("checks", {}), dict) else {}
    incidents = runtime_state.get("incidents_last_200", {}) if isinstance(runtime_state.get("incidents_last_200", {}), dict) else {}
    sev = incidents.get("by_severity", {}) if isinstance(incidents.get("by_severity", {}), dict) else {}
    autopilot = runtime_state.get("autopilot", {}) if isinstance(runtime_state.get("autopilot", {}), dict) else {}
    scan_drift = runtime_state.get("scan_drift", {}) if isinstance(runtime_state.get("scan_drift", {}), dict) else {}
    active_drift = scan_drift.get("active", []) if isinstance(scan_drift.get("active", []), list) else []
    scan_cadence = runtime_state.get("scan_cadence", {}) if isinstance(runtime_state.get("scan_cadence", {}), dict) else {}
    active_cadence = scan_cadence.get("active", []) if isinstance(scan_cadence.get("active", []), list) else []
    execution_guard = runtime_state.get("execution_guard", {}) if isinstance(runtime_state.get("execution_guard", {}), dict) else {}
    guard_markets = execution_guard.get("markets", {}) if isinstance(execution_guard.get("markets", {}), dict) else {}
    market_loop = runtime_state.get("market_loop", {}) if isinstance(runtime_state.get("market_loop", {}), dict) else {}
    exposure_map = runtime_state.get("exposure_map", {}) if isinstance(runtime_state.get("exposure_map", {}), dict) else {}
    top_positions = exposure_map.get("top_positions", []) if isinstance(exposure_map.get("top_positions", []), list) else []
    drawdown_guard = runtime_state.get("drawdown_guard", {}) if isinstance(runtime_state.get("drawdown_guard", {}), dict) else {}
    stop_flag = runtime_state.get("stop_flag", {}) if isinstance(runtime_state.get("stop_flag", {}), dict) else {}

    reject_warn = float(settings.get("runtime_alert_scan_reject_warn_pct", 65.0) or 65.0)
    reject_crit = float(settings.get("runtime_alert_scan_reject_crit_pct", 85.0) or 85.0)
    incident_warn = int(float(settings.get("runtime_alert_incident_warn_count", 8) or 8))
    incident_crit = int(float(settings.get("runtime_alert_incident_crit_count", 20) or 20))
    error_warn = int(float(settings.get("runtime_alert_error_incident_warn_count", 2) or 2))
    error_crit = int(float(settings.get("runtime_alert_error_incident_crit_count", 6) or 6))
    startup_warn = int(float(settings.get("runtime_alert_startup_warning_warn_count", 2) or 2))
    drift_warn = int(float(settings.get("runtime_alert_drift_spike_warn_count", 1) or 1))
    drift_crit = int(float(settings.get("runtime_alert_drift_spike_crit_count", 3) or 3))
    cadence_warn = int(float(settings.get("runtime_alert_cadence_warn_count", 1) or 1))
    cadence_crit = int(float(settings.get("runtime_alert_cadence_crit_count", 2) or 2))
    market_loop_stale_s = float(settings.get("runtime_alert_market_loop_stale_s", 90.0) or 90.0)
    exposure_warn_pct = float(settings.get("runtime_alert_exposure_concentration_warn_pct", 55.0) or 55.0)
    exposure_crit_pct = float(settings.get("runtime_alert_exposure_concentration_crit_pct", 75.0) or 75.0)

    s_reject_raw = float(stocks.get("reject_rate_pct", 0.0) or 0.0)
    f_reject_raw = float(forex.get("reject_rate_pct", 0.0) or 0.0)
    s_dom = str(stocks.get("reject_dominant_reason", "") or "").strip().lower()
    f_dom = str(forex.get("reject_dominant_reason", "") or "").strip().lower()
    s_dom_ratio = float(stocks.get("reject_dominant_ratio_pct", 0.0) or 0.0)
    f_dom_ratio = float(forex.get("reject_dominant_ratio_pct", 0.0) or 0.0)
    s_leaders = int(stocks.get("leaders_total", 0) or 0)
    f_leaders = int(forex.get("leaders_total", 0) or 0)
    s_scores = int(stocks.get("scores_total", 0) or 0)
    f_scores = int(forex.get("scores_total", 0) or 0)
    unknown_dom_cap = max(0.0, float(settings.get("runtime_alert_reject_unknown_dom_cap_pct", 64.0) or 64.0))

    def _effective_reject_pressure(
        rate: float,
        dominant: str,
        dominant_ratio: float,
        leaders_total: int,
        scores_total: int,
    ) -> float:
        dom = str(dominant or "").strip().lower()
        if leaders_total > 0 and dom in {"cooldown", "warmup_pending"} and dominant_ratio >= 60.0:
            # Cooldown/warmup-dominated rejects with healthy leaders are normal pressure, not scanner failure.
            return 0.0
        if leaders_total > 0 and scores_total > 0 and (not dom):
            # Some producers omit dominant reject reason; if leaders and scores exist,
            # do not treat 100% reject as hard-fail without context.
            return min(max(0.0, float(rate or 0.0)), unknown_dom_cap)
        return max(0.0, float(rate or 0.0))

    s_reject = _effective_reject_pressure(s_reject_raw, s_dom, s_dom_ratio, s_leaders, s_scores)
    f_reject = _effective_reject_pressure(f_reject_raw, f_dom, f_dom_ratio, f_leaders, f_scores)
    max_reject = max(s_reject, f_reject)
    incident_count_total = int(incidents.get("count", 0) or 0)
    warn_count = int((sev.get("warning", 0) or 0) + (sev.get("warn", 0) or 0))
    err_count = int((sev.get("error", 0) or 0) + (sev.get("critical", 0) or 0) + (sev.get("high", 0) or 0))
    incident_count = int(warn_count + err_count)
    warns = len(list(checks.get("warnings", []) or []))
    startup_warnings = [str(x or "") for x in list(checks.get("warnings", []) or [])]
    checks_ok = bool(checks.get("ok", False))
    api_unstable = bool(autopilot.get("api_unstable", False))
    drift_count = int(len(active_drift))
    cadence_count = int(len(active_cadence))
    cadence_critical = 0
    for row in active_cadence:
        if not isinstance(row, dict):
            continue
        if str(row.get("level", "") or "").strip().lower() == "critical":
            cadence_critical += 1
    loop_age_s = int(float(market_loop.get("age_s", -1) or -1))
    try:
        now_ts = int(runtime_state.get("ts", 0) or 0)
    except Exception:
        now_ts = 0
    check_ts = 0
    if isinstance(checks, dict):
        try:
            check_ts = int(checks.get("ts", 0) or 0)
        except Exception:
            check_ts = 0
    startup_age_s = max(0, (now_ts - check_ts)) if (now_ts > 0 and check_ts > 0 and now_ts >= check_ts) else 0
    startup_grace_s = max(0.0, float(settings.get("runtime_alert_startup_grace_s", 180.0) or 180.0))
    loop_stale = bool(loop_age_s >= int(max(10.0, market_loop_stale_s)))
    loop_crit = bool(loop_age_s >= int(max(20.0, market_loop_stale_s * 3.0)))
    if startup_age_s > 0 and startup_age_s < startup_grace_s:
        loop_stale = False
        loop_crit = False
    guard_active = 0
    for row in guard_markets.values():
        if not isinstance(row, dict):
            continue
        try:
            if int(row.get("disabled_until", 0) or 0) > int(now_ts or 0):
                guard_active += 1
        except Exception:
            continue
    top_exposure_pct = 0.0
    if top_positions and isinstance(top_positions[0], dict):
        top_exposure_pct = float(top_positions[0].get("pct_of_total_exposure", 0.0) or 0.0)

    reasons: List[str] = []
    hints: List[str] = []
    severity = "ok"

    def bump(to: str) -> None:
        nonlocal severity
        order = {"ok": 0, "warn": 1, "critical": 2}
        if order.get(to, 0) > order.get(severity, 0):
            severity = to

    if (not checks_ok) or err_count >= error_crit or max_reject >= reject_crit or drift_count >= drift_crit or cadence_critical >= cadence_crit or loop_crit or top_exposure_pct >= exposure_crit_pct:
        bump("critical")
    if bool(drawdown_guard.get("triggered_recent", False)) or bool(stop_flag.get("active", False)):
        bump("critical")
    if warns >= startup_warn or err_count >= error_warn or incident_count >= incident_warn or max_reject >= reject_warn or api_unstable or drift_count >= drift_warn or cadence_count >= cadence_warn or loop_stale or top_exposure_pct >= exposure_warn_pct or guard_active > 0:
        bump("warn")

    if not checks_ok:
        reasons.append("startup_checks_failed")
        hints.append("Fix runtime_startup_checks errors before enabling unattended trading.")
    if warns >= startup_warn:
        reasons.append("startup_warnings")
        hints.append("Review startup warnings in runtime_startup_checks.json.")
    if any(w.startswith("key_rotation_due:") for w in startup_warnings):
        reasons.append("key_rotation_due")
        hints.append("API key age exceeded rotation threshold; rotate credentials.")
    if max_reject >= reject_warn:
        reasons.append("scan_reject_pressure")
        hints.append("High scanner rejection rate: loosen gates or improve input data quality.")
    if err_count >= error_warn:
        reasons.append("error_incidents")
        hints.append("Recent runtime errors elevated; inspect incidents and broker health.")
    if api_unstable:
        reasons.append("api_unstable")
        hints.append("Autopilot detected API instability; keep request pace conservative.")
    if drift_count >= drift_warn:
        reasons.append("scanner_reject_spike")
        hints.append("Scanner reject spike detected; review data gates and universe quality filters.")
    if cadence_count >= cadence_warn:
        reasons.append("cadence_drift_pressure")
        hints.append("Scanner cadence drift active; scanner loops are running slower than configured cadence.")
    if loop_stale:
        reasons.append("market_loop_stale")
        hints.append("Market loop heartbeat is stale; verify markets runner process and loop heartbeat output.")
    if top_exposure_pct >= exposure_warn_pct:
        reasons.append("exposure_concentration")
        hints.append("Exposure concentration is high; diversify or tighten per-asset caps.")
    if guard_active > 0:
        reasons.append("execution_temporarily_disabled")
        hints.append("Execution is temporarily paused due to repeated broker failures; cooldown in progress.")
    if bool(drawdown_guard.get("triggered_recent", False)):
        reasons.append("drawdown_guard_triggered")
        hints.append("Global drawdown guard recently triggered; stop trading remains in effect.")
    if bool(stop_flag.get("active", False)):
        reasons.append("stop_flag_active")
        hints.append("Stop flag file is present; trading should remain paused until reviewed.")

    quickfix: List[str] = []
    for r in reasons:
        tip = str(_QUICKFIX_MAP.get(r, "") or "").strip()
        if tip and tip not in quickfix:
            quickfix.append(tip)

    return {
        "severity": severity,
        "reasons": reasons[:8],
        "hints": hints[:8],
        "quickfix_suggestions": quickfix[:5],
        "metrics": {
            "stocks_reject_rate_pct": round(s_reject, 3),
            "forex_reject_rate_pct": round(f_reject, 3),
            "stocks_reject_rate_raw_pct": round(s_reject_raw, 3),
            "forex_reject_rate_raw_pct": round(f_reject_raw, 3),
            "incident_count_last_200": int(incident_count),
            "incident_count_total_last_200": int(incident_count_total),
            "warning_incidents_last_200": int(warn_count),
            "error_incidents_last_200": int(err_count),
            "startup_warning_count": int(warns),
            "checks_ok": bool(checks_ok),
            "api_unstable": bool(api_unstable),
            "drift_spike_active_count": int(drift_count),
            "scan_cadence_active_count": int(cadence_count),
            "scan_cadence_critical_count": int(cadence_critical),
            "market_loop_age_s": int(loop_age_s),
            "market_loop_stale": bool(loop_stale),
            "execution_guard_active_markets": int(guard_active),
            "top_exposure_pct_of_total": round(top_exposure_pct, 4),
            "drawdown_guard_triggered_recent": bool(drawdown_guard.get("triggered_recent", False)),
            "stop_flag_active": bool(stop_flag.get("active", False)),
        },
        "thresholds": {
            "scan_reject_warn_pct": float(reject_warn),
            "scan_reject_crit_pct": float(reject_crit),
            "reject_unknown_dom_cap_pct": float(unknown_dom_cap),
            "incident_warn_count": int(incident_warn),
            "incident_crit_count": int(incident_crit),
            "error_incident_warn_count": int(error_warn),
            "error_incident_crit_count": int(error_crit),
            "startup_warning_warn_count": int(startup_warn),
            "drift_spike_warn_count": int(drift_warn),
            "drift_spike_crit_count": int(drift_crit),
            "cadence_warn_count": int(cadence_warn),
            "cadence_crit_count": int(cadence_crit),
            "market_loop_stale_s": float(market_loop_stale_s),
            "startup_grace_s": float(startup_grace_s),
            "exposure_concentration_warn_pct": float(exposure_warn_pct),
            "exposure_concentration_crit_pct": float(exposure_crit_pct),
        },
    }
