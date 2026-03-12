from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, Iterable, List

from app.scanner_quality import effective_reject_pressure

_TRANSIENT_INCIDENT_TTL_S: Dict[str, int] = {
    "ui_market_panel_desync": 300,
    "market_panel_refresh_failed": 300,
    "runner_watchdog_restart": 600,
    "runner_child_exit": 600,
    "runner_script_path_changed": 600,
    "runner_script_hot_reload": 600,
    "runner_forced_shutdown": 600,
    "runner_child_start_failed": 900,
    "runner_child_crash_loop": 900,
    "runner_missing_script": 900,
    "stocks_snapshot_failed": 900,
    "forex_snapshot_failed": 900,
    "stocks_thinker_error": 900,
    "stocks_thinker_failed": 900,
    "stocks_trader_error": 900,
    "stocks_trader_failed": 900,
    "forex_thinker_error": 900,
    "forex_thinker_failed": 900,
    "forex_trader_error": 900,
    "forex_trader_failed": 900,
    "market_trends_update_failed": 900,
}

_STARTUP_CHECK_INFO_WARNINGS = {
    "stale_pid_file_removed",
}


def _safe_read_json(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _safe_read_jsonl(path: str, max_lines: int = 400) -> List[Dict[str, Any]]:
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


def _sev(v: str) -> str:
    txt = str(v or "").strip().lower()
    if txt in {"critical", "error", "high"}:
        return "critical"
    if txt in {"warn", "warning", "medium"}:
        return "warning"
    return "info"


def _severity_rank(v: str) -> int:
    sev = _sev(v)
    if sev == "critical":
        return 3
    if sev == "warning":
        return 2
    if sev == "ok":
        return 0
    return 1


def _market_from_incident(row: Dict[str, Any]) -> str:
    details = row.get("details", {}) if isinstance(row.get("details", {}), dict) else {}
    market = str(details.get("market", "") or "").strip().lower()
    if market in {"stocks", "forex", "crypto"}:
        return market
    evt = str(row.get("event", "") or "").strip().lower()
    if "stock" in evt:
        return "stocks"
    if "forex" in evt:
        return "forex"
    if "kucoin" in evt or "crypto" in evt:
        return "crypto"
    return "global"


def _runtime_now_ts(runtime_state: Dict[str, Any]) -> int:
    rs = runtime_state if isinstance(runtime_state, dict) else {}
    try:
        ts = int(rs.get("ts", 0) or 0)
    except Exception:
        ts = 0
    return ts if ts > 0 else int(time.time())


def _incident_is_recent(row: Dict[str, Any], runtime_state: Dict[str, Any], ttl_s: int) -> bool:
    try:
        ts = int(float(row.get("ts", 0) or 0))
    except Exception:
        ts = 0
    if ts <= 0:
        return False
    return (_runtime_now_ts(runtime_state) - ts) <= max(30, int(ttl_s or 0))


def _runtime_alert_reasons(runtime_state: Dict[str, Any]) -> set[str]:
    rs = runtime_state if isinstance(runtime_state, dict) else {}
    alerts = rs.get("alerts", {}) if isinstance(rs.get("alerts", {}), dict) else {}
    out: set[str] = set()
    for row in list(alerts.get("reasons", []) or []):
        key = str(row or "").strip().lower()
        if key:
            out.add(key)
    return out


def _active_cadence_markets(runtime_state: Dict[str, Any]) -> set[str]:
    rs = runtime_state if isinstance(runtime_state, dict) else {}
    out: set[str] = set()
    scan_cadence = rs.get("scan_cadence", {}) if isinstance(rs.get("scan_cadence", {}), dict) else {}
    for row in list(scan_cadence.get("active", []) or []):
        if not isinstance(row, dict):
            continue
        market = str(row.get("market", "") or "").strip().lower()
        if market in {"stocks", "forex", "crypto"}:
            out.add(market)
    return out


def _active_drift_markets(runtime_state: Dict[str, Any]) -> set[str]:
    rs = runtime_state if isinstance(runtime_state, dict) else {}
    out: set[str] = set()
    scan_drift = rs.get("scan_drift", {}) if isinstance(rs.get("scan_drift", {}), dict) else {}
    for row in list(scan_drift.get("active", []) or []):
        if not isinstance(row, dict):
            continue
        market = str(row.get("market", "") or "").strip().lower()
        if market in {"stocks", "forex", "crypto"}:
            out.add(market)
    return out


def _startup_checks_active(runtime_state: Dict[str, Any]) -> bool:
    rs = runtime_state if isinstance(runtime_state, dict) else {}
    checks = rs.get("checks", {}) if isinstance(rs.get("checks", {}), dict) else {}
    if not bool(checks.get("ok", False)):
        return True
    warnings = list(checks.get("warnings", []) or []) if isinstance(checks.get("warnings", []), list) else []
    errors = list(checks.get("errors", []) or []) if isinstance(checks.get("errors", []), list) else []
    warnings = [str(row or "").strip().lower() for row in warnings if str(row or "").strip()]
    warnings = [row for row in warnings if row not in _STARTUP_CHECK_INFO_WARNINGS]
    return bool(warnings or errors)


def _market_loop_issue_active(runtime_state: Dict[str, Any]) -> bool:
    rs = runtime_state if isinstance(runtime_state, dict) else {}
    if "market_loop_stale" in _runtime_alert_reasons(rs):
        return True
    alerts = rs.get("alerts", {}) if isinstance(rs.get("alerts", {}), dict) else {}
    metrics = alerts.get("metrics", {}) if isinstance(alerts.get("metrics", {}), dict) else {}
    return bool(metrics.get("market_loop_stale", False))


def _runner_child_pid(runtime_state: Dict[str, Any], child: str) -> int:
    rs = runtime_state if isinstance(runtime_state, dict) else {}
    runner = rs.get("runner", {}) if isinstance(rs.get("runner", {}), dict) else {}
    children = runner.get("children", {}) if isinstance(runner.get("children", {}), dict) else {}
    try:
        pid = int(children.get(child, 0) or 0)
    except Exception:
        pid = 0
    return pid if pid > 0 else 0


def _autopilot_issue_active(runtime_state: Dict[str, Any]) -> bool:
    rs = runtime_state if isinstance(runtime_state, dict) else {}
    autopilot = rs.get("autopilot", {}) if isinstance(rs.get("autopilot", {}), dict) else {}
    if not autopilot:
        return False
    if bool(autopilot.get("issue_open", False)):
        return True
    if bool(autopilot.get("api_unstable", False)):
        return True
    if not bool(autopilot.get("markets_healthy", True)):
        return True
    try:
        status_ts = int(autopilot.get("ts", 0) or 0)
    except Exception:
        status_ts = 0
    if status_ts > 0 and (_runtime_now_ts(rs) - status_ts) > 240:
        return True
    return False


def _runner_restart_incident_active(row: Dict[str, Any], runtime_state: Dict[str, Any]) -> bool:
    details = row.get("details", {}) if isinstance(row.get("details", {}), dict) else {}
    child = str(details.get("child", "") or "").strip().lower()
    ttl_s = int(_TRANSIENT_INCIDENT_TTL_S.get(str(row.get("event", "") or "").strip().lower(), 0) or 0)
    if not child:
        return _incident_is_recent(row, runtime_state, ttl_s)
    child_pid = _runner_child_pid(runtime_state, child)
    if child == "autopilot":
        return child_pid <= 0 or _autopilot_issue_active(runtime_state)
    if child == "markets":
        return child_pid <= 0 or _market_loop_issue_active(runtime_state)
    if child in {"thinker", "trader"}:
        return child_pid <= 0
    return _incident_is_recent(row, runtime_state, ttl_s)


def _incident_is_active(row: Dict[str, Any], runtime_state: Dict[str, Any]) -> bool:
    evt = str(row.get("event", "") or "").strip().lower()
    market = _market_from_incident(row)
    if evt == "scanner_cadence_drift":
        return market in _active_cadence_markets(runtime_state)
    if evt == "scanner_reject_spike":
        return market in _active_drift_markets(runtime_state)
    if evt == "runner_startup_check":
        return _startup_checks_active(runtime_state)
    if evt in {"runner_watchdog_restart", "runner_child_exit"}:
        return _runner_restart_incident_active(row, runtime_state)
    if evt in {"runner_market_loop_status_stale", "runner_market_loop_restart"}:
        return _market_loop_issue_active(runtime_state)
    ttl_s = int(_TRANSIENT_INCIDENT_TTL_S.get(evt, 0) or 0)
    if ttl_s > 0:
        return _incident_is_recent(row, runtime_state, ttl_s)
    return True


def _dedupe_notification_rows(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    keep: Dict[tuple[str, str, str, str, str], Dict[str, Any]] = {}
    for row in list(rows or []):
        if not isinstance(row, dict):
            continue
        key = (
            str(row.get("market", "global") or "global").strip().lower(),
            str(row.get("source", "") or "").strip().lower(),
            _sev(str(row.get("severity", "info") or "info")),
            str(row.get("title", "") or "").strip(),
            str(row.get("message", "") or "").strip(),
        )
        prev = keep.get(key)
        if prev is None:
            keep[key] = row
            continue
        prev_key = (int(prev.get("ts", 0) or 0), _severity_rank(str(prev.get("severity", "info") or "info")))
        next_key = (int(row.get("ts", 0) or 0), _severity_rank(str(row.get("severity", "info") or "info")))
        if next_key >= prev_key:
            keep[key] = row
    return list(keep.values())


def build_notification_center_payload(
    runtime_state: Dict[str, Any],
    incidents_rows: Iterable[Dict[str, Any]] | None = None,
    max_items: int = 220,
) -> Dict[str, Any]:
    rs = runtime_state if isinstance(runtime_state, dict) else {}
    out_rows: List[Dict[str, Any]] = []

    ts_now = int(rs.get("ts", 0) or 0) or int(time.time())
    alerts = rs.get("alerts", {}) if isinstance(rs.get("alerts", {}), dict) else {}
    reasons = [str(x or "").strip() for x in list(alerts.get("reasons", []) or []) if str(x or "").strip()]
    hints = [str(x or "").strip() for x in list(alerts.get("hints", []) or []) if str(x or "").strip()]
    sev = _sev(str(alerts.get("severity", "info") or "info"))
    for i, reason in enumerate(reasons[:8]):
        hint = hints[i] if i < len(hints) else ""
        out_rows.append(
            {
                "id": f"alert_{i}_{ts_now}",
                "ts": int(ts_now),
                "severity": sev,
                "market": "global",
                "source": "runtime_alerts",
                "title": reason,
                "message": hint or reason,
            }
        )

    trends = rs.get("market_trends", {}) if isinstance(rs.get("market_trends", {}), dict) else {}
    for market in ("stocks", "forex"):
        row = trends.get(market, {}) if isinstance(trends.get(market, {}), dict) else {}
        quality = row.get("quality_aggregates", {}) if isinstance(row.get("quality_aggregates", {}), dict) else {}
        rel = row.get("data_source_reliability", {}) if isinstance(row.get("data_source_reliability", {}), dict) else {}
        why = row.get("why_not_traded", {}) if isinstance(row.get("why_not_traded", {}), dict) else {}
        reject_raw = float(quality.get("reject_rate_raw_pct", quality.get("reject_rate_pct", 0.0)) or 0.0)
        reject = effective_reject_pressure(
            reject_raw,
            dominant_reason=quality.get("dominant_reason", ""),
            dominant_ratio_pct=quality.get("reject_dominant_ratio_pct", 0.0),
            leaders_total=quality.get("leaders_total", 0),
            scores_total=quality.get("scores_total", 0),
        )
        rel_score = float(rel.get("score", 0.0) or 0.0)
        why_reason = str(why.get("reason", "") or "").strip()
        if reject >= 90.0:
            out_rows.append(
                {
                    "id": f"{market}_reject_{ts_now}",
                    "ts": int(ts_now),
                    "severity": "warning",
                    "market": market,
                    "source": "market_trends",
                    "title": "High scanner rejection pressure",
                    "message": f"Reject rate {reject:.1f}% is suppressing candidate flow.",
                }
            )
        if rel_score < 70.0:
            out_rows.append(
                {
                    "id": f"{market}_reliability_{ts_now}",
                    "ts": int(ts_now),
                    "severity": ("critical" if rel_score < 55.0 else "warning"),
                    "market": market,
                    "source": "market_trends",
                    "title": "Data reliability degraded",
                    "message": f"Reliability score {rel_score:.1f}/100.",
                }
            )
        if why_reason:
            out_rows.append(
                {
                    "id": f"{market}_why_not_{ts_now}",
                    "ts": int(ts_now),
                    "severity": "info",
                    "market": market,
                    "source": "execution_gate",
                    "title": "Why top candidate was not traded",
                    "message": why_reason,
                }
            )

    for row in list(incidents_rows or []):
        if not isinstance(row, dict):
            continue
        if not _incident_is_active(row, rs):
            continue
        ts = int(float(row.get("ts", 0) or 0))
        if ts <= 0:
            continue
        severity = _sev(str(row.get("severity", "info") or "info"))
        if severity == "info":
            continue
        msg = str(row.get("msg", "") or "").strip()
        evt = str(row.get("event", "") or "").strip()
        out_rows.append(
            {
                "id": f"inc_{ts}_{evt[:24]}",
                "ts": int(ts),
                "severity": severity,
                "market": _market_from_incident(row),
                "source": "incidents",
                "title": evt or "runtime_incident",
                "message": msg[:220],
            }
        )

    out_rows = _dedupe_notification_rows(out_rows)
    out_rows = sorted(
        out_rows,
        key=lambda r: (
            int(r.get("ts", 0) or 0),
            _severity_rank(str(r.get("severity", "info") or "info")),
        ),
        reverse=True,
    )
    out_rows = out_rows[: max(10, int(max_items))]

    by_market: Dict[str, Dict[str, int]] = {}
    by_sev: Dict[str, int] = {"critical": 0, "warning": 0, "info": 0}
    for row in out_rows:
        market = str(row.get("market", "global") or "global").strip().lower()
        severity = _sev(str(row.get("severity", "info") or "info"))
        by_market.setdefault(market, {"critical": 0, "warning": 0, "info": 0, "total": 0})
        by_market[market][severity] = int(by_market[market].get(severity, 0) or 0) + 1
        by_market[market]["total"] = int(by_market[market].get("total", 0) or 0) + 1
        by_sev[severity] = int(by_sev.get(severity, 0) or 0) + 1

    return {
        "ts": int(ts_now),
        "total": int(len(out_rows)),
        "by_market": by_market,
        "by_severity": by_sev,
        "items": out_rows,
    }


def build_notification_center_from_hub(hub_dir: str, runtime_state: Dict[str, Any] | None = None) -> Dict[str, Any]:
    rs = runtime_state if isinstance(runtime_state, dict) else _safe_read_json(os.path.join(hub_dir, "runtime_state.json"))
    if runtime_state is None:
        try:
            from app.health_rules import evaluate_runtime_alerts

            settings = _safe_read_json(os.path.join(os.path.dirname(str(hub_dir or "")), "gui_settings.json"))
            if isinstance(rs, dict):
                rs = dict(rs)
                rs["alerts"] = evaluate_runtime_alerts(rs, settings if isinstance(settings, dict) else {})
        except Exception:
            pass
    incidents = _safe_read_jsonl(os.path.join(hub_dir, "incidents.jsonl"), max_lines=500)
    return build_notification_center_payload(rs, incidents_rows=incidents, max_items=220)
