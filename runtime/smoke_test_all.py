from __future__ import annotations

import json
import os
import sys
import time
from typing import Any, Dict

if __package__ in (None, ""):
    _ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if _ROOT not in sys.path:
        sys.path.insert(0, _ROOT)

from app.path_utils import read_settings_file, resolve_runtime_paths, resolve_settings_path
from app.scan_diagnostics_schema import normalize_scan_diagnostics
from app.settings_utils import sanitize_settings
from engines.forex_thinker import run_scan as run_forex_scan
from engines.forex_trader import run_step as run_forex_step
from engines.stock_thinker import run_scan as run_stock_scan
from engines.stock_trader import run_step as run_stock_step
from runtime.pt_autopilot import run_once as run_autopilot_once
from runtime.pt_autofix import run_once as run_autofix_once


def _safe_json(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def main() -> int:
    base_dir, settings_path_boot, hub_dir, _ = resolve_runtime_paths(__file__, "smoke_test_all")
    settings_path = resolve_settings_path(base_dir) or settings_path_boot or os.path.join(base_dir, "gui_settings.json")
    settings = read_settings_file(settings_path, module_name="smoke_test_all") or {}
    settings = sanitize_settings(settings if isinstance(settings, dict) else {})

    out: Dict[str, Any] = {"ts": int(time.time()), "ok": True, "steps": {}}

    # 1) stocks scanner
    try:
        s_scan = run_stock_scan(settings, hub_dir)
        out["steps"]["stock_scan"] = {
            "state": str((s_scan or {}).get("state", "")),
            "msg": str((s_scan or {}).get("msg", ""))[:180],
        }
    except Exception as exc:
        out["ok"] = False
        out["steps"]["stock_scan"] = {"error": f"{type(exc).__name__}: {exc}"}

    # 2) stocks trader
    try:
        s_step = run_stock_step(settings, hub_dir)
        out["steps"]["stock_trader"] = {
            "state": str((s_step or {}).get("state", "")),
            "msg": str((s_step or {}).get("msg", ""))[:180],
        }
    except Exception as exc:
        out["ok"] = False
        out["steps"]["stock_trader"] = {"error": f"{type(exc).__name__}: {exc}"}

    # 3) forex scanner
    try:
        f_scan = run_forex_scan(settings, hub_dir)
        out["steps"]["forex_scan"] = {
            "state": str((f_scan or {}).get("state", "")),
            "msg": str((f_scan or {}).get("msg", ""))[:180],
        }
    except Exception as exc:
        out["ok"] = False
        out["steps"]["forex_scan"] = {"error": f"{type(exc).__name__}: {exc}"}

    # 4) forex trader
    try:
        f_step = run_forex_step(settings, hub_dir)
        out["steps"]["forex_trader"] = {
            "state": str((f_step or {}).get("state", "")),
            "msg": str((f_step or {}).get("msg", ""))[:180],
        }
    except Exception as exc:
        out["ok"] = False
        out["steps"]["forex_trader"] = {"error": f"{type(exc).__name__}: {exc}"}

    # 5) autopilot dry-run
    try:
        auto = run_autopilot_once(dry_run=True)
        out["steps"]["autopilot"] = {
            "stable_cycles": int((auto or {}).get("stable_cycles", 0) or 0),
            "api_unstable": bool((auto or {}).get("api_unstable", False)),
            "changes": dict((auto or {}).get("changes", {}) or {}),
        }
    except Exception as exc:
        out["ok"] = False
        out["steps"]["autopilot"] = {"error": f"{type(exc).__name__}: {exc}"}

    # 6) autofix dry-run
    try:
        autofix = run_autofix_once(dry_run=True)
        out["steps"]["autofix"] = {
            "enabled": bool((autofix or {}).get("enabled", False)),
            "mode": str((autofix or {}).get("mode", "")),
            "tickets_created": int((autofix or {}).get("tickets_created", 0) or 0),
        }
    except Exception as exc:
        out["ok"] = False
        out["steps"]["autofix"] = {"error": f"{type(exc).__name__}: {exc}"}

    out["files"] = {
        "stock_status": _safe_json(os.path.join(hub_dir, "stocks", "stock_trader_status.json")),
        "forex_status": _safe_json(os.path.join(hub_dir, "forex", "forex_trader_status.json")),
        "stock_scan_diagnostics": _safe_json(os.path.join(hub_dir, "stocks", "scan_diagnostics.json")),
        "forex_scan_diagnostics": _safe_json(os.path.join(hub_dir, "forex", "scan_diagnostics.json")),
        "stock_universe_quality": _safe_json(os.path.join(hub_dir, "stocks", "universe_quality.json")),
        "forex_universe_quality": _safe_json(os.path.join(hub_dir, "forex", "universe_quality.json")),
        "runtime_startup_checks": _safe_json(os.path.join(hub_dir, "runtime_startup_checks.json")),
        "runtime_state": _safe_json(os.path.join(hub_dir, "runtime_state.json")),
        "autofix_status": _safe_json(os.path.join(hub_dir, "autofix_status.json")),
        "market_loop_status": _safe_json(os.path.join(hub_dir, "market_loop_status.json")),
        "scanner_cadence_drift": _safe_json(os.path.join(hub_dir, "scanner_cadence_drift.json")),
        "market_sla_metrics": _safe_json(os.path.join(hub_dir, "market_sla_metrics.json")),
        "market_trends": _safe_json(os.path.join(hub_dir, "market_trends.json")),
        "recent_incidents": {},
    }
    out["files"]["stock_status"] = {
        "state": str((out["files"]["stock_status"] or {}).get("state", "")),
        "msg": str((out["files"]["stock_status"] or {}).get("msg", ""))[:180],
    }
    out["files"]["forex_status"] = {
        "state": str((out["files"]["forex_status"] or {}).get("state", "")),
        "msg": str((out["files"]["forex_status"] or {}).get("msg", ""))[:180],
    }
    stock_diag = normalize_scan_diagnostics(out["files"]["stock_scan_diagnostics"], market="stocks")
    forex_diag = normalize_scan_diagnostics(out["files"]["forex_scan_diagnostics"], market="forex")
    out["files"]["stock_scan_diagnostics"] = {
        "state": str(stock_diag.get("state", "")),
        "msg": str(stock_diag.get("msg", ""))[:180],
        "schema_version": int(stock_diag.get("schema_version", 0) or 0),
        "leaders_total": int(stock_diag.get("leaders_total", 0) or 0),
        "scores_total": int(stock_diag.get("scores_total", 0) or 0),
    }
    out["files"]["forex_scan_diagnostics"] = {
        "state": str(forex_diag.get("state", "")),
        "msg": str(forex_diag.get("msg", ""))[:180],
        "schema_version": int(forex_diag.get("schema_version", 0) or 0),
        "leaders_total": int(forex_diag.get("leaders_total", 0) or 0),
        "scores_total": int(forex_diag.get("scores_total", 0) or 0),
    }
    out["files"]["stock_universe_quality"] = {
        "summary": str((out["files"]["stock_universe_quality"] or {}).get("summary", ""))[:160],
        "reject_rate_pct": float((out["files"]["stock_universe_quality"] or {}).get("reject_rate_pct", 0.0) or 0.0),
        "candidate_churn_pct": float((out["files"]["stock_universe_quality"] or {}).get("candidate_churn_pct", 0.0) or 0.0),
        "leaders_total": int((out["files"]["stock_universe_quality"] or {}).get("leaders_total", 0) or 0),
    }
    out["files"]["forex_universe_quality"] = {
        "summary": str((out["files"]["forex_universe_quality"] or {}).get("summary", ""))[:160],
        "reject_rate_pct": float((out["files"]["forex_universe_quality"] or {}).get("reject_rate_pct", 0.0) or 0.0),
        "candidate_churn_pct": float((out["files"]["forex_universe_quality"] or {}).get("candidate_churn_pct", 0.0) or 0.0),
        "leaders_total": int((out["files"]["forex_universe_quality"] or {}).get("leaders_total", 0) or 0),
    }
    out["files"]["runtime_startup_checks"] = {
        "ok": bool((out["files"]["runtime_startup_checks"] or {}).get("ok", False)),
        "errors": int(len(list((out["files"]["runtime_startup_checks"] or {}).get("errors", []) or []))),
        "warnings": int(len(list((out["files"]["runtime_startup_checks"] or {}).get("warnings", []) or []))),
    }
    rstate = out["files"]["runtime_state"] if isinstance(out["files"]["runtime_state"], dict) else {}
    runner = rstate.get("runner", {}) if isinstance(rstate.get("runner", {}), dict) else {}
    checks = rstate.get("checks", {}) if isinstance(rstate.get("checks", {}), dict) else {}
    scan_health = rstate.get("scan_health", {}) if isinstance(rstate.get("scan_health", {}), dict) else {}
    scan_stocks = scan_health.get("stocks", {}) if isinstance(scan_health.get("stocks", {}), dict) else {}
    scan_forex = scan_health.get("forex", {}) if isinstance(scan_health.get("forex", {}), dict) else {}
    incidents = rstate.get("incidents_last_200", {}) if isinstance(rstate.get("incidents_last_200", {}), dict) else {}
    alerts = rstate.get("alerts", {}) if isinstance(rstate.get("alerts", {}), dict) else {}
    scan_drift = rstate.get("scan_drift", {}) if isinstance(rstate.get("scan_drift", {}), dict) else {}
    drift_active = scan_drift.get("active", []) if isinstance(scan_drift.get("active", []), list) else []
    scan_cadence = rstate.get("scan_cadence", {}) if isinstance(rstate.get("scan_cadence", {}), dict) else {}
    cadence_active = scan_cadence.get("active", []) if isinstance(scan_cadence.get("active", []), list) else []
    exec_guard = rstate.get("execution_guard", {}) if isinstance(rstate.get("execution_guard", {}), dict) else {}
    drawdown_guard = rstate.get("drawdown_guard", {}) if isinstance(rstate.get("drawdown_guard", {}), dict) else {}
    stop_flag = rstate.get("stop_flag", {}) if isinstance(rstate.get("stop_flag", {}), dict) else {}
    guard_markets = exec_guard.get("markets", {}) if isinstance(exec_guard.get("markets", {}), dict) else {}
    guard_active = 0
    for row in guard_markets.values():
        if not isinstance(row, dict):
            continue
        if int(row.get("disabled_until", 0) or 0) > int(rstate.get("ts", 0) or 0):
            guard_active += 1
    exposure = rstate.get("exposure_map", {}) if isinstance(rstate.get("exposure_map", {}), dict) else {}
    top_positions = exposure.get("top_positions", []) if isinstance(exposure.get("top_positions", []), list) else []
    top_exposure_pct = 0.0
    if top_positions and isinstance(top_positions[0], dict):
        top_exposure_pct = float(top_positions[0].get("pct_of_total_exposure", 0.0) or 0.0)
    out["files"]["runtime_state"] = {
        "runner_state": str(runner.get("state", "") or ""),
        "checks_ok": bool(checks.get("ok", False)),
        "scan_stocks_state": str(scan_stocks.get("state", "") or ""),
        "scan_forex_state": str(scan_forex.get("state", "") or ""),
        "incident_count_last_200": int(incidents.get("count", 0) or 0),
        "alert_severity": str(alerts.get("severity", "") or ""),
        "alert_reasons": int(len(list(alerts.get("reasons", []) or []))),
        "scan_drift_active": int(len(drift_active)),
        "scan_cadence_active": int(len(cadence_active)),
        "execution_guard_active_markets": int(guard_active),
        "drawdown_guard_triggered_recent": bool(drawdown_guard.get("triggered_recent", False)),
        "stop_flag_active": bool(stop_flag.get("active", False)),
        "total_exposure_usd": float(exposure.get("total_exposure_usd", 0.0) or 0.0),
        "top_exposure_pct": round(top_exposure_pct, 4),
    }
    ml = out["files"]["market_loop_status"] if isinstance(out["files"]["market_loop_status"], dict) else {}
    out["files"]["market_loop_status"] = {
        "ts": int(ml.get("ts", 0) or 0),
        "next_snapshot_ts": int(ml.get("next_snapshot_ts", 0) or 0),
        "next_stocks_scan_ts": int(ml.get("next_stocks_scan_ts", 0) or 0),
        "next_forex_scan_ts": int(ml.get("next_forex_scan_ts", 0) or 0),
        "stocks_scan_state": str(((ml.get("stocks_cycle", {}) if isinstance(ml.get("stocks_cycle", {}), dict) else {}).get("scan_state", "") or "")),
        "forex_scan_state": str(((ml.get("forex_cycle", {}) if isinstance(ml.get("forex_cycle", {}), dict) else {}).get("scan_state", "") or "")),
    }
    cadence = out["files"]["scanner_cadence_drift"] if isinstance(out["files"]["scanner_cadence_drift"], dict) else {}
    out["files"]["scanner_cadence_drift"] = {
        "ts": int(cadence.get("ts", 0) or 0),
        "active_count": int(len(list(cadence.get("active", []) or []))),
        "markets": sorted(list((cadence.get("markets", {}) or {}).keys()))[:4] if isinstance(cadence.get("markets", {}), dict) else [],
    }
    msla = (out["files"]["market_sla_metrics"] or {}).get("metrics", {})
    if not isinstance(msla, dict):
        msla = {}
    out["files"]["market_sla_metrics"] = {
        "keys": sorted(list(msla.keys()))[:12],
        "count": int(len(msla)),
    }
    mtr = out["files"]["market_trends"] if isinstance(out["files"]["market_trends"], dict) else {}
    s_tr = mtr.get("stocks", {}) if isinstance(mtr.get("stocks", {}), dict) else {}
    f_tr = mtr.get("forex", {}) if isinstance(mtr.get("forex", {}), dict) else {}
    s_stale = s_tr.get("stale_signal", {}) if isinstance(s_tr.get("stale_signal", {}), dict) else {}
    f_stale = f_tr.get("stale_signal", {}) if isinstance(f_tr.get("stale_signal", {}), dict) else {}
    s_quality = s_tr.get("quality_aggregates", {}) if isinstance(s_tr.get("quality_aggregates", {}), dict) else {}
    f_quality = f_tr.get("quality_aggregates", {}) if isinstance(f_tr.get("quality_aggregates", {}), dict) else {}
    s_cadence = s_tr.get("cadence_aggregates", {}) if isinstance(s_tr.get("cadence_aggregates", {}), dict) else {}
    f_cadence = f_tr.get("cadence_aggregates", {}) if isinstance(f_tr.get("cadence_aggregates", {}), dict) else {}
    out["files"]["market_trends"] = {
        "stocks_divergence_24h": int(s_tr.get("divergence_24h", 0) or 0),
        "forex_divergence_24h": int(f_tr.get("divergence_24h", 0) or 0),
        "stocks_stale_p95_s": float(s_stale.get("p95_s", 0.0) or 0.0),
        "forex_stale_p95_s": float(f_stale.get("p95_s", 0.0) or 0.0),
        "stocks_quality_reject_pct": float(s_quality.get("reject_rate_pct", 0.0) or 0.0),
        "forex_quality_reject_pct": float(f_quality.get("reject_rate_pct", 0.0) or 0.0),
        "stocks_cadence_level": str(s_cadence.get("level", "")),
        "forex_cadence_level": str(f_cadence.get("level", "")),
    }
    try:
        sev_counts: Dict[str, int] = {}
        lines = []
        incidents_path = os.path.join(hub_dir, "incidents.jsonl")
        with open(incidents_path, "r", encoding="utf-8") as f:
            lines = [ln.strip() for ln in f if ln.strip()]
        for ln in lines[-200:]:
            try:
                row = json.loads(ln)
            except Exception:
                continue
            sev = str(row.get("severity", "info") or "info").strip().lower()
            sev_counts[sev] = int(sev_counts.get(sev, 0)) + 1
        out["files"]["recent_incidents"] = {
            "count_last_200": int(len(lines[-200:])),
            "by_severity": sev_counts,
        }
    except Exception:
        out["files"]["recent_incidents"] = {"count_last_200": 0, "by_severity": {}}

    out_path = os.path.join(hub_dir, "smoke_test_report.json")
    tmp = f"{out_path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)
    os.replace(tmp, out_path)

    print(json.dumps(out, indent=2))
    return 0 if bool(out.get("ok", False)) else 1


if __name__ == "__main__":
    raise SystemExit(main())
