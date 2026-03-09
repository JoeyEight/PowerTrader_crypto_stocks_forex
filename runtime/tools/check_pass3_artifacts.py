from __future__ import annotations

import json
import os
import sys
import time
from typing import Any, Dict

_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if __package__ in (None, ""):
    if _ROOT not in sys.path:
        sys.path.insert(0, _ROOT)

from app.path_utils import resolve_runtime_paths
from app.scan_diagnostics_schema import normalize_scan_diagnostics


def _safe_json(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def main() -> int:
    probe_file = os.path.join(_ROOT, "runtime", "pt_runner.py")
    _base, _settings, hub_dir, _ = resolve_runtime_paths(probe_file, "check_pass3_artifacts")
    checks = {
        "stocks_scan_diagnostics": os.path.join(hub_dir, "stocks", "scan_diagnostics.json"),
        "forex_scan_diagnostics": os.path.join(hub_dir, "forex", "scan_diagnostics.json"),
        "stocks_universe_quality": os.path.join(hub_dir, "stocks", "universe_quality.json"),
        "forex_universe_quality": os.path.join(hub_dir, "forex", "universe_quality.json"),
        "scanner_cadence_drift": os.path.join(hub_dir, "scanner_cadence_drift.json"),
        "runtime_state": os.path.join(hub_dir, "runtime_state.json"),
        "market_trends": os.path.join(hub_dir, "market_trends.json"),
    }

    out: Dict[str, Any] = {"ts": int(time.time()), "ok": True, "checks": {}}
    for key, path in checks.items():
        exists = bool(os.path.isfile(path))
        payload = _safe_json(path) if exists else {}
        has_data = bool(payload)
        out["checks"][key] = {
            "exists": exists,
            "has_data": has_data,
            "path": path,
        }
        if (not exists) or (not has_data):
            out["ok"] = False

    stocks_raw = _safe_json(checks["stocks_scan_diagnostics"])
    forex_raw = _safe_json(checks["forex_scan_diagnostics"])
    stocks_diag = normalize_scan_diagnostics(stocks_raw, market="stocks")
    forex_diag = normalize_scan_diagnostics(forex_raw, market="forex")
    runtime_state = _safe_json(checks["runtime_state"])
    market_trends = _safe_json(checks["market_trends"])

    def _scan_diag_ok(raw: Dict[str, Any], diag: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "schema_version": int(raw.get("schema_version", 0) or 0),
            "normalized_schema_version": int(diag.get("schema_version", 0) or 0),
            "has_churn_fields": ("candidate_churn_pct" in diag and "leader_churn_pct" in diag),
            "leaders_total": int(diag.get("leaders_total", 0) or 0),
        }

    runtime_alerts = runtime_state.get("alerts", {}) if isinstance(runtime_state.get("alerts", {}), dict) else {}
    runtime_metrics = runtime_alerts.get("metrics", {}) if isinstance(runtime_alerts.get("metrics", {}), dict) else {}
    runtime_extras = {
        "has_scan_cadence": isinstance(runtime_state.get("scan_cadence", {}), dict),
        "has_broker_backoff": isinstance(runtime_state.get("broker_backoff", {}), dict),
        "has_market_loop_age_metric": ("market_loop_age_s" in runtime_metrics),
    }
    out["checks"]["stocks_scan_diagnostics"]["details"] = _scan_diag_ok(stocks_raw, stocks_diag)
    out["checks"]["forex_scan_diagnostics"]["details"] = _scan_diag_ok(forex_raw, forex_diag)
    out["checks"]["runtime_state"]["details"] = runtime_extras

    tr_stocks = market_trends.get("stocks", {}) if isinstance(market_trends.get("stocks", {}), dict) else {}
    tr_forex = market_trends.get("forex", {}) if isinstance(market_trends.get("forex", {}), dict) else {}
    trend_details = {
        "stocks_has_quality_aggregates": isinstance(tr_stocks.get("quality_aggregates", {}), dict),
        "stocks_has_cadence_aggregates": isinstance(tr_stocks.get("cadence_aggregates", {}), dict),
        "forex_has_quality_aggregates": isinstance(tr_forex.get("quality_aggregates", {}), dict),
        "forex_has_cadence_aggregates": isinstance(tr_forex.get("cadence_aggregates", {}), dict),
    }
    out["checks"]["market_trends"]["details"] = trend_details

    if int(out["checks"]["stocks_scan_diagnostics"]["details"]["schema_version"]) < 2:
        out["ok"] = False
    if int(out["checks"]["forex_scan_diagnostics"]["details"]["schema_version"]) < 2:
        out["ok"] = False
    if not all(bool(v) for v in runtime_extras.values()):
        out["ok"] = False
    if not all(bool(v) for v in trend_details.values()):
        out["ok"] = False

    print(json.dumps(out, indent=2))
    return 0 if bool(out.get("ok", False)) else 1


if __name__ == "__main__":
    raise SystemExit(main())
