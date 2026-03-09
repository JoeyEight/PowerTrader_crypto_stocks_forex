from __future__ import annotations

from typing import Any, Dict, List


def evaluate_live_mode_checklist(runtime_state: Dict[str, Any]) -> Dict[str, Any]:
    rt = runtime_state if isinstance(runtime_state, dict) else {}
    checks = rt.get("checks", {}) if isinstance(rt.get("checks", {}), dict) else {}
    alerts = rt.get("alerts", {}) if isinstance(rt.get("alerts", {}), dict) else {}
    api_quota = rt.get("api_quota", {}) if isinstance(rt.get("api_quota", {}), dict) else {}
    guard = rt.get("execution_guard", {}) if isinstance(rt.get("execution_guard", {}), dict) else {}
    guard_markets = guard.get("markets", {}) if isinstance(guard.get("markets", {}), dict) else {}
    ts = int(rt.get("ts", 0) or 0)

    reasons: List[str] = []
    if not bool(checks.get("ok", False)):
        reasons.append("startup_checks_not_ok")
    if str(alerts.get("severity", "ok") or "ok").strip().lower() not in {"ok"}:
        reasons.append("runtime_alerts_not_green")
    if str(api_quota.get("status", "ok") or "ok").strip().lower() == "critical":
        reasons.append("api_quota_critical")

    for market, row in guard_markets.items():
        if not isinstance(row, dict):
            continue
        if int(row.get("disabled_until", 0) or 0) > ts:
            reasons.append(f"execution_guard_active:{market}")

    return {"ok": len(reasons) == 0, "reasons": reasons}
