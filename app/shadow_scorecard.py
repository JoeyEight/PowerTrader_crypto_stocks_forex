from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List


def _safe_read_json(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _f(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _market_scorecard(hub_dir: str, market: str) -> Dict[str, Any]:
    m = str(market or "").strip().lower()
    if m not in {"stocks", "forex"}:
        return {"market": m, "state": "ERROR", "msg": "unsupported market"}

    trends = _safe_read_json(os.path.join(hub_dir, "market_trends.json"))
    trend = trends.get(m, {}) if isinstance(trends.get(m, {}), dict) else {}
    walk = _safe_read_json(os.path.join(hub_dir, "walkforward_report.json"))
    walk_row = walk.get(m, {}) if isinstance(walk.get(m, {}), dict) else {}
    calib = _safe_read_json(os.path.join(hub_dir, "confidence_calibration.json"))
    calib_row = calib.get(m, {}) if isinstance(calib.get(m, {}), dict) else {}
    regimes = _safe_read_json(os.path.join(hub_dir, "market_regimes.json"))
    regime_row = regimes.get(m, {}) if isinstance(regimes.get(m, {}), dict) else {}

    quality = trend.get("quality_aggregates", {}) if isinstance(trend.get("quality_aggregates", {}), dict) else {}
    discrepancy = trend.get("discrepancy_tracker", {}) if isinstance(trend.get("discrepancy_tracker", {}), dict) else {}
    reliability = trend.get("data_source_reliability", {}) if isinstance(trend.get("data_source_reliability", {}), dict) else {}
    cadence = trend.get("cadence_aggregates", {}) if isinstance(trend.get("cadence_aggregates", {}), dict) else {}

    reject = _f(quality.get("reject_rate_pct", 100.0), 100.0)
    divergence = _f(discrepancy.get("divergence_pressure_pct", 100.0), 100.0)
    reliab = _f(reliability.get("score", 0.0), 0.0)
    latest_window = walk_row.get("latest_window", {}) if isinstance(walk_row.get("latest_window", {}), dict) else {}
    latest_test = latest_window.get("test", {}) if isinstance(latest_window.get("test", {}), dict) else {}
    walk_success = _f(latest_test.get("win_rate_pct", 0.0), 0.0)
    calib_samples = int(calib_row.get("samples", 0) or 0)

    score = 100.0
    score -= min(40.0, max(0.0, reject - 70.0) * 0.8)
    score -= min(25.0, max(0.0, divergence - 50.0) * 0.35)
    score += min(15.0, max(0.0, reliab - 70.0) * 0.3)
    score += min(10.0, max(0.0, walk_success - 50.0) * 0.2)
    if str(cadence.get("level", "ok") or "ok").strip().lower() == "critical":
        score -= 12.0
    if calib_samples < 12:
        score -= 10.0
    score = max(0.0, min(100.0, score))

    blockers: List[str] = []
    warns: List[str] = []
    if reject >= 96.0:
        blockers.append("reject_rate_too_high")
    elif reject >= 90.0:
        warns.append("reject_rate_high")
    if divergence >= 90.0:
        blockers.append("shadow_divergence_too_high")
    elif divergence >= 75.0:
        warns.append("shadow_divergence_high")
    if reliab < 55.0:
        blockers.append("data_reliability_low")
    elif reliab < 70.0:
        warns.append("data_reliability_moderate")
    if str(cadence.get("level", "ok") or "ok").strip().lower() == "critical":
        blockers.append("scanner_cadence_critical")
    if calib_samples < 6:
        blockers.append("calibration_samples_insufficient")
    elif calib_samples < 20:
        warns.append("calibration_samples_low")

    stage_gate = "PASS"
    if blockers:
        stage_gate = "BLOCK"
    elif warns:
        stage_gate = "WARN"

    return {
        "market": m,
        "ts": int(time.time()),
        "readiness_score": round(score, 4),
        "promotion_gate": stage_gate,
        "blockers": blockers[:8],
        "warnings": warns[:8],
        "metrics": {
            "reject_rate_pct": round(reject, 4),
            "shadow_divergence_pct": round(divergence, 4),
            "data_reliability_score": round(reliab, 4),
            "walkforward_test_win_rate_pct": round(walk_success, 4),
            "calibration_samples": int(calib_samples),
            "dominant_regime": str(regime_row.get("dominant_regime", "unknown") or "unknown"),
            "cadence_level": str(cadence.get("level", "ok") or "ok"),
        },
    }


def build_shadow_scorecards(hub_dir: str) -> Dict[str, Any]:
    stocks = _market_scorecard(hub_dir, "stocks")
    forex = _market_scorecard(hub_dir, "forex")
    ready = bool(str(stocks.get("promotion_gate", "") or "") == "PASS" and str(forex.get("promotion_gate", "") or "") == "PASS")
    return {
        "ts": int(time.time()),
        "stocks": stocks,
        "forex": forex,
        "all_markets_pass": bool(ready),
    }
