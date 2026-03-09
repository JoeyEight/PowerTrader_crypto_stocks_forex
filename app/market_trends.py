from __future__ import annotations

import json
import os
import re
import time
from typing import Any, Dict, Iterable, List

from app.scan_diagnostics_schema import normalize_scan_diagnostics


def _safe_read_json(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _safe_read_jsonl(path: str, max_lines: int = 2000) -> List[Dict[str, Any]]:
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


def _percentile(values: List[float], q: float) -> float:
    if not values:
        return 0.0
    arr = sorted(float(x) for x in values)
    idx = int(round((len(arr) - 1) * max(0.0, min(1.0, float(q)))))
    idx = max(0, min(idx, len(arr) - 1))
    return float(arr[idx])


def parse_stale_signal_seconds(msg: Any) -> int:
    text = str(msg or "")
    m = re.search(r"signal\s+stale\s*\((\d+)s\s*>\s*(\d+)s\)", text, flags=re.IGNORECASE)
    if not m:
        return 0
    try:
        return max(0, int(m.group(1)))
    except Exception:
        return 0


def _audit_event_counts(rows: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for row in rows:
        evt = str(row.get("event", "") or "").strip().lower()
        if not evt:
            continue
        out[evt] = int(out.get(evt, 0)) + 1
    return out


def _recent_rows(rows: List[Dict[str, Any]], horizon_s: int = 86400) -> List[Dict[str, Any]]:
    now = int(time.time())
    out: List[Dict[str, Any]] = []
    for row in rows:
        ts = int(row.get("ts", 0) or 0)
        if ts <= 0:
            continue
        if (now - ts) <= int(horizon_s):
            out.append(row)
    return out


def _spread_series_from_rankings(rows: List[Dict[str, Any]]) -> List[float]:
    vals: List[float] = []
    for row in rows:
        top = row.get("top", [])
        if not isinstance(top, list):
            continue
        for cand in top[:5]:
            if not isinstance(cand, dict):
                continue
            try:
                spr = float(cand.get("spread_bps", 0.0) or 0.0)
            except Exception:
                spr = 0.0
            if spr > 0.0:
                vals.append(spr)
    return vals


def _quality_aggregate(scan_diag: Dict[str, Any], quality_report: Dict[str, Any]) -> Dict[str, Any]:
    diag = scan_diag if isinstance(scan_diag, dict) else {}
    report = quality_report if isinstance(quality_report, dict) else {}
    reject_summary = diag.get("reject_summary", {}) if isinstance(diag.get("reject_summary", {}), dict) else {}
    report_reasons = report.get("rejection_reasons", []) if isinstance(report.get("rejection_reasons", []), list) else []
    dominant_reason = ""
    if report_reasons and isinstance(report_reasons[0], dict):
        dominant_reason = str(report_reasons[0].get("reason", "") or "").strip().lower()
    if not dominant_reason:
        dominant_reason = str(reject_summary.get("dominant_reason", "") or "").strip().lower()
    reject_rate = float(report.get("reject_rate_pct", reject_summary.get("reject_rate_pct", 0.0)) or 0.0)
    candidate_churn = float(report.get("candidate_churn_pct", diag.get("candidate_churn_pct", 0.0)) or 0.0)
    leader_churn = float(report.get("leader_churn_pct", diag.get("leader_churn_pct", 0.0)) or 0.0)
    gate_pass_pct = float(report.get("gate_pass_pct", 0.0) or 0.0)
    leaders_total = int(report.get("leaders_total", diag.get("leaders_total", 0)) or 0)
    scores_total = int(report.get("scores_total", diag.get("scores_total", 0)) or 0)
    return {
        "reject_rate_pct": round(max(0.0, min(100.0, reject_rate)), 3),
        "candidate_churn_pct": round(max(0.0, min(100.0, candidate_churn)), 3),
        "leader_churn_pct": round(max(0.0, min(100.0, leader_churn)), 3),
        "gate_pass_pct": round(max(0.0, min(100.0, gate_pass_pct)), 3),
        "dominant_reason": dominant_reason,
        "leaders_total": max(0, leaders_total),
        "scores_total": max(0, scores_total),
    }


def _cadence_aggregate(hub_dir: str, market: str) -> Dict[str, Any]:
    cadence = _safe_read_json(os.path.join(hub_dir, "scanner_cadence_drift.json"))
    markets = cadence.get("markets", {}) if isinstance(cadence.get("markets", {}), dict) else {}
    row = markets.get(market, {}) if isinstance(markets.get(market, {}), dict) else {}
    active = cadence.get("active", []) if isinstance(cadence.get("active", []), list) else []
    active_rows = [
        a
        for a in active
        if isinstance(a, dict) and str(a.get("market", "") or "").strip().lower() == str(market or "").strip().lower()
    ]
    level = str(row.get("level", "ok") or "ok").strip().lower()
    late_pct = float(row.get("late_pct", 0.0) or 0.0)
    observed_s = float(row.get("observed_s", 0.0) or 0.0)
    expected_s = float(row.get("expected_s", 0.0) or 0.0)
    return {
        "level": level,
        "late_pct": round(max(0.0, late_pct), 3),
        "observed_s": round(max(0.0, observed_s), 3),
        "expected_s": round(max(0.0, expected_s), 3),
        "active": bool(active_rows or level in {"warning", "critical"}),
        "active_alerts_total": int(len(active_rows)),
    }


def build_market_trend_summary(hub_dir: str, market: str) -> Dict[str, Any]:
    m = str(market or "").strip().lower()
    if m not in {"stocks", "forex"}:
        return {"market": m, "state": "ERROR", "msg": "unsupported market"}

    mdir = os.path.join(hub_dir, m)
    audit_rows = _safe_read_jsonl(os.path.join(mdir, "execution_audit.jsonl"), max_lines=3000)
    rank_rows = _safe_read_jsonl(os.path.join(mdir, "scanner_rankings.jsonl"), max_lines=1200)
    trader_status = _safe_read_json(os.path.join(mdir, f"{m[:-1] if m.endswith('s') else m}_trader_status.json"))
    thinker_status = _safe_read_json(os.path.join(mdir, f"{m[:-1] if m.endswith('s') else m}_thinker_status.json"))
    scan_diag = normalize_scan_diagnostics(_safe_read_json(os.path.join(mdir, "scan_diagnostics.json")), market=m)
    quality_report = _safe_read_json(os.path.join(mdir, "universe_quality.json"))

    recent_audit = _recent_rows(audit_rows, horizon_s=86400)
    stale_secs = [parse_stale_signal_seconds(row.get("msg", "")) for row in recent_audit]
    stale_secs = [int(x) for x in stale_secs if int(x) > 0]

    audit_spreads: List[float] = []
    for row in recent_audit:
        try:
            spr = float(row.get("spread_bps", 0.0) or 0.0)
        except Exception:
            spr = 0.0
        if spr > 0.0:
            audit_spreads.append(spr)
    ranking_spreads = _spread_series_from_rankings(rank_rows[-200:])
    spread_series = list(audit_spreads) + list(ranking_spreads)

    scores: List[float] = []
    for row in rank_rows[-200:]:
        top = row.get("top", [])
        if not isinstance(top, list):
            continue
        for cand in top[:5]:
            if not isinstance(cand, dict):
                continue
            try:
                s = abs(float(cand.get("score", 0.0) or 0.0))
            except Exception:
                s = 0.0
            if s > 0.0:
                scores.append(s)

    event_counts_24h = _audit_event_counts(recent_audit)
    all_event_counts = _audit_event_counts(audit_rows)
    divergence_24h = int(event_counts_24h.get("shadow_live_divergence", 0))
    quality_agg = _quality_aggregate(scan_diag, quality_report)
    cadence_agg = _cadence_aggregate(hub_dir, m)
    chart_map = thinker_status.get("top_chart_map", {}) if isinstance(thinker_status.get("top_chart_map", {}), dict) else {}
    chart_coverage = {
        "symbols_cached": int(len([k for k, v in chart_map.items() if str(k).strip() and isinstance(v, list)])),
        "bars_total": int(sum(len(list(v or [])) for v in chart_map.values() if isinstance(v, list))),
        "fallback_cached": bool(thinker_status.get("fallback_cached", False)),
    }

    return {
        "market": m,
        "ts": int(time.time()),
        "event_counts_24h": event_counts_24h,
        "event_counts_total": all_event_counts,
        "divergence_24h": int(divergence_24h),
        "stale_signal": {
            "count_24h": int(len(stale_secs)),
            "avg_s": round((sum(stale_secs) / max(1, len(stale_secs))), 3) if stale_secs else 0.0,
            "p95_s": round(_percentile([float(x) for x in stale_secs], 0.95), 3),
            "max_s": int(max(stale_secs) if stale_secs else 0),
        },
        "spread_bps": {
            "samples": int(len(spread_series)),
            "avg": round((sum(spread_series) / max(1, len(spread_series))), 4) if spread_series else 0.0,
            "p95": round(_percentile(spread_series, 0.95), 4),
        },
        "signal_score_abs": {
            "samples": int(len(scores)),
            "p50": round(_percentile(scores, 0.50), 6),
            "p95": round(_percentile(scores, 0.95), 6),
        },
        "quality_aggregates": quality_agg,
        "cadence_aggregates": cadence_agg,
        "chart_coverage": chart_coverage,
        "trader_state": str(trader_status.get("state", "") or ""),
        "trader_msg": str(trader_status.get("msg", "") or "")[:180],
    }


def build_trends_payload(hub_dir: str) -> Dict[str, Any]:
    stocks = build_market_trend_summary(hub_dir, "stocks")
    forex = build_market_trend_summary(hub_dir, "forex")
    return {
        "ts": int(time.time()),
        "stocks": stocks,
        "forex": forex,
    }
