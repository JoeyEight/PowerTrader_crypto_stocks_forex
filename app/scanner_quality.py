from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping


def _norm_ids(values: Iterable[Any]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for item in values:
        s = str(item or "").strip().upper()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def turnover_pct(prev_ids: Iterable[Any], cur_ids: Iterable[Any]) -> float:
    prev = set(_norm_ids(prev_ids))
    cur = set(_norm_ids(cur_ids))
    union = prev | cur
    if not union:
        return 0.0
    diff = prev ^ cur
    return round((100.0 * float(len(diff)) / float(len(union))), 2)


def _reason_percentages(counts: Mapping[str, Any], total: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    denom = max(1, int(total or 0))
    for key, value in (counts or {}).items():
        reason = str(key or "unknown").strip().lower() or "unknown"
        try:
            count = max(0, int(value or 0))
        except Exception:
            count = 0
        if count <= 0:
            continue
        rows.append(
            {
                "reason": reason,
                "count": int(count),
                "pct": round((100.0 * float(count) / float(denom)), 3),
            }
        )
    rows.sort(key=lambda r: (-int(r.get("count", 0)), str(r.get("reason", ""))))
    return rows


def _source_breakdown(scored_rows: Iterable[Dict[str, Any]], rejected_rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    counts: Dict[str, int] = {}
    for row in scored_rows:
        if not isinstance(row, dict):
            continue
        src = str(row.get("data_source", "") or "").strip().lower()
        if not src:
            continue
        counts[src] = int(counts.get(src, 0)) + 1
    for row in rejected_rows:
        if not isinstance(row, dict):
            continue
        src = str(row.get("source", "") or "").strip().lower()
        if not src:
            continue
        counts[src] = int(counts.get(src, 0)) + 1
    total = max(1, int(sum(counts.values())))
    out: List[Dict[str, Any]] = []
    for src, count in counts.items():
        out.append({"source": str(src), "count": int(count), "pct": round((100.0 * float(count) / float(total)), 3)})
    out.sort(key=lambda r: (-int(r.get("count", 0)), str(r.get("source", ""))))
    return out


def build_universe_quality_report(
    *,
    market: str,
    ts: int,
    mode: str,
    universe_total: int,
    candidates_total: int,
    scores_total: int,
    leaders_total: int,
    reject_summary: Dict[str, Any],
    rejected_rows: List[Dict[str, Any]],
    scored_rows: List[Dict[str, Any]],
    candidate_churn_pct: float,
    leader_churn_pct: float,
) -> Dict[str, Any]:
    universe_n = max(0, int(universe_total or 0))
    candidates_n = max(0, int(candidates_total or 0))
    scores_n = max(0, int(scores_total or 0))
    leaders_n = max(0, int(leaders_total or 0))

    try:
        reject_rate_pct = float((reject_summary.get("reject_rate_pct", 0.0) if isinstance(reject_summary, dict) else 0.0) or 0.0)
    except Exception:
        reject_rate_pct = 0.0
    reject_rate_pct = max(0.0, min(100.0, reject_rate_pct))

    acceptance_rate_pct = round((100.0 * float(candidates_n) / float(max(1, universe_n))), 3)
    score_survival_pct = round((100.0 * float(scores_n) / float(max(1, candidates_n))), 3)
    leader_yield_pct = round((100.0 * float(leaders_n) / float(max(1, scores_n))), 3)

    counts = (reject_summary.get("counts", {}) if isinstance(reject_summary, dict) else {})
    if not isinstance(counts, dict):
        counts = {}
    reason_breakdown = _reason_percentages(counts, int(max(1, candidates_n if candidates_n > 0 else universe_n)))
    source_mix = _source_breakdown(scored_rows, rejected_rows)

    gates = {
        "universe_nonempty": bool(universe_n > 0),
        "candidate_coverage_ok": bool(candidates_n >= max(1, int(universe_n * 0.10))) if universe_n > 0 else False,
        "score_coverage_ok": bool(scores_n >= max(1, int(candidates_n * 0.10))) if candidates_n > 0 else False,
        "leaders_present": bool(leaders_n > 0),
        "reject_rate_ok": bool(reject_rate_pct <= 80.0),
    }
    passed = int(sum(1 for v in gates.values() if bool(v)))
    total = max(1, int(len(gates)))

    if reject_rate_pct >= 80.0:
        summary = f"Reject-heavy cycle ({reject_rate_pct:.1f}%)."
    elif leaders_n <= 0:
        summary = "No leaders ranked this cycle."
    elif scores_n <= 0:
        summary = "No scored rows survived quality gates."
    else:
        summary = f"Healthy cycle with {leaders_n} leaders from {candidates_n} candidates."

    return {
        "market": str(market or "").strip().lower(),
        "ts": int(ts or 0),
        "mode": str(mode or ""),
        "summary": summary,
        "universe_total": int(universe_n),
        "candidates_total": int(candidates_n),
        "scores_total": int(scores_n),
        "leaders_total": int(leaders_n),
        "acceptance_rate_pct": float(acceptance_rate_pct),
        "reject_rate_pct": float(round(reject_rate_pct, 3)),
        "score_survival_pct": float(score_survival_pct),
        "leader_yield_pct": float(leader_yield_pct),
        "candidate_churn_pct": float(round(float(candidate_churn_pct or 0.0), 3)),
        "leader_churn_pct": float(round(float(leader_churn_pct or 0.0), 3)),
        "rejection_reasons": reason_breakdown,
        "data_source_mix": source_mix,
        "gates": gates,
        "gate_passed": int(passed),
        "gate_total": int(total),
        "gate_pass_pct": round((100.0 * float(passed) / float(total)), 3),
    }


def quality_hints(report: Dict[str, Any]) -> List[str]:
    if not isinstance(report, dict):
        return []
    hints: List[str] = []
    try:
        reject_rate = float(report.get("reject_rate_pct", 0.0) or 0.0)
    except Exception:
        reject_rate = 0.0
    try:
        churn = float(report.get("candidate_churn_pct", 0.0) or 0.0)
    except Exception:
        churn = 0.0
    try:
        leaders = int(report.get("leaders_total", 0) or 0)
    except Exception:
        leaders = 0

    reasons = report.get("rejection_reasons", []) if isinstance(report.get("rejection_reasons", []), list) else []
    dominant = ""
    if reasons and isinstance(reasons[0], dict):
        dominant = str(reasons[0].get("reason", "") or "").strip().lower()

    if reject_rate >= 75.0:
        hints.append("Universe quality is reject-heavy; tune dominant gate before raising scan breadth.")
    if dominant:
        hints.append(f"Dominant reject reason: {dominant}.")
    if churn >= 65.0:
        hints.append("Candidate churn is high; consider slower cadence or tighter stability gates.")
    if leaders <= 0:
        hints.append("No leaders ranked; inspect source coverage and score thresholds.")
    if not hints:
        hints.append("Universe quality is stable for this cycle.")
    return hints[:3]
