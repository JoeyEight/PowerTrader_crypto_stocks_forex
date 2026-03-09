from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping

SCAN_DIAGNOSTICS_SCHEMA_VERSION = 2


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return int(default)


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    txt = str(value or "").strip().lower()
    if txt in {"1", "true", "yes", "on", "y", "t"}:
        return True
    if txt in {"0", "false", "no", "off", "n", "f"}:
        return False
    return bool(default)


def _norm_ids(values: Iterable[Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for row in values:
        s = str(row or "").strip().upper()
        if (not s) or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _market_name(raw_market: Any, fallback: str = "") -> str:
    m = str(raw_market or fallback or "").strip().lower()
    if m in {"stock"}:
        return "stocks"
    if m in {"fx"}:
        return "forex"
    return m


def normalize_scan_diagnostics(payload: Mapping[str, Any] | None, market: str = "") -> Dict[str, Any]:
    source = dict(payload) if isinstance(payload, Mapping) else {}
    in_version = max(1, _as_int(source.get("schema_version", 1), 1))
    out = dict(source)
    out["market"] = _market_name(out.get("market"), fallback=market)
    out["schema_version"] = int(SCAN_DIAGNOSTICS_SCHEMA_VERSION)
    if in_version != SCAN_DIAGNOSTICS_SCHEMA_VERSION:
        out["schema_compat_from"] = int(in_version)
    else:
        out.pop("schema_compat_from", None)

    out["ts"] = _as_int(out.get("ts", 0), 0)
    out["state"] = str(out.get("state", "") or "").strip().upper()
    out["msg"] = str(out.get("msg", "") or "").strip()
    out["mode"] = str(out.get("mode", "") or "").strip()
    out["market_open"] = _as_bool(out.get("market_open", False), False)

    for key in ("universe_total", "candidates_total", "scores_total", "leaders_total"):
        out[key] = max(0, _as_int(out.get(key, 0), 0))

    out["top_symbol"] = str(out.get("top_symbol", "") or "").strip().upper()
    out["top_score"] = _as_float(out.get("top_score", 0.0), 0.0)

    reject_summary = out.get("reject_summary", {}) if isinstance(out.get("reject_summary", {}), dict) else {}
    counts = reject_summary.get("counts", {}) if isinstance(reject_summary.get("counts", {}), dict) else {}
    clean_counts: Dict[str, int] = {}
    for key, value in counts.items():
        reason = str(key or "").strip().lower()
        if not reason:
            continue
        clean_counts[reason] = max(0, _as_int(value, 0))
    reject_summary = dict(reject_summary)
    reject_summary["counts"] = clean_counts
    reject_summary["reject_rate_pct"] = max(0.0, min(100.0, _as_float(reject_summary.get("reject_rate_pct", 0.0), 0.0)))
    reject_summary["dominant_reason"] = str(reject_summary.get("dominant_reason", "") or "").strip().lower()
    reject_summary["dominant_ratio_pct"] = max(
        0.0,
        min(100.0, _as_float(reject_summary.get("dominant_ratio_pct", 0.0), 0.0)),
    )
    out["reject_summary"] = reject_summary

    out["candidate_symbols"] = _norm_ids(out.get("candidate_symbols", []) if isinstance(out.get("candidate_symbols", []), list) else [])
    out["leader_symbols"] = _norm_ids(out.get("leader_symbols", []) if isinstance(out.get("leader_symbols", []), list) else [])
    out["candidate_churn_pct"] = max(0.0, min(100.0, _as_float(out.get("candidate_churn_pct", 0.0), 0.0)))
    out["leader_churn_pct"] = max(0.0, min(100.0, _as_float(out.get("leader_churn_pct", 0.0), 0.0)))

    summary = str(out.get("quality_summary", "") or "").strip()
    if not summary and isinstance(out.get("universe_quality", {}), dict):
        summary = str((out.get("universe_quality", {}) or {}).get("summary", "") or "").strip()
    out["quality_summary"] = summary
    return out


def with_scan_schema(payload: Mapping[str, Any] | None, market: str = "") -> Dict[str, Any]:
    seeded = dict(payload) if isinstance(payload, Mapping) else {}
    seeded["schema_version"] = int(SCAN_DIAGNOSTICS_SCHEMA_VERSION)
    if market:
        seeded["market"] = _market_name(market)
    out = normalize_scan_diagnostics(seeded, market=market)
    out.pop("schema_compat_from", None)
    return out

