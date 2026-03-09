from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, Iterable, List


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
    if "autofix" in evt:
        return "ai_assist"
    return "global"


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
        reject = float(quality.get("reject_rate_pct", 0.0) or 0.0)
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

    out_rows = sorted(out_rows, key=lambda r: (int(r.get("ts", 0) or 0), str(r.get("severity", ""))), reverse=True)
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
    incidents = _safe_read_jsonl(os.path.join(hub_dir, "incidents.jsonl"), max_lines=500)
    return build_notification_center_payload(rs, incidents_rows=incidents, max_items=220)
