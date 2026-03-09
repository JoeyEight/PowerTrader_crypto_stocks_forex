from __future__ import annotations

import argparse
import json
import os
import time
from typing import Any, Dict, List

try:
    from app.path_utils import resolve_runtime_paths
except ModuleNotFoundError:  # pragma: no cover - script bootstrap path
    import sys

    _ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if _ROOT not in sys.path:
        sys.path.insert(0, _ROOT)
    from app.path_utils import resolve_runtime_paths


def _safe_json(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _safe_jsonl(path: str, max_lines: int = 4000) -> List[Dict[str, Any]]:
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


def _safe_lines(path: str, max_lines: int = 4000) -> List[str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            rows = [str(ln or "").rstrip("\n") for ln in f]
    except Exception:
        return []
    if not rows:
        return []
    return rows[-max(1, int(max_lines)) :]


def _normalize_log_line(line: str) -> str:
    txt = str(line or "").strip()
    if not txt:
        return ""
    if len(txt) >= 20 and txt[4] == "-" and txt[7] == "-" and txt[10] == " " and txt[13] == ":" and txt[16] == ":":
        txt = txt[19:].strip()
    if " pid=" in txt:
        txt = txt.split(" pid=", 1)[0].strip() + " pid=*"
    if "order_id=" in txt:
        left = txt.split("order_id=", 1)[0].strip()
        txt = f"{left} order_id=*"
    return txt


def _log_spam_metrics(hub_dir: str) -> Dict[str, Any]:
    logs_dir = os.path.join(hub_dir, "logs")
    files = ["thinker.log", "markets.log", "trader.log", "runner.log", "autopilot.log", "autofix.log"]
    counts: Dict[str, int] = {}
    total = 0
    for name in files:
        path = os.path.join(logs_dir, name)
        for raw in _safe_lines(path, max_lines=2400):
            norm = _normalize_log_line(raw)
            if not norm:
                continue
            total += 1
            counts[norm] = int(counts.get(norm, 0)) + 1
    if not counts or total <= 0:
        return {"level": "ok", "total_lines": int(total), "top_repeat_count": 0, "top_repeat_ratio": 0.0, "top_repeat_line": ""}
    top_line, top_count = max(counts.items(), key=lambda item: int(item[1]))
    ratio = float(top_count) / float(max(1, total))
    level = "ok"
    if top_count >= 200 and ratio >= 0.25:
        level = "critical"
    elif top_count >= 80 and ratio >= 0.15:
        level = "warning"
    return {
        "level": level,
        "total_lines": int(total),
        "distinct_lines": int(len(counts)),
        "top_repeat_count": int(top_count),
        "top_repeat_ratio": round(float(ratio), 4),
        "top_repeat_line": str(top_line)[:180],
    }


def _market_status(hub_dir: str, market: str) -> Dict[str, Any]:
    prefix = "stock" if str(market or "").strip().lower() == "stocks" else "forex"
    thinker_path = os.path.join(hub_dir, market, f"{prefix}_thinker_status.json")
    trader_path = os.path.join(hub_dir, market, f"{prefix}_trader_status.json")
    diag_path = os.path.join(hub_dir, market, "scan_diagnostics.json")
    thinker = _safe_json(thinker_path)
    trader = _safe_json(trader_path)
    diag = _safe_json(diag_path)
    health = thinker.get("health", {}) if isinstance(thinker.get("health", {}), dict) else {}
    if not health and isinstance(trader.get("health", {}), dict):
        health = trader.get("health", {}) or {}
    chart_map = thinker.get("top_chart_map", {}) if isinstance(thinker.get("top_chart_map", {}), dict) else {}
    fallback_cached = bool(thinker.get("fallback_cached", False))
    fallback_age_s = int(float(thinker.get("fallback_age_s", 0) or 0)) if fallback_cached else 0
    level = "ok"
    if str(diag.get("state", "") or "").strip().upper() == "ERROR":
        level = "critical"
    elif fallback_cached and fallback_age_s >= 1800:
        level = "critical"
    elif (not bool(health.get("data_ok", True))) or fallback_cached:
        level = "warning"
    return {
        "market": market,
        "level": level,
        "scan_state": str(diag.get("state", "") or ""),
        "scan_msg": str(diag.get("msg", "") or "")[:180],
        "leaders_total": int(diag.get("leaders_total", 0) or 0),
        "scores_total": int(diag.get("scores_total", 0) or 0),
        "fallback_cached": fallback_cached,
        "fallback_age_s": int(fallback_age_s),
        "leader_mode": str(thinker.get("leader_mode", "") or ""),
        "data_ok": bool(health.get("data_ok", True)),
        "chart_cache_symbols": int(len([k for k, v in chart_map.items() if str(k).strip() and isinstance(v, list)])),
        "trader_state": str(trader.get("state", "") or ""),
        "trader_msg": str(trader.get("msg", "") or "")[:180],
    }


def build_stability_report(hub_dir: str, max_error_incidents_24h: int = 15) -> Dict[str, Any]:
    now_ts = int(time.time())
    runtime_state = _safe_json(os.path.join(hub_dir, "runtime_state.json"))
    market_trends = _safe_json(os.path.join(hub_dir, "market_trends.json"))
    incidents = _safe_jsonl(os.path.join(hub_dir, "incidents.jsonl"), max_lines=3000)
    cutoff = now_ts - 86400
    incidents_24h = [row for row in incidents if int(row.get("ts", 0) or 0) >= cutoff]

    by_sev: Dict[str, int] = {}
    for row in incidents_24h:
        sev = str(row.get("severity", "info") or "info").strip().lower()
        by_sev[sev] = int(by_sev.get(sev, 0)) + 1
    error_count_24h = int(by_sev.get("error", 0))
    log_health = _log_spam_metrics(hub_dir)

    stocks = _market_status(hub_dir, "stocks")
    forex = _market_status(hub_dir, "forex")
    markets = [stocks, forex]
    critical_markets = [m["market"] for m in markets if str(m.get("level", "")) == "critical"]
    warning_markets = [m["market"] for m in markets if str(m.get("level", "")) == "warning"]

    alert = runtime_state.get("alerts", {}) if isinstance(runtime_state.get("alerts", {}), dict) else {}
    checks_ok = bool(runtime_state.get("checks_ok", False))
    severity = str(alert.get("severity", "info") or "info").strip().lower()
    pass_gate = bool(
        checks_ok
        and severity not in {"critical"}
        and (not critical_markets)
        and str(log_health.get("level", "ok")) != "critical"
        and error_count_24h <= int(max_error_incidents_24h)
    )

    return {
        "ts": int(now_ts),
        "hub_dir": str(hub_dir),
        "pass": pass_gate,
        "limits": {"max_error_incidents_24h": int(max_error_incidents_24h)},
        "runtime": {
            "checks_ok": checks_ok,
            "alert_severity": severity,
            "scan_stocks_state": str(runtime_state.get("scan_stocks_state", "") or ""),
            "scan_forex_state": str(runtime_state.get("scan_forex_state", "") or ""),
            "execution_guard_active_markets": int(runtime_state.get("execution_guard_active_markets", 0) or 0),
        },
        "incidents_24h": {
            "count": int(len(incidents_24h)),
            "error_count": int(error_count_24h),
            "by_severity": by_sev,
        },
        "logs": dict(log_health),
        "markets": {
            "stocks": stocks,
            "forex": forex,
            "critical": critical_markets,
            "warning": warning_markets,
        },
        "trends": {
            "stocks_divergence_24h": int((market_trends.get("stocks", {}) if isinstance(market_trends.get("stocks", {}), dict) else {}).get("divergence_24h", 0) or 0),
            "forex_divergence_24h": int((market_trends.get("forex", {}) if isinstance(market_trends.get("forex", {}), dict) else {}).get("divergence_24h", 0) or 0),
        },
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Summarize runtime stability posture for Stocks/Forex.")
    ap.add_argument("--hub-dir", default="", help="Optional explicit hub_data path.")
    ap.add_argument("--strict", action="store_true", help="Return non-zero if stability pass gate fails.")
    ap.add_argument("--max-error-incidents-24h", type=int, default=15)
    ap.add_argument("--write", action="store_true", help="Write report under hub_data/exports/diagnostics.")
    args = ap.parse_args()

    probe_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "pt_runner.py")
    _base, _settings, auto_hub_dir, _ = resolve_runtime_paths(probe_file, "stability_audit")
    hub_dir = os.path.abspath(str(args.hub_dir or auto_hub_dir))
    report = build_stability_report(hub_dir, max_error_incidents_24h=max(1, int(args.max_error_incidents_24h or 15)))

    if bool(args.write):
        out_dir = os.path.join(hub_dir, "exports", "diagnostics")
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, f"stability_audit_{time.strftime('%Y%m%d_%H%M%S')}.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        report["report_path"] = out_path

    print(json.dumps(report, indent=2))
    if bool(args.strict) and (not bool(report.get("pass", False))):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
