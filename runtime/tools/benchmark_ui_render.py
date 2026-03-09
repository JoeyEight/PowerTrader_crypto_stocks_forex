from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from typing import Any, Dict, List

if __package__ in (None, ""):
    _ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if _ROOT not in sys.path:
        sys.path.insert(0, _ROOT)


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


def _f(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _simulate_chart_rows(hub_dir: str, market: str) -> Dict[str, Any]:
    m = str(market or "stocks").strip().lower()
    mdir = os.path.join(hub_dir, m)
    thinker_name = "stock_thinker_status.json" if m == "stocks" else "forex_thinker_status.json"
    thinker = _safe_read_json(os.path.join(mdir, thinker_name))
    chart_map = thinker.get("top_chart_map", {}) if isinstance(thinker.get("top_chart_map", {}), dict) else {}
    rankings = _safe_read_jsonl(os.path.join(mdir, "scanner_rankings.jsonl"), max_lines=80)

    # Simulate preparing chart/tooltip payloads for visible symbols.
    points = 0
    overlays = 0
    last_price = 0.0
    for symbol, bars in list(chart_map.items())[:12]:
        rows = list(bars or []) if isinstance(bars, list) else []
        for row in rows[-180:]:
            if not isinstance(row, dict):
                continue
            c = _f(row.get("c", row.get("close", row.get("price", 0.0))), 0.0)
            if c <= 0.0:
                continue
            last_price = c
            points += 1
            overlays += 1 if ("trail" in row or "dca" in row or "avg" in row) else 0

    tooltip_rows = 0
    for snap in rankings[-5:]:
        top = snap.get("top", []) if isinstance(snap.get("top", []), list) else []
        tooltip_rows += int(len(top[:10]))

    return {
        "symbols": int(len(chart_map)),
        "points": int(points),
        "overlay_points": int(overlays),
        "tooltip_rows": int(tooltip_rows),
        "last_price": round(float(last_price), 6),
    }


def run_benchmark(hub_dir: str, market: str, iterations: int) -> Dict[str, Any]:
    vals: List[float] = []
    latest: Dict[str, Any] = {}
    for _ in range(max(1, int(iterations))):
        t0 = time.perf_counter()
        latest = _simulate_chart_rows(hub_dir, market)
        vals.append((time.perf_counter() - t0) * 1000.0)
    arr = sorted(vals)
    p95 = arr[int(round((len(arr) - 1) * 0.95))] if arr else 0.0
    return {
        "market": str(market),
        "iterations": int(max(1, int(iterations))),
        "avg_ms": round((sum(vals) / max(1, len(vals))), 4) if vals else 0.0,
        "median_ms": round(statistics.median(vals), 4) if vals else 0.0,
        "p95_ms": round(float(p95), 4),
        "max_ms": round((max(vals) if vals else 0.0), 4),
        "payload": latest,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Benchmark chart payload render prep cost.")
    ap.add_argument("--hub-dir", default="hub_data")
    ap.add_argument("--market", default="stocks", choices=["stocks", "forex", "both"])
    ap.add_argument("--iterations", type=int, default=200)
    args = ap.parse_args()

    hub_dir = os.path.abspath(str(args.hub_dir or "hub_data"))
    market = str(args.market or "stocks").strip().lower()
    out: Dict[str, Any] = {"ts": int(time.time()), "hub_dir": hub_dir}
    if market == "both":
        out["stocks"] = run_benchmark(hub_dir, "stocks", int(args.iterations))
        out["forex"] = run_benchmark(hub_dir, "forex", int(args.iterations))
    else:
        out[market] = run_benchmark(hub_dir, market, int(args.iterations))
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
