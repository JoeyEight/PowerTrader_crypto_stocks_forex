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

from app.market_trends import build_market_trend_summary


def _percentile(values: List[float], q: float) -> float:
    if not values:
        return 0.0
    arr = sorted(float(v) for v in values)
    idx = int(round((len(arr) - 1) * max(0.0, min(1.0, float(q)))))
    idx = max(0, min(idx, len(arr) - 1))
    return float(arr[idx])


def run_benchmark(hub_dir: str, market: str, iterations: int) -> Dict[str, Any]:
    vals: List[float] = []
    rows: List[Dict[str, Any]] = []
    iters = max(1, int(iterations))
    for _ in range(iters):
        t0 = time.perf_counter()
        row = build_market_trend_summary(hub_dir, market)
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        vals.append(float(elapsed_ms))
        rows.append(row if isinstance(row, dict) else {})
    sample = rows[-1] if rows else {}
    quality = sample.get("quality_aggregates", {}) if isinstance(sample.get("quality_aggregates", {}), dict) else {}
    cadence = sample.get("cadence_aggregates", {}) if isinstance(sample.get("cadence_aggregates", {}), dict) else {}
    return {
        "market": str(market),
        "iterations": int(iters),
        "avg_ms": round((sum(vals) / max(1, len(vals))), 4) if vals else 0.0,
        "median_ms": round(statistics.median(vals), 4) if vals else 0.0,
        "p95_ms": round(_percentile(vals, 0.95), 4),
        "max_ms": round((max(vals) if vals else 0.0), 4),
        "sample": {
            "divergence_24h": int(sample.get("divergence_24h", 0) or 0),
            "quality_reject_rate_pct": round(_safe_float(quality.get("reject_rate_pct", 0.0)), 4),
            "cadence_level": str(cadence.get("level", "") or ""),
        },
    }


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def main() -> int:
    ap = argparse.ArgumentParser(description="Benchmark scanner trend-summary throughput.")
    ap.add_argument("--hub-dir", default="hub_data")
    ap.add_argument("--market", default="stocks", choices=["stocks", "forex"])
    ap.add_argument("--iterations", type=int, default=40)
    args = ap.parse_args()

    hub_dir = os.path.abspath(str(args.hub_dir or "hub_data"))
    out = {
        "ts": int(time.time()),
        "hub_dir": hub_dir,
        "result": run_benchmark(hub_dir, str(args.market), int(args.iterations)),
    }
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
