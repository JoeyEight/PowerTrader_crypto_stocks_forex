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


def _collect_crypto_positions(hub_dir: str) -> List[Dict[str, Any]]:
    data = _safe_read_json(os.path.join(hub_dir, "trader_data.json"))
    pos = data.get("positions", {}) if isinstance(data.get("positions", {}), dict) else {}
    out: List[Dict[str, Any]] = []
    for coin, row in pos.items():
        if not isinstance(row, dict):
            continue
        val = _f(row.get("value_usd", 0.0), 0.0)
        if val <= 0.0:
            continue
        out.append(
            {
                "market": "crypto",
                "symbol": str(coin or "").strip().upper(),
                "value_usd": round(val, 6),
                "qty": _f(row.get("quantity", 0.0), 0.0),
            }
        )
    return out


def _collect_stock_positions(hub_dir: str) -> List[Dict[str, Any]]:
    data = _safe_read_json(os.path.join(hub_dir, "stocks", "alpaca_status.json"))
    rows = data.get("raw_positions", []) if isinstance(data.get("raw_positions", []), list) else []
    out: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        sym = str(row.get("symbol", "") or "").strip().upper()
        if not sym:
            continue
        val = max(0.0, _f(row.get("market_value", 0.0), 0.0))
        if val <= 0.0:
            continue
        out.append(
            {
                "market": "stocks",
                "symbol": sym,
                "value_usd": round(val, 6),
                "qty": _f(row.get("qty", 0.0), 0.0),
            }
        )
    return out


def _collect_forex_positions(hub_dir: str) -> List[Dict[str, Any]]:
    data = _safe_read_json(os.path.join(hub_dir, "forex", "oanda_status.json"))
    rows = data.get("raw_positions", []) if isinstance(data.get("raw_positions", []), list) else []
    out: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        inst = str(row.get("instrument", "") or "").strip().upper()
        if not inst:
            continue
        margin = max(0.0, _f(row.get("marginUsed", 0.0), 0.0))
        units = abs(_f((row.get("long") or {}).get("units", 0.0), 0.0)) + abs(_f((row.get("short") or {}).get("units", 0.0), 0.0))
        if margin <= 0.0 and units <= 0.0:
            continue
        out.append(
            {
                "market": "forex",
                "symbol": inst,
                "value_usd": round(margin, 6),
                "units": round(units, 6),
            }
        )
    return out


def build_exposure_payload(hub_dir: str) -> Dict[str, Any]:
    positions = _collect_crypto_positions(hub_dir) + _collect_stock_positions(hub_dir) + _collect_forex_positions(hub_dir)

    by_market: Dict[str, float] = {"crypto": 0.0, "stocks": 0.0, "forex": 0.0}
    for row in positions:
        m = str(row.get("market", "") or "").strip().lower()
        if m not in by_market:
            by_market[m] = 0.0
        by_market[m] = by_market[m] + max(0.0, _f(row.get("value_usd", 0.0), 0.0))

    total = sum(float(v) for v in by_market.values())
    positions_sorted = sorted(positions, key=lambda r: float(r.get("value_usd", 0.0) or 0.0), reverse=True)
    top = positions_sorted[:12]
    heatmap = []
    for row in top:
        v = max(0.0, _f(row.get("value_usd", 0.0), 0.0))
        pct = (100.0 * v / total) if total > 0.0 else 0.0
        heatmap.append(
            {
                "market": str(row.get("market", "") or ""),
                "symbol": str(row.get("symbol", "") or ""),
                "value_usd": round(v, 6),
                "pct_of_total_exposure": round(pct, 4),
            }
        )

    market_pct = {
        k: round(((100.0 * float(v) / total) if total > 0.0 else 0.0), 4)
        for k, v in by_market.items()
    }
    return {
        "ts": int(time.time()),
        "total_exposure_usd": round(total, 6),
        "by_market_usd": {k: round(float(v), 6) for k, v in by_market.items()},
        "by_market_pct": market_pct,
        "top_positions": heatmap,
        "position_count": int(len(positions)),
    }
