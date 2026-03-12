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


def _collect_account_values(hub_dir: str) -> Dict[str, float]:
    trader_data = _safe_read_json(os.path.join(hub_dir, "trader_data.json"))
    crypto_account = trader_data.get("account", {}) if isinstance(trader_data.get("account", {}), dict) else {}

    stocks_trader = _safe_read_json(os.path.join(hub_dir, "stocks", "stock_trader_status.json"))
    stocks_status = _safe_read_json(os.path.join(hub_dir, "stocks", "alpaca_status.json"))

    forex_trader = _safe_read_json(os.path.join(hub_dir, "forex", "forex_trader_status.json"))
    forex_status = _safe_read_json(os.path.join(hub_dir, "forex", "oanda_status.json"))

    account_values = {
        "crypto": max(
            0.0,
            _f(
                crypto_account.get(
                    "total_account_value",
                    trader_data.get("account_value_usd", 0.0),
                ),
                0.0,
            ),
        ),
        "stocks": max(
            0.0,
            _f(
                stocks_trader.get(
                    "account_value_usd",
                    stocks_status.get("equity", 0.0),
                ),
                0.0,
            ),
        ),
        "forex": max(
            0.0,
            _f(
                forex_trader.get(
                    "account_value_usd",
                    forex_status.get("nav", 0.0),
                ),
                0.0,
            ),
        ),
    }
    return {k: round(float(v), 6) for k, v in account_values.items()}


def _quote_currency(symbol: str) -> str:
    s = str(symbol or "").strip().upper()
    if "_" in s:
        parts = s.split("_")
        if len(parts) == 2 and parts[1]:
            return parts[1]
    if "-" in s:
        parts = s.split("-")
        if len(parts) == 2 and parts[1]:
            return parts[1]
    if "/" in s:
        parts = s.split("/")
        if len(parts) == 2 and parts[1]:
            return parts[1]
    # Stocks are effectively USD-quoted for this app's brokers.
    if s.isalpha() and (2 <= len(s) <= 6):
        return "USD"
    return ""


def _cross_market_warnings(positions: List[Dict[str, Any]], total: float, by_market: Dict[str, float]) -> List[Dict[str, Any]]:
    if total <= 0.0:
        return []
    warnings: List[Dict[str, Any]] = []
    by_quote: Dict[str, float] = {}
    for row in positions:
        sym = str(row.get("symbol", "") or "")
        q = _quote_currency(sym)
        if not q:
            continue
        by_quote[q] = float(by_quote.get(q, 0.0)) + max(0.0, _f(row.get("value_usd", 0.0), 0.0))

    usd_quote = float(by_quote.get("USD", 0.0))
    usd_quote_pct = (100.0 * usd_quote / total) if total > 0.0 else 0.0
    if usd_quote_pct >= 85.0:
        warnings.append(
            {
                "id": "usd_quote_concentration",
                "severity": "warning",
                "msg": f"USD-quoted exposure is concentrated ({usd_quote_pct:.1f}% of total).",
                "metric": round(usd_quote_pct, 4),
            }
        )

    stock_pct = (100.0 * float(by_market.get("stocks", 0.0)) / total) if total > 0.0 else 0.0
    forex_pct = (100.0 * float(by_market.get("forex", 0.0)) / total) if total > 0.0 else 0.0
    if (stock_pct + forex_pct) >= 90.0:
        warnings.append(
            {
                "id": "usd_macro_correlation",
                "severity": "warning",
                "msg": "Stocks + Forex dominate exposure; portfolio can be highly sensitive to USD macro regime shifts.",
                "metric": round(stock_pct + forex_pct, 4),
            }
        )

    return warnings[:6]


def build_exposure_payload(hub_dir: str) -> Dict[str, Any]:
    positions = _collect_crypto_positions(hub_dir) + _collect_stock_positions(hub_dir) + _collect_forex_positions(hub_dir)
    account_values = _collect_account_values(hub_dir)

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
        market = str(row.get("market", "") or "").strip().lower()
        market_account_value = max(0.0, _f(account_values.get(market, 0.0), 0.0))
        pct_of_market_account = (100.0 * v / market_account_value) if market_account_value > 0.0 else 0.0
        heatmap.append(
            {
                "market": str(row.get("market", "") or ""),
                "symbol": str(row.get("symbol", "") or ""),
                "value_usd": round(v, 6),
                "pct_of_total_exposure": round(pct, 4),
                "pct_of_market_account": round(pct_of_market_account, 4),
            }
        )

    market_pct = {
        k: round(((100.0 * float(v) / total) if total > 0.0 else 0.0), 4)
        for k, v in by_market.items()
    }
    correlation_warnings = _cross_market_warnings(positions_sorted, total, by_market)
    return {
        "ts": int(time.time()),
        "total_exposure_usd": round(total, 6),
        "account_value_by_market_usd": {k: round(float(v), 6) for k, v in account_values.items()},
        "total_account_value_usd": round(sum(float(v) for v in account_values.values()), 6),
        "by_market_usd": {k: round(float(v), 6) for k, v in by_market.items()},
        "by_market_pct": market_pct,
        "top_positions": heatmap,
        "position_count": int(len(positions)),
        "correlation_warnings": correlation_warnings,
    }
