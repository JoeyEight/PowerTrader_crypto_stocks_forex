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


def _safe_read_jsonl(path: str, max_lines: int = 800) -> List[Dict[str, Any]]:
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


def _extract_symbol(row: Dict[str, Any], market: str) -> str:
    if market == "stocks":
        return str(row.get("symbol", "") or "").strip().upper()
    return str(row.get("pair", row.get("instrument", "")) or "").strip().upper()


def _series_from_bars(rows: Iterable[Dict[str, Any]]) -> List[float]:
    out: List[float] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        v = 0.0
        for key in ("c", "close", "price", "last"):
            if key in row:
                v = _f(row.get(key), 0.0)
                if v > 0.0:
                    break
        if v > 0.0:
            out.append(float(v))
    return out


def classify_regime_from_series(series: List[float]) -> Dict[str, Any]:
    vals = [float(v) for v in list(series or []) if float(v) > 0.0]
    if len(vals) < 8:
        return {
            "regime": "unknown",
            "confidence": "LOW",
            "samples": int(len(vals)),
            "change_pct": 0.0,
            "volatility_pct": 0.0,
            "range_pct": 0.0,
            "trend_slope_pct_per_bar": 0.0,
        }

    first = float(vals[0])
    last = float(vals[-1])
    change_pct = ((last - first) / max(1e-9, first)) * 100.0
    deltas: List[float] = []
    for i in range(1, len(vals)):
        prev = float(vals[i - 1])
        cur = float(vals[i])
        deltas.append(((cur - prev) / max(1e-9, prev)) * 100.0)
    abs_deltas = [abs(x) for x in deltas]
    vol_pct = (sum(abs_deltas) / max(1, len(abs_deltas))) if abs_deltas else 0.0
    max_v = max(vals)
    min_v = min(vals)
    range_pct = ((max_v - min_v) / max(1e-9, last)) * 100.0
    slope_pct = change_pct / max(1.0, float(len(vals) - 1))

    trend_signal = (abs(change_pct) >= 0.90 and abs(slope_pct) >= 0.025)
    regime = "range"
    if trend_signal and vol_pct <= 1.20:
        regime = "trend_up" if change_pct > 0 else "trend_down"
    elif vol_pct >= 1.20 or (vol_pct >= 0.60 and range_pct >= 4.50):
        regime = "high_volatility"
    elif vol_pct <= 0.14 and range_pct <= 0.90:
        regime = "low_volatility_range"

    conf_score = 0
    if len(vals) >= 32:
        conf_score += 1
    if abs(change_pct) >= 1.2:
        conf_score += 1
    if vol_pct >= 0.25:
        conf_score += 1
    if range_pct >= 1.5:
        conf_score += 1
    confidence = "LOW"
    if conf_score >= 3:
        confidence = "HIGH"
    elif conf_score >= 2:
        confidence = "MED"

    return {
        "regime": regime,
        "confidence": confidence,
        "samples": int(len(vals)),
        "change_pct": round(float(change_pct), 6),
        "volatility_pct": round(float(vol_pct), 6),
        "range_pct": round(float(range_pct), 6),
        "trend_slope_pct_per_bar": round(float(slope_pct), 6),
    }


def _pick_focus_symbol(thinker: Dict[str, Any], market: str) -> str:
    top_pick = thinker.get("top_pick", {}) if isinstance(thinker.get("top_pick", {}), dict) else {}
    if market == "stocks":
        sym = str(top_pick.get("symbol", "") or "").strip().upper()
    else:
        sym = str(top_pick.get("pair", "") or "").strip().upper()
    if sym:
        return sym
    top = thinker.get("top", []) if isinstance(thinker.get("top", []), list) else []
    for row in top:
        if not isinstance(row, dict):
            continue
        sym = _extract_symbol(row, market)
        if sym:
            return sym
    chart_map = thinker.get("top_chart_map", {}) if isinstance(thinker.get("top_chart_map", {}), dict) else {}
    for key in chart_map.keys():
        sym = str(key or "").strip().upper()
        if sym:
            return sym
    return ""


def build_market_regime_payload(hub_dir: str, market: str) -> Dict[str, Any]:
    m = str(market or "").strip().lower()
    if m not in {"stocks", "forex"}:
        return {"market": m, "state": "ERROR", "msg": "unsupported market"}

    mdir = os.path.join(hub_dir, m)
    thinker_name = "stock_thinker_status.json" if m == "stocks" else "forex_thinker_status.json"
    thinker = _safe_read_json(os.path.join(mdir, thinker_name))
    rankings = _safe_read_jsonl(os.path.join(mdir, "scanner_rankings.jsonl"), max_lines=120)
    chart_map = thinker.get("top_chart_map", {}) if isinstance(thinker.get("top_chart_map", {}), dict) else {}

    by_symbol: List[Dict[str, Any]] = []
    for symbol, bars_raw in list(chart_map.items())[:24]:
        bars = list(bars_raw or []) if isinstance(bars_raw, list) else []
        series = _series_from_bars(bars)
        regime = classify_regime_from_series(series)
        by_symbol.append(
            {
                "symbol": str(symbol or "").strip().upper(),
                "bars": int(len(series)),
                **regime,
            }
        )

    if not by_symbol:
        for row in rankings[-1:]:
            top = row.get("top", []) if isinstance(row.get("top", []), list) else []
            for cand in top[:8]:
                if not isinstance(cand, dict):
                    continue
                sym = _extract_symbol(cand, m)
                if not sym:
                    continue
                regime_name = "trend_down" if _f(cand.get("score", 0.0), 0.0) < 0.0 else "trend_up"
                by_symbol.append(
                    {
                        "symbol": sym,
                        "bars": int(cand.get("bars_count", 0) or 0),
                        "regime": regime_name,
                        "confidence": str(cand.get("confidence", "LOW") or "LOW"),
                        "change_pct": round(_f(cand.get("change_24h_pct", 0.0), 0.0), 6),
                        "volatility_pct": round(_f(cand.get("volatility_pct", 0.0), 0.0), 6),
                        "range_pct": 0.0,
                        "trend_slope_pct_per_bar": 0.0,
                        "samples": int(cand.get("bars_count", 0) or 0),
                    }
                )

    focus = _pick_focus_symbol(thinker, m)
    focus_row = {}
    for row in by_symbol:
        if str(row.get("symbol", "") or "") == focus and focus:
            focus_row = row
            break
    if not focus_row and by_symbol:
        focus_row = by_symbol[0]
        focus = str(focus_row.get("symbol", "") or "")

    counts: Dict[str, int] = {}
    for row in by_symbol:
        reg = str(row.get("regime", "unknown") or "unknown").strip().lower()
        counts[reg] = int(counts.get(reg, 0)) + 1
    dominant = "unknown"
    if counts:
        dominant = sorted(counts.items(), key=lambda it: (it[1], it[0]), reverse=True)[0][0]

    return {
        "ts": int(time.time()),
        "market": m,
        "focus_symbol": focus,
        "focus": focus_row,
        "dominant_regime": dominant,
        "regime_counts": counts,
        "symbols": by_symbol[:12],
        "samples": int(len(by_symbol)),
    }


def build_all_market_regimes(hub_dir: str) -> Dict[str, Any]:
    return {
        "ts": int(time.time()),
        "stocks": build_market_regime_payload(hub_dir, "stocks"),
        "forex": build_market_regime_payload(hub_dir, "forex"),
    }
