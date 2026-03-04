from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from broker_alpaca import AlpacaBrokerClient
from path_utils import resolve_runtime_paths

BASE_DIR, _SETTINGS_PATH, HUB_DATA_DIR, _BOOT_SETTINGS = resolve_runtime_paths(__file__, "stock_thinker")

DEFAULT_STOCK_UNIVERSE = ["AAPL", "MSFT", "NVDA", "AMZN", "META", "TSLA", "SPY", "QQQ"]
ROLLOUT_ORDER = {"legacy": 0, "scan_expanded": 1, "risk_caps": 2, "execution_v2": 3}


def _float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


def _request_json(url: str, headers: Dict[str, str], timeout: float = 10.0) -> Any:
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _rollout_at_least(settings: Dict[str, Any], stage: str) -> bool:
    cur = str(settings.get("market_rollout_stage", "legacy") or "legacy").strip().lower()
    return int(ROLLOUT_ORDER.get(cur, 0)) >= int(ROLLOUT_ORDER.get(stage, 0))


def _parse_watchlist(settings: Dict[str, Any]) -> List[str]:
    raw = str(settings.get("stock_universe_symbols", "") or "")
    out: List[str] = []
    for tok in raw.replace("\n", ",").split(","):
        s = tok.strip().upper()
        if s and s not in out:
            out.append(s)
    return out


def _cache_path(hub_dir: str) -> str:
    return os.path.join(hub_dir, "stocks", "stock_universe_cache.json")


def _load_universe_cache(hub_dir: str, ttl_s: int = 3600) -> List[str]:
    path = _cache_path(hub_dir)
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if not isinstance(payload, dict):
            return []
        ts = int(payload.get("ts", 0) or 0)
        if (int(time.time()) - ts) > int(ttl_s):
            return []
        symbols = payload.get("symbols", []) or []
        if not isinstance(symbols, list):
            return []
        out = [str(x).strip().upper() for x in symbols if str(x).strip()]
        return out
    except Exception:
        return []


def _save_universe_cache(hub_dir: str, symbols: List[str]) -> None:
    path = _cache_path(hub_dir)
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"ts": int(time.time()), "symbols": symbols}, f, indent=2)
        os.replace(tmp, path)
    except Exception:
        pass


def _fetch_bars_for_symbols(
    base_url: str,
    headers: Dict[str, str],
    symbols: List[str],
    start_iso: str,
    end_iso: str,
    feed: str,
) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}
    chunk = 50
    for i in range(0, len(symbols), chunk):
        part = symbols[i:i + chunk]
        params = {
            "symbols": ",".join(part),
            "timeframe": "1Hour",
            "limit": "96",
            "adjustment": "raw",
            "feed": feed,
            "start": start_iso,
            "end": end_iso,
        }
        url = f"{base_url}/v2/stocks/bars?{urllib.parse.urlencode(params)}"
        payload = _request_json(url, headers=headers, timeout=15.0)
        bars = payload.get("bars", {}) or {}
        if isinstance(bars, dict):
            for sym, rows in bars.items():
                key = str(sym).strip().upper()
                if not key:
                    continue
                out[key] = list(rows or [])
    return out


def _score_bars(symbol: str, bars: List[Dict[str, Any]]) -> Dict[str, Any]:
    closes = []
    for row in bars or []:
        if not isinstance(row, dict):
            continue
        close_val = _float(row.get("c", row.get("close", 0.0)), 0.0)
        if close_val > 0:
            closes.append(close_val)
    if len(closes) < 8:
        return {
            "symbol": symbol,
            "score": -9999.0,
            "side": "watch",
            "last": closes[-1] if closes else 0.0,
            "change_6h_pct": 0.0,
            "change_24h_pct": 0.0,
            "volatility_pct": 0.0,
            "confidence": "LOW",
            "reason": "Not enough bars",
        }

    last_px = closes[-1]
    px_6 = closes[max(0, len(closes) - 7)]
    px_24 = closes[max(0, len(closes) - min(24, len(closes)))]
    change_6 = ((last_px - px_6) / px_6) * 100.0 if px_6 > 0 else 0.0
    change_24 = ((last_px - px_24) / px_24) * 100.0 if px_24 > 0 else 0.0

    step_moves = []
    for idx in range(1, len(closes)):
        prev_px = closes[idx - 1]
        cur_px = closes[idx]
        if prev_px > 0:
            step_moves.append(abs(((cur_px - prev_px) / prev_px) * 100.0))
    volatility = (sum(step_moves[-12:]) / max(1, len(step_moves[-12:]))) if step_moves else 0.0

    score = (change_6 * 0.65) + (change_24 * 0.25) + (volatility * 0.10)
    side = "long" if score > 0 else "watch"
    abs_score = abs(score)
    if abs_score >= 4.0:
        confidence = "HIGH"
    elif abs_score >= 1.75:
        confidence = "MED"
    else:
        confidence = "LOW"

    reason = f"6h {change_6:+.2f}% | 24h {change_24:+.2f}% | vol {volatility:.2f}%"
    return {
        "symbol": symbol,
        "score": round(score, 4),
        "side": side,
        "last": round(last_px, 6),
        "change_6h_pct": round(change_6, 4),
        "change_24h_pct": round(change_24, 4),
        "volatility_pct": round(volatility, 4),
        "confidence": confidence,
        "reason": reason,
    }


def _select_universe(settings: Dict[str, Any], hub_dir: str, api_key: str, secret: str) -> List[str]:
    mode = str(settings.get("stock_universe_mode", "core") or "core").strip().lower()
    if mode == "watchlist":
        watch = _parse_watchlist(settings)
        return watch if watch else list(DEFAULT_STOCK_UNIVERSE)
    if mode != "all_tradable_filtered":
        return list(DEFAULT_STOCK_UNIVERSE)
    if not _rollout_at_least(settings, "scan_expanded"):
        return list(DEFAULT_STOCK_UNIVERSE)

    cached = _load_universe_cache(hub_dir, ttl_s=3600)
    if cached:
        return cached

    client = AlpacaBrokerClient(
        api_key_id=api_key,
        secret_key=secret,
        base_url=str(settings.get("alpaca_base_url", "https://paper-api.alpaca.markets") or ""),
        data_url=str(settings.get("alpaca_data_url", "https://data.alpaca.markets") or ""),
    )
    assets = client.list_tradable_assets()
    symbols: List[str] = []
    for row in assets:
        if not isinstance(row, dict):
            continue
        if not bool(row.get("tradable", False)):
            continue
        if str(row.get("status", "")).lower() != "active":
            continue
        if str(row.get("class", "")).lower() not in ("us_equity", "us_equities", ""):
            continue
        sym = str(row.get("symbol", "") or "").strip().upper()
        if sym and sym not in symbols:
            symbols.append(sym)
    if not symbols:
        return list(DEFAULT_STOCK_UNIVERSE)
    _save_universe_cache(hub_dir, symbols)
    return symbols


def run_scan(settings: Dict[str, Any], hub_dir: str) -> Dict[str, Any]:
    api_key = str(settings.get("alpaca_api_key_id", "") or "").strip()
    secret = str(settings.get("alpaca_secret_key", "") or "").strip()
    base_url = str(settings.get("alpaca_data_url", settings.get("alpaca_base_url", "https://data.alpaca.markets")) or "").strip().rstrip("/")

    if not api_key or not secret:
        return {
            "state": "NOT CONFIGURED",
            "ai_state": "Credentials missing",
            "msg": "Add Alpaca keys in Settings",
            "universe": list(DEFAULT_STOCK_UNIVERSE),
            "leaders": [],
            "updated_at": int(time.time()),
        }

    headers = {
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": secret,
    }

    now_utc = datetime.now(timezone.utc)
    start_utc = now_utc - timedelta(days=10)
    start_iso = start_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    end_iso = now_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")

    universe = _select_universe(settings, hub_dir, api_key, secret)
    max_scan = max(8, int(float(settings.get("stock_scan_max_symbols", 60) or 60)))
    if len(universe) > max_scan and _rollout_at_least(settings, "scan_expanded"):
        # Pre-filter by snapshot price/liquidity before bars request.
        try:
            client = AlpacaBrokerClient(
                api_key_id=api_key,
                secret_key=secret,
                base_url=str(settings.get("alpaca_base_url", "https://paper-api.alpaca.markets") or ""),
                data_url=base_url,
            )
            chunk = 100
            min_price = max(0.0, float(settings.get("stock_min_price", 5.0) or 5.0))
            max_price = max(min_price, float(settings.get("stock_max_price", 500.0) or 500.0))
            min_dollar_vol = max(0.0, float(settings.get("stock_min_dollar_volume", 5_000_000.0) or 5_000_000.0))
            ranked: List[tuple[float, str]] = []
            for i in range(0, len(universe), chunk):
                part = universe[i:i + chunk]
                snaps = client.get_mid_prices(part)
                for sym in part:
                    px = float(snaps.get(sym, 0.0) or 0.0)
                    if px <= 0.0 or px < min_price or px > max_price:
                        continue
                    ranked.append((px, sym))
            ranked.sort(key=lambda x: x[0], reverse=True)
            universe = [sym for _, sym in ranked[:max_scan]] or universe[:max_scan]
            _ = min_dollar_vol  # reserved for stricter filters later; kept for config compatibility
        except Exception:
            universe = universe[:max_scan]
    else:
        universe = universe[:max_scan]

    scored: List[Dict[str, Any]] = []
    last_exc: Exception | None = None
    universe_used = list(universe)

    # Try IEX first (paper-friendly), then SIP fallback if account supports it.
    for feed in ("iex", "sip"):
        try:
            bars_by_symbol = _fetch_bars_for_symbols(base_url, headers, universe_used, start_iso, end_iso, feed)
            scored = []
            for symbol in universe_used:
                scored.append(_score_bars(symbol, list(bars_by_symbol.get(symbol, []) or [])))
            if any(float(row.get("score", -9999.0)) > -9999.0 for row in scored):
                break
        except Exception as exc:
            last_exc = exc
            scored = []

    if not scored:
        if isinstance(last_exc, urllib.error.HTTPError):
            return {
                "state": "ERROR",
                "ai_state": "HTTP error",
                "msg": f"HTTP {last_exc.code}: {last_exc.reason}",
                "universe": universe_used,
                "leaders": [],
                "updated_at": int(time.time()),
            }
        if isinstance(last_exc, urllib.error.URLError):
            return {
                "state": "ERROR",
                "ai_state": "Network error",
                "msg": f"Network error: {last_exc.reason}",
                "universe": universe_used,
                "leaders": [],
                "updated_at": int(time.time()),
            }
        return {
            "state": "ERROR",
            "ai_state": "Scan failed",
            "msg": (f"{type(last_exc).__name__}: {last_exc}" if last_exc else "No bar data returned"),
            "universe": universe_used,
            "leaders": [],
            "updated_at": int(time.time()),
        }

    scored.sort(key=lambda row: float(row.get("score", -9999.0)), reverse=True)
    leaders = [row for row in scored if str(row.get("side", "")).lower() == "long"][:5]
    top_pick = leaders[0] if leaders else (scored[0] if scored else None)
    msg = "No viable long candidates"
    if top_pick:
        msg = f"Top pick {top_pick['symbol']} | {top_pick['reason']}"
    return {
        "state": "READY",
        "ai_state": "Scan ready",
        "msg": msg,
        "universe": universe_used,
        "leaders": leaders,
        "all_scores": scored[:12],
        "top_pick": top_pick,
        "updated_at": int(time.time()),
        "pdt_note": "Paper mode can still simulate PDT protections; live day-trading may be limited under $25k.",
    }


def main() -> int:
    print("stock_thinker.py is designed to be imported by the hub/runner first.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
