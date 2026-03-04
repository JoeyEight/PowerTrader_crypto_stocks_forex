from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple
from zoneinfo import ZoneInfo

from broker_alpaca import AlpacaBrokerClient
from path_utils import resolve_runtime_paths

BASE_DIR, _SETTINGS_PATH, HUB_DATA_DIR, _BOOT_SETTINGS = resolve_runtime_paths(__file__, "stock_thinker")

DEFAULT_STOCK_UNIVERSE = ["AAPL", "MSFT", "NVDA", "AMZN", "META", "TSLA", "SPY", "QQQ"]
ROLLOUT_ORDER = {
    "legacy": 0,
    "scan_expanded": 1,
    "risk_caps": 2,
    "execution_v2": 3,
    "shadow_only": 4,
    "live_guarded": 5,
}


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


def _now_et() -> datetime:
    return datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))


def _market_open_now() -> bool:
    now = _now_et()
    if now.weekday() >= 5:
        return False
    mins = (now.hour * 60) + now.minute
    return (9 * 60 + 30) <= mins < (16 * 60)


def _cache_path(hub_dir: str) -> str:
    return os.path.join(hub_dir, "stocks", "stock_universe_cache.json")


def _rankings_path(hub_dir: str) -> str:
    return os.path.join(hub_dir, "stocks", "scanner_rankings.jsonl")


def _execution_audit_path(hub_dir: str) -> str:
    return os.path.join(hub_dir, "stocks", "execution_audit.jsonl")


def _warmup_queue_path(hub_dir: str) -> str:
    return os.path.join(hub_dir, "stocks", "warmup_queue.json")


def _rotate_jsonl(path: str, max_bytes: int = 25 * 1024 * 1024, keep: int = 10) -> None:
    try:
        if not os.path.isfile(path):
            return
        if os.path.getsize(path) <= int(max_bytes):
            return
        ts = time.strftime("%Y%m%d_%H%M%S", time.localtime())
        rotated = f"{path}.{ts}"
        os.replace(path, rotated)
        prefix = os.path.basename(path) + "."
        base_dir = os.path.dirname(path)
        olds = sorted([os.path.join(base_dir, n) for n in os.listdir(base_dir) if n.startswith(prefix)])
        if len(olds) > int(keep):
            for old in olds[:-keep]:
                try:
                    os.remove(old)
                except Exception:
                    pass
    except Exception:
        pass


def _append_jsonl(path: str, row: Dict[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        _rotate_jsonl(path)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, separators=(",", ":")) + "\n")
    except Exception:
        pass


def _load_universe_cache(hub_dir: str, ttl_s: int = 1800) -> List[str]:
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


def _parse_watchlist(settings: Dict[str, Any]) -> List[str]:
    raw = str(settings.get("stock_universe_symbols", "") or "")
    out: List[str] = []
    for tok in raw.replace("\n", ",").split(","):
        s = tok.strip().upper()
        if s and s not in out:
            out.append(s)
    return out


def _load_warmup_queue(hub_dir: str, ttl_s: int = 7200) -> Dict[str, Dict[str, Any]]:
    path = _warmup_queue_path(hub_dir)
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        rows = payload.get("symbols", {}) if isinstance(payload, dict) else {}
        if not isinstance(rows, dict):
            return {}
        now_ts = int(time.time())
        out: Dict[str, Dict[str, Any]] = {}
        for sym, row in rows.items():
            s = str(sym or "").strip().upper()
            if not s or not isinstance(row, dict):
                continue
            last_seen = int(row.get("last_seen", 0) or 0)
            if (now_ts - last_seen) > int(ttl_s):
                continue
            out[s] = row
        return out
    except Exception:
        return {}


def _save_warmup_queue(hub_dir: str, queue_map: Dict[str, Dict[str, Any]]) -> None:
    path = _warmup_queue_path(hub_dir)
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"ts": int(time.time()), "symbols": queue_map}, f, indent=2)
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
                if key:
                    out[key] = list(rows or [])
    return out


def _score_bars(symbol: str, bars: List[Dict[str, Any]], spread_bps: float = 0.0) -> Dict[str, Any]:
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
            "spread_bps": round(float(spread_bps), 4),
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

    spread_penalty = max(0.0, float(spread_bps) / 8.0)
    score = (change_6 * 0.60) + (change_24 * 0.25) + (volatility * 0.20) - spread_penalty
    side = "long" if score > 0 else "watch"
    abs_score = abs(score)
    if abs_score >= 4.0:
        confidence = "HIGH"
    elif abs_score >= 1.75:
        confidence = "MED"
    else:
        confidence = "LOW"
    reason = f"6h {change_6:+.2f}% | 24h {change_24:+.2f}% | vol {volatility:.2f}% | spr {float(spread_bps):.2f}bps"
    return {
        "symbol": symbol,
        "score": round(score, 6),
        "side": side,
        "last": round(last_px, 6),
        "change_6h_pct": round(change_6, 6),
        "change_24h_pct": round(change_24, 6),
        "volatility_pct": round(volatility, 6),
        "spread_bps": round(float(spread_bps), 4),
        "confidence": confidence,
        "reason": reason,
    }


def _bar_quality(bars: List[Dict[str, Any]]) -> Dict[str, float]:
    if not bars:
        return {"valid_ratio": 0.0, "stale_hours": 9999.0}
    valid = 0
    latest_ts = 0.0
    for row in bars:
        if not isinstance(row, dict):
            continue
        c = _float(row.get("c", 0.0), 0.0)
        if c > 0:
            valid += 1
        t = str(row.get("t", "") or "").strip()
        if t:
            try:
                ts = _parse_iso_ts(t)
                latest_ts = max(latest_ts, ts)
            except Exception:
                pass
    ratio = float(valid) / float(max(1, len(bars)))
    stale_h = 9999.0
    if latest_ts > 0:
        stale_h = max(0.0, (time.time() - latest_ts) / 3600.0)
    return {"valid_ratio": ratio, "stale_hours": stale_h}


def _parse_iso_ts(raw_ts: str) -> float:
    s = str(raw_ts or "").strip()
    if not s:
        return 0.0
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    if "." in s:
        try:
            head, tail = s.split(".", 1)
            tz_idx = max(tail.rfind("+"), tail.rfind("-"))
            if tz_idx > 0:
                frac = tail[:tz_idx]
                tz = tail[tz_idx:]
            else:
                frac = tail
                tz = ""
            frac = (frac + "000000")[:6]
            s = f"{head}.{frac}{tz}"
        except Exception:
            pass
    return datetime.fromisoformat(s).timestamp()


def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _compute_outcome_map(hub_dir: str, limit: int = 500) -> Dict[str, Dict[str, float]]:
    path = _execution_audit_path(hub_dir)
    recent: List[Dict[str, Any]] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for ln in f:
                ln = ln.strip()
                if not ln:
                    continue
                try:
                    obj = json.loads(ln)
                except Exception:
                    continue
                if str(obj.get("event", "")).lower() in {"exit", "shadow_exit"}:
                    recent.append(obj)
    except Exception:
        return {}
    if len(recent) > int(limit):
        recent = recent[-int(limit):]
    per: Dict[str, List[float]] = {}
    for row in recent:
        sym = str(row.get("symbol", "") or "").strip().upper()
        if not sym:
            continue
        pnl = _float(row.get("pnl_pct", 0.0), 0.0)
        per.setdefault(sym, []).append(pnl)
    out: Dict[str, Dict[str, float]] = {}
    for sym, pnls in per.items():
        wins = sum(1 for p in pnls if p > 0.0)
        out[sym] = {
            "hit_rate_pct": round((100.0 * wins / max(1, len(pnls))), 2),
            "avg_pnl_pct": round((sum(pnls) / max(1, len(pnls))), 4),
            "samples": float(len(pnls)),
        }
    return out


def _calibrated_prob(score: float, hit_rate_pct: float, avg_pnl_pct: float) -> float:
    # Lightweight calibration: blend score magnitude + realized hit rate + expectancy.
    score_term = max(0.0, min(1.0, abs(score) / 3.0))
    hit_term = max(0.0, min(1.0, float(hit_rate_pct) / 100.0))
    pnl_term = max(0.0, min(1.0, (float(avg_pnl_pct) + 2.0) / 4.0))
    return round((0.45 * score_term) + (0.40 * hit_term) + (0.15 * pnl_term), 4)


def _select_universe(settings: Dict[str, Any], hub_dir: str, api_key: str, secret: str) -> List[str]:
    mode = str(settings.get("stock_universe_mode", "all_tradable_filtered") or "all_tradable_filtered").strip().lower()
    watch = _parse_watchlist(settings)
    if mode == "watchlist":
        return watch if watch else list(DEFAULT_STOCK_UNIVERSE)
    if mode == "core":
        return watch if watch else list(DEFAULT_STOCK_UNIVERSE)
    if mode != "all_tradable_filtered":
        return watch if watch else list(DEFAULT_STOCK_UNIVERSE)
    if not _rollout_at_least(settings, "scan_expanded"):
        return watch if watch else list(DEFAULT_STOCK_UNIVERSE)

    cached = _load_universe_cache(hub_dir, ttl_s=1800)
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
        return watch if watch else list(DEFAULT_STOCK_UNIVERSE)
    _save_universe_cache(hub_dir, symbols)
    return symbols


def _parse_feed_order(settings: Dict[str, Any]) -> List[str]:
    raw = str(settings.get("stock_data_feeds", "sip,iex") or "sip,iex")
    out: List[str] = []
    for tok in raw.replace(";", ",").split(","):
        feed = str(tok or "").strip().lower()
        if feed in {"sip", "iex"} and feed not in out:
            out.append(feed)
    if not out:
        out = ["sip", "iex"]
    return out


def run_scan(settings: Dict[str, Any], hub_dir: str) -> Dict[str, Any]:
    api_key = str(settings.get("alpaca_api_key_id", "") or "").strip()
    secret = str(settings.get("alpaca_secret_key", "") or "").strip()
    base_url = str(settings.get("alpaca_data_url", settings.get("alpaca_base_url", "https://data.alpaca.markets")) or "").strip().rstrip("/")
    ts_now = int(time.time())
    if not api_key or not secret:
        return {
            "state": "NOT CONFIGURED",
            "ai_state": "Credentials missing",
            "msg": "Add Alpaca keys in Settings",
            "universe": list(DEFAULT_STOCK_UNIVERSE),
            "leaders": [],
            "all_scores": [],
            "updated_at": ts_now,
            "market_open": _market_open_now(),
        }

    headers = {"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": secret}
    now_utc = datetime.now(timezone.utc)
    start_utc = now_utc - timedelta(days=10)
    start_iso = _iso_utc(start_utc)
    end_iso = _iso_utc(now_utc)
    daily_start_iso = _iso_utc(now_utc - timedelta(days=220))
    daily_end_iso = end_iso
    universe = _select_universe(settings, hub_dir, api_key, secret)
    max_scan = max(8, int(float(settings.get("stock_scan_max_symbols", 120) or 120)))
    universe = universe[:max_scan]

    data_url = str(settings.get("alpaca_data_url", "https://data.alpaca.markets") or "").strip().rstrip("/")
    if (not data_url) or ("paper-api.alpaca.markets" in data_url):
        data_url = "https://data.alpaca.markets"
    base_url = data_url

    client = AlpacaBrokerClient(
        api_key_id=api_key,
        secret_key=secret,
        base_url=str(settings.get("alpaca_base_url", "https://paper-api.alpaca.markets") or ""),
        data_url=data_url,
    )
    feed_order = _parse_feed_order(settings)
    snap: Dict[str, Dict[str, float]] = {}
    for feed in feed_order:
        snap = client.get_snapshot_details(universe, feed=feed)
        if snap:
            break
    min_price = max(0.0, float(settings.get("stock_min_price", 2.0) or 2.0))
    max_price = max(min_price, float(settings.get("stock_max_price", 500.0) or 500.0))
    min_dollar_vol = max(0.0, float(settings.get("stock_min_dollar_volume", 2_000_000.0) or 2_000_000.0))
    max_spread_bps = max(0.0, float(settings.get("stock_max_spread_bps", 40.0) or 40.0))
    gate_market_hours = bool(settings.get("stock_gate_market_hours_scan", True))
    use_daily_when_closed = bool(settings.get("stock_scan_use_daily_when_closed", True))
    closed_max_stale_hours = max(1.0, float(settings.get("stock_closed_max_stale_hours", 96.0) or 96.0))
    min_bars_required = max(8, int(float(settings.get("stock_min_bars_required", 24) or 24)))

    candidates: List[str] = []
    rejected: List[Dict[str, Any]] = []
    rejected_seen: set[tuple[str, str, str]] = set()

    def add_rejected(row: Dict[str, Any]) -> None:
        sym = str(row.get("symbol", "") or "").strip().upper()
        reason = str(row.get("reason", "") or "").strip().lower()
        source = str(row.get("source", "") or "").strip().lower()
        key = (sym, reason, source)
        if key in rejected_seen:
            return
        rejected_seen.add(key)
        rejected.append(row)

    warm_queue = _load_warmup_queue(hub_dir)
    market_open = _market_open_now()
    now_ts = int(time.time())

    # Warmup prefetch queue: pull extra daily history for recently short symbols.
    if warm_queue:
        for sym in list(warm_queue.keys())[:50]:
            try:
                wb = client.get_stock_bars(
                    sym,
                    timeframe="1Day",
                    limit=120,
                    feed=feed_order[0] if feed_order else "sip",
                    start_iso=daily_start_iso,
                    end_iso=daily_end_iso,
                )
                warm_queue[sym]["bars_count"] = int(len(wb or []))
                warm_queue[sym]["last_seen"] = now_ts
                if int(warm_queue[sym]["bars_count"]) >= min_bars_required:
                    warm_queue.pop(sym, None)
                else:
                    prev_retry = int(warm_queue[sym].get("retry_s", 60) or 60)
                    warm_queue[sym]["retry_s"] = min(900, max(60, prev_retry * 2))
                    warm_queue[sym]["retry_after"] = now_ts + int(warm_queue[sym]["retry_s"])
            except Exception:
                pass

    for sym in universe:
        d = snap.get(sym, {}) if isinstance(snap, dict) else {}
        px = _float(d.get("mid", 0.0), 0.0)
        spread_bps = _float(d.get("spread_bps", 0.0), 0.0)
        dollar_vol = _float(d.get("dollar_vol", 0.0), 0.0)
        warm = warm_queue.get(sym, {}) or {}
        retry_after = int(warm.get("retry_after", 0) or 0)
        if retry_after > now_ts and int(warm.get("bars_count", 0) or 0) < min_bars_required:
            add_rejected(
                {
                    "symbol": sym,
                    "reason": "warmup_pending",
                    "bars_count": int(warm.get("bars_count", 0) or 0),
                    "source": str(warm.get("source", "") or ""),
                }
            )
            continue
        # Keep scanner active off-hours. Market-hours gating is enforced in trader preflight.
        # When closed, we prefer daily bars for better historical coverage.
        if px > 0.0 and (px < min_price or px > max_price):
            add_rejected({"symbol": sym, "reason": "price_band", "price": px})
            continue
        if max_spread_bps > 0.0 and spread_bps > 0.0 and spread_bps > max_spread_bps:
            add_rejected({"symbol": sym, "reason": "spread", "spread_bps": spread_bps})
            continue
        if dollar_vol > 0.0 and dollar_vol < min_dollar_vol:
            add_rejected({"symbol": sym, "reason": "liquidity", "dollar_vol": dollar_vol})
            continue
        candidates.append(sym)
    if not candidates:
        candidates = universe[: min(12, len(universe))]

    scored: List[Dict[str, Any]] = []
    last_exc: Exception | None = None
    bars_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    min_valid_ratio = max(0.0, min(1.0, float(settings.get("stock_min_valid_bars_ratio", 0.70) or 0.70)))
    max_stale_hours = max(0.5, float(settings.get("stock_max_stale_hours", 6.0) or 6.0))
    max_stale_hours_effective = max_stale_hours
    if (not market_open) and use_daily_when_closed:
        max_stale_hours_effective = max(max_stale_hours, closed_max_stale_hours)
    for feed in feed_order:
        try:
            if market_open or (not use_daily_when_closed):
                bars_by_symbol = _fetch_bars_for_symbols(base_url, headers, candidates, start_iso, end_iso, feed)
            else:
                bars_by_symbol = {}
            scored = []
            for symbol in candidates:
                symbol_bars: List[Dict[str, Any]] = []
                data_source = ""
                if market_open or (not use_daily_when_closed):
                    symbol_bars = list(bars_by_symbol.get(symbol, []) or [])
                    data_source = "batch_1h"
                else:
                    try:
                        symbol_bars = client.get_stock_bars(
                            symbol,
                            timeframe="1Day",
                            limit=120,
                            feed=feed,
                            start_iso=daily_start_iso,
                            end_iso=daily_end_iso,
                        )
                        data_source = "symbol_1d"
                    except Exception:
                        symbol_bars = []
                        data_source = "symbol_1d"
                # Fallback path: if batch bars are sparse/missing, try symbol endpoint directly.
                if len(symbol_bars) < min_bars_required:
                    try:
                        if market_open:
                            symbol_bars = client.get_stock_bars(symbol, timeframe="1Hour", limit=160, feed=feed)
                            data_source = "symbol_1h"
                        else:
                            # Closed-session fallback: prefer richer intraday window if daily bars are sparse.
                            symbol_bars = client.get_stock_bars(
                                symbol,
                                timeframe="1Hour",
                                limit=240,
                                feed=feed,
                                start_iso=start_iso,
                                end_iso=end_iso,
                            )
                            data_source = "symbol_1h"
                    except Exception:
                        symbol_bars = list(symbol_bars or [])
                if len(symbol_bars) < min_bars_required:
                    # Last resort for thin symbols / feed limitations: daily bars.
                    try:
                        if data_source != "symbol_1d":
                            symbol_bars = client.get_stock_bars(
                                symbol,
                                timeframe="1Day",
                                limit=120,
                                feed=feed,
                                start_iso=daily_start_iso,
                                end_iso=daily_end_iso,
                            )
                            data_source = "symbol_1d"
                        elif market_open:
                            symbol_bars = client.get_stock_bars(symbol, timeframe="1Hour", limit=160, feed=feed)
                            data_source = "symbol_1h"
                    except Exception:
                        symbol_bars = list(symbol_bars or [])
                bars_count = int(len(symbol_bars or []))
                if bars_count < min_bars_required:
                    retry_s = min(900, max(60, int((warm_queue.get(symbol, {}) or {}).get("retry_s", 60) or 60) * 2))
                    warm_queue[symbol] = {
                        "last_seen": now_ts,
                        "bars_count": bars_count,
                        "reason": "insufficient_bars",
                        "source": data_source,
                        "retry_s": retry_s,
                        "retry_after": now_ts + retry_s,
                    }
                    add_rejected(
                        {
                            "symbol": symbol,
                            "reason": "insufficient_bars",
                            "bars_count": bars_count,
                            "source": f"{data_source}:{feed}",
                            "min_bars_required": min_bars_required,
                            "requested_start": daily_start_iso if data_source == "symbol_1d" else start_iso,
                            "requested_end": daily_end_iso if data_source == "symbol_1d" else end_iso,
                        }
                    )
                    continue
                spread_bps = _float((snap.get(symbol, {}) or {}).get("spread_bps", 0.0), 0.0)
                row = _score_bars(symbol, symbol_bars, spread_bps=spread_bps)
                row["spread_bps"] = round(spread_bps, 4)
                row["dollar_vol"] = round(_float((snap.get(symbol, {}) or {}).get("dollar_vol", 0.0), 0.0), 2)
                row["bars_count"] = bars_count
                row["data_source"] = f"{data_source}:{feed}"
                q = _bar_quality(symbol_bars)
                row["valid_ratio"] = round(float(q.get("valid_ratio", 0.0)), 4)
                row["stale_hours"] = round(float(q.get("stale_hours", 9999.0)), 3)
                row["data_quality_ok"] = bool((row["valid_ratio"] >= min_valid_ratio) and (row["stale_hours"] <= max_stale_hours_effective))
                # MTF confirmation: compare 1h direction with higher timeframe proxy.
                mtf_side = "watch"
                try:
                    bars_4h = client.get_stock_bars(symbol, timeframe="4Hour", limit=36, feed=feed)
                    if len(bars_4h) < 8:
                        bars_4h = client.get_stock_bars(symbol, timeframe="1Day", limit=36, feed=feed)
                    mtf = _score_bars(symbol, bars_4h, spread_bps=spread_bps)
                    mtf_score = float(mtf.get("score", 0.0) or 0.0)
                    mtf_side = "long" if mtf_score > 0 else "watch"
                except Exception:
                    mtf_side = "watch"
                row["mtf_side"] = mtf_side
                row["mtf_confirmed"] = bool((str(row.get("side", "watch")).lower() == "long") and (mtf_side == "long"))
                if str(row.get("side", "watch")).lower() == "long" and (not row["mtf_confirmed"]):
                    row["score"] = round(float(row.get("score", 0.0)) * 0.70, 6)
                    row["reason"] = f"{row.get('reason', '')} | mtf mismatch"
                if not bool(row.get("data_quality_ok", True)):
                    add_rejected(
                        {
                            "symbol": symbol,
                            "reason": "data_quality",
                            "valid_ratio": row.get("valid_ratio"),
                            "stale_hours": row.get("stale_hours"),
                            "bars_count": bars_count,
                            "source": f"{data_source}:{feed}",
                        }
                    )
                    row["side"] = "watch"
                    continue
                if float(row.get("score", -9999.0) or -9999.0) <= -9999.0:
                    add_rejected(
                        {
                            "symbol": symbol,
                            "reason": "insufficient_bars",
                            "bars_count": bars_count,
                            "source": f"{data_source}:{feed}",
                        }
                    )
                    continue
                scored.append(row)
            if any(float(row.get("score", -9999.0)) > -9999.0 for row in scored):
                break
        except Exception as exc:
            last_exc = exc
            scored = []

    if not scored:
        # Keep thinker responsive even when bars are sparse/off-hours; execution still gates entries.
        msg = (f"{type(last_exc).__name__}: {last_exc}" if last_exc else "No viable symbols after data-quality gates")
        _append_jsonl(
            _rankings_path(hub_dir),
            {
                "ts": ts_now,
                "state": "READY",
                "market_open": market_open,
                "mode": ("closed_daily" if ((not market_open) and use_daily_when_closed) else "intraday"),
                "reason": msg,
                "universe_total": len(universe),
                "candidates": len(candidates),
                "rejected": rejected[:100],
                "top": [],
            },
        )
        _save_warmup_queue(hub_dir, warm_queue)
        reason_counts: Dict[str, int] = {}
        for r in rejected:
            reason = str((r or {}).get("reason", "unknown") or "unknown")
            reason_counts[reason] = int(reason_counts.get(reason, 0)) + 1
        total_rejected = int(len(rejected))
        dominant_reason = max(reason_counts, key=reason_counts.get) if reason_counts else ""
        dominant_ratio = (float(reason_counts.get(dominant_reason, 0)) / float(max(1, total_rejected))) if total_rejected > 0 else 0.0
        reject_rate_pct = (100.0 * total_rejected / float(max(1, len(universe)))) if universe else 0.0
        reject_warn_pct = max(10.0, float(settings.get("stock_reject_drift_warn_pct", 65.0) or 65.0))
        drift_warning = bool((reject_rate_pct >= reject_warn_pct) and (dominant_ratio >= 0.60))
        return {
            "state": "READY",
            "ai_state": "Scan ready",
            "msg": msg,
            "universe": candidates,
            "leaders": [],
            "all_scores": [],
            "updated_at": ts_now,
            "market_open": market_open,
            "rejected": rejected[:30],
            "reject_summary": {
                "total_rejected": total_rejected,
                "reject_rate_pct": round(reject_rate_pct, 2),
                "dominant_reason": dominant_reason,
                "dominant_ratio_pct": round(dominant_ratio * 100.0, 2),
                "counts": reason_counts,
            },
            "health": {"data_ok": False, "broker_ok": True, "orders_ok": True, "drift_warning": drift_warning},
            "pdt_note": "Paper mode can still simulate PDT protections; live day-trading may be limited under $25k.",
        }

    scored.sort(key=lambda row: float(row.get("score", -9999.0)), reverse=True)
    outcome_map = _compute_outcome_map(hub_dir)
    for row in scored:
        sym = str(row.get("symbol", "") or "").strip().upper()
        m = outcome_map.get(sym, {})
        hr = float(m.get("hit_rate_pct", 50.0) or 50.0)
        ap = float(m.get("avg_pnl_pct", 0.0) or 0.0)
        smp = float(m.get("samples", 0.0) or 0.0)
        row["hit_rate_pct"] = round(hr, 2)
        row["avg_pnl_pct"] = round(ap, 4)
        row["calib_prob"] = _calibrated_prob(float(row.get("score", 0.0) or 0.0), hr, ap)
        row["samples"] = int(smp)
        quality_score = (
            (100.0 * float(row.get("valid_ratio", 0.0)))
            - (2.0 * float(row.get("spread_bps", 0.0)))
            + (0.8 * hr)
        )
        row["quality_score"] = round(quality_score, 3)
    # Universe health ranking and execution bucket.
    ranked_health = sorted(scored, key=lambda r: float(r.get("quality_score", -9999.0)), reverse=True)
    exec_n = max(6, int(len(ranked_health) * 0.35))
    exec_bucket = {str(r.get("symbol", "")).strip().upper() for r in ranked_health[:exec_n]}
    for row in scored:
        row["eligible_for_entry"] = str(row.get("symbol", "")).strip().upper() in exec_bucket
        if (not row["eligible_for_entry"]) and (str(row.get("side", "watch")).lower() == "long"):
            row["reason"] = f"{row.get('reason','')} | universe health bucket"
            row["side"] = "watch"

    # Adaptive threshold based on aggregate volatility regime.
    vols = [float(r.get("volatility_pct", 0.0) or 0.0) for r in scored if float(r.get("volatility_pct", 0.0) or 0.0) > 0]
    vol_med = (sorted(vols)[len(vols) // 2] if vols else 0.0)
    base_thr = max(0.05, float(settings.get("stock_score_threshold", 0.2) or 0.2))
    adaptive_threshold = round(base_thr * (1.25 if vol_med >= 0.65 else 1.0), 4)

    leaders = [row for row in scored if str(row.get("side", "")).lower() == "long"][:10]
    top_pick = leaders[0] if leaders else (scored[0] if scored else None)
    msg = "No viable long candidates"
    if top_pick:
        msg = f"Top pick {top_pick['symbol']} | {top_pick['reason']}"
    top_symbol = str((top_pick or {}).get("symbol", "") or "").strip().upper()
    top_chart = []
    if top_symbol:
        for bar in list(bars_by_symbol.get(top_symbol, []) or [])[-80:]:
            if not isinstance(bar, dict):
                continue
            top_chart.append({"t": bar.get("t"), "o": bar.get("o"), "h": bar.get("h"), "l": bar.get("l"), "c": bar.get("c"), "v": bar.get("v")})

    _append_jsonl(
        _rankings_path(hub_dir),
        {
            "ts": ts_now,
            "state": "READY",
            "market_open": market_open,
            "mode": ("closed_daily" if ((not market_open) and use_daily_when_closed) else "intraday"),
            "universe_total": len(universe),
            "candidates": len(candidates),
            "rejected": rejected[:100],
            "top": leaders[:20],
        },
    )
    # Persist warmup queue (newly queued or cleared).
    _save_warmup_queue(hub_dir, warm_queue)

    reason_counts: Dict[str, int] = {}
    for r in rejected:
        reason = str((r or {}).get("reason", "unknown") or "unknown")
        reason_counts[reason] = int(reason_counts.get(reason, 0)) + 1
    total_rejected = int(len(rejected))
    dominant_reason = max(reason_counts, key=reason_counts.get) if reason_counts else ""
    dominant_ratio = (float(reason_counts.get(dominant_reason, 0)) / float(max(1, total_rejected))) if total_rejected > 0 else 0.0
    reject_rate_pct = (100.0 * total_rejected / float(max(1, len(universe)))) if universe else 0.0
    reject_warn_pct = max(10.0, float(settings.get("stock_reject_drift_warn_pct", 65.0) or 65.0))
    drift_warning = bool((reject_rate_pct >= reject_warn_pct) and (dominant_ratio >= 0.60))

    return {
        "state": "READY",
        "ai_state": "Scan ready",
        "msg": msg,
        "universe": candidates,
        "leaders": leaders[:10],
        "all_scores": scored[:40],
        "top_pick": top_pick,
        "top_chart": top_chart,
        "adaptive_threshold": adaptive_threshold,
        "updated_at": ts_now,
        "market_open": market_open,
        "rejected": rejected[:30],
        "reject_summary": {
            "total_rejected": total_rejected,
            "reject_rate_pct": round(reject_rate_pct, 2),
            "dominant_reason": dominant_reason,
            "dominant_ratio_pct": round(dominant_ratio * 100.0, 2),
            "counts": reason_counts,
        },
        "health": {"data_ok": True, "broker_ok": True, "orders_ok": True, "drift_warning": drift_warning},
        "pdt_note": "Paper mode can still simulate PDT protections; live day-trading may be limited under $25k.",
    }


def main() -> int:
    print("stock_thinker.py is designed to be imported by the hub/runner first.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
