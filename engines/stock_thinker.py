from __future__ import annotations

import json
import os
import random
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

from app.credential_utils import get_alpaca_creds
from app.http_utils import retry_after_from_urllib_http_error
from app.path_utils import resolve_runtime_paths
from app.scan_diagnostics_schema import with_scan_schema
from app.scanner_quality import build_universe_quality_report, quality_hints, turnover_pct
from brokers.broker_alpaca import AlpacaBrokerClient

BASE_DIR, _SETTINGS_PATH, HUB_DATA_DIR, _BOOT_SETTINGS = resolve_runtime_paths(__file__, "stock_thinker")

DEFAULT_STOCK_UNIVERSE = ["AAPL", "MSFT", "NVDA", "AMZN", "META", "TSLA", "SPY", "QQQ"]
_UNIVERSE_CACHE_SCHEMA = 2
_SCANNABLE_EXCHANGES = {"NASDAQ", "NYSE", "AMEX", "ARCA", "BATS"}
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
    max_attempts = 4
    base_backoff_s = 0.35
    retry_http_codes = {408, 425, 429, 500, 502, 503, 504}
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            last_exc = exc
            code = int(getattr(exc, "code", 0) or 0)
            if code not in retry_http_codes or attempt >= max_attempts:
                raise
            retry_after = retry_after_from_urllib_http_error(exc, max_wait_s=120.0)
            wait_s = max(
                retry_after,
                min(6.0, (base_backoff_s * (2 ** (attempt - 1))) + random.uniform(0.0, 0.35)),
            )
            time.sleep(wait_s)
        except urllib.error.URLError as exc:
            last_exc = exc
            if attempt >= max_attempts:
                raise
            wait_s = min(6.0, (base_backoff_s * (2 ** (attempt - 1))) + random.uniform(0.0, 0.35))
            time.sleep(wait_s)
        except Exception as exc:
            last_exc = exc
            if attempt >= max_attempts:
                raise
            wait_s = min(4.0, (0.2 * attempt) + random.uniform(0.0, 0.2))
            time.sleep(wait_s)
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("stock_thinker request failed")


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


def _scan_diag_path(hub_dir: str) -> str:
    return os.path.join(hub_dir, "stocks", "scan_diagnostics.json")


def _feed_health_path(hub_dir: str) -> str:
    return os.path.join(hub_dir, "stocks", "feed_health.json")


def _symbol_cooldown_path(hub_dir: str) -> str:
    return os.path.join(hub_dir, "stocks", "symbol_cooldown.json")


def _quality_report_path(hub_dir: str) -> str:
    return os.path.join(hub_dir, "stocks", "universe_quality.json")


def _norm_id_list(value: Any) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    items = value if isinstance(value, list) else []
    for row in items:
        s = str(row or "").strip().upper()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _compact_chart_bars(rows: List[Dict[str, Any]], limit: int = 120) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    take_n = max(2, int(limit))
    for bar in list(rows or [])[-take_n:]:
        if not isinstance(bar, dict):
            continue
        c = _float(bar.get("c", 0.0), 0.0)
        if c <= 0.0:
            continue
        o = _float(bar.get("o", c), c)
        h = _float(bar.get("h", max(o, c)), max(o, c))
        low_px = _float(bar.get("l", min(o, c)), min(o, c))
        out.append(
            {
                "t": str(bar.get("t", "") or ""),
                "o": float(o),
                "h": float(max(h, o, c)),
                "l": float(min(low_px, o, c)),
                "c": float(c),
                "v": _float(bar.get("v", 0.0), 0.0),
            }
        )
    return out


def _build_top_chart_map(
    leaders: List[Dict[str, Any]],
    bars_lookup: Dict[str, List[Dict[str, Any]]],
    max_symbols: int = 6,
    limit: int = 120,
) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}
    seen: set[str] = set()
    for row in list(leaders or []):
        if len(out) >= max(1, int(max_symbols)):
            break
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol", "") or "").strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        bars = _compact_chart_bars(list(bars_lookup.get(symbol, []) or []), limit=limit)
        if len(bars) >= 2:
            out[symbol] = bars
    return out


def _leader_rank_score(row: Dict[str, Any]) -> float:
    score = _float(row.get("score", 0.0), 0.0)
    quality = _float(row.get("quality_score", 0.0), 0.0)
    calib_prob = _float(row.get("calib_prob", 0.0), 0.0)
    spread_bps = _float(row.get("spread_bps", 0.0), 0.0)
    valid_ratio = _float(row.get("valid_ratio", 0.0), 0.0)
    eligible_bonus = 2.0 if bool(row.get("eligible_for_entry", True)) else -2.0
    data_bonus = 1.0 if bool(row.get("data_quality_ok", True)) else -1.0
    return (
        (score * 12.0)
        + (quality * 0.08)
        + (calib_prob * 8.0)
        + (valid_ratio * 4.0)
        + eligible_bonus
        + data_bonus
        - (spread_bps * 0.05)
    )


def _apply_leader_hysteresis(
    leaders: List[Dict[str, Any]],
    prev_symbol: str,
    margin_pct: float,
) -> tuple[List[Dict[str, Any]], bool]:
    rows = [dict(r) for r in list(leaders or []) if isinstance(r, dict)]
    target = str(prev_symbol or "").strip().upper()
    margin = max(0.0, float(margin_pct or 0.0))
    if (not rows) or (not target) or margin <= 0.0:
        return rows, False
    top = rows[0]
    top_score = abs(_float(top.get("leader_rank_score", top.get("score", 0.0)), 0.0))
    top_side = str(top.get("side", "watch") or "watch").strip().lower()
    if top_score <= 0.0:
        return rows, False
    for idx, row in enumerate(rows[1:], start=1):
        symbol = str(row.get("symbol", "") or "").strip().upper()
        if symbol != target:
            continue
        row_side = str(row.get("side", "watch") or "watch").strip().lower()
        if row_side != top_side:
            return rows, False
        row_score = abs(_float(row.get("leader_rank_score", row.get("score", 0.0)), 0.0))
        if row_score <= 0.0:
            return rows, False
        delta_pct = ((top_score - row_score) / max(1e-9, top_score)) * 100.0
        if delta_pct <= margin:
            return [row] + rows[:idx] + rows[idx + 1 :], True
        return rows, False
    return rows, False


def _stock_scan_window_policy(settings: Dict[str, Any], now_et: datetime | None = None) -> Dict[str, Any]:
    now = now_et or _now_et()
    if now.weekday() >= 5:
        return {"active": False, "window": "OFF", "score_mult": 1.0, "minutes": 0, "since_open_min": -1, "to_close_min": -1}
    mins = (now.hour * 60) + now.minute
    open_m = (9 * 60) + 30
    close_m = 16 * 60
    since_open = int(mins - open_m)
    to_close = int(close_m - mins)
    open_window = max(0, int(float(settings.get("stock_scan_open_cooldown_minutes", 15) or 15)))
    close_window = max(0, int(float(settings.get("stock_scan_close_cooldown_minutes", 15) or 15)))
    open_mult = max(0.5, min(1.0, float(settings.get("stock_scan_open_score_mult", 0.85) or 0.85)))
    close_mult = max(0.5, min(1.0, float(settings.get("stock_scan_close_score_mult", 0.90) or 0.90)))
    if since_open >= 0 and since_open < open_window:
        return {
            "active": True,
            "window": "OPENING",
            "score_mult": float(open_mult),
            "minutes": int(open_window),
            "since_open_min": int(since_open),
            "to_close_min": int(to_close),
        }
    if to_close > 0 and to_close <= close_window:
        return {
            "active": True,
            "window": "CLOSING",
            "score_mult": float(close_mult),
            "minutes": int(close_window),
            "since_open_min": int(since_open),
            "to_close_min": int(to_close),
        }
    return {
        "active": False,
        "window": "NONE",
        "score_mult": 1.0,
        "minutes": 0,
        "since_open_min": int(since_open),
        "to_close_min": int(to_close),
    }


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
        schema = int(payload.get("schema", 1) or 1)
        if schema != int(_UNIVERSE_CACHE_SCHEMA):
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
            json.dump({"schema": int(_UNIVERSE_CACHE_SCHEMA), "ts": int(time.time()), "symbols": symbols}, f, indent=2)
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


def _save_scan_diagnostics(hub_dir: str, payload: Dict[str, Any]) -> None:
    path = _scan_diag_path(hub_dir)
    try:
        row = with_scan_schema(payload, market="stocks")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(row, f, indent=2)
        os.replace(tmp, path)
    except Exception:
        pass


def _load_json_map(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_json_map(path: str, payload: Dict[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        os.replace(tmp, path)
    except Exception:
        pass


def _thinker_status_path(hub_dir: str) -> str:
    return os.path.join(hub_dir, "stocks", "stock_thinker_status.json")


def _cached_scan_fallback(
    hub_dir: str,
    ts_now: int,
    msg: str,
    universe: List[str],
    market_open: bool,
) -> Dict[str, Any] | None:
    prev = _load_json_map(_thinker_status_path(hub_dir))
    if not prev:
        return None
    leaders = list(prev.get("leaders", []) or []) if isinstance(prev.get("leaders", []), list) else []
    all_scores = list(prev.get("all_scores", []) or []) if isinstance(prev.get("all_scores", []), list) else []
    top_pick_raw = prev.get("top_pick", {})
    top_pick = dict(top_pick_raw) if isinstance(top_pick_raw, dict) else {}
    top_chart = list(prev.get("top_chart", []) or []) if isinstance(prev.get("top_chart", []), list) else []
    top_chart_map_raw = prev.get("top_chart_map", {})
    top_chart_map = dict(top_chart_map_raw) if isinstance(top_chart_map_raw, dict) else {}
    if not top_chart:
        top_symbol = str((top_pick or {}).get("symbol", "") or "").strip().upper()
        if top_symbol and isinstance(top_chart_map.get(top_symbol, None), list):
            top_chart = list(top_chart_map.get(top_symbol, []) or [])
    if (not leaders) and (not all_scores) and (not top_chart):
        return None

    prev_updated = int(float(prev.get("updated_at", prev.get("ts", 0)) or 0))
    age_s = max(0, int(ts_now - prev_updated)) if prev_updated > 0 else 0
    fallback_msg = f"{str(msg or '').strip()} | using cached scan ({age_s}s old)"
    prev_hints = list(prev.get("hints", []) or []) if isinstance(prev.get("hints", []), list) else []
    hints = [f"Network/data degraded; serving cached leaders ({age_s}s old)."]
    for h in prev_hints[:4]:
        sh = str(h or "").strip()
        if sh and sh not in hints:
            hints.append(sh)

    reject_summary = (dict(prev.get("reject_summary", {})) if isinstance(prev.get("reject_summary", {}), dict) else {})
    return {
        "state": "READY",
        "ai_state": "Scan degraded (cached)",
        "msg": fallback_msg,
        "universe": list(universe or prev.get("universe", []) or []),
        "leaders": leaders[:10],
        "all_scores": all_scores[:40],
        "top_pick": (top_pick if top_pick else (leaders[0] if leaders and isinstance(leaders[0], dict) else None)),
        "top_chart": top_chart[-120:],
        "top_chart_map": top_chart_map,
        "top_chart_source": str(prev.get("top_chart_source", "") or ""),
        "adaptive_threshold": float(prev.get("adaptive_threshold", 0.2) or 0.2),
        "updated_at": int(ts_now),
        "market_open": bool(market_open),
        "rejected": list(prev.get("rejected", []) or [])[:30],
        "reject_summary": reject_summary,
        "feed_order": list(prev.get("feed_order", []) or [])[:4],
        "hints": hints[:5],
        "candidate_churn_pct": float(prev.get("candidate_churn_pct", 0.0) or 0.0),
        "leader_churn_pct": float(prev.get("leader_churn_pct", 0.0) or 0.0),
        "window_policy": (dict(prev.get("window_policy", {})) if isinstance(prev.get("window_policy", {}), dict) else {}),
        "window_policy_hits": int(prev.get("window_policy_hits", 0) or 0),
        "universe_quality": (dict(prev.get("universe_quality", {})) if isinstance(prev.get("universe_quality", {}), dict) else {}),
        "leader_mode": str(prev.get("leader_mode", "cached") or "cached"),
        "leader_stability_applied": bool(prev.get("leader_stability_applied", False)),
        "leader_stability_prev_symbol": str(prev.get("leader_stability_prev_symbol", "") or ""),
        "fallback_cached": True,
        "health": {"data_ok": False, "broker_ok": True, "orders_ok": True, "drift_warning": True},
        "pdt_note": "Paper mode can still simulate PDT protections; live day-trading may be limited under $25k.",
    }


def _adaptive_feed_order(base_order: List[str], feed_health: Dict[str, Any]) -> List[str]:
    rows = (feed_health or {}).get("feeds", {}) if isinstance(feed_health, dict) else {}
    if not isinstance(rows, dict):
        rows = {}

    def _score(feed: str) -> float:
        row = rows.get(feed, {}) if isinstance(rows.get(feed, {}), dict) else {}
        ok_count = float(row.get("ok_count", 0.0) or 0.0)
        err_count = float(row.get("err_count", 0.0) or 0.0)
        bars_avg = float(row.get("avg_bars", 0.0) or 0.0)
        fresh_bonus = 0.0
        try:
            age = max(0.0, time.time() - float(row.get("updated_ts", 0.0) or 0.0))
            fresh_bonus = max(0.0, 3.0 - min(3.0, age / 1800.0))
        except Exception:
            fresh_bonus = 0.0
        return (ok_count * 2.0) - (err_count * 1.2) + (bars_avg * 0.02) + fresh_bonus

    unique = []
    for f in list(base_order or []):
        ff = str(f or "").strip().lower()
        if ff and ff not in unique:
            unique.append(ff)
    if not unique:
        unique = ["sip", "iex"]
    return sorted(unique, key=lambda x: _score(x), reverse=True)


def _update_feed_health(feed_health: Dict[str, Any], feed: str, ok: bool, bars_total: int = 0) -> Dict[str, Any]:
    data = dict(feed_health if isinstance(feed_health, dict) else {})
    rows = data.get("feeds", {})
    if not isinstance(rows, dict):
        rows = {}
    key = str(feed or "").strip().lower()
    if not key:
        return data
    row = rows.get(key, {})
    if not isinstance(row, dict):
        row = {}
    row["ok_count"] = int(row.get("ok_count", 0) or 0) + (1 if ok else 0)
    row["err_count"] = int(row.get("err_count", 0) or 0) + (0 if ok else 1)
    prev_samples = int(row.get("samples", 0) or 0)
    new_samples = prev_samples + 1
    prev_avg = float(row.get("avg_bars", 0.0) or 0.0)
    row["avg_bars"] = ((prev_avg * prev_samples) + float(max(0, bars_total))) / float(max(1, new_samples))
    row["samples"] = int(new_samples)
    row["updated_ts"] = int(time.time())
    rows[key] = row
    data["feeds"] = rows
    data["ts"] = int(time.time())
    return data


def _cooldown_reasons(settings: Dict[str, Any], key: str, default_csv: str) -> set[str]:
    raw = str(settings.get(key, default_csv) or default_csv)
    out = set()
    for tok in raw.replace(";", ",").split(","):
        r = str(tok or "").strip().lower()
        if r:
            out.add(r)
    return out


def _apply_symbol_cooldown(
    cooldown_map: Dict[str, Any],
    symbol: str,
    reason: str,
    settings: Dict[str, Any],
    now_ts: int,
) -> None:
    sym = str(symbol or "").strip().upper()
    rsn = str(reason or "").strip().lower()
    if not sym or not rsn:
        return
    reasons = _cooldown_reasons(
        settings,
        "stock_symbol_cooldown_reject_reasons",
        "data_quality,insufficient_bars,spread,liquidity",
    )
    if rsn not in reasons:
        return
    mins = max(1, int(float(settings.get("stock_symbol_cooldown_minutes", 30) or 30)))
    min_hits = max(1, int(float(settings.get("stock_symbol_cooldown_min_hits", 2) or 2)))
    row = cooldown_map.get(sym, {})
    if not isinstance(row, dict):
        row = {}
    hit_count = int(row.get("hit_count", 0) or 0) + 1
    until = int(row.get("until", 0) or 0)
    if hit_count >= min_hits:
        until = int(now_ts + (mins * 60))
        hit_count = 0
    cooldown_map[sym] = {
        "symbol": sym,
        "reason": rsn,
        "hit_count": int(hit_count),
        "until": int(until),
        "updated_ts": int(now_ts),
    }


def _prune_cooldown_map(cooldown_map: Dict[str, Any], now_ts: int) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for sym, row in (cooldown_map or {}).items():
        if not isinstance(row, dict):
            continue
        s = str(sym or "").strip().upper()
        if not s:
            continue
        until = int(row.get("until", 0) or 0)
        updated = int(row.get("updated_ts", 0) or 0)
        if until > now_ts:
            out[s] = row
            continue
        # Keep recent hit counters for a short period even if no active lockout.
        if (now_ts - updated) <= 7200:
            out[s] = row
    return out


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
            # Prefer latest bars; API sorts by symbol then timestamp.
            "sort": "desc",
        }
        url = f"{base_url}/v2/stocks/bars?{urllib.parse.urlencode(params)}"
        payload = _request_json(url, headers=headers, timeout=15.0)
        bars = payload.get("bars", {}) or {}
        if isinstance(bars, dict):
            for sym, rows in bars.items():
                key = str(sym).strip().upper()
                if key:
                    norm_rows = [row for row in list(rows or []) if isinstance(row, dict)]
                    norm_rows.sort(key=lambda row: str(row.get("t", "") or ""))
                    out[key] = norm_rows
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
        reason_logic = "Insufficient market history for a reliable trend call"
        reason_data = "bars<8 on 1h sample"
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
            "reason_logic": reason_logic,
            "reason_data": reason_data,
            "reason": reason_logic,
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
    if side == "long":
        if change_6 >= 0.0 and change_24 >= 0.0:
            reason_logic = "Uptrend pressure from positive 6h/24h momentum"
        else:
            reason_logic = "Long bias from recent upside, but momentum is mixed"
    else:
        if change_6 <= 0.0 and change_24 <= 0.0:
            reason_logic = "Downtrend pressure; watchlist only until long trigger appears"
        elif change_6 <= 0.0:
            reason_logic = "Near-term weakness; kept on watchlist for reversal confirmation"
        else:
            reason_logic = "Watchlist candidate with mixed momentum signals"
    if abs(float(score)) < 0.10 and volatility < 0.20:
        reason_logic = "Range/low-volatility behavior; no clear directional edge"
    reason_data = f"6h {change_6:+.2f}% | 24h {change_24:+.2f}% | vol {volatility:.2f}% | spr {float(spread_bps):.2f}bps"
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
        "reason_logic": reason_logic,
        "reason_data": reason_data,
        "reason": reason_logic,
    }


def _append_reason_parts(row: Dict[str, Any], logic: str = "", data: str = "") -> None:
    cur_logic = str(row.get("reason_logic", row.get("reason", "")) or "").strip()
    cur_data = str(row.get("reason_data", "") or "").strip()
    add_logic = str(logic or "").strip()
    add_data = str(data or "").strip()
    if add_logic:
        cur_logic = f"{cur_logic} | {add_logic}" if cur_logic else add_logic
    if add_data:
        cur_data = f"{cur_data} | {add_data}" if cur_data else add_data
    row["reason_logic"] = cur_logic
    row["reason_data"] = cur_data
    row["reason"] = cur_logic if cur_logic else cur_data


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


def _market_hints_from_rejects(reject_summary: Dict[str, Any]) -> List[str]:
    counts = (reject_summary or {}).get("counts", {}) or {}
    if not isinstance(counts, dict):
        counts = {}
    dominant = str((reject_summary or {}).get("dominant_reason", "") or "").strip().lower()
    rate = float((reject_summary or {}).get("reject_rate_pct", 0.0) or 0.0)
    hints: List[str] = []
    if rate >= 70.0:
        hints.append("High reject rate: widen universe or relax one gate at a time.")
    if dominant == "data_quality":
        hints.append("Data quality dominates: lower min valid bars ratio or increase max stale hours.")
    elif dominant == "insufficient_bars":
        hints.append("Insufficient bars dominates: reduce min bars required or let warmup run longer.")
    elif dominant == "spread":
        hints.append("Spread dominates: raise max spread bps slightly or focus on large-cap symbols.")
    elif dominant == "liquidity":
        hints.append("Liquidity dominates: lower min dollar volume or scan fewer, more liquid names.")
    elif dominant == "price_band":
        hints.append("Price band dominates: expand min/max price range in settings.")
    elif dominant == "warmup_pending":
        hints.append("Warmup pending: keep scanner running to hydrate sparse symbols.")
    if not hints and counts:
        hints.append("Scanner healthy; tune score threshold for more/less selectivity.")
    return hints[:3]


_REJECT_REASON_PRIORITY = {
    "data_quality": 100,
    "insufficient_bars": 90,
    "warmup_pending": 80,
    "spread": 70,
    "liquidity": 60,
    "price_band": 50,
    "unknown": 10,
}


def _summarize_rejections(rejected: List[Dict[str, Any]], universe_size: int) -> Dict[str, Any]:
    # Summarize by symbol (not raw events) so reject rate remains bounded to 0..100%.
    best_by_symbol: Dict[str, Dict[str, Any]] = {}
    for row in list(rejected or []):
        if not isinstance(row, dict):
            continue
        sym = str(row.get("symbol", "") or "").strip().upper()
        reason = str(row.get("reason", "unknown") or "unknown").strip().lower() or "unknown"
        if not sym:
            continue
        cur = best_by_symbol.get(sym)
        cur_reason = str((cur or {}).get("reason", "unknown") or "unknown").strip().lower() if isinstance(cur, dict) else "unknown"
        cur_pri = int(_REJECT_REASON_PRIORITY.get(cur_reason, 0))
        new_pri = int(_REJECT_REASON_PRIORITY.get(reason, 0))
        if (cur is None) or (new_pri > cur_pri):
            best_by_symbol[sym] = {"symbol": sym, "reason": reason}

    reason_counts: Dict[str, int] = {}
    for row in best_by_symbol.values():
        reason = str(row.get("reason", "unknown") or "unknown")
        reason_counts[reason] = int(reason_counts.get(reason, 0)) + 1

    total_unique = int(len(best_by_symbol))
    dominant_reason = max(reason_counts.items(), key=lambda item: item[1])[0] if reason_counts else ""
    dominant_ratio = (float(reason_counts.get(dominant_reason, 0)) / float(max(1, total_unique))) if total_unique > 0 else 0.0
    reject_rate_pct = (100.0 * float(total_unique) / float(max(1, int(universe_size or 0)))) if universe_size else 0.0
    return {
        "total_rejected": total_unique,
        "total_rejected_events": int(len(rejected or [])),
        "reject_rate_pct": round(reject_rate_pct, 2),
        "dominant_reason": dominant_reason,
        "dominant_ratio_pct": round(dominant_ratio * 100.0, 2),
        "counts": reason_counts,
    }


def _symbol_is_scannable(symbol: str) -> bool:
    sym = str(symbol or "").strip().upper()
    if not sym:
        return False
    if not re.fullmatch(r"[A-Z]{1,5}", sym):
        return False
    return True


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
        exch = str(row.get("exchange", "") or "").strip().upper()
        if exch and (exch not in _SCANNABLE_EXCHANGES):
            continue
        marginable = row.get("marginable", None)
        if marginable is not None and (not bool(marginable)):
            continue
        fractionable = row.get("fractionable", None)
        if fractionable is not None and (not bool(fractionable)):
            continue
        sym = str(row.get("symbol", "") or "").strip().upper()
        if not _symbol_is_scannable(sym):
            continue
        if sym and sym not in symbols:
            symbols.append(sym)
    symbols.sort(key=lambda s: (len(s), s))
    if watch:
        merged = [s for s in watch if _symbol_is_scannable(s)]
        for s in symbols:
            if s not in merged:
                merged.append(s)
        symbols = merged
    if not symbols:
        return watch if watch else list(DEFAULT_STOCK_UNIVERSE)
    _save_universe_cache(hub_dir, symbols)
    return symbols


def _parse_feed_order(settings: Dict[str, Any]) -> List[str]:
    raw = str(settings.get("stock_data_feeds", "iex,sip") or "iex,sip")
    out: List[str] = []
    for tok in raw.replace(";", ",").split(","):
        feed = str(tok or "").strip().lower()
        if feed in {"sip", "iex"} and feed not in out:
            out.append(feed)
    if not out:
        out = ["iex", "sip"]
    return out


def run_scan(settings: Dict[str, Any], hub_dir: str) -> Dict[str, Any]:
    prev_diag = _load_json_map(_scan_diag_path(hub_dir))
    prev_candidates = _norm_id_list(prev_diag.get("candidate_symbols", []))
    prev_leaders = _norm_id_list(prev_diag.get("leader_symbols", []))
    prev_top_symbol = str(prev_diag.get("top_symbol", "") or "").strip().upper()
    if not prev_top_symbol:
        prev_status = _load_json_map(_thinker_status_path(hub_dir))
        prev_top = prev_status.get("top_pick", {}) if isinstance(prev_status.get("top_pick", {}), dict) else {}
        prev_top_symbol = str(prev_top.get("symbol", "") or "").strip().upper()
    api_key, secret = get_alpaca_creds(settings, base_dir=BASE_DIR)
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
            "top_chart": [],
            "top_chart_map": {},
            "updated_at": ts_now,
            "market_open": _market_open_now(),
        }

    market_open = _market_open_now()
    headers = {"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": secret}
    now_utc = datetime.now(timezone.utc)
    start_utc = now_utc - timedelta(days=10)
    start_iso = _iso_utc(start_utc)
    end_iso = _iso_utc(now_utc)
    daily_start_iso = _iso_utc(now_utc - timedelta(days=220))
    daily_end_iso = end_iso
    try:
        universe = _select_universe(settings, hub_dir, api_key, secret)
    except Exception as exc:
        err_msg = f"{type(exc).__name__}: {exc}"
        fallback = _cached_scan_fallback(
            hub_dir,
            ts_now,
            err_msg,
            universe=list(prev_candidates or DEFAULT_STOCK_UNIVERSE),
            market_open=market_open,
        )
        if fallback:
            return fallback
        return {
            "state": "ERROR",
            "ai_state": "Scan failed",
            "msg": err_msg,
            "universe": list(prev_candidates or DEFAULT_STOCK_UNIVERSE),
            "leaders": [],
            "all_scores": [],
            "top_chart": [],
            "top_chart_map": {},
            "updated_at": ts_now,
            "market_open": market_open,
        }
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
    feed_order_cfg = _parse_feed_order(settings)
    feed_health = _load_json_map(_feed_health_path(hub_dir))
    feed_order = _adaptive_feed_order(feed_order_cfg, feed_health)
    feed_errors: Dict[str, str] = {}
    snap: Dict[str, Dict[str, float]] = {}
    snap_last_exc: Exception | None = None
    for feed in feed_order:
        try:
            snap = client.get_snapshot_details(universe, feed=feed)
            feed_health = _update_feed_health(feed_health, feed, ok=bool(snap), bars_total=len(snap))
            if snap:
                break
        except Exception as exc:
            snap_last_exc = exc
            try:
                feed_errors[str(feed or "").strip().lower()] = f"{type(exc).__name__}: {exc}"
            except Exception:
                pass
            feed_health = _update_feed_health(feed_health, feed, ok=False, bars_total=0)
            continue
    if (not snap) and (snap_last_exc is not None):
        _append_jsonl(
            _rankings_path(hub_dir),
            {
                "ts": int(ts_now),
                "state": "WARN",
                "reason": f"snapshot degraded: {type(snap_last_exc).__name__}: {snap_last_exc}",
                "universe_total": len(universe),
            },
        )
    min_price = max(0.0, float(settings.get("stock_min_price", 2.0) or 2.0))
    max_price = max(min_price, float(settings.get("stock_max_price", 500.0) or 500.0))
    min_dollar_vol = max(0.0, float(settings.get("stock_min_dollar_volume", 2_000_000.0) or 2_000_000.0))
    max_spread_bps = max(0.0, float(settings.get("stock_max_spread_bps", 40.0) or 40.0))
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
    cooldown_state = _load_json_map(_symbol_cooldown_path(hub_dir))
    cooldown_map = cooldown_state.get("symbols", {}) if isinstance(cooldown_state.get("symbols", {}), dict) else {}
    market_open = bool(market_open)
    now_ts = int(time.time())
    window_policy = _stock_scan_window_policy(settings) if market_open else {"active": False, "window": "OFF", "score_mult": 1.0, "minutes": 0}
    window_policy_hits = 0
    cooldown_map = _prune_cooldown_map(cooldown_map, now_ts)

    # Warmup prefetch queue: pull extra daily history for recently short symbols.
    if warm_queue:
        for sym in list(warm_queue.keys())[:50]:
            best_count = 0
            best_source = str((warm_queue.get(sym, {}) or {}).get("source", "") or "")
            for feed in (feed_order or ["sip", "iex"]):
                try:
                    wb = client.get_stock_bars(
                        sym,
                        timeframe="1Day",
                        limit=120,
                        feed=feed,
                        start_iso=daily_start_iso,
                        end_iso=daily_end_iso,
                    )
                    n = int(len(wb or []))
                    if n > best_count:
                        best_count = n
                        best_source = f"symbol_1d:{feed}"
                    if n >= min_bars_required:
                        break
                except Exception:
                    continue
            try:
                warm_queue[sym]["bars_count"] = int(best_count)
                warm_queue[sym]["source"] = str(best_source or warm_queue[sym].get("source", ""))
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
        c_row = cooldown_map.get(sym, {}) if isinstance(cooldown_map.get(sym, {}), dict) else {}
        if int(c_row.get("until", 0) or 0) > now_ts:
            add_rejected(
                {
                    "symbol": sym,
                    "reason": "cooldown",
                    "cooldown_until": int(c_row.get("until", 0) or 0),
                    "source": "cooldown",
                }
            )
            continue
        d = snap.get(sym, {}) if isinstance(snap, dict) else {}
        px = _float(d.get("mid", 0.0), 0.0)
        spread_bps = _float(d.get("spread_bps", 0.0), 0.0)
        dollar_vol = _float(d.get("dollar_vol", 0.0), 0.0)
        warm = warm_queue.get(sym, {}) or {}
        retry_after = int(warm.get("retry_after", 0) or 0)
        if (not market_open) and (retry_after > now_ts) and int(warm.get("bars_count", 0) or 0) < min_bars_required:
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
            _apply_symbol_cooldown(cooldown_map, sym, "price_band", settings, now_ts)
            continue
        if max_spread_bps > 0.0 and spread_bps > 0.0 and spread_bps > max_spread_bps:
            add_rejected({"symbol": sym, "reason": "spread", "spread_bps": spread_bps})
            _apply_symbol_cooldown(cooldown_map, sym, "spread", settings, now_ts)
            continue
        if market_open and dollar_vol <= 0.0:
            add_rejected({"symbol": sym, "reason": "liquidity", "dollar_vol": dollar_vol})
            _apply_symbol_cooldown(cooldown_map, sym, "liquidity", settings, now_ts)
            continue
        if dollar_vol > 0.0 and dollar_vol < min_dollar_vol:
            add_rejected({"symbol": sym, "reason": "liquidity", "dollar_vol": dollar_vol})
            _apply_symbol_cooldown(cooldown_map, sym, "liquidity", settings, now_ts)
            continue
        candidates.append(sym)
    if not candidates:
        candidates = universe[: min(12, len(universe))]

    scored: List[Dict[str, Any]] = []
    last_exc: Exception | None = None
    bars_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    best_bars_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
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
                        if (not market_open) and data_source != "symbol_1d":
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
                    _apply_symbol_cooldown(cooldown_map, symbol, "insufficient_bars", settings, now_ts)
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
                    _append_reason_parts(
                        row,
                        logic="Multi-timeframe mismatch lowered conviction",
                        data="mtf mismatch",
                    )
                if bool(window_policy.get("active", False)) and str(row.get("side", "watch")).lower() == "long":
                    raw_score = float(row.get("score", 0.0) or 0.0)
                    mult = float(window_policy.get("score_mult", 1.0) or 1.0)
                    row["score_raw"] = round(raw_score, 6)
                    row["score"] = round(raw_score * mult, 6)
                    row["window_policy"] = str(window_policy.get("window", "NONE") or "NONE")
                    row["window_score_mult"] = round(mult, 4)
                    row["window_minutes"] = int(window_policy.get("minutes", 0) or 0)
                    _append_reason_parts(
                        row,
                        logic=f"{str(row['window_policy']).title()} session dampener reduced entry conviction",
                        data=f"{str(row['window_policy']).lower()} window x{mult:.2f}",
                    )
                    window_policy_hits += 1
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
                    _apply_symbol_cooldown(cooldown_map, symbol, "data_quality", settings, now_ts)
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
                    _apply_symbol_cooldown(cooldown_map, symbol, "insufficient_bars", settings, now_ts)
                    continue
                best_bars_by_symbol[symbol] = list(symbol_bars or [])
                scored.append(row)
            bars_total = 0
            try:
                bars_total = int(sum(len(list(v or [])) for v in best_bars_by_symbol.values()))
            except Exception:
                bars_total = 0
            feed_health = _update_feed_health(feed_health, feed, ok=bool(scored), bars_total=bars_total)
            if any(float(row.get("score", -9999.0)) > -9999.0 for row in scored):
                break
        except Exception as exc:
            last_exc = exc
            try:
                if str(feed or "").strip().lower() not in feed_errors:
                    feed_errors[str(feed or "").strip().lower()] = f"{type(exc).__name__}: {exc}"
            except Exception:
                pass
            feed_health = _update_feed_health(feed_health, feed, ok=False, bars_total=0)
            scored = []

    if not scored:
        # Keep thinker responsive even when bars are sparse/off-hours; execution still gates entries.
        feed_issue_parts: List[str] = []
        for feed_name, raw_err in list(feed_errors.items())[:3]:
            err_txt = str(raw_err or "").strip()
            if (not err_txt) and (last_exc is not None):
                err_txt = f"{type(last_exc).__name__}: {last_exc}"
            low = err_txt.lower()
            if ("http error 403" in low) or ("forbidden" in low):
                err_txt = "403 Forbidden (feed entitlement)"
            elif len(err_txt) > 92:
                err_txt = err_txt[:89] + "..."
            feed_issue_parts.append(f"{feed_name}:{err_txt}")
        if rejected:
            msg = "No viable symbols after data-quality gates"
        else:
            msg = (f"{type(last_exc).__name__}: {last_exc}" if last_exc else "No viable symbols after data-quality gates")
        if feed_issue_parts:
            msg = f"{msg} | feed issues: {', '.join(feed_issue_parts)}"
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
        _save_json_map(_feed_health_path(hub_dir), feed_health)
        _save_json_map(_symbol_cooldown_path(hub_dir), {"ts": int(time.time()), "symbols": cooldown_map})
        reject_summary = _summarize_rejections(rejected, len(universe))
        candidate_churn_pct = turnover_pct(prev_candidates, candidates)
        leader_churn_pct = turnover_pct(prev_leaders, [])
        quality_report = build_universe_quality_report(
            market="stocks",
            ts=int(ts_now),
            mode=("closed_daily" if ((not market_open) and use_daily_when_closed) else "intraday"),
            universe_total=int(len(universe)),
            candidates_total=int(len(candidates)),
            scores_total=0,
            leaders_total=0,
            reject_summary=dict(reject_summary),
            rejected_rows=list(rejected),
            scored_rows=[],
            candidate_churn_pct=float(candidate_churn_pct),
            leader_churn_pct=float(leader_churn_pct),
        )
        _save_json_map(_quality_report_path(hub_dir), quality_report)
        reason_counts = dict(reject_summary.get("counts", {}) or {})
        dominant_reason = str(reject_summary.get("dominant_reason", "") or "")
        dominant_ratio = float(reject_summary.get("dominant_ratio_pct", 0.0) or 0.0) / 100.0
        reject_rate_pct = float(reject_summary.get("reject_rate_pct", 0.0) or 0.0)
        reject_warn_pct = max(10.0, float(settings.get("stock_reject_drift_warn_pct", 65.0) or 65.0))
        drift_warning = bool((reject_rate_pct >= reject_warn_pct) and (dominant_ratio >= 0.60))
        _save_scan_diagnostics(
            hub_dir,
            {
                "ts": ts_now,
                "state": "READY",
                "mode": ("closed_daily" if ((not market_open) and use_daily_when_closed) else "intraday"),
                "market_open": bool(market_open),
                "universe_total": int(len(universe)),
                "candidates_total": int(len(candidates)),
                "scores_total": 0,
                "leaders_total": 0,
                "top_symbol": "",
                "top_score": 0.0,
                "msg": str(msg),
                "reject_summary": dict(reject_summary),
                "feed_order": list(feed_order),
                "feed_health": dict((feed_health.get("feeds", {}) if isinstance(feed_health.get("feeds", {}), dict) else {})),
                "cooldown_active": int(sum(1 for v in (cooldown_map or {}).values() if int((v or {}).get("until", 0) or 0) > now_ts)),
                "window_policy": dict(window_policy),
                "window_policy_hits": int(window_policy_hits),
                "candidate_symbols": list(candidates),
                "leader_symbols": [],
                "leader_mode": "none",
                "leader_stability_applied": False,
                "leader_stability_prev_symbol": str(prev_top_symbol),
                "candidate_churn_pct": float(candidate_churn_pct),
                "leader_churn_pct": float(leader_churn_pct),
                "quality_summary": str(quality_report.get("summary", "") or ""),
            },
        )
        hints = _market_hints_from_rejects(
            {
                "counts": reason_counts,
                "reject_rate_pct": round(reject_rate_pct, 2),
                "dominant_reason": dominant_reason,
            }
        )
        if any("403 forbidden" in str(x).lower() for x in feed_issue_parts):
            hints.insert(0, "Feed entitlement warning: SIP can return 403 in paper mode; IEX fallback remains active.")
        for h in quality_hints(quality_report):
            if h not in hints:
                hints.append(h)
        fallback = _cached_scan_fallback(
            hub_dir,
            ts_now,
            msg,
            universe=list(candidates),
            market_open=bool(market_open),
        )
        if fallback:
            _save_scan_diagnostics(
                hub_dir,
                {
                    "ts": ts_now,
                    "state": "READY",
                    "mode": ("closed_daily" if ((not market_open) and use_daily_when_closed) else "intraday"),
                    "market_open": bool(market_open),
                    "universe_total": int(len(list(fallback.get("universe", []) or []))),
                    "candidates_total": int(len(list(fallback.get("universe", []) or []))),
                    "scores_total": int(len(list(fallback.get("all_scores", []) or []))),
                    "leaders_total": int(len(list(fallback.get("leaders", []) or []))),
                    "top_symbol": str(((fallback.get("top_pick", {}) if isinstance(fallback.get("top_pick", {}), dict) else {}).get("symbol", "")) or ""),
                    "top_score": float(((fallback.get("top_pick", {}) if isinstance(fallback.get("top_pick", {}), dict) else {}).get("score", 0.0)) or 0.0),
                    "msg": str(fallback.get("msg", "") or msg),
                    "reject_summary": (dict(fallback.get("reject_summary", {})) if isinstance(fallback.get("reject_summary", {}), dict) else {}),
                    "feed_order": list(feed_order),
                    "feed_health": dict((feed_health.get("feeds", {}) if isinstance(feed_health.get("feeds", {}), dict) else {})),
                    "cooldown_active": int(sum(1 for v in (cooldown_map or {}).values() if int((v or {}).get("until", 0) or 0) > now_ts)),
                    "window_policy": dict(window_policy),
                    "window_policy_hits": int(window_policy_hits),
                    "candidate_symbols": list(candidates),
                    "leader_symbols": [str((row or {}).get("symbol", "") or "").strip().upper() for row in list(fallback.get("leaders", []) or []) if isinstance(row, dict)],
                    "leader_mode": str(fallback.get("leader_mode", "cached") or "cached"),
                    "leader_stability_applied": bool(fallback.get("leader_stability_applied", False)),
                    "leader_stability_prev_symbol": str(fallback.get("leader_stability_prev_symbol", prev_top_symbol) or prev_top_symbol),
                    "candidate_churn_pct": float(fallback.get("candidate_churn_pct", candidate_churn_pct) or candidate_churn_pct),
                    "leader_churn_pct": float(fallback.get("leader_churn_pct", leader_churn_pct) or leader_churn_pct),
                    "quality_summary": str((dict(fallback.get("universe_quality", {})).get("summary", "") if isinstance(fallback.get("universe_quality", {}), dict) else "") or ""),
                    "fallback_cached": True,
                },
            )
            return fallback
        return {
            "state": "READY",
            "ai_state": "Scan ready",
            "msg": msg,
            "universe": candidates,
            "leaders": [],
            "all_scores": [],
            "top_chart": [],
            "top_chart_map": {},
            "updated_at": ts_now,
            "market_open": market_open,
            "rejected": rejected[:30],
            "reject_summary": reject_summary,
            "hints": hints[:5],
            "candidate_churn_pct": float(candidate_churn_pct),
            "leader_churn_pct": float(leader_churn_pct),
            "leader_mode": "none",
            "leader_stability_applied": False,
            "leader_stability_prev_symbol": str(prev_top_symbol),
            "universe_quality": quality_report,
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
            _append_reason_parts(
                row,
                logic="Demoted to watchlist by universe quality ranking",
                data="universe health bucket",
            )
            row["side"] = "watch"
        row["leader_rank_score"] = round(_leader_rank_score(row), 6)

    # Adaptive threshold based on aggregate volatility regime.
    vols = [float(r.get("volatility_pct", 0.0) or 0.0) for r in scored if float(r.get("volatility_pct", 0.0) or 0.0) > 0]
    vol_med = (sorted(vols)[len(vols) // 2] if vols else 0.0)
    base_thr = max(0.05, float(settings.get("stock_score_threshold", 0.2) or 0.2))
    adaptive_threshold = round(base_thr * (1.25 if vol_med >= 0.65 else 1.0), 4)

    leaders_long = sorted(
        [row for row in scored if str(row.get("side", "")).lower() == "long"],
        key=lambda r: float(r.get("leader_rank_score", r.get("score", -9999.0)) or -9999.0),
        reverse=True,
    )[:10]
    leader_mode = "long"
    leaders = list(leaders_long)
    publish_watch = bool(settings.get("stock_scan_publish_watch_leaders", True))
    if (not leaders) and publish_watch and scored:
        watch_n = max(1, min(10, int(float(settings.get("stock_scan_watch_leaders_count", 6) or 6))))
        leaders = sorted(
            list(scored),
            key=lambda r: float(r.get("leader_rank_score", r.get("score", -9999.0)) or -9999.0),
            reverse=True,
        )[:watch_n]
        leader_mode = "watch_fallback"
    try:
        stability_margin = max(0.0, min(100.0, float(settings.get("stock_leader_stability_margin_pct", 10.0) or 10.0)))
    except Exception:
        stability_margin = 10.0
    leaders, stability_applied = _apply_leader_hysteresis(leaders, prev_top_symbol, stability_margin)
    top_pick = leaders[0] if leaders else (scored[0] if scored else None)
    msg = "No viable long candidates"
    if leader_mode == "watch_fallback":
        msg = "No long setups yet; showing strongest watchlist candidates"
    if top_pick:
        msg = f"Top pick {top_pick['symbol']} | {top_pick['reason']}"
    top_symbol = str((top_pick or {}).get("symbol", "") or "").strip().upper()
    top_chart: List[Dict[str, Any]] = []
    chart_seed: List[Dict[str, Any]] = []
    if isinstance(top_pick, dict):
        chart_seed.append(dict(top_pick))
    chart_seed.extend([dict(r) for r in leaders[:10] if isinstance(r, dict)])
    chart_map_symbols = max(2, int(float(settings.get("market_chart_cache_symbols", 8) or 8)))
    chart_map_bars = max(40, int(float(settings.get("market_chart_cache_bars", 120) or 120)))
    top_chart_map = _build_top_chart_map(
        chart_seed,
        best_bars_by_symbol,
        max_symbols=chart_map_symbols,
        limit=chart_map_bars,
    )
    top_source = str((top_pick or {}).get("data_source", "") or "")
    if top_symbol:
        top_chart = list(top_chart_map.get(top_symbol, []) or [])
    if (not top_chart) and top_symbol:
        source_bars = list(best_bars_by_symbol.get(top_symbol, []) or bars_by_symbol.get(top_symbol, []) or [])
        top_chart = _compact_chart_bars(source_bars, limit=chart_map_bars)

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
    _save_json_map(_feed_health_path(hub_dir), feed_health)
    _save_json_map(_symbol_cooldown_path(hub_dir), {"ts": int(time.time()), "symbols": cooldown_map})

    reject_summary = _summarize_rejections(rejected, len(universe))
    leader_symbols = [str((row or {}).get("symbol", "") or "").strip().upper() for row in leaders if isinstance(row, dict)]
    candidate_churn_pct = turnover_pct(prev_candidates, candidates)
    leader_churn_pct = turnover_pct(prev_leaders, leader_symbols)
    quality_report = build_universe_quality_report(
        market="stocks",
        ts=int(ts_now),
        mode=("closed_daily" if ((not market_open) and use_daily_when_closed) else "intraday"),
        universe_total=int(len(universe)),
        candidates_total=int(len(candidates)),
        scores_total=int(len(scored)),
        leaders_total=int(len(leaders)),
        reject_summary=dict(reject_summary),
        rejected_rows=list(rejected),
        scored_rows=list(scored),
        candidate_churn_pct=float(candidate_churn_pct),
        leader_churn_pct=float(leader_churn_pct),
    )
    _save_json_map(_quality_report_path(hub_dir), quality_report)
    reason_counts = dict(reject_summary.get("counts", {}) or {})
    dominant_reason = str(reject_summary.get("dominant_reason", "") or "")
    dominant_ratio = float(reject_summary.get("dominant_ratio_pct", 0.0) or 0.0) / 100.0
    reject_rate_pct = float(reject_summary.get("reject_rate_pct", 0.0) or 0.0)
    reject_warn_pct = max(10.0, float(settings.get("stock_reject_drift_warn_pct", 65.0) or 65.0))
    drift_warning = bool((reject_rate_pct >= reject_warn_pct) and (dominant_ratio >= 0.60))
    _save_scan_diagnostics(
        hub_dir,
        {
            "ts": ts_now,
            "state": "READY",
            "mode": ("closed_daily" if ((not market_open) and use_daily_when_closed) else "intraday"),
            "market_open": bool(market_open),
            "universe_total": int(len(universe)),
            "candidates_total": int(len(candidates)),
            "scores_total": int(len(scored)),
            "leaders_total": int(len(leaders)),
            "top_symbol": str((top_pick or {}).get("symbol", "") or ""),
            "top_score": float((top_pick or {}).get("score", 0.0) or 0.0),
            "msg": str(msg),
            "reject_summary": dict(reject_summary),
            "feed_order": list(feed_order),
            "feed_health": dict((feed_health.get("feeds", {}) if isinstance(feed_health.get("feeds", {}), dict) else {})),
            "cooldown_active": int(sum(1 for v in (cooldown_map or {}).values() if int((v or {}).get("until", 0) or 0) > now_ts)),
            "window_policy": dict(window_policy),
            "window_policy_hits": int(window_policy_hits),
            "candidate_symbols": list(candidates),
            "leader_symbols": list(leader_symbols),
            "leader_mode": str(leader_mode),
            "leader_stability_applied": bool(stability_applied),
            "leader_stability_prev_symbol": str(prev_top_symbol),
            "candidate_churn_pct": float(candidate_churn_pct),
            "leader_churn_pct": float(leader_churn_pct),
            "quality_summary": str(quality_report.get("summary", "") or ""),
        },
    )
    hints = _market_hints_from_rejects(
        {
            "counts": reason_counts,
            "reject_rate_pct": round(reject_rate_pct, 2),
            "dominant_reason": dominant_reason,
        }
    )
    for h in quality_hints(quality_report):
        if h not in hints:
            hints.append(h)

    return {
        "state": "READY",
        "ai_state": "Scan ready",
        "msg": msg,
        "universe": candidates,
        "leaders": leaders[:10],
        "all_scores": scored[:40],
        "top_pick": top_pick,
        "top_chart": top_chart,
        "top_chart_map": top_chart_map,
        "top_chart_source": top_source,
        "adaptive_threshold": adaptive_threshold,
        "updated_at": ts_now,
        "market_open": market_open,
        "rejected": rejected[:30],
        "reject_summary": reject_summary,
        "feed_order": list(feed_order),
        "hints": hints[:5],
        "candidate_churn_pct": float(candidate_churn_pct),
        "leader_churn_pct": float(leader_churn_pct),
        "leader_mode": str(leader_mode),
        "leader_stability_applied": bool(stability_applied),
        "leader_stability_prev_symbol": str(prev_top_symbol),
        "window_policy": dict(window_policy),
        "window_policy_hits": int(window_policy_hits),
        "universe_quality": quality_report,
        "health": {"data_ok": True, "broker_ok": True, "orders_ok": True, "drift_warning": drift_warning},
        "pdt_note": "Paper mode can still simulate PDT protections; live day-trading may be limited under $25k.",
    }


def main() -> int:
    print("stock_thinker.py is designed to be imported by the hub/runner first.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
