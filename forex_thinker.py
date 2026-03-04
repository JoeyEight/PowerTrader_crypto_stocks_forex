from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List

from broker_oanda import OandaBrokerClient
from path_utils import resolve_runtime_paths

BASE_DIR, _SETTINGS_PATH, HUB_DATA_DIR, _BOOT_SETTINGS = resolve_runtime_paths(__file__, "forex_thinker")

DEFAULT_FX_UNIVERSE = ["EUR_USD", "GBP_USD", "USD_JPY", "AUD_USD", "USD_CAD", "EUR_JPY"]
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


def _rankings_path(hub_dir: str) -> str:
    return os.path.join(hub_dir, "forex", "scanner_rankings.jsonl")


def _execution_audit_path(hub_dir: str) -> str:
    return os.path.join(hub_dir, "forex", "execution_audit.jsonl")


def _append_jsonl(path: str, row: Dict[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, separators=(",", ":")) + "\n")
    except Exception:
        pass


def _parse_pairs(settings: Dict[str, Any]) -> List[str]:
    raw = str(settings.get("forex_universe_pairs", "") or "")
    out: List[str] = []
    for tok in raw.replace("\n", ",").split(","):
        p = tok.strip().upper()
        if p and p not in out:
            out.append(p)
    return out


def _score_candles(pair: str, candles: List[Dict[str, Any]], spread_bps: float = 0.0) -> Dict[str, Any]:
    closes = []
    highs = []
    lows = []
    for row in candles or []:
        if not isinstance(row, dict):
            continue
        if not bool(row.get("complete", True)):
            continue
        mid = row.get("mid", {}) or {}
        c = _float(mid.get("c", 0.0), 0.0)
        h = _float(mid.get("h", 0.0), 0.0)
        l = _float(mid.get("l", 0.0), 0.0)
        if c > 0:
            closes.append(c)
        if h > 0:
            highs.append(h)
        if l > 0:
            lows.append(l)
    if len(closes) < 8:
        return {
            "pair": pair,
            "score": -9999.0,
            "side": "watch",
            "last": closes[-1] if closes else 0.0,
            "change_6h_pct": 0.0,
            "change_24h_pct": 0.0,
            "volatility_pct": 0.0,
            "spread_bps": round(float(spread_bps), 4),
            "confidence": "LOW",
            "reason": "Not enough candles",
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
    spread_penalty = max(0.0, float(spread_bps) / 10.0)
    score = (change_6 * 0.60) + (change_24 * 0.25) + (volatility * 0.20) - spread_penalty
    side = "long" if score > 0 else "short"
    abs_score = abs(score)
    if abs_score >= 0.45:
        confidence = "HIGH"
    elif abs_score >= 0.20:
        confidence = "MED"
    else:
        confidence = "LOW"
    reason = f"6h {change_6:+.3f}% | 24h {change_24:+.3f}% | vol {volatility:.3f}% | spr {float(spread_bps):.2f}bps"
    return {
        "pair": pair,
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


def _bar_quality(candles: List[Dict[str, Any]]) -> Dict[str, float]:
    if not candles:
        return {"valid_ratio": 0.0, "stale_hours": 9999.0}
    valid = 0
    latest_ts = 0.0
    for row in candles:
        if not isinstance(row, dict):
            continue
        c = _float(((row.get("mid") or {}).get("c", 0.0)), 0.0)
        if c > 0:
            valid += 1
        t = str(row.get("time", "") or "").strip()
        if t:
            try:
                ts = _parse_iso_ts(t)
                latest_ts = max(latest_ts, ts)
            except Exception:
                pass
    ratio = float(valid) / float(max(1, len(candles)))
    stale_h = 9999.0
    if latest_ts > 0:
        stale_h = max(0.0, (time.time() - latest_ts) / 3600.0)
    return {"valid_ratio": ratio, "stale_hours": stale_h}


def _parse_iso_ts(raw_ts: str) -> float:
    s = str(raw_ts or "").strip()
    if not s:
        return 0.0
    # OANDA may emit nanosecond precision; datetime.fromisoformat supports up to microseconds.
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
        pair = str(row.get("instrument", "") or row.get("pair", "") or "").strip().upper()
        if not pair:
            continue
        pnl = _float(row.get("pnl_pct", 0.0), 0.0)
        per.setdefault(pair, []).append(pnl)
    out: Dict[str, Dict[str, float]] = {}
    for pair, pnls in per.items():
        wins = sum(1 for p in pnls if p > 0.0)
        out[pair] = {
            "hit_rate_pct": round((100.0 * wins / max(1, len(pnls))), 2),
            "avg_pnl_pct": round((sum(pnls) / max(1, len(pnls))), 4),
            "samples": float(len(pnls)),
        }
    return out


def _calibrated_prob(score: float, hit_rate_pct: float, avg_pnl_pct: float) -> float:
    score_term = max(0.0, min(1.0, abs(score) / 0.8))
    hit_term = max(0.0, min(1.0, float(hit_rate_pct) / 100.0))
    pnl_term = max(0.0, min(1.0, (float(avg_pnl_pct) + 1.0) / 2.0))
    return round((0.45 * score_term) + (0.40 * hit_term) + (0.15 * pnl_term), 4)


def run_scan(settings: Dict[str, Any], hub_dir: str) -> Dict[str, Any]:
    account_id = str(settings.get("oanda_account_id", "") or "").strip()
    token = str(settings.get("oanda_api_token", "") or "").strip()
    rest_url = str(settings.get("oanda_rest_url", "https://api-fxpractice.oanda.com") or "").strip().rstrip("/")
    ts_now = int(time.time())
    if not account_id or not token or not rest_url:
        return {
            "state": "NOT CONFIGURED",
            "ai_state": "Credentials missing",
            "msg": "Add OANDA account/token in Settings",
            "universe": list(DEFAULT_FX_UNIVERSE),
            "leaders": [],
            "all_scores": [],
            "updated_at": ts_now,
        }

    client = OandaBrokerClient(account_id=account_id, api_token=token, rest_url=rest_url)
    parsed = _parse_pairs(settings)
    if parsed:
        universe = parsed
    else:
        universe = client.list_tradeable_instruments() or list(DEFAULT_FX_UNIVERSE)
    max_scan = max(4, int(float(settings.get("forex_scan_max_pairs", 24) or 24)))
    universe = universe[:max_scan]

    max_spread_bps = max(0.0, float(settings.get("forex_max_spread_bps", 8.0) or 8.0))
    min_volatility_pct = max(0.0, float(settings.get("forex_min_volatility_pct", 0.01) or 0.01))
    min_bars_required = max(8, int(float(settings.get("forex_min_bars_required", 24) or 24)))
    price_rows = client.get_pricing_details(universe)
    candidates: List[str] = []
    rejected: List[Dict[str, Any]] = []
    for pair in universe:
        p = price_rows.get(pair, {})
        spr = _float(p.get("spread_bps", 0.0), 0.0)
        if max_spread_bps > 0.0 and spr > max_spread_bps:
            rejected.append({"pair": pair, "reason": "spread", "spread_bps": spr})
            continue
        candidates.append(pair)
    if not candidates:
        candidates = universe[: min(12, len(universe))]

    scored = []
    min_valid_ratio = max(0.0, min(1.0, float(settings.get("forex_min_valid_bars_ratio", 0.70) or 0.70)))
    max_stale_hours = max(0.5, float(settings.get("forex_max_stale_hours", 8.0) or 8.0))
    try:
        for pair in candidates:
            params = urllib.parse.urlencode({"price": "M", "granularity": "H1", "count": "48"})
            url = f"{rest_url}/v3/instruments/{pair}/candles?{params}"
            payload = _request_json(url, headers={"Authorization": f"Bearer {token}"}, timeout=10.0)
            candles = payload.get("candles", []) or []
            if not isinstance(candles, list):
                candles = []
            bars_count = int(len(candles))
            if bars_count < min_bars_required:
                rejected.append(
                    {
                        "pair": pair,
                        "reason": "insufficient_bars",
                        "bars_count": bars_count,
                        "source": "oanda_h1",
                        "min_bars_required": min_bars_required,
                    }
                )
                continue
            spread_bps = _float((price_rows.get(pair, {}) or {}).get("spread_bps", 0.0), 0.0)
            row = _score_candles(pair, candles, spread_bps=spread_bps)
            row["spread_bps"] = round(spread_bps, 4)
            row["bars_count"] = bars_count
            row["data_source"] = "oanda_h1"
            q = _bar_quality(candles)
            row["valid_ratio"] = round(float(q.get("valid_ratio", 0.0)), 4)
            row["stale_hours"] = round(float(q.get("stale_hours", 9999.0)), 3)
            row["data_quality_ok"] = bool((row["valid_ratio"] >= min_valid_ratio) and (row["stale_hours"] <= max_stale_hours))
            # MTF confirmation from H4.
            mtf_side = "watch"
            try:
                c4 = client.get_candles(pair, granularity="H4", count=40)
                m4 = _score_candles(pair, c4, spread_bps=spread_bps)
                ms = float(m4.get("score", 0.0) or 0.0)
                mtf_side = ("long" if ms > 0 else "short")
            except Exception:
                mtf_side = "watch"
            row["mtf_side"] = mtf_side
            row["mtf_confirmed"] = bool(str(row.get("side", "watch")).lower() == mtf_side)
            if not row["mtf_confirmed"]:
                row["score"] = round(float(row.get("score", 0.0)) * 0.75, 6)
                row["reason"] = f"{row.get('reason','')} | mtf mismatch"
            if _float(row.get("volatility_pct", 0.0), 0.0) < min_volatility_pct:
                rejected.append({"pair": pair, "reason": "low_volatility", "volatility_pct": row.get("volatility_pct", 0.0)})
                continue
            if not row["data_quality_ok"]:
                rejected.append(
                    {
                        "pair": pair,
                        "reason": "data_quality",
                        "valid_ratio": row.get("valid_ratio"),
                        "stale_hours": row.get("stale_hours"),
                        "bars_count": row.get("bars_count"),
                        "source": row.get("data_source"),
                    }
                )
                continue
            scored.append(row)
        scored.sort(key=lambda row: abs(float(row.get("score", 0.0))), reverse=True)
        outcome_map = _compute_outcome_map(hub_dir)
        for row in scored:
            pair = str(row.get("pair", "") or "").strip().upper()
            m = outcome_map.get(pair, {})
            hr = float(m.get("hit_rate_pct", 50.0) or 50.0)
            ap = float(m.get("avg_pnl_pct", 0.0) or 0.0)
            smp = int(float(m.get("samples", 0.0) or 0.0))
            row["hit_rate_pct"] = round(hr, 2)
            row["avg_pnl_pct"] = round(ap, 4)
            row["samples"] = smp
            row["calib_prob"] = _calibrated_prob(float(row.get("score", 0.0) or 0.0), hr, ap)
            row["quality_score"] = round(
                (100.0 * float(row.get("valid_ratio", 0.0)))
                - (3.0 * float(row.get("spread_bps", 0.0)))
                + (0.7 * hr),
                3,
            )
        ranked_health = sorted(scored, key=lambda r: float(r.get("quality_score", -9999.0)), reverse=True)
        exec_n = max(6, int(len(ranked_health) * 0.40))
        exec_bucket = {str(r.get("pair", "")).strip().upper() for r in ranked_health[:exec_n]}
        for row in scored:
            row["eligible_for_entry"] = str(row.get("pair", "")).strip().upper() in exec_bucket
            if not row["eligible_for_entry"]:
                row["reason"] = f"{row.get('reason','')} | universe health bucket"
                row["side"] = "watch"
        leaders = scored[:10]
        top_pick = leaders[0] if leaders else None
        msg = "No FX leaders"
        if top_pick:
            msg = f"Top pair {top_pick['pair']} | {top_pick['side']} | {top_pick['reason']}"
        vols = [float(r.get("volatility_pct", 0.0) or 0.0) for r in scored if float(r.get("volatility_pct", 0.0) or 0.0) > 0]
        vol_med = (sorted(vols)[len(vols) // 2] if vols else 0.0)
        base_thr = max(0.02, float(settings.get("forex_score_threshold", 0.2) or 0.2))
        adaptive_threshold = round(base_thr * (1.20 if vol_med >= 0.12 else 1.0), 4)
        top_pair = str((top_pick or {}).get("pair", "") or "").strip().upper()
        top_chart = []
        if top_pair:
            params = urllib.parse.urlencode({"price": "M", "granularity": "H1", "count": "80"})
            url = f"{rest_url}/v3/instruments/{top_pair}/candles?{params}"
            payload = _request_json(url, headers={"Authorization": f"Bearer {token}"}, timeout=10.0)
            for c in list((payload.get("candles", []) or []))[-80:]:
                if not isinstance(c, dict):
                    continue
                m = c.get("mid", {}) or {}
                top_chart.append({"t": c.get("time"), "o": m.get("o"), "h": m.get("h"), "l": m.get("l"), "c": m.get("c"), "v": c.get("volume")})
        _append_jsonl(
            _rankings_path(hub_dir),
            {
                "ts": ts_now,
                "state": "READY",
                "universe_total": len(universe),
                "candidates": len(candidates),
                "rejected": rejected[:100],
                "top": leaders[:20],
            },
        )
        reason_counts: Dict[str, int] = {}
        for r in rejected:
            reason = str((r or {}).get("reason", "unknown") or "unknown")
            reason_counts[reason] = int(reason_counts.get(reason, 0)) + 1
        total_rejected = int(len(rejected))
        dominant_reason = max(reason_counts, key=reason_counts.get) if reason_counts else ""
        dominant_ratio = (float(reason_counts.get(dominant_reason, 0)) / float(max(1, total_rejected))) if total_rejected > 0 else 0.0
        reject_rate_pct = (100.0 * total_rejected / float(max(1, len(universe)))) if universe else 0.0
        reject_warn_pct = max(10.0, float(settings.get("forex_reject_drift_warn_pct", 65.0) or 65.0))
        drift_warning = bool((reject_rate_pct >= reject_warn_pct) and (dominant_ratio >= 0.60))
        return {
            "state": "READY",
            "ai_state": "Scan ready",
            "msg": msg,
            "universe": list(candidates),
            "leaders": leaders[:10],
            "all_scores": scored[:40],
            "top_pick": top_pick,
            "top_chart": top_chart,
            "adaptive_threshold": adaptive_threshold,
            "updated_at": ts_now,
            "rejected": rejected[:30],
            "reject_summary": {
                "total_rejected": total_rejected,
                "reject_rate_pct": round(reject_rate_pct, 2),
                "dominant_reason": dominant_reason,
                "dominant_ratio_pct": round(dominant_ratio * 100.0, 2),
                "counts": reason_counts,
            },
            "health": {"data_ok": True, "broker_ok": True, "orders_ok": True, "drift_warning": drift_warning},
        }
    except urllib.error.HTTPError as exc:
        _append_jsonl(_rankings_path(hub_dir), {"ts": ts_now, "state": "ERROR", "reason": f"HTTP {exc.code}: {exc.reason}"})
        return {
            "state": "ERROR",
            "ai_state": "HTTP error",
            "msg": f"HTTP {exc.code}: {exc.reason}",
            "universe": list(universe),
            "leaders": [],
            "all_scores": [],
            "updated_at": ts_now,
        }
    except urllib.error.URLError as exc:
        _append_jsonl(_rankings_path(hub_dir), {"ts": ts_now, "state": "ERROR", "reason": f"Network error: {exc.reason}"})
        return {
            "state": "ERROR",
            "ai_state": "Network error",
            "msg": f"Network error: {exc.reason}",
            "universe": list(universe),
            "leaders": [],
            "all_scores": [],
            "updated_at": ts_now,
        }
    except Exception as exc:
        _append_jsonl(_rankings_path(hub_dir), {"ts": ts_now, "state": "ERROR", "reason": f"{type(exc).__name__}: {exc}"})
        return {
            "state": "ERROR",
            "ai_state": "Scan failed",
            "msg": f"{type(exc).__name__}: {exc}",
            "universe": list(universe),
            "leaders": [],
            "all_scores": [],
            "updated_at": ts_now,
        }


def main() -> int:
    print("forex_thinker.py is designed to be imported by the hub/runner first.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
