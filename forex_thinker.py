from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List

from path_utils import resolve_runtime_paths

BASE_DIR, _SETTINGS_PATH, HUB_DATA_DIR, _BOOT_SETTINGS = resolve_runtime_paths(__file__, "forex_thinker")

DEFAULT_FX_UNIVERSE = ["EUR_USD", "GBP_USD", "USD_JPY", "AUD_USD", "USD_CAD", "EUR_JPY"]
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


def _parse_pairs(settings: Dict[str, Any]) -> List[str]:
    raw = str(settings.get("forex_universe_pairs", "") or "")
    out: List[str] = []
    for tok in raw.replace("\n", ",").split(","):
        p = tok.strip().upper()
        if p and p not in out:
            out.append(p)
    return out


def _score_candles(pair: str, candles: List[Dict[str, Any]]) -> Dict[str, Any]:
    closes = []
    for row in candles or []:
        if not isinstance(row, dict):
            continue
        if not bool(row.get("complete", True)):
            continue
        mid = row.get("mid", {}) or {}
        close_val = _float(mid.get("c", 0.0), 0.0)
        if close_val > 0:
            closes.append(close_val)
    if len(closes) < 8:
        return {
            "pair": pair,
            "score": -9999.0,
            "side": "watch",
            "last": closes[-1] if closes else 0.0,
            "change_6h_pct": 0.0,
            "change_24h_pct": 0.0,
            "volatility_pct": 0.0,
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

    score = (change_6 * 0.60) + (change_24 * 0.30) + (volatility * 0.10)
    side = "long" if score > 0 else "short"
    abs_score = abs(score)
    if abs_score >= 0.45:
        confidence = "HIGH"
    elif abs_score >= 0.20:
        confidence = "MED"
    else:
        confidence = "LOW"

    reason = f"6h {change_6:+.3f}% | 24h {change_24:+.3f}% | vol {volatility:.3f}%"
    return {
        "pair": pair,
        "score": round(score, 6),
        "side": side,
        "last": round(last_px, 6),
        "change_6h_pct": round(change_6, 6),
        "change_24h_pct": round(change_24, 6),
        "volatility_pct": round(volatility, 6),
        "confidence": confidence,
        "reason": reason,
    }


def run_scan(settings: Dict[str, Any], hub_dir: str) -> Dict[str, Any]:
    account_id = str(settings.get("oanda_account_id", "") or "").strip()
    token = str(settings.get("oanda_api_token", "") or "").strip()
    rest_url = str(settings.get("oanda_rest_url", "https://api-fxpractice.oanda.com") or "").strip().rstrip("/")
    if not account_id or not token or not rest_url:
        return {
            "state": "NOT CONFIGURED",
            "ai_state": "Credentials missing",
            "msg": "Add OANDA account/token in Settings",
            "universe": list(DEFAULT_FX_UNIVERSE),
            "leaders": [],
            "updated_at": int(time.time()),
        }

    universe = list(DEFAULT_FX_UNIVERSE)
    if _rollout_at_least(settings, "scan_expanded"):
        parsed = _parse_pairs(settings)
        if parsed:
            universe = parsed
    max_scan = max(4, int(float(settings.get("forex_scan_max_pairs", 16) or 16)))
    universe = universe[:max_scan]

    headers = {"Authorization": f"Bearer {token}"}
    scored = []
    try:
        for pair in universe:
            params = urllib.parse.urlencode(
                {
                    "price": "M",
                    "granularity": "H1",
                    "count": "36",
                }
            )
            url = f"{rest_url}/v3/instruments/{pair}/candles?{params}"
            payload = _request_json(url, headers=headers, timeout=10.0)
            candles = payload.get("candles", []) or []
            if not isinstance(candles, list):
                candles = []
            scored.append(_score_candles(pair, candles))
        scored.sort(key=lambda row: abs(float(row.get("score", 0.0))), reverse=True)
        leaders = scored[:5]
        top_pick = leaders[0] if leaders else None
        msg = "No FX leaders"
        if top_pick:
            msg = f"Top pair {top_pick['pair']} | {top_pick['side']} | {top_pick['reason']}"
        return {
            "state": "READY",
            "ai_state": "Scan ready",
            "msg": msg,
            "universe": list(universe),
            "leaders": leaders,
            "top_pick": top_pick,
            "updated_at": int(time.time()),
        }
    except urllib.error.HTTPError as exc:
        return {
            "state": "ERROR",
            "ai_state": "HTTP error",
            "msg": f"HTTP {exc.code}: {exc.reason}",
            "universe": list(universe),
            "leaders": [],
            "updated_at": int(time.time()),
        }
    except urllib.error.URLError as exc:
        return {
            "state": "ERROR",
            "ai_state": "Network error",
            "msg": f"Network error: {exc.reason}",
            "universe": list(universe),
            "leaders": [],
            "updated_at": int(time.time()),
        }
    except Exception as exc:
        return {
            "state": "ERROR",
            "ai_state": "Scan failed",
            "msg": f"{type(exc).__name__}: {exc}",
            "universe": list(universe),
            "leaders": [],
            "updated_at": int(time.time()),
        }


def main() -> int:
    print("forex_thinker.py is designed to be imported by the hub/runner first.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
