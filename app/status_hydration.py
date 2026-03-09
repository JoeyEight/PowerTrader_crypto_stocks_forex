from __future__ import annotations

import json
from typing import Any, Dict, List

from app.scan_diagnostics_schema import normalize_scan_diagnostics


def safe_read_json_dict(path: str) -> Dict[str, Any]:
    if not str(path or "").strip():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def safe_read_jsonl_dicts(path: str, limit: int = 200) -> List[Dict[str, Any]]:
    if not str(path or "").strip():
        return []
    rows: List[Dict[str, Any]] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for ln in f:
                txt = str(ln or "").strip()
                if not txt:
                    continue
                try:
                    obj = json.loads(txt)
                except Exception:
                    continue
                if isinstance(obj, dict):
                    rows.append(obj)
    except Exception:
        return []
    lim = max(1, int(limit or 200))
    return rows[-lim:]


def load_market_status_bundle(
    *,
    status_path: str,
    trader_path: str,
    thinker_path: str,
    scan_diag_path: str,
    history_path: str = "",
    history_limit: int = 120,
    market_key: str = "",
) -> Dict[str, Any]:
    guessed_market = str(market_key or "").strip().lower()
    if not guessed_market:
        path_low = str(scan_diag_path or "").lower()
        if "/stocks/" in path_low or "\\stocks\\" in path_low:
            guessed_market = "stocks"
        elif "/forex/" in path_low or "\\forex\\" in path_low:
            guessed_market = "forex"
    return {
        "status": safe_read_json_dict(status_path),
        "trader": safe_read_json_dict(trader_path),
        "thinker": safe_read_json_dict(thinker_path),
        "scan_diagnostics": normalize_scan_diagnostics(safe_read_json_dict(scan_diag_path), market=guessed_market),
        "history": safe_read_jsonl_dicts(history_path, limit=history_limit),
    }
