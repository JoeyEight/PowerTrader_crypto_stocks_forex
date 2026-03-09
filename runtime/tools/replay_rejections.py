from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any, Dict

_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if __package__ in (None, ""):
    if _ROOT not in sys.path:
        sys.path.insert(0, _ROOT)

from app.path_utils import read_settings_file, resolve_runtime_paths, resolve_settings_path  # noqa: E402
from app.rejection_replay import build_market_rejection_replay, build_rejection_replay_report  # noqa: E402
from app.settings_utils import sanitize_settings  # noqa: E402


def _safe_write_json(path: str, payload: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=True)
    os.replace(tmp, path)


def _default_out_path(hub_dir: str, market: str) -> str:
    m = str(market or "both").strip().lower()
    if m in {"stocks", "forex"}:
        return os.path.join(hub_dir, f"rejection_replay_{m}.json")
    return os.path.join(hub_dir, "rejection_replay.json")


def main() -> int:
    ap = argparse.ArgumentParser(description="Replay rejected/scored candidates for threshold tuning.")
    ap.add_argument("--market", default="both", choices=["both", "stocks", "forex"])
    ap.add_argument("--output", default="", help="Optional output JSON path.")
    args = ap.parse_args()

    probe = os.path.join(_ROOT, "runtime", "pt_runner.py")
    base_dir, _settings, hub_dir, _ = resolve_runtime_paths(probe, "replay_rejections")
    settings_path = resolve_settings_path(base_dir)
    raw = read_settings_file(settings_path, module_name="replay_rejections") or {}
    settings = sanitize_settings(raw if isinstance(raw, dict) else {})

    market = str(args.market or "both").strip().lower()
    if market == "both":
        payload = build_rejection_replay_report(hub_dir, settings)
    else:
        payload = {"ts": int(time.time()), market: build_market_rejection_replay(hub_dir, market, settings=settings)}

    out_path = str(args.output or "").strip()
    if not out_path:
        out_path = _default_out_path(hub_dir, market)
    if not os.path.isabs(out_path):
        out_path = os.path.abspath(os.path.join(base_dir, out_path))
    _safe_write_json(out_path, payload if isinstance(payload, dict) else {})

    summary: Dict[str, Any] = {"ts": int(time.time()), "market": market, "output": out_path}
    for mk in ("stocks", "forex"):
        if mk not in payload or not isinstance(payload.get(mk), dict):
            continue
        row = payload.get(mk, {}) if isinstance(payload.get(mk, {}), dict) else {}
        rec = row.get("recommendation", {}) if isinstance(row.get("recommendation", {}), dict) else {}
        summary[mk] = {
            "state": str(row.get("state", "") or ""),
            "current_threshold": float(row.get("current_threshold", 0.0) or 0.0),
            "recommended_threshold": float(rec.get("recommended_threshold", 0.0) or 0.0),
            "scored_rows": int(row.get("scored_rows", 0) or 0),
        }
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
