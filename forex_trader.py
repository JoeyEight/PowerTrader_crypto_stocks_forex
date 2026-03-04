from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List, Tuple

from broker_oanda import OandaBrokerClient
from path_utils import resolve_runtime_paths

BASE_DIR, _SETTINGS_PATH, HUB_DATA_DIR, _BOOT_SETTINGS = resolve_runtime_paths(__file__, "forex_trader")
ROLLOUT_ORDER = {"legacy": 0, "scan_expanded": 1, "risk_caps": 2, "execution_v2": 3}


def _safe_read_json(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _safe_write_json(path: str, data: Dict[str, Any]) -> None:
    try:
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, path)
    except Exception:
        pass


def _rollout_at_least(settings: Dict[str, Any], stage: str) -> bool:
    cur = str(settings.get("market_rollout_stage", "legacy") or "legacy").strip().lower()
    return int(ROLLOUT_ORDER.get(cur, 0)) >= int(ROLLOUT_ORDER.get(stage, 0))


def _parse_positions(raw_positions: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in raw_positions or []:
        if not isinstance(row, dict):
            continue
        inst = str(row.get("instrument", "") or "").strip().upper()
        if not inst:
            continue
        long_leg = row.get("long", {}) or {}
        short_leg = row.get("short", {}) or {}
        try:
            long_units = float(long_leg.get("units", 0.0) or 0.0)
        except Exception:
            long_units = 0.0
        try:
            short_units = float(short_leg.get("units", 0.0) or 0.0)
        except Exception:
            short_units = 0.0
        try:
            long_avg = float(long_leg.get("averagePrice", 0.0) or 0.0)
        except Exception:
            long_avg = 0.0
        try:
            short_avg = float(short_leg.get("averagePrice", 0.0) or 0.0)
        except Exception:
            short_avg = 0.0
        out[inst] = {
            "instrument": inst,
            "long_units": long_units,
            "short_units": short_units,
            "long_avg": long_avg,
            "short_avg": short_avg,
        }
    return out


def _pnl_pct(position: Dict[str, Any], mid_px: float) -> Tuple[str, float]:
    lu = float(position.get("long_units", 0.0) or 0.0)
    su = float(position.get("short_units", 0.0) or 0.0)
    if lu > 0:
        avg = float(position.get("long_avg", 0.0) or 0.0)
        if avg > 0:
            return "long", ((mid_px - avg) / avg) * 100.0
        return "long", 0.0
    if su < 0:
        avg = float(position.get("short_avg", 0.0) or 0.0)
        if avg > 0:
            return "short", ((avg - mid_px) / avg) * 100.0
        return "short", 0.0
    return "flat", 0.0


def run_step(settings: Dict[str, Any], hub_dir: str) -> Dict[str, Any]:
    forex_dir = os.path.join(hub_dir, "forex")
    os.makedirs(forex_dir, exist_ok=True)
    thinker_path = os.path.join(forex_dir, "forex_thinker_status.json")
    state_path = os.path.join(forex_dir, "forex_trader_state.json")

    auto_enabled = bool(settings.get("forex_auto_trade_enabled", False))
    trade_units = int(float(settings.get("forex_trade_units", 1000) or 1000))
    max_open_positions = max(1, int(float(settings.get("forex_max_open_positions", 1) or 1)))
    score_threshold = float(settings.get("forex_score_threshold", 0.2) or 0.2)
    profit_target_pct = float(settings.get("forex_profit_target_pct", 0.25) or 0.25)
    trailing_gap_pct = float(settings.get("forex_trailing_gap_pct", 0.15) or 0.15)
    enable_exec_v2 = _rollout_at_least(settings, "execution_v2")
    enable_risk_caps = _rollout_at_least(settings, "risk_caps")
    max_total_exposure_pct = max(0.0, float(settings.get("forex_max_total_exposure_pct", 0.0) or 0.0))

    client = OandaBrokerClient(
        account_id=str(settings.get("oanda_account_id", "") or ""),
        api_token=str(settings.get("oanda_api_token", "") or ""),
        rest_url=str(settings.get("oanda_rest_url", "https://api-fxpractice.oanda.com") or ""),
    )
    if not client.configured():
        return {
            "state": "IDLE",
            "trader_state": "Credentials missing",
            "msg": "OANDA credentials not configured",
            "auto_enabled": auto_enabled,
            "updated_at": int(time.time()),
        }

    broker_snap = client.fetch_snapshot()
    raw_positions = list(broker_snap.get("raw_positions", []) or [])
    positions = _parse_positions(raw_positions)
    thinker = _safe_read_json(thinker_path)
    top_pick = thinker.get("top_pick", {}) or {}
    if not isinstance(top_pick, dict):
        top_pick = {}

    state = _safe_read_json(state_path)
    trail_state = state.get("trail", {}) or {}
    if not isinstance(trail_state, dict):
        trail_state = {}
    actions: List[str] = []
    now_ts = int(time.time())

    all_instruments = set(positions.keys())
    top_inst = str(top_pick.get("pair", "") or "").strip().upper()
    if top_inst:
        all_instruments.add(top_inst)
    prices = client.get_mid_prices(sorted(all_instruments))

    # Manage open positions with profit-arm + trailing-exit behavior.
    for inst, pos in positions.items():
        mid_px = float(prices.get(inst, 0.0) or 0.0)
        if mid_px <= 0:
            continue
        side, pnl = _pnl_pct(pos, mid_px)
        st = trail_state.get(inst, {}) or {}
        armed = bool(st.get("armed", False))
        peak = float(st.get("peak_pct", pnl) or pnl)
        if pnl >= profit_target_pct:
            armed = True
            peak = max(peak, pnl)
        if armed:
            peak = max(peak, pnl)
            if pnl <= (peak - trailing_gap_pct):
                close_side = "long" if side == "long" else "short"
                ok, msg, _ = client.close_position(inst, side=close_side)
                actions.append(f"CLOSE {inst} {close_side} | {'OK' if ok else 'FAIL'} | {msg}")
                if ok:
                    trail_state.pop(inst, None)
                else:
                    trail_state[inst] = {"armed": armed, "peak_pct": peak, "last_pnl_pct": pnl, "updated_at": now_ts}
                continue
        trail_state[inst] = {"armed": armed, "peak_pct": peak, "last_pnl_pct": pnl, "updated_at": now_ts}

    # Entry on strongest thinker pick when auto mode is on.
    entry_msg = "Auto-trade disabled"
    if auto_enabled:
        score = float(top_pick.get("score", 0.0) or 0.0)
        side = str(top_pick.get("side", "watch") or "watch").strip().lower()
        if not top_inst:
            entry_msg = "No top pair from thinker"
        elif side not in ("long", "short"):
            entry_msg = f"Top pair {top_inst} is WATCH"
        elif abs(score) < score_threshold:
            entry_msg = f"Top score below threshold ({score:.4f} < {score_threshold:.4f})"
        elif len(positions) >= max_open_positions and top_inst not in positions:
            entry_msg = f"Max open positions reached ({len(positions)}/{max_open_positions})"
        elif top_inst in positions:
            entry_msg = f"Already in position: {top_inst}"
        elif enable_risk_caps and max_total_exposure_pct > 0.0:
            nav_txt = str((broker_snap or {}).get("msg", "") or "")
            nav = 0.0
            try:
                if "NAV" in nav_txt:
                    nav = float(nav_txt.split("NAV", 1)[1].strip().split(" ", 1)[0])
            except Exception:
                nav = 0.0
            if nav > 0.0:
                held_pct = (float(len(positions)) / float(max_open_positions or 1)) * 100.0
                if held_pct >= max_total_exposure_pct and top_inst not in positions:
                    entry_msg = f"Risk cap: exposure proxy {held_pct:.2f}% exceeds {max_total_exposure_pct:.2f}%"
                    top_inst = ""
        elif not enable_exec_v2:
            entry_msg = "Execution gated by rollout stage (set execution_v2)"
        else:
            units = abs(trade_units)
            if side == "short":
                units = -units
            ok, msg, _ = client.place_market_order(top_inst, units)
            actions.append(f"ENTRY {top_inst} {side.upper()} units={units} | {'OK' if ok else 'FAIL'} | {msg}")
            entry_msg = f"Entry {'placed' if ok else 'failed'} for {top_inst}"
    else:
        entry_msg = "Auto-trade disabled (practice-safe)"

    out_state = {
        "trail": trail_state,
        "last_actions": actions[-50:],
        "updated_at": now_ts,
    }
    _safe_write_json(state_path, out_state)

    msg_parts = [entry_msg]
    if actions:
        msg_parts.append(actions[-1])

    return {
        "state": "READY",
        "trader_state": ("Practice auto-run" if auto_enabled else "Practice manual-ready"),
        "msg": " | ".join(x for x in msg_parts if x),
        "actions": actions[-8:],
        "open_positions": len(positions),
        "auto_enabled": auto_enabled,
        "rollout_stage": str(settings.get("market_rollout_stage", "legacy") or "legacy"),
        "execution_enabled": enable_exec_v2,
        "updated_at": now_ts,
    }


def main() -> int:
    print("forex_trader.py is designed to be imported by the hub/runner first.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
