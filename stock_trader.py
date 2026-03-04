from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List

from broker_alpaca import AlpacaBrokerClient
from path_utils import resolve_runtime_paths

BASE_DIR, _SETTINGS_PATH, HUB_DATA_DIR, _BOOT_SETTINGS = resolve_runtime_paths(__file__, "stock_trader")
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


def _parse_positions(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol", "") or "").strip().upper()
        if not symbol:
            continue
        try:
            qty = float(row.get("qty", 0.0) or 0.0)
        except Exception:
            qty = 0.0
        try:
            avg = float(row.get("avg_entry_price", 0.0) or 0.0)
        except Exception:
            avg = 0.0
        try:
            mv = float(row.get("market_value", 0.0) or 0.0)
        except Exception:
            mv = 0.0
        out[symbol] = {"symbol": symbol, "qty": qty, "avg_entry_price": avg, "market_value": mv}
    return out


def run_step(settings: Dict[str, Any], hub_dir: str) -> Dict[str, Any]:
    stocks_dir = os.path.join(hub_dir, "stocks")
    os.makedirs(stocks_dir, exist_ok=True)
    thinker_path = os.path.join(stocks_dir, "stock_thinker_status.json")
    state_path = os.path.join(stocks_dir, "stock_trader_state.json")

    auto_enabled = bool(settings.get("stock_auto_trade_enabled", False))
    trade_notional = max(1.0, float(settings.get("stock_trade_notional_usd", 100.0) or 100.0))
    max_open_positions = max(1, int(float(settings.get("stock_max_open_positions", 1) or 1)))
    score_threshold = max(0.0, float(settings.get("stock_score_threshold", 0.2) or 0.2))
    profit_target_pct = max(0.0, float(settings.get("stock_profit_target_pct", 0.35) or 0.35))
    trailing_gap_pct = max(0.0, float(settings.get("stock_trailing_gap_pct", 0.2) or 0.2))
    max_day_trades = max(0, int(float(settings.get("stock_max_day_trades", 3) or 3)))
    enable_exec_v2 = _rollout_at_least(settings, "execution_v2")
    enable_risk_caps = _rollout_at_least(settings, "risk_caps")
    max_pos_usd = max(0.0, float(settings.get("stock_max_position_usd_per_symbol", 0.0) or 0.0))
    max_total_exposure_pct = max(0.0, float(settings.get("stock_max_total_exposure_pct", 0.0) or 0.0))

    client = AlpacaBrokerClient(
        api_key_id=str(settings.get("alpaca_api_key_id", "") or ""),
        secret_key=str(settings.get("alpaca_secret_key", "") or ""),
        base_url=str(settings.get("alpaca_base_url", "https://paper-api.alpaca.markets") or ""),
        data_url=str(settings.get("alpaca_data_url", "https://data.alpaca.markets") or ""),
    )
    if not client.configured():
        return {
            "state": "IDLE",
            "trader_state": "Credentials missing",
            "msg": "Alpaca credentials not configured",
            "auto_enabled": auto_enabled,
            "updated_at": int(time.time()),
        }

    thinker = _safe_read_json(thinker_path)
    top_pick = thinker.get("top_pick", {}) or {}
    if not isinstance(top_pick, dict):
        top_pick = {}

    now_ts = int(time.time())
    today = time.strftime("%Y-%m-%d", time.localtime(now_ts))
    state = _safe_read_json(state_path)
    trail_state = state.get("trail", {}) or {}
    opened_today = state.get("opened_today", {}) or {}
    day_trades = state.get("day_trades", {}) or {}
    if not isinstance(trail_state, dict):
        trail_state = {}
    if not isinstance(opened_today, dict):
        opened_today = {}
    if not isinstance(day_trades, dict):
        day_trades = {}

    # Retain only today's day-trade counter.
    day_trades_today = int(float(day_trades.get(today, 0) or 0))
    day_trades = {today: day_trades_today}
    opened_today = {str(k).upper(): int(v) for k, v in opened_today.items() if str(k).strip()}

    raw_positions = client.list_positions()
    positions = _parse_positions(raw_positions)
    all_symbols = set(positions.keys())
    top_symbol = str(top_pick.get("symbol", "") or "").strip().upper()
    if top_symbol:
        all_symbols.add(top_symbol)
    prices = client.get_mid_prices(sorted(all_symbols))
    acct = client.get_account_summary()
    equity = float(acct.get("equity", 0.0) or 0.0) if isinstance(acct, dict) else 0.0
    total_positions_value = 0.0
    for pos in positions.values():
        total_positions_value += max(0.0, float(pos.get("market_value", 0.0) or 0.0))

    actions: List[str] = []
    for symbol, pos in positions.items():
        qty = float(pos.get("qty", 0.0) or 0.0)
        avg = float(pos.get("avg_entry_price", 0.0) or 0.0)
        mid = float(prices.get(symbol, 0.0) or 0.0)
        if qty <= 0 or avg <= 0 or mid <= 0:
            continue
        pnl = ((mid - avg) / avg) * 100.0
        st = trail_state.get(symbol, {}) or {}
        armed = bool(st.get("armed", False))
        peak = float(st.get("peak_pct", pnl) or pnl)
        if pnl >= profit_target_pct:
            armed = True
            peak = max(peak, pnl)
        if armed:
            peak = max(peak, pnl)
            if pnl <= (peak - trailing_gap_pct):
                ok, msg, _ = client.close_position(symbol)
                actions.append(f"CLOSE {symbol} | {'OK' if ok else 'FAIL'} | {msg}")
                if ok:
                    trail_state.pop(symbol, None)
                    if symbol in opened_today:
                        day_trades_today += 1
                        day_trades[today] = day_trades_today
                        opened_today.pop(symbol, None)
                else:
                    trail_state[symbol] = {"armed": armed, "peak_pct": peak, "last_pnl_pct": pnl, "updated_at": now_ts}
                continue
        trail_state[symbol] = {"armed": armed, "peak_pct": peak, "last_pnl_pct": pnl, "updated_at": now_ts}

    entry_msg = "Auto-trade disabled (paper-safe)"
    if auto_enabled:
        score = float(top_pick.get("score", 0.0) or 0.0)
        side = str(top_pick.get("side", "watch") or "watch").strip().lower()
        if not top_symbol:
            entry_msg = "No top symbol from thinker"
        elif side != "long":
            entry_msg = f"Top pick {top_symbol} is {side.upper()}"
        elif score < score_threshold:
            entry_msg = f"Top score below threshold ({score:.4f} < {score_threshold:.4f})"
        elif len(positions) >= max_open_positions and top_symbol not in positions:
            entry_msg = f"Max open positions reached ({len(positions)}/{max_open_positions})"
        elif top_symbol in positions:
            entry_msg = f"Already in position: {top_symbol}"
        elif max_day_trades > 0 and day_trades_today >= max_day_trades:
            entry_msg = f"PDT guard: day-trade cap reached ({day_trades_today}/{max_day_trades})"
        elif enable_risk_caps and max_pos_usd > 0.0 and (float(positions.get(top_symbol, {}).get("market_value", 0.0) or 0.0) + trade_notional) > max_pos_usd:
            entry_msg = f"Risk cap: {top_symbol} projected position exceeds ${max_pos_usd:.2f}"
        elif enable_risk_caps and max_total_exposure_pct > 0.0 and equity > 0.0 and (((total_positions_value + trade_notional) / equity) * 100.0) > max_total_exposure_pct:
            entry_msg = f"Risk cap: projected exposure exceeds {max_total_exposure_pct:.2f}%"
        elif not enable_exec_v2:
            entry_msg = "Execution gated by rollout stage (set execution_v2)"
        else:
            ok, msg, _ = client.place_market_order(top_symbol, side="buy", notional=trade_notional)
            actions.append(f"ENTRY {top_symbol} BUY ${trade_notional:.2f} | {'OK' if ok else 'FAIL'} | {msg}")
            entry_msg = f"Entry {'placed' if ok else 'failed'} for {top_symbol}"
            if ok:
                opened_today[top_symbol] = now_ts
    else:
        entry_msg = "Auto-trade disabled (paper-safe)"

    out_state = {
        "trail": trail_state,
        "opened_today": opened_today,
        "day_trades": day_trades,
        "last_actions": actions[-50:],
        "updated_at": now_ts,
    }
    _safe_write_json(state_path, out_state)

    msg_parts = [entry_msg]
    if actions:
        msg_parts.append(actions[-1])
    return {
        "state": "READY",
        "trader_state": ("Paper auto-run" if auto_enabled else "Paper manual-ready"),
        "msg": " | ".join(x for x in msg_parts if x),
        "actions": actions[-8:],
        "open_positions": len(positions),
        "auto_enabled": auto_enabled,
        "day_trades_today": day_trades_today,
        "rollout_stage": str(settings.get("market_rollout_stage", "legacy") or "legacy"),
        "execution_enabled": enable_exec_v2,
        "updated_at": now_ts,
    }


def main() -> int:
    print("stock_trader.py is designed to be imported by the hub/runner first.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
