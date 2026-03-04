from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from broker_oanda import OandaBrokerClient
from path_utils import resolve_runtime_paths

BASE_DIR, _SETTINGS_PATH, HUB_DATA_DIR, _BOOT_SETTINGS = resolve_runtime_paths(__file__, "forex_trader")
ROLLOUT_ORDER = {
    "legacy": 0,
    "scan_expanded": 1,
    "risk_caps": 2,
    "execution_v2": 3,
    "shadow_only": 4,
    "live_guarded": 5,
}


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


def _append_jsonl(path: str, row: Dict[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, separators=(",", ":")) + "\n")
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


def _session_blocked(settings: Dict[str, Any]) -> bool:
    mode = str(settings.get("forex_session_mode", "all") or "all").strip().lower()
    if mode in {"all", ""}:
        return False
    now_utc = datetime.now(timezone.utc)
    hour = now_utc.hour
    if mode == "london_ny":
        return not (12 <= hour <= 16)
    if mode == "london":
        return not (7 <= hour <= 15)
    if mode == "ny":
        return not (12 <= hour <= 20)
    if mode == "asia":
        return not (0 <= hour <= 8)
    return False


def _daily_loss_guard_triggered(audit_path: str, max_loss_usd: float, max_loss_pct: float, nav: float) -> bool:
    if max_loss_usd <= 0.0 and max_loss_pct <= 0.0:
        return False
    today = time.strftime("%Y-%m-%d", time.localtime())
    loss_usd = 0.0
    try:
        with open(audit_path, "r", encoding="utf-8") as f:
            for ln in f:
                ln = ln.strip()
                if not ln:
                    continue
                try:
                    row = json.loads(ln)
                except Exception:
                    continue
                if str(row.get("date", "")) != today:
                    continue
                if str(row.get("event", "")).lower() not in {"exit", "shadow_exit"}:
                    continue
                pnl_usd = float(row.get("pnl_usd", 0.0) or 0.0)
                if pnl_usd < 0:
                    loss_usd += abs(pnl_usd)
    except Exception:
        return False
    if max_loss_usd > 0.0 and loss_usd >= max_loss_usd:
        return True
    if max_loss_pct > 0.0 and nav > 0.0 and ((loss_usd / nav) * 100.0) >= max_loss_pct:
        return True
    return False


def _parse_order_id(msg: str, payload: Dict[str, Any]) -> str:
    oid = ""
    try:
        oid = str((payload or {}).get("orderFillTransaction", {}).get("id", "") or "").strip()
    except Exception:
        oid = ""
    if oid:
        return oid
    txt = str(msg or "")
    if "order_id=" in txt:
        return txt.split("order_id=", 1)[1].strip().split(" ", 1)[0]
    return ""


def _safe_float_from_dict(d: Dict[str, Any], keys: List[str]) -> float:
    for k in keys:
        try:
            if k in d:
                return float(d.get(k, 0.0) or 0.0)
        except Exception:
            continue
    return 0.0


def _crypto_holdings_usd(hub_dir: str) -> float:
    data = _safe_read_json(os.path.join(hub_dir, "trader_status.json"))
    if not isinstance(data, dict):
        return 0.0
    return _safe_float_from_dict(
        data,
        [
            "total_holdings_value",
            "holdings_value",
            "holdingsValue",
            "holdings_usd",
        ],
    )


def _market_status_exposure_usd(hub_dir: str, market_key: str) -> float:
    if market_key == "stocks":
        path = os.path.join(hub_dir, "stocks", "stock_trader_status.json")
    elif market_key == "forex":
        path = os.path.join(hub_dir, "forex", "forex_trader_status.json")
    else:
        path = os.path.join(hub_dir, market_key, f"{market_key}_trader_status.json")
    data = _safe_read_json(path)
    if not isinstance(data, dict):
        return 0.0
    return _safe_float_from_dict(data, ["exposure_usd", "total_positions_value_usd", "positions_value_usd"])


def run_step(settings: Dict[str, Any], hub_dir: str) -> Dict[str, Any]:
    forex_dir = os.path.join(hub_dir, "forex")
    os.makedirs(forex_dir, exist_ok=True)
    thinker_path = os.path.join(forex_dir, "forex_thinker_status.json")
    state_path = os.path.join(forex_dir, "forex_trader_state.json")
    audit_path = os.path.join(forex_dir, "execution_audit.jsonl")
    health_path = os.path.join(forex_dir, "health_status.json")

    auto_enabled = bool(settings.get("forex_auto_trade_enabled", False))
    trade_units = int(float(settings.get("forex_trade_units", 1000) or 1000))
    max_open_positions = max(1, int(float(settings.get("forex_max_open_positions", 1) or 1)))
    score_threshold = float(settings.get("forex_score_threshold", 0.2) or 0.2)
    guarded_score_mult = max(1.0, float(settings.get("forex_live_guarded_score_mult", 1.15) or 1.15))
    profit_target_pct = float(settings.get("forex_profit_target_pct", 0.25) or 0.25)
    trailing_gap_pct = float(settings.get("forex_trailing_gap_pct", 0.15) or 0.15)
    max_total_exposure_pct = max(0.0, float(settings.get("forex_max_total_exposure_pct", 0.0) or 0.0))
    max_pos_usd = max(0.0, float(settings.get("forex_max_position_usd_per_pair", 0.0) or 0.0))
    max_daily_loss_usd = max(0.0, float(settings.get("forex_max_daily_loss_usd", 0.0) or 0.0))
    max_daily_loss_pct = max(0.0, float(settings.get("forex_max_daily_loss_pct", 0.0) or 0.0))
    stage = str(settings.get("market_rollout_stage", "legacy") or "legacy").strip().lower()
    enable_exec_v2 = _rollout_at_least(settings, "execution_v2")
    enable_risk_caps = _rollout_at_least(settings, "risk_caps")
    shadow_only = stage == "shadow_only"
    live_guarded = stage == "live_guarded"

    client = OandaBrokerClient(
        account_id=str(settings.get("oanda_account_id", "") or ""),
        api_token=str(settings.get("oanda_api_token", "") or ""),
        rest_url=str(settings.get("oanda_rest_url", "https://api-fxpractice.oanda.com") or ""),
    )
    now_ts = int(time.time())
    if not client.configured():
        return {
            "state": "IDLE",
            "trader_state": "Credentials missing",
            "msg": "OANDA credentials not configured",
            "auto_enabled": auto_enabled,
            "updated_at": now_ts,
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
    cooldown_until = state.get("cooldown_until", {}) or {}
    loss_streak = int(float(state.get("loss_streak", 0) or 0))
    last_divergence_ts = int(float(state.get("last_divergence_ts", 0) or 0))
    last_divergence_msg = str(state.get("last_divergence_msg", "") or "")
    open_meta = state.get("open_meta", {}) or {}
    pending = state.get("pending", {}) or {}
    if not isinstance(trail_state, dict):
        trail_state = {}
    if not isinstance(cooldown_until, dict):
        cooldown_until = {}
    if not isinstance(open_meta, dict):
        open_meta = {}
    if not isinstance(pending, dict):
        pending = {}
    cooldown_until = {str(k).upper(): float(v) for k, v in cooldown_until.items() if str(k).strip()}
    open_meta = {str(k).upper(): (v if isinstance(v, dict) else {}) for k, v in open_meta.items() if str(k).strip()}
    actions: List[str] = []
    thinker_health = thinker.get("health", {}) if isinstance(thinker, dict) else {}
    thinker_data_ok = bool((thinker_health or {}).get("data_ok", True))
    drift_warning = False
    all_instruments = set(positions.keys())
    top_inst = str(top_pick.get("pair", "") or "").strip().upper()
    if top_inst:
        all_instruments.add(top_inst)
    prices = client.get_mid_prices(sorted(all_instruments))

    nav = 0.0
    nav_text = str((broker_snap or {}).get("msg", "") or "")
    try:
        if "NAV" in nav_text:
            nav = float(nav_text.split("NAV", 1)[1].strip().split(" ", 1)[0])
    except Exception:
        nav = 0.0

    total_exposure_usd = 0.0
    for inst, pos in positions.items():
        mid_px = float(prices.get(inst, 0.0) or 0.0)
        if mid_px <= 0:
            continue
        lu = abs(float(pos.get("long_units", 0.0) or 0.0))
        su = abs(float(pos.get("short_units", 0.0) or 0.0))
        total_exposure_usd += (lu + su) * mid_px

    today = time.strftime("%Y-%m-%d", time.localtime(now_ts))
    if pending:
        p_inst = str(pending.get("instrument", "") or "").strip().upper()
        p_ts = float(pending.get("ts", 0.0) or 0.0)
        if p_inst and p_inst in positions:
            actions.append(f"RECONCILE OK {p_inst} reflected")
            pending = {}
        elif p_inst and (now_ts - p_ts) > 90:
            drift_warning = True
            actions.append(f"RECONCILE WARN {p_inst} not reflected")
            _append_jsonl(
                audit_path,
                {"ts": now_ts, "date": today, "event": "reconcile_warning", "instrument": p_inst, "msg": "pending timed out"},
            )
            pending = {}
    for inst, pos in positions.items():
        mid_px = float(prices.get(inst, 0.0) or 0.0)
        if mid_px <= 0:
            continue
        side, pnl = _pnl_pct(pos, mid_px)
        meta = open_meta.get(inst, {}) or {}
        mfe = max(float(meta.get("mfe_pct", pnl) or pnl), pnl)
        mae = min(float(meta.get("mae_pct", pnl) or pnl), pnl)
        entry_ts = float(meta.get("entry_ts", now_ts) or now_ts)
        open_meta[inst] = {"entry_ts": entry_ts, "mfe_pct": mfe, "mae_pct": mae, "last_pnl_pct": pnl}
        units = abs(float(pos.get("long_units", 0.0) or 0.0)) + abs(float(pos.get("short_units", 0.0) or 0.0))
        avg_px = float(pos.get("long_avg", 0.0) or 0.0) if side == "long" else float(pos.get("short_avg", 0.0) or 0.0)
        pnl_usd = ((mid_px - avg_px) * units) if (side == "long" and avg_px > 0) else ((avg_px - mid_px) * units if avg_px > 0 else 0.0)
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
                ok, msg, payload = client.close_position(inst, side=close_side)
                actions.append(f"CLOSE {inst} {close_side} | {'OK' if ok else 'FAIL'} | {msg}")
                _append_jsonl(
                    audit_path,
                    {
                        "ts": now_ts,
                        "date": today,
                        "event": "exit" if ok else "exit_fail",
                        "instrument": inst,
                        "side": close_side,
                        "units": units,
                        "price": mid_px,
                        "pnl_pct": pnl,
                        "pnl_usd": pnl_usd,
                        "mfe_pct": round(mfe, 4),
                        "mae_pct": round(mae, 4),
                        "hold_s": max(0, int(now_ts - entry_ts)),
                        "ok": ok,
                        "msg": msg,
                        "payload": payload if isinstance(payload, dict) else {},
                    },
                )
                if ok:
                    trail_state.pop(inst, None)
                    open_meta.pop(inst, None)
                    if pnl_usd < 0:
                        loss_streak += 1
                        cooldown_until[inst] = float(now_ts + max(60, int(float(settings.get("forex_loss_cooldown_seconds", 1800) or 1800))))
                    else:
                        loss_streak = 0
                else:
                    trail_state[inst] = {"armed": armed, "peak_pct": peak, "last_pnl_pct": pnl, "updated_at": now_ts}
                continue
        trail_state[inst] = {"armed": armed, "peak_pct": peak, "last_pnl_pct": pnl, "updated_at": now_ts}

    entry_msg = "Auto-trade disabled"
    if auto_enabled:
        score = float(top_pick.get("score", 0.0) or 0.0)
        calib_prob = float(top_pick.get("calib_prob", 0.0) or 0.0)
        sample_count = int(float(top_pick.get("samples", 0) or 0))
        bars_count = int(float(top_pick.get("bars_count", 0) or 0))
        side = str(top_pick.get("side", "watch") or "watch").strip().lower()
        signal_age_s = max(0, int(now_ts - int(float(thinker.get("updated_at", now_ts) or now_ts))))
        max_signal_age_s = max(30, int(float(settings.get("forex_max_signal_age_seconds", 300) or 300)))
        min_bars_required = max(8, int(float(settings.get("forex_min_bars_required", 24) or 24)))
        min_samples_guarded = max(0, int(float(settings.get("forex_min_samples_live_guarded", 5) or 5)))
        adaptive_thr = float(thinker.get("adaptive_threshold", score_threshold) or score_threshold)
        required_score = ((adaptive_thr if adaptive_thr > 0 else score_threshold) * (guarded_score_mult if live_guarded else 1.0))
        max_slippage_bps = max(0.0, float(settings.get("forex_max_slippage_bps", 6.0) or 6.0))
        max_loss_streak = max(0, int(float(settings.get("forex_max_loss_streak", 3) or 3)))
        global_cap_pct = max(0.0, float(settings.get("market_max_total_exposure_pct", 0.0) or 0.0))
        top_pricing = client.get_pricing_details([top_inst]) if top_inst else {}
        top_mid = float((top_pricing.get(top_inst, {}) or {}).get("mid", 0.0) or 0.0)
        top_spread_bps = float((top_pricing.get(top_inst, {}) or {}).get("spread_bps", 0.0) or 0.0)
        crypto_exposure_usd = _crypto_holdings_usd(hub_dir)
        stocks_exposure_usd = _market_status_exposure_usd(hub_dir, "stocks")
        projected_global_exposure = total_exposure_usd + max(0.0, abs(float(trade_units)) * max(top_mid, float(prices.get(top_inst, 0.0) or 0.0))) + crypto_exposure_usd + stocks_exposure_usd
        if live_guarded and (calib_prob <= 0.0):
            calib_prob = 0.5
        if not top_inst:
            entry_msg = "No top pair from thinker"
        elif signal_age_s > max_signal_age_s:
            entry_msg = f"Signal stale ({signal_age_s}s > {max_signal_age_s}s)"
        elif bars_count > 0 and bars_count < min_bars_required:
            entry_msg = f"Bars preflight failed ({bars_count} < {min_bars_required})"
        elif side not in ("long", "short"):
            entry_msg = f"Top pair {top_inst} is WATCH"
        elif not bool(top_pick.get("eligible_for_entry", True)):
            entry_msg = "Universe health gate: pair not in eligible bucket"
        elif not bool(top_pick.get("data_quality_ok", True)):
            entry_msg = "Data quality gate blocked entry"
        elif top_mid <= 0.0:
            entry_msg = "Quote preflight failed: missing mid price"
        elif live_guarded and sample_count < min_samples_guarded:
            entry_msg = f"Calibration sample gate ({sample_count} < {min_samples_guarded})"
        elif live_guarded and calib_prob < float(settings.get("forex_min_calib_prob_live_guarded", 0.56) or 0.56):
            entry_msg = f"Calibrated confidence gate ({calib_prob:.2f})"
        elif abs(score) < required_score:
            entry_msg = f"Top score below threshold ({score:.4f} < {required_score:.4f})"
        elif max_loss_streak > 0 and loss_streak >= max_loss_streak:
            entry_msg = f"Loss-streak guard active ({loss_streak}/{max_loss_streak})"
        elif float(cooldown_until.get(top_inst, 0.0) or 0.0) > float(now_ts):
            cd_left = int(float(cooldown_until.get(top_inst, 0.0) - now_ts))
            entry_msg = f"Cooldown active for {top_inst} ({cd_left}s)"
        elif len(positions) >= max_open_positions and top_inst not in positions:
            entry_msg = f"Max open positions reached ({len(positions)}/{max_open_positions})"
        elif top_inst in positions:
            entry_msg = f"Already in position: {top_inst}"
        elif _session_blocked(settings):
            entry_msg = "Session gate: blocked for current UTC hour"
        elif _daily_loss_guard_triggered(audit_path, max_daily_loss_usd, max_daily_loss_pct, nav):
            entry_msg = "Daily loss guard active: blocking new entries"
        elif max_slippage_bps > 0.0 and top_spread_bps > max_slippage_bps:
            entry_msg = f"Slippage guard: spread {top_spread_bps:.2f}bps > {max_slippage_bps:.2f}bps"
        elif enable_risk_caps and max_pos_usd > 0.0 and top_inst not in positions:
            est_entry_notional = abs(float(trade_units)) * float((top_pricing.get(top_inst, {}) or {}).get("mid", 0.0) or 0.0)
            if est_entry_notional <= 0.0:
                est_entry_notional = abs(float(trade_units)) * float(prices.get(top_inst, 0.0) or 0.0)
            if est_entry_notional > max_pos_usd:
                entry_msg = f"Risk cap: projected pair notional exceeds ${max_pos_usd:.2f}"
        elif enable_risk_caps and max_total_exposure_pct > 0.0 and nav > 0.0 and top_inst not in positions:
            est_entry_notional = abs(float(trade_units)) * float((top_pricing.get(top_inst, {}) or {}).get("mid", 0.0) or 0.0)
            if est_entry_notional <= 0.0:
                est_entry_notional = abs(float(trade_units)) * float(prices.get(top_inst, 0.0) or 0.0)
            projected_pct = ((total_exposure_usd + max(0.0, est_entry_notional)) / nav) * 100.0
            if projected_pct > max_total_exposure_pct:
                entry_msg = f"Risk cap: projected exposure exceeds {max_total_exposure_pct:.2f}%"
        elif global_cap_pct > 0.0 and nav > 0.0 and ((projected_global_exposure / nav) * 100.0) > global_cap_pct:
            entry_msg = f"Global cap: projected cross-market exposure exceeds {global_cap_pct:.2f}%"
        elif not enable_exec_v2:
            entry_msg = "Execution gated by rollout stage"
        elif shadow_only:
            entry_msg = f"SHADOW entry simulated for {top_inst}"
            actions.append(f"SHADOW ENTRY {top_inst} {side.upper()} units={abs(trade_units)}")
            _append_jsonl(
                audit_path,
                {
                    "ts": now_ts,
                    "date": today,
                    "event": "shadow_entry",
                    "instrument": top_inst,
                    "side": side,
                    "units": abs(trade_units),
                    "score": score,
                    "calib_prob": calib_prob,
                    "samples": sample_count,
                    "bars_count": bars_count,
                    "spread_bps": top_spread_bps,
                    "ok": True,
                    "msg": "shadow_only stage",
                },
            )
        else:
            units = abs(trade_units)
            if side == "short":
                units = -units
            client_id = f"ptfx-{top_inst}-{now_ts}"
            ok, msg, payload = client.place_market_order(
                top_inst,
                units,
                client_order_id=client_id,
                max_retries=max(1, int(float(settings.get("forex_order_retry_count", 2) or 2))),
            )
            actions.append(f"ENTRY {top_inst} {side.upper()} units={units} | {'OK' if ok else 'FAIL'} | {msg}")
            oid = _parse_order_id(msg, payload if isinstance(payload, dict) else {})
            _append_jsonl(
                audit_path,
                {
                    "ts": now_ts,
                    "date": today,
                    "event": "entry" if ok else "entry_fail",
                    "instrument": top_inst,
                    "side": side,
                    "units": units,
                    "score": score,
                    "calib_prob": calib_prob,
                    "samples": sample_count,
                    "bars_count": bars_count,
                    "spread_bps": top_spread_bps,
                    "client_order_id": client_id,
                    "order_id": oid,
                    "ok": ok,
                    "msg": msg,
                    "payload": payload if isinstance(payload, dict) else {},
                },
            )
            entry_msg = f"Entry {'placed' if ok else 'failed'} for {top_inst}"
            if ok:
                open_meta[top_inst] = {"entry_ts": now_ts, "mfe_pct": 0.0, "mae_pct": 0.0, "last_pnl_pct": 0.0}
                pending = {"instrument": top_inst, "side": side, "units": units, "ts": now_ts, "order_id": oid, "client_order_id": client_id}
                recon_positions = _parse_positions(list((client.fetch_snapshot() or {}).get("raw_positions", []) or []))
                if top_inst not in recon_positions:
                    drift_warning = True
                    _append_jsonl(
                        audit_path,
                        {
                            "ts": now_ts,
                            "date": today,
                            "event": "reconcile_warning",
                            "instrument": top_inst,
                            "msg": "submitted but not reflected in open positions",
                        },
                    )
    else:
        entry_msg = "Auto-trade disabled (practice-safe)"

    out_state = {
        "trail": trail_state,
        "cooldown_until": cooldown_until,
        "loss_streak": int(loss_streak),
        "open_meta": open_meta,
        "pending": pending,
        "last_divergence_ts": int(last_divergence_ts),
        "last_divergence_msg": str(last_divergence_msg),
        "last_actions": actions[-80:],
        "updated_at": now_ts,
    }
    _safe_write_json(state_path, out_state)
    _safe_write_json(
        health_path,
        {
            "ts": now_ts,
            "data_ok": thinker_data_ok,
            "broker_ok": True,
            "orders_ok": True,
            "drift_warning": drift_warning,
        },
    )

    msg_parts = [entry_msg]
    if actions:
        msg_parts.append(actions[-1])
    if auto_enabled and (not shadow_only):
        try:
            if side in {"long", "short"} and top_inst and ("Entry placed" not in entry_msg) and ("Already in position" not in entry_msg):
                should_log = ((now_ts - int(last_divergence_ts)) >= 300) or (str(entry_msg) != str(last_divergence_msg))
                if should_log:
                    _append_jsonl(
                        audit_path,
                        {
                            "ts": now_ts,
                            "date": today,
                            "event": "shadow_live_divergence",
                            "instrument": top_inst,
                            "side": side,
                            "score": float(top_pick.get("score", 0.0) or 0.0),
                            "msg": entry_msg,
                        },
                    )
                    last_divergence_ts = int(now_ts)
                    last_divergence_msg = str(entry_msg)
        except Exception:
            pass
    out_state["last_divergence_ts"] = int(last_divergence_ts)
    out_state["last_divergence_msg"] = str(last_divergence_msg)
    _safe_write_json(state_path, out_state)
    return {
        "state": "READY",
        "trader_state": ("Practice auto-run" if auto_enabled else "Practice manual-ready"),
        "msg": " | ".join(x for x in msg_parts if x),
        "actions": actions[-12:],
        "open_positions": len(positions),
        "auto_enabled": auto_enabled,
        "rollout_stage": str(settings.get("market_rollout_stage", "legacy") or "legacy"),
        "execution_enabled": enable_exec_v2 and (not shadow_only),
        "exposure_usd": round(total_exposure_usd, 4),
        "crypto_exposure_usd": round(crypto_exposure_usd, 4) if auto_enabled else round(_crypto_holdings_usd(hub_dir), 4),
        "other_market_exposure_usd": round(stocks_exposure_usd, 4) if auto_enabled else round(_market_status_exposure_usd(hub_dir, "stocks"), 4),
        "account_value_usd": round(nav, 4),
        "updated_at": now_ts,
        "health": {"data_ok": thinker_data_ok, "broker_ok": True, "orders_ok": True, "drift_warning": drift_warning},
    }


def main() -> int:
    print("forex_trader.py is designed to be imported by the hub/runner first.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
