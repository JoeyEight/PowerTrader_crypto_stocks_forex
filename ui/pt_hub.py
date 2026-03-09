from __future__ import annotations
import os
import sys
import json
import csv
import time
import math
import textwrap
import queue
import threading
import subprocess
import shutil
import glob
import bisect
import signal
import zipfile
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import tkinter as tk
import tkinter.font as tkfont
from tkinter import ttk, filedialog, messagebox
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.patches import Rectangle
from matplotlib.ticker import FuncFormatter
from matplotlib.transforms import blended_transform_factory
from app.path_utils import resolve_runtime_paths, resolve_settings_path, read_settings_file, log_once
from app.settings_utils import sanitize_settings
from app.live_mode_guard import evaluate_live_mode_checklist
from app.market_awareness import build_awareness_payload
from app.status_hydration import load_market_status_bundle
from brokers.broker_alpaca import AlpacaBrokerClient
from brokers.broker_oanda import OandaBrokerClient
from app.credential_utils import (
    alpaca_credential_paths,
    get_alpaca_creds,
    get_oanda_creds,
    get_robinhood_creds_from_env,
    normalize_start_allocation_pct,
    oanda_credential_paths,
    robinhood_credential_paths,
    get_robinhood_creds_from_files,
)
from engines.stock_thinker import run_scan as run_stock_scan
from engines.forex_thinker import run_scan as run_forex_scan
from engines.stock_trader import run_step as run_stock_trader_step
from engines.forex_trader import run_step as run_forex_trader_step

DARK_BG = "#070B10"
DARK_BG2 = "#0B1220"
DARK_PANEL = "#0E1626"
DARK_PANEL2 = "#121C2F"
DARK_BORDER = "#243044"
DARK_FG = "#C7D1DB"
DARK_MUTED = "#8B949E"
DARK_ACCENT = "#00FF66"   
DARK_ACCENT2 = "#00E5FF"   
DARK_SELECT_BG = "#17324A"
DARK_SELECT_FG = "#00FF66"
AUTOFIX_REQUEST_MAX_CHARS = 8000
AUTOFIX_CHAT_TEMPLATES: Dict[str, str] = {
    "UI Layout parity": (
        "Update the Stocks and Forex charts to match Crypto UX behavior.\n"
        "Keep existing visual style, improve resize handling, and include tests."
    ),
    "Scanner reliability": (
        "Investigate scanner reliability issues.\n"
        "Analyze logs and improve fallback handling with minimal risk."
    ),
    "Execution explainability": (
        "Improve execution rationale on dashboard.\n"
        "Show plain-language reason first and detailed metrics on hover/tooltips."
    ),
    "Performance pass": (
        "Run a focused performance pass on this feature.\n"
        "Reduce unnecessary redraws and avoid blocking UI operations."
    ),
    "Error recovery": (
        "Improve error recovery and user feedback for this workflow.\n"
        "Add clear remediation guidance and safe retry behavior."
    ),
}
BADGE_STYLES: Dict[str, Tuple[str, str, str]] = {
    "good": ("#0F2B1D", "#6CFFB0", "#1E5A3C"),
    "warn": ("#2C2312", "#FFD27A", "#6A5324"),
    "bad": ("#2A1718", "#FF8D80", "#6A2C33"),
    "info": ("#12243A", "#8BD8FF", "#204A70"),
    "muted": ("#141B28", "#A7B4C4", "#2A3A52"),
}
BASE_DIR, SETTINGS_PATH, DEFAULT_HUB_DATA_DIR, _BOOT_SETTINGS = resolve_runtime_paths(__file__, "pt_hub")


@dataclass
class _WrapItem:
    w: tk.Widget
    padx: Tuple[int, int] = (0, 0)
    pady: Tuple[int, int] = (0, 0)


class WrapFrame(ttk.Frame):

    def __init__(self, parent, **kwargs):
        super().__init__(parent, **kwargs)
        self._items: List[_WrapItem] = []
        self._reflow_pending = False
        self._in_reflow = False
        self.bind("<Configure>", self._schedule_reflow)

    def add(self, widget: tk.Widget, padx=(0, 0), pady=(0, 0)) -> None:
        self._items.append(_WrapItem(widget, padx=padx, pady=pady))
        self._schedule_reflow()

    def clear(self, destroy_widgets: bool = True) -> None:

        for it in list(self._items):
            try:
                it.w.grid_forget()
            except Exception:
                pass
            if destroy_widgets:
                try:
                    it.w.destroy()
                except Exception:
                    pass
        self._items = []
        self._schedule_reflow()

    def _schedule_reflow(self, event=None) -> None:
        if self._reflow_pending:
            return
        self._reflow_pending = True
        self.after_idle(self._reflow)

    def _reflow(self) -> None:
        if self._in_reflow:
            self._reflow_pending = False
            return

        self._reflow_pending = False
        self._in_reflow = True
        try:
            width = self.winfo_width()
            if width <= 1:
                return
            usable_width = max(1, width - 6)

            for it in self._items:
                it.w.grid_forget()

            row = 0
            col = 0
            x = 0

            for it in self._items:
                reqw = max(it.w.winfo_reqwidth(), it.w.winfo_width())

                needed = 10 + reqw + it.padx[0] + it.padx[1]

                if col > 0 and (x + needed) > usable_width:
                    row += 1
                    col = 0
                    x = 0

                it.w.grid(row=row, column=col, sticky="w", padx=it.padx, pady=it.pady)
                x += needed
                col += 1
        finally:
            self._in_reflow = False


class NeuralSignalTile(ttk.Frame):

    def __init__(self, parent: tk.Widget, coin: str, bar_height: int = 52, levels: int = 8, trade_start_level: int = 3):
        super().__init__(parent)
        self.coin = coin

        self._hover_on = False
        self._normal_canvas_bg = DARK_PANEL2
        self._hover_canvas_bg = DARK_PANEL
        self._normal_border = DARK_BORDER
        self._hover_border = DARK_ACCENT2
        self._normal_fg = DARK_FG
        self._hover_fg = DARK_ACCENT2

        self._levels = max(2, int(levels))             
        self._display_levels = self._levels - 1        

        self._bar_h = int(bar_height)
        self._bar_w = 12
        self._gap = 16
        self._pad = 6

        self._base_fill = DARK_PANEL
        self._long_fill = "blue"
        self._short_fill = "orange"

        self.title_lbl = ttk.Label(self, text=coin)
        self.title_lbl.pack(anchor="center")

        w = (self._pad * 2) + (self._bar_w * 2) + self._gap
        h = (self._pad * 2) + self._bar_h

        self.canvas = tk.Canvas(
            self,
            width=w,
            height=h,
            bg=self._normal_canvas_bg,
            highlightthickness=1,
            highlightbackground=self._normal_border,
        )
        self.canvas.pack(padx=2, pady=(2, 0))

        x0 = self._pad
        x1 = x0 + self._bar_w
        x2 = x1 + self._gap
        x3 = x2 + self._bar_w
        yb = self._pad + self._bar_h

        # Build segmented bars: 7 segments for levels 1..7 (level 0 is "no highlight")
        self._long_segs: List[int] = []
        self._short_segs: List[int] = []

        for seg in range(self._display_levels):
            # seg=0 is bottom segment (level 1), seg=display_levels-1 is top segment (level 7)
            y_top = int(round(yb - ((seg + 1) * self._bar_h / self._display_levels)))
            y_bot = int(round(yb - (seg * self._bar_h / self._display_levels)))

            self._long_segs.append(
                self.canvas.create_rectangle(
                    x0, y_top, x1, y_bot,
                    fill=self._base_fill,
                    outline=DARK_BORDER,
                    width=1,
                )
            )
            self._short_segs.append(
                self.canvas.create_rectangle(
                    x2, y_top, x3, y_bot,
                    fill=self._base_fill,
                    outline=DARK_BORDER,
                    width=1,
                )
            )

        # Trade-start marker line (boundary before the trade-start level).
        # Example: trade_start_level=3 => line after 2nd block (between 2 and 3).
        self._trade_line_geom = (x0, x1, x2, x3, yb)
        self._trade_line_long = self.canvas.create_line(x0, yb, x1, yb, fill=DARK_FG, width=2)
        self._trade_line_short = self.canvas.create_line(x2, yb, x3, yb, fill=DARK_FG, width=2)
        self._trade_start_level = 3
        self.set_trade_start_level(trade_start_level)


        self.value_lbl = ttk.Label(self, text="L:0 S:0")
        self.value_lbl.pack(anchor="center", pady=(1, 0))

        self.set_values(0, 0)

    def set_hover(self, on: bool) -> None:
        """Visually highlight the tile on hover (like a button hover state)."""
        if bool(on) == bool(self._hover_on):
            return
        self._hover_on = bool(on)

        try:
            if self._hover_on:
                self.canvas.configure(
                    bg=self._hover_canvas_bg,
                    highlightbackground=self._hover_border,
                    highlightthickness=2,
                )
                self.title_lbl.configure(foreground=self._hover_fg)
                self.value_lbl.configure(foreground=self._hover_fg)
            else:
                self.canvas.configure(
                    bg=self._normal_canvas_bg,
                    highlightbackground=self._normal_border,
                    highlightthickness=1,
                )
                self.title_lbl.configure(foreground=self._normal_fg)
                self.value_lbl.configure(foreground=self._normal_fg)
        except Exception:
            pass

    def set_trade_start_level(self, level: Any) -> None:
        """Move the marker line to the boundary before the chosen start level."""
        self._trade_start_level = self._clamp_trade_start_level(level)
        self._update_trade_lines()

    def _clamp_trade_start_level(self, value: Any) -> int:
        try:
            v = int(float(value))
        except Exception:
            v = 3
        # Trade starts at levels 1..display_levels (usually 1..7)
        return max(1, min(v, self._display_levels))

    def _update_trade_lines(self) -> None:
        try:
            x0, x1, x2, x3, yb = self._trade_line_geom
        except Exception:
            return

        k = max(0, min(int(self._trade_start_level) - 1, self._display_levels))
        y = int(round(yb - (k * self._bar_h / self._display_levels)))

        try:
            self.canvas.coords(self._trade_line_long, x0, y, x1, y)
            self.canvas.coords(self._trade_line_short, x2, y, x3, y)
        except Exception:
            pass



    def _clamp_level(self, value: Any) -> int:
        try:
            v = int(float(value))
        except Exception:
            v = 0
        return max(0, min(v, self._levels - 1))  # logical clamp: 0..7

    def _set_level(self, seg_ids: List[int], level: int, active_fill: str) -> None:
        # Reset all segments to base
        for rid in seg_ids:
            self.canvas.itemconfigure(rid, fill=self._base_fill)

        # Level 0 -> show nothing (no highlight)
        if level <= 0:
            return

        # Level 1..7 -> fill from bottom up through the current level
        idx = level - 1  # level 1 maps to seg index 0
        if idx < 0:
            return
        if idx >= len(seg_ids):
            idx = len(seg_ids) - 1

        for i in range(idx + 1):
            self.canvas.itemconfigure(seg_ids[i], fill=active_fill)


    def set_values(self, long_sig: Any, short_sig: Any) -> None:
        ls = self._clamp_level(long_sig)
        ss = self._clamp_level(short_sig)

        self.value_lbl.config(text=f"L:{ls} S:{ss}")
        self._set_level(self._long_segs, ls, self._long_fill)
        self._set_level(self._short_segs, ss, self._short_fill)









# -----------------------------
# Settings / Paths
# -----------------------------

DEFAULT_SETTINGS = {
    "main_neural_dir": "",
    "coins": ["BTC", "ETH", "XRP", "BNB", "DOGE"],
    "trade_start_level": 3,  # trade starts when long signal >= this level (1..7)
    "start_allocation_pct": 0.5,  # % of total account value for initial entry (min $0.50 per coin)
    "dca_multiplier": 2.0,  # DCA buy size = current value * this (2.0 => total scales ~3x per DCA)
    "dca_levels": [-2.5, -5.0, -10.0, -20.0, -30.0, -40.0, -50.0],  # Hard DCA triggers (percent PnL)
    "max_dca_buys_per_24h": 2,  # max DCA buys per coin in rolling 24h window (0 disables DCA buys)

    # --- Trailing Profit Margin settings (used by pt_trader.py; shown in GUI settings) ---
    "pm_start_pct_no_dca": 5.0,
    "pm_start_pct_with_dca": 2.5,
    "trailing_gap_pct": 0.5,
    "max_position_usd_per_coin": 0.0,
    "max_total_exposure_pct": 0.0,

    "default_timeframe": "1hour",
    "timeframes": [
        "1min", "5min", "15min", "30min",
        "1hour", "2hour", "4hour", "8hour", "12hour",
        "1day", "1week"
    ],
    "candles_limit": 120,
    "ui_refresh_seconds": 1.0,
    "chart_refresh_seconds": 10.0,
    "auto_start_trading_when_all_trained": True,
    "hub_data_dir": "",  # if blank, defaults to <this_dir>/hub_data
    "script_neural_runner2": "engines/pt_thinker.py",
    "script_neural_trainer": "engines/pt_trainer.py",
    "script_trader": "engines/pt_trader.py",
    "crypto_trader_loop_sleep_s": 1.0,
    "crypto_trader_error_sleep_s": 1.5,
    "script_autopilot": "runtime/pt_autopilot.py",
    "script_autofix": "runtime/pt_autofix.py",
    "autofix_enabled": True,
    "autofix_mode": "report_only",  # report_only | manual | shadow_apply
    "autofix_allow_live_apply": False,
    "autofix_poll_interval_s": 45.0,
    "autofix_max_fixes_per_day": 2,
    "autofix_model": "gpt-5-mini",
    "autofix_api_base": "https://api.openai.com/v1",
    "autofix_request_timeout_s": 25.0,
    "autofix_test_command": "python -m unittest tests.test_settings_sanitize tests.test_runner_watchdog",
    "kucoin_min_interval_sec": 0.40,
    "kucoin_cache_ttl_sec": 2.5,
    "kucoin_stale_max_sec": 120.0,
    "kucoin_unsupported_cooldown_s": 21600.0,
    "crypto_price_error_log_cooldown_s": 120.0,
    "crypto_dynamic_enabled": True,
    "crypto_dynamic_pool_symbols": "BTC,ETH,XRP,BNB,DOGE,SOL,ADA,PAXG,AVAX,LINK,LTC,UNI,AAVE,DOT,ATOM,MATIC",
    "crypto_dynamic_target_count": 8,
    "crypto_dynamic_scan_interval_s": 300,
    "crypto_dynamic_min_projected_edge_pct": 0.25,
    "crypto_dynamic_max_new_per_scan": 1,
    "crypto_dynamic_auto_train": True,
    "crypto_dynamic_max_trainers": 1,
    "crypto_dynamic_rotation_cooldown_s": 900,
    "market_chart_cache_symbols": 8,
    "market_chart_cache_bars": 120,
    "market_fallback_scan_max_age_s": 7200.0,
    "market_fallback_snapshot_max_age_s": 1800.0,
    "alpaca_api_key_id": "",
    "alpaca_secret_key": "",
    "alpaca_base_url": "https://paper-api.alpaca.markets",
    "alpaca_data_url": "https://data.alpaca.markets",
    "alpaca_paper_mode": True,
    "market_rollout_stage": "legacy",  # legacy | scan_expanded | risk_caps | execution_v2 | shadow_only | live_guarded
    "settings_control_mode": "self_managed",  # preset_managed | self_managed
    "settings_profile": "balanced",  # guarded | balanced | performance
    "ui_font_scale_preset": "normal",  # small | normal | large
    "ui_layout_preset": "auto",  # auto | compact | normal | wide
    "stock_universe_mode": "all_tradable_filtered",  # core | watchlist | all_tradable_filtered
    "stock_universe_symbols": "AAPL,MSFT,NVDA,AMZN,META,TSLA,SPY,QQQ",
    "stock_scan_max_symbols": 160,
    "stock_min_price": 5.0,
    "stock_max_price": 500.0,
    "stock_min_dollar_volume": 5000000.0,
    "stock_max_spread_bps": 40.0,
    "stock_gate_market_hours_scan": True,
    "stock_min_bars_required": 24,
    "stock_min_valid_bars_ratio": 0.7,
    "stock_max_stale_hours": 6.0,
    "stock_scan_open_cooldown_minutes": 15,
    "stock_scan_close_cooldown_minutes": 15,
    "stock_scan_open_score_mult": 0.85,
    "stock_scan_close_score_mult": 0.90,
    "stock_scan_publish_watch_leaders": True,
    "stock_scan_watch_leaders_count": 6,
    "stock_leader_stability_margin_pct": 10.0,
    "stock_show_rejected_rows": False,
    "stock_auto_trade_enabled": False,
    "stock_block_entries_on_cached_scan": True,
    "stock_cached_scan_hard_block_age_s": 1800,
    "stock_cached_scan_entry_size_mult": 0.60,
    "stock_require_data_quality_ok_for_entries": True,
    "stock_require_reject_rate_max_pct": 92.0,
    "stock_trade_notional_usd": 100.0,
    "stock_max_open_positions": 1,
    "stock_score_threshold": 0.2,
    "stock_profit_target_pct": 0.35,
    "stock_trailing_gap_pct": 0.2,
    "stock_max_day_trades": 3,
    "stock_max_position_usd_per_symbol": 0.0,
    "stock_max_total_exposure_pct": 0.0,
    "stock_block_new_entries_near_close": True,
    "stock_no_new_entries_mins_to_close": 15,
    "stock_live_guarded_score_mult": 1.2,
    "stock_min_calib_prob_live_guarded": 0.58,
    "stock_max_slippage_bps": 35.0,
    "stock_order_retry_count": 2,
    "stock_max_loss_streak": 3,
    "stock_loss_streak_size_step_pct": 0.15,
    "stock_loss_streak_size_floor_pct": 0.40,
    "stock_loss_cooldown_seconds": 1800,
    "stock_max_daily_loss_usd": 0.0,
    "stock_max_daily_loss_pct": 0.0,
    "stock_min_samples_live_guarded": 5,
    "stock_max_signal_age_seconds": 300,
    "stock_reject_drift_warn_pct": 65.0,
    "oanda_account_id": "",
    "oanda_api_token": "",
    "oanda_rest_url": "https://api-fxpractice.oanda.com",
    "oanda_stream_url": "https://stream-fxpractice.oanda.com",
    "oanda_practice_mode": True,
    "forex_auto_trade_enabled": False,
    "forex_universe_pairs": "",
    "forex_scan_max_pairs": 32,
    "forex_max_spread_bps": 8.0,
    "forex_min_volatility_pct": 0.01,
    "forex_min_bars_required": 24,
    "forex_min_valid_bars_ratio": 0.7,
    "forex_max_stale_hours": 8.0,
    "forex_session_weight_enabled": True,
    "forex_session_weight_floor": 0.85,
    "forex_session_weight_ceiling": 1.10,
    "forex_leader_stability_margin_pct": 12.0,
    "forex_show_rejected_rows": False,
    "forex_trade_units": 1000,
    "forex_block_entries_on_cached_scan": True,
    "forex_cached_scan_hard_block_age_s": 1200,
    "forex_cached_scan_entry_size_mult": 0.65,
    "forex_require_data_quality_ok_for_entries": True,
    "forex_require_reject_rate_max_pct": 92.0,
    "forex_max_open_positions": 1,
    "forex_max_position_usd_per_pair": 0.0,
    "forex_score_threshold": 0.2,
    "forex_profit_target_pct": 0.25,
    "forex_trailing_gap_pct": 0.15,
    "forex_max_total_exposure_pct": 0.0,
    "forex_session_mode": "all",  # all | london_ny | london | ny | asia
    "forex_live_guarded_score_mult": 1.15,
    "forex_min_calib_prob_live_guarded": 0.56,
    "forex_max_slippage_bps": 6.0,
    "forex_order_retry_count": 2,
    "forex_max_loss_streak": 3,
    "forex_loss_streak_size_step_pct": 0.15,
    "forex_loss_streak_size_floor_pct": 0.40,
    "forex_loss_cooldown_seconds": 1800,
    "forex_max_daily_loss_usd": 0.0,
    "forex_max_daily_loss_pct": 0.0,
    "forex_min_samples_live_guarded": 5,
    "forex_max_signal_age_seconds": 300,
    "forex_reject_drift_warn_pct": 65.0,
    "market_max_total_exposure_pct": 0.0,
    "market_bg_stocks_interval_s": 15.0,
    "market_bg_forex_interval_s": 10.0,
    "stock_trader_step_interval_s": 18.0,
    "forex_trader_step_interval_s": 12.0,
    "runner_crash_lockout_s": 180.0,
    "runtime_api_quota_warn_15m": 4,
    "runtime_api_quota_crit_15m": 10,
    "runtime_alert_cadence_warn_count": 1,
    "runtime_alert_cadence_crit_count": 2,
    "runtime_alert_cadence_late_warn_pct": 80.0,
    "runtime_alert_cadence_late_crit_pct": 180.0,
    "runtime_alert_cadence_min_samples": 3,
    "runtime_alert_cadence_cooldown_s": 300,
    "runtime_alert_market_loop_stale_s": 90.0,
    "runtime_incidents_max_lines": 25000,
    "runtime_events_max_lines": 50000,
    "broker_failure_disable_threshold": 4,
    "broker_failure_disable_cooldown_s": 900,
    "broker_order_retry_after_cap_s": 300.0,
    "market_loop_jitter_pct": 0.10,
    "market_settings_reload_interval_s": 8.0,
    "paper_only_unless_checklist_green": True,
    "key_rotation_warn_days": 90,
    "data_cache_max_age_days": 14.0,
    "scanner_quality_max_age_days": 14.0,
    "data_cache_max_total_mb": 300,
    "global_max_drawdown_pct": 0.0,
    "global_drawdown_lookback_hours": 24,
    "auto_start_scripts": False,
}

_READ_INT_FILE_CACHE: Dict[str, Tuple[float, int]] = {}











SETTINGS_FILE = "gui_settings.json"


def _safe_read_json(path: str) -> Optional[dict]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, PermissionError, OSError, json.JSONDecodeError, ValueError) as exc:
        log_once(
            f"pt_hub:_safe_read_json:{path}:{type(exc).__name__}",
            f"[pt_hub._safe_read_json] path={path} {type(exc).__name__}: {exc}",
        )
        return None


def _safe_write_json(path: str, data: dict) -> None:
    try:
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, path)
    except (PermissionError, OSError, TypeError, ValueError) as exc:
        log_once(
            f"pt_hub:_safe_write_json:{path}:{type(exc).__name__}",
            f"[pt_hub._safe_write_json] path={path} {type(exc).__name__}: {exc}",
        )


def _read_trade_history_jsonl(path: str) -> List[dict]:
    """
    Reads hub_data/trade_history.jsonl written by pt_trader.py.
    Returns a list of dicts (only buy/sell rows).
    """
    out: List[dict] = []
    try:
        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as f:
                for ln in f:
                    ln = ln.strip()
                    if not ln:
                        continue
                    try:
                        obj = json.loads(ln)
                        side = str(obj.get("side", "")).lower().strip()
                        if side not in ("buy", "sell"):
                            continue
                        out.append(obj)
                    except Exception:
                        continue
    except Exception:
        pass
    return out


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)



def _fmt_money(x: float) -> str:
    """Format a USD *amount* (account value, position value, etc.) as dollars with 2 decimals."""
    try:
        return f"${float(x):,.2f}"
    except Exception:
        return "N/A"


def _fmt_price(x: Any) -> str:
    """
    Format a USD *price/level* with dynamic decimals based on magnitude.
    Examples:
      50234.12   -> $50,234.12
      123.4567   -> $123.457
      1.234567   -> $1.2346
      0.06234567 -> $0.062346
      0.00012345 -> $0.00012345
    """
    try:
        if x is None:
            return "N/A"

        v = float(x)
        if not math.isfinite(v):
            return "N/A"

        sign = "-" if v < 0 else ""
        av = abs(v)

        # Choose decimals by magnitude (more detail for smaller prices).
        if av >= 1000:
            dec = 2
        elif av >= 100:
            dec = 3
        elif av >= 1:
            dec = 4
        elif av >= 0.1:
            dec = 5
        elif av >= 0.01:
            dec = 6
        elif av >= 0.001:
            dec = 7
        else:
            dec = 8

        s = f"{av:,.{dec}f}"
        if "." in s:
            s = s.rstrip("0").rstrip(".")

        return f"{sign}${s}"
    except Exception:
        return "N/A"


def _fmt_pct(x: float) -> str:
    try:
        return f"{float(x):+.2f}%"
    except Exception:
        return "N/A"


def _now_str() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


# -----------------------------
# Neural folder detection
# -----------------------------

def build_coin_folders(main_dir: str, coins: List[str]) -> Dict[str, str]:
    """
    Coin folder layout:
      every coin (including BTC) uses <main_dir>/<COIN>

    Returns { "BTC": "...", "ETH": "...", ... }
    """
    out: Dict[str, str] = {}
    main_dir = main_dir or BASE_DIR

    for c in coins:
        c = c.upper().strip()
        if not c:
            continue
        p = os.path.join(main_dir, c)
        try:
            os.makedirs(p, exist_ok=True)
        except Exception:
            pass
        out[c] = p

    if "BTC" not in out:
        btc_dir = os.path.join(main_dir, "BTC")
        try:
            os.makedirs(btc_dir, exist_ok=True)
        except Exception:
            pass
        out["BTC"] = btc_dir

    return out


def read_price_levels_from_html(path: str) -> List[float]:
    """
    pt_thinker writes a python-list-like string into low_bound_prices.html / high_bound_prices.html.

    Example (commas often remain):
        "43210.1, 43100.0, 42950.5"

    So we normalize separators before parsing.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = f.read().strip()

        if not raw:
            return []

        # Normalize common separators that pt_thinker can leave behind
        raw = (
            raw.replace(",", " ")
               .replace("[", " ")
               .replace("]", " ")
               .replace("'", " ")
        )

        vals: List[float] = []
        for tok in raw.split():
            try:
                v = float(tok)

                # Filter obvious sentinel values used by pt_thinker for "inactive" slots
                if v <= 0:
                    continue
                if v >= 9e15:  # pt_thinker uses 99999999999999999
                    continue


                vals.append(v)
            except Exception:
                pass

        # De-dupe while preserving order (small rounding to avoid float-noise duplicates)
        out: List[float] = []
        seen = set()
        for v in vals:
            key = round(v, 12)
            if key in seen:
                continue
            seen.add(key)
            out.append(v)

        return out
    except Exception:
        return []



def read_int_from_file(path: str) -> int:
    try:
        mtime = os.path.getmtime(path)
    except (FileNotFoundError, PermissionError, OSError):
        return 0
    hit = _READ_INT_FILE_CACHE.get(path)
    if hit and hit[0] == mtime:
        return int(hit[1])
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = f.read().strip()
        val = int(float(raw))
    except (FileNotFoundError, PermissionError, OSError, ValueError) as exc:
        log_once(
            f"pt_hub:read_int_from_file:{path}:{type(exc).__name__}",
            f"[pt_hub.read_int_from_file] path={path} {type(exc).__name__}: {exc}",
        )
        val = 0
    _READ_INT_FILE_CACHE[path] = (mtime, val)
    return val


def read_short_signal(folder: str) -> int:
    txt = os.path.join(folder, "short_dca_signal.txt")
    if os.path.isfile(txt):
        return read_int_from_file(txt)
    else:
        return 0


# -----------------------------
# Candle fetching (KuCoin)
# -----------------------------

class CandleFetcher:
    """
    Uses kucoin-python if available; otherwise falls back to KuCoin REST via requests.
    """
    def __init__(self):
        self._mode = "kucoin_client"
        self._market = None
        try:
            from kucoin.client import Market  # type: ignore
            self._market = Market(url="https://api.kucoin.com")
        except Exception:
            self._mode = "rest"
            self._market = None

        if self._mode == "rest":
            import requests  # local import
            self._requests = requests

        # Small in-memory cache to keep timeframe switching snappy.
        # key: (pair, timeframe, limit) -> (saved_time_epoch, candles)
        self._cache: Dict[Tuple[str, str, int], Tuple[float, List[dict]]] = {}
        self._last_error: Dict[Tuple[str, str, int], Tuple[float, str]] = {}
        self._cache_ttl_seconds: float = 10.0
        self._lock = threading.Lock()
        self._pending: set[Tuple[str, str, int]] = set()
        self._result_q: "queue.Queue[Tuple[Tuple[str, str, int], float, List[dict], str]]" = queue.Queue()


    def _fetch_klines_sync(self, pair: str, timeframe: str, limit: int, now: float) -> Tuple[List[dict], str]:
        """
        Returns candles oldest->newest as:
          [{"ts": int, "open": float, "high": float, "low": float, "close": float}, ...]
        """
        limit = int(limit or 0)

        # rough window (timeframe-dependent) so we get enough candles
        tf_seconds = {
            "1min": 60, "5min": 300, "15min": 900, "30min": 1800,
            "1hour": 3600, "2hour": 7200, "4hour": 14400, "8hour": 28800, "12hour": 43200,
            "1day": 86400, "1week": 604800
        }.get(timeframe, 3600)

        end_at = int(now)
        start_at = end_at - (tf_seconds * max(200, (limit + 50) if limit else 250))

        if self._mode == "kucoin_client" and self._market is not None:
            try:
                # IMPORTANT: limit the server response by passing startAt/endAt.
                # This avoids downloading a huge default kline set every switch.
                try:
                    raw = self._market.get_kline(pair, timeframe, startAt=start_at, endAt=end_at)  # type: ignore
                except Exception:
                    # fallback if that client version doesn't accept kwargs
                    raw = self._market.get_kline(pair, timeframe)  # returns newest->oldest

                candles: List[dict] = []
                for row in raw:
                    # KuCoin kline row format:
                    # [time, open, close, high, low, volume, turnover]
                    ts = int(float(row[0]))
                    o = float(row[1]); c = float(row[2]); h = float(row[3]); l = float(row[4])
                    candles.append({"ts": ts, "open": o, "high": h, "low": l, "close": c})
                candles.sort(key=lambda x: x["ts"])
                if limit and len(candles) > limit:
                    candles = candles[-limit:]
                return candles, ""
            except Exception as exc:
                return [], f"kucoin client: {type(exc).__name__}"

        # REST fallback
        last_err = "unknown error"
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                url = "https://api.kucoin.com/api/v1/market/candles"
                params = {"symbol": pair, "type": timeframe, "startAt": start_at, "endAt": end_at}
                resp = self._requests.get(url, params=params, timeout=10)
                resp.raise_for_status()
                j = resp.json()
                if isinstance(j, dict):
                    code = str(j.get("code", "") or "").strip()
                    if code and code != "200000":
                        msg = str(j.get("msg", "") or "").strip()
                        raise RuntimeError(f"KuCoin error {code}: {msg}")
                data = j.get("data", []) if isinstance(j, dict) else []  # newest->oldest
                candles: List[dict] = []
                for row in data:
                    ts = int(float(row[0]))
                    o = float(row[1]); c = float(row[2]); h = float(row[3]); l = float(row[4])
                    candles.append({"ts": ts, "open": o, "high": h, "low": l, "close": c})
                candles.sort(key=lambda x: x["ts"])
                if limit and len(candles) > limit:
                    candles = candles[-limit:]
                if candles:
                    return candles, ""
                last_err = "empty candle payload"
            except Exception as exc:
                last_err = f"{type(exc).__name__}: {exc}"
            if attempt < max_attempts:
                time.sleep(0.35 * attempt)
        return [], f"rest fetch failed: {last_err}"


    def _start_fetch(self, cache_key: Tuple[str, str, int]) -> None:
        with self._lock:
            if cache_key in self._pending:
                return
            self._pending.add(cache_key)

        def _worker() -> None:
            pair, timeframe, limit = cache_key
            now = time.time()
            candles, err = self._fetch_klines_sync(pair, timeframe, limit, now)
            try:
                self._result_q.put((cache_key, now, candles, err))
            except Exception:
                pass

        threading.Thread(target=_worker, daemon=True).start()


    def drain_results(self) -> bool:
        changed = False
        while True:
            try:
                cache_key, now, candles, err = self._result_q.get_nowait()
            except queue.Empty:
                break
            with self._lock:
                self._pending.discard(cache_key)
                if candles:
                    self._cache[cache_key] = (now, candles)
                    self._last_error.pop(cache_key, None)
                    changed = True
                elif err:
                    self._last_error[cache_key] = (now, str(err)[:220])
        return changed


    def get_klines(self, symbol: str, timeframe: str, limit: int = 120) -> List[dict]:
        symbol = symbol.upper().strip()
        pair = f"{symbol}-USDT"
        limit = int(limit or 0)
        now = time.time()
        cache_key = (pair, timeframe, limit)
        with self._lock:
            cached = self._cache.get(cache_key)
        if cached and (now - float(cached[0])) <= float(self._cache_ttl_seconds):
            return cached[1]

        self._start_fetch(cache_key)
        if cached:
            return cached[1]
        return []

    def get_last_error(self, symbol: str, timeframe: str, limit: int = 120, max_age_s: float = 180.0) -> str:
        symbol = symbol.upper().strip()
        pair = f"{symbol}-USDT"
        cache_key = (pair, timeframe, int(limit or 0))
        now = time.time()
        with self._lock:
            row = self._last_error.get(cache_key)
        if not row:
            return ""
        ts, msg = row
        if (now - float(ts)) > float(max_age_s):
            return ""
        return str(msg or "")



# -----------------------------
# Chart widget
# -----------------------------

class CandleChart(ttk.Frame):
    def __init__(
        self,
        parent: tk.Widget,
        fetcher: CandleFetcher,
        coin: str,
        settings_getter,
        trade_history_path: str,
    ):
        super().__init__(parent)
        self.fetcher = fetcher
        self.coin = coin
        self.settings_getter = settings_getter
        self.trade_history_path = trade_history_path

        self.timeframe_var = tk.StringVar(value=self.settings_getter()["default_timeframe"])


        top = ttk.Frame(self)
        top.pack(fill="x", padx=6, pady=(4, 4))

        controls_row = ttk.Frame(top)
        controls_row.pack(fill="x")

        status_row = ttk.Frame(top)
        status_row.pack(fill="x", pady=(2, 0))

        ttk.Label(controls_row, text=f"{coin} chart").pack(side="left")

        display_controls = ttk.Frame(controls_row)
        display_controls.pack(side="left", padx=(10, 0))

        ttk.Label(display_controls, text="Timeframe:").pack(side="left", padx=(0, 4))
        self.tf_combo = ttk.Combobox(
            display_controls,
            textvariable=self.timeframe_var,
            values=self.settings_getter()["timeframes"],
            state="readonly",
            width=10,
        )
        self.tf_combo.pack(side="left")

        # Debounce rapid timeframe changes so redraws don't stack
        self._tf_after_id = None

        def _debounced_tf_change(*_):
            try:
                if self._tf_after_id:
                    self.after_cancel(self._tf_after_id)
            except Exception:
                pass

            def _do():
                # Ask the hub to refresh charts on the next tick (single refresh)
                try:
                    self.event_generate("<<TimeframeChanged>>", when="tail")
                except Exception:
                    pass

            self._tf_after_id = self.after(120, _do)

        self.tf_combo.bind("<<ComboboxSelected>>", _debounced_tf_change)

        self.detailed_overlays_var = tk.BooleanVar(value=False)
        self.detailed_overlays_chk = ttk.Checkbutton(
            display_controls,
            text="Detailed overlays",
            variable=self.detailed_overlays_var,
            command=lambda: self.event_generate("<<TimeframeChanged>>", when="tail"),
        )
        self.detailed_overlays_chk.pack(side="left", padx=(10, 0))


        self.neural_status_label = ttk.Label(status_row, text="Neural: N/A")
        self.neural_status_label.pack(side="left")

        self.chart_key_label = ttk.Label(status_row, text="Key: ★ Trail  ◆ DCA  ● Avg")
        self.chart_key_label.pack(side="left", padx=(12, 0))

        self.last_update_label = ttk.Label(status_row, text="Last: N/A")
        self.last_update_label.pack(side="right")

        # Figure
        # IMPORTANT: keep a stable DPI and resize the figure to the widget's pixel size.
        # On Windows scaling, trying to "sync DPI" via winfo_fpixels("1i") can produce the
        # exact right-side blank/covered region you're seeing.
        self.fig = Figure(figsize=(6.5, 3.5), dpi=100)
        self.fig.patch.set_facecolor(DARK_BG)

        # Keep a small margin for the title and two-line x-axis labels, but otherwise
        # let the plot use as much of the canvas as possible.
        self.fig.subplots_adjust(left=0.05, bottom=0.12, right=0.982, top=0.89)

        self.ax = self.fig.add_subplot(111)
        self._apply_dark_chart_style()
        self.ax.set_title(f"{coin}", color=DARK_FG)

        self.canvas = FigureCanvasTkAgg(self.fig, master=self)
        canvas_w = self.canvas.get_tk_widget()
        canvas_w.configure(bg=DARK_BG)

        # Remove horizontal padding here so the chart widget truly fills the container.
        canvas_w.pack(fill="both", expand=True, padx=0, pady=(0, 6))

        # Keep the matplotlib figure EXACTLY the same pixel size as the Tk widget.
        # FigureCanvasTkAgg already sizes its backing PhotoImage to e.width/e.height.
        # Multiplying by tk scaling here makes the renderer larger than the PhotoImage,
        # which produces the "blank/covered strip" on the right.
        self._last_canvas_px = (0, 0)
        self._resize_after_id = None

        def _on_canvas_configure(e):
            try:
                w = int(e.width)
                h = int(e.height)
                if w <= 1 or h <= 1:
                    return

                if (w, h) == self._last_canvas_px:
                    return
                self._last_canvas_px = (w, h)

                dpi = float(self.fig.get_dpi() or 100.0)
                self.fig.set_size_inches(w / dpi, h / dpi, forward=True)

                # Debounce redraws during live resize
                if self._resize_after_id:
                    try:
                        self.after_cancel(self._resize_after_id)
                    except Exception:
                        pass
                self._resize_after_id = self.after_idle(self.canvas.draw_idle)
            except Exception:
                pass

        canvas_w.bind("<Configure>", _on_canvas_configure, add="+")







        self._last_refresh = 0.0


    def _apply_dark_chart_style(self) -> None:
        """Apply dark styling (called on init and after every ax.clear())."""
        try:
            self.fig.patch.set_facecolor(DARK_BG)
            self.ax.set_facecolor(DARK_PANEL)
            self.ax.tick_params(colors=DARK_FG)
            for spine in self.ax.spines.values():
                spine.set_color(DARK_BORDER)
            self.ax.grid(True, color=DARK_BORDER, linewidth=0.6, alpha=0.35)
        except Exception:
            pass

    def refresh(
        self,
        coin_folders: Dict[str, str],
        current_buy_price: Optional[float] = None,
        current_sell_price: Optional[float] = None,
        trail_line: Optional[float] = None,
        dca_line_price: Optional[float] = None,
        avg_cost_basis: Optional[float] = None,
        quantity: Optional[float] = None,
    ) -> None:



        cfg = self.settings_getter()

        tf = self.timeframe_var.get().strip()
        max_trade_labels = 1
        LABEL_MIN_SPACING_PX = 14.0

        # Default to a cleaner chart and allow quick toggle without changing app settings.
        if not hasattr(self, "_chart_level_mode"):
            self._chart_level_mode = "clean"
        if not hasattr(self, "_chart_level_mode_bound"):
            try:
                canvas_w = self.canvas.get_tk_widget()

                def _toggle_chart_level_mode(_e=None):
                    try:
                        self._chart_level_mode = (
                            "detailed" if self._chart_level_mode == "clean" else "clean"
                        )
                        if hasattr(self, "detailed_overlays_var"):
                            self.detailed_overlays_var.set(self._chart_level_mode == "detailed")
                        self.event_generate("<<TimeframeChanged>>", when="tail")
                    except Exception:
                        self._chart_level_mode = "clean"
                        if hasattr(self, "detailed_overlays_var"):
                            self.detailed_overlays_var.set(False)

                # Double-click the chart to switch between Clean and Detailed overlays.
                canvas_w.bind("<Double-Button-1>", _toggle_chart_level_mode, add="+")
                self._chart_level_mode_bound = True
            except Exception:
                self._chart_level_mode_bound = False

        try:
            show_detailed_levels = bool(self.detailed_overlays_var.get())
        except Exception:
            show_detailed_levels = (getattr(self, "_chart_level_mode", "clean") == "detailed")
        self._chart_level_mode = "detailed" if show_detailed_levels else "clean"
        try:
            self.chart_key_label.config(
                text=("Key: ★ Trail  ◆ DCA  ● Avg  A Ask  B Bid" if show_detailed_levels else "Key: ★ Trail  ◆ DCA  ● Avg")
            )
        except Exception:
            pass

        if not hasattr(self, "_legend_hover_bound"):
            try:
                canvas_w = self.canvas.get_tk_widget()

                def _reset_hover_lines() -> None:
                    try:
                        for item in getattr(self, "_line_hover_targets", []):
                            artist = item.get("artist")
                            if artist is None:
                                continue
                            artist.set_linewidth(float(item.get("line_width", 1.0)))
                            artist.set_alpha(float(item.get("alpha", 0.9)))
                        self._active_hover_line = None
                    except Exception:
                        pass

                def _set_hover_line(active_item) -> None:
                    if getattr(self, "_active_hover_line", None) is active_item:
                        return
                    _reset_hover_lines()
                    if not active_item:
                        return
                    try:
                        artist = active_item.get("artist")
                        if artist is not None:
                            artist.set_linewidth(float(active_item.get("hover_line_width", active_item.get("line_width", 1.0))))
                            artist.set_alpha(float(active_item.get("hover_alpha", 1.0)))
                            self._active_hover_line = active_item
                    except Exception:
                        self._active_hover_line = None

                def _hide_legend_tooltip(_e=None):
                    _reset_hover_lines()
                    try:
                        tw = getattr(self, "_legend_tooltip_win", None)
                        if tw is not None and tw.winfo_exists():
                            tw.destroy()
                    except Exception:
                        pass
                    self._legend_tooltip_win = None
                    self._legend_tooltip_label = None

                def _show_legend_tooltip(x_root: int, y_root: int, text: str):
                    try:
                        tw = getattr(self, "_legend_tooltip_win", None)
                        lbl = getattr(self, "_legend_tooltip_label", None)
                        if tw is None or (not tw.winfo_exists()) or lbl is None or (not lbl.winfo_exists()):
                            tw = tk.Toplevel(canvas_w)
                            tw.withdraw()
                            tw.overrideredirect(True)
                            try:
                                tw.attributes("-topmost", True)
                            except Exception:
                                pass
                            lbl = tk.Label(
                                tw,
                                text=text,
                                justify="left",
                                anchor="w",
                                padx=8,
                                pady=6,
                                bg=DARK_BG2,
                                fg=DARK_FG,
                                bd=1,
                                relief="solid",
                            )
                            lbl.pack()
                            self._legend_tooltip_win = tw
                            self._legend_tooltip_label = lbl
                        else:
                            lbl.config(text=text)
                        tw.geometry(f"+{int(x_root) + 14}+{int(y_root) + 12}")
                        tw.deiconify()
                    except Exception:
                        pass

                def _line_tooltip_for_event(mpl_event):
                    x_disp = float(mpl_event.x)
                    y_disp = float(mpl_event.y)

                    for bbox_item in getattr(self, "_hover_regions_px", []):
                        try:
                            x0, y0, x1, y1 = bbox_item["bbox"]
                            if x0 <= x_disp <= x1 and y0 <= y_disp <= y1:
                                return str(bbox_item.get("text", "") or "").strip(), None
                        except Exception:
                            continue

                    if mpl_event.inaxes is not self.ax:
                        return "", None

                    try:
                        ax_bbox = self.ax.get_window_extent()
                        if not (ax_bbox.x0 <= x_disp <= ax_bbox.x1 and ax_bbox.y0 <= y_disp <= ax_bbox.y1):
                            return "", None
                    except Exception:
                        pass

                    nearest_text = ""
                    nearest_item = None
                    nearest_dist = 9.0
                    for line_item in getattr(self, "_line_hover_targets", []):
                        try:
                            dist = abs(y_disp - float(line_item["y_disp"]))
                            if dist <= nearest_dist:
                                nearest_text = str(line_item.get("text", "") or "").strip()
                                nearest_item = line_item
                                nearest_dist = dist
                        except Exception:
                            continue
                    return nearest_text, nearest_item

                def _on_legend_motion(mpl_event):
                    try:
                        if mpl_event.x is None or mpl_event.y is None:
                            _hide_legend_tooltip()
                            return
                        tip_txt, active_item = _line_tooltip_for_event(mpl_event)
                        if not tip_txt:
                            _hide_legend_tooltip()
                            return
                        _set_hover_line(active_item)
                        x_disp = float(mpl_event.x)
                        y_disp = float(mpl_event.y)
                        gui_evt = getattr(mpl_event, "guiEvent", None)
                        if gui_evt is not None and hasattr(gui_evt, "x_root") and hasattr(gui_evt, "y_root"):
                            x_root = int(gui_evt.x_root)
                            y_root = int(gui_evt.y_root)
                        else:
                            x_root = int(canvas_w.winfo_rootx() + x_disp)
                            y_root = int(canvas_w.winfo_rooty() + (canvas_w.winfo_height() - y_disp))
                        _show_legend_tooltip(x_root, y_root, tip_txt)
                    except Exception:
                        _hide_legend_tooltip()

                self._legend_hover_cid = self.canvas.mpl_connect("motion_notify_event", _on_legend_motion)
                self._legend_hover_leave_cid = self.canvas.mpl_connect("figure_leave_event", _hide_legend_tooltip)
                self._legend_hover_bound = True
            except Exception:
                self._legend_hover_bound = False

        def _nearest_levels(levels: List[float], anchor: Optional[float], keep: int = 2) -> List[float]:
            try:
                vals = [float(v) for v in levels if math.isfinite(float(v)) and float(v) > 0]
            except Exception:
                vals = []
            if show_detailed_levels:
                return vals
            try:
                aa = float(anchor)
                vals.sort(key=lambda v: abs(v - aa))
                return vals[:keep]
            except Exception:
                return vals[:keep]

        limit = int(cfg.get("candles_limit", 120))

        candles = self.fetcher.get_klines(self.coin, tf, limit=limit)

        folder = coin_folders.get(self.coin, "")
        low_path = os.path.join(folder, "low_bound_prices.html")
        high_path = os.path.join(folder, "high_bound_prices.html")

        # --- Cached neural reads (per path, by mtime) ---
        if not hasattr(self, "_neural_cache"):
            self._neural_cache = {}  # path -> (mtime, value)

        def _cached(path: str, loader, default):
            try:
                mtime = os.path.getmtime(path)
            except Exception:
                return default
            hit = self._neural_cache.get(path)
            if hit and hit[0] == mtime:
                return hit[1]
            v = loader(path)
            self._neural_cache[path] = (mtime, v)
            return v

        long_levels = _cached(low_path, read_price_levels_from_html, []) if folder else []
        short_levels = _cached(high_path, read_price_levels_from_html, []) if folder else []

        current_mid_price = None
        try:
            if (
                current_buy_price is not None
                and current_sell_price is not None
                and float(current_buy_price) > 0
                and float(current_sell_price) > 0
            ):
                current_mid_price = (float(current_buy_price) + float(current_sell_price)) / 2.0
        except Exception:
            current_mid_price = None
        anchor_price = current_mid_price if current_mid_price is not None else avg_cost_basis

        try:
            position_qty = float(quantity or 0.0)
        except Exception:
            position_qty = 0.0

        def _line_impact_text(line_name: str, line_price: Optional[float], meaning: str) -> str:
            try:
                price_txt = _fmt_price(float(line_price))
            except Exception:
                price_txt = "N/A"
            base = f"{line_name}: {price_txt}\nMeaning: {meaning}"
            try:
                lp = float(line_price)
                avg = float(avg_cost_basis or 0.0)
                qtyf = float(position_qty or 0.0)
                if lp > 0 and avg > 0 and qtyf > 0:
                    est_value = qtyf * lp
                    est_cost = qtyf * avg
                    est_pnl = est_value - est_cost
                    est_pct = ((lp - avg) / avg) * 100.0
                    base += (
                        f"\nImpact if hit: value {_fmt_money(est_value)}"
                        f" | est. PnL {est_pnl:+.2f} ({est_pct:+.2f}%)"
                    )
            except Exception:
                pass
            return base

        long_sig_path = os.path.join(folder, "long_dca_signal.txt")
        long_sig = _cached(long_sig_path, read_int_from_file, 0) if folder else 0
        short_sig = read_short_signal(folder) if folder else 0

        # --- Avoid full ax.clear() (expensive). Just clear artists. ---
        try:
            self.ax.lines.clear()
            self.ax.patches.clear()
            self.ax.collections.clear()  # scatter dots live here
            self.ax.texts.clear()        # labels/annotations live here
        except Exception:
            # fallback if matplotlib version lacks .clear() on these lists
            self.ax.cla()
            self._apply_dark_chart_style()


        if not candles:
            self._legend_panel_text = f"{self.coin}: waiting for candle data..."
            self._legend_tooltip_text = ""
            self._legend_bbox_px = None
            self._legend_hover_artist = None
            self._hover_regions_px = []
            self._hover_text_artists = []
            self._line_hover_targets = []
            err = ""
            try:
                err = str(self.fetcher.get_last_error(self.coin, tf, limit=limit) or "").strip()
            except Exception:
                err = ""
            if err:
                err_short = (err[:90] + "...") if len(err) > 90 else err
                self.ax.set_title(f"{self.coin} ({tf}) - no candles ({err_short})", color=DARK_FG)
                try:
                    self.neural_status_label.config(text=f"Neural: N/A | feed error")
                except Exception:
                    pass
            else:
                self.ax.set_title(f"{self.coin} ({tf}) - no candles", color=DARK_FG)
            try:
                self.ax.text(
                    0.5,
                    0.5,
                    "No candle data available yet\nCheck feed health, timeframe, or retry in a few seconds.",
                    transform=self.ax.transAxes,
                    ha="center",
                    va="center",
                    color=DARK_MUTED,
                    fontsize=10,
                    bbox={"facecolor": DARK_PANEL, "edgecolor": DARK_BORDER, "pad": 8},
                )
            except Exception:
                pass
            self.canvas.draw_idle()
            return


        # Candlestick drawing (green up / red down) - batch rectangles
        xs = getattr(self, "_xs", None)
        if not xs or len(xs) != len(candles):
            xs = list(range(len(candles)))
            self._xs = xs

        rects = []
        for i, c in enumerate(candles):
            o = float(c["open"])
            cl = float(c["close"])
            h = float(c["high"])
            l = float(c["low"])

            up = cl >= o
            candle_color = "green" if up else "red"

            # wick
            self.ax.plot([i, i], [l, h], linewidth=1, color=candle_color)

            # body
            bottom = min(o, cl)
            height = abs(cl - o)
            if height < 1e-12:
                height = 1e-12

            rects.append(
                Rectangle(
                    (i - 0.35, bottom),
                    0.7,
                    height,
                    facecolor=candle_color,
                    edgecolor=candle_color,
                    linewidth=1,
                    alpha=0.9,
                )
            )

        for r in rects:
            self.ax.add_patch(r)

        # Lock y-limits to candle range so overlay lines can go offscreen without expanding the chart.
        try:
            y_low = min(float(c["low"]) for c in candles)
            y_high = max(float(c["high"]) for c in candles)
            pad = (y_high - y_low) * 0.03
            if not math.isfinite(pad) or pad <= 0:
                pad = max(abs(y_low) * 0.001, 1e-6)
            self.ax.set_ylim(y_low - pad, y_high + pad)
        except Exception:
            pass

        # Reset the axes to its base geometry; chart legend now lives in the side panel.
        try:
            if not hasattr(self, "_base_ax_pos"):
                self._base_ax_pos = self.ax.get_position().frozen()
            self.ax.set_position(self._base_ax_pos)
        except Exception:
            pass



        # Overlay Neural levels (blue long, orange short)
        levels_to_draw_long = _nearest_levels(long_levels, anchor_price, keep=2)
        levels_to_draw_short = _nearest_levels(short_levels, anchor_price, keep=2)
        line_hover_targets = []
        for lv in levels_to_draw_long:
            try:
                yy = float(lv)
                artist = self.ax.axhline(
                    y=yy,
                    linewidth=1,
                    color="blue",
                    alpha=(0.8 if show_detailed_levels else 0.65),
                )
                line_hover_targets.append({
                    "y": yy,
                    "artist": artist,
                    "line_width": 1.0,
                    "hover_line_width": 1.8,
                    "alpha": (0.8 if show_detailed_levels else 0.65),
                    "hover_alpha": 1.0,
                    "text": _line_impact_text(
                        "Long level",
                        yy,
                        "Neural long support/reference level; price moving near it strengthens bullish context.",
                    ),
                })
            except Exception:
                pass

        for lv in levels_to_draw_short:
            try:
                yy = float(lv)
                artist = self.ax.axhline(
                    y=yy,
                    linewidth=1,
                    color="orange",
                    alpha=(0.8 if show_detailed_levels else 0.65),
                )
                line_hover_targets.append({
                    "y": yy,
                    "artist": artist,
                    "line_width": 1.0,
                    "hover_line_width": 1.8,
                    "alpha": (0.8 if show_detailed_levels else 0.65),
                    "hover_alpha": 1.0,
                    "text": _line_impact_text(
                        "Short level",
                        yy,
                        "Neural short resistance/reference level; price moving near it strengthens bearish context.",
                    ),
                })
            except Exception:
                pass


        # Overlay Trailing PM line (sell) and next DCA line
        try:
            if trail_line is not None and float(trail_line) > 0:
                yy = float(trail_line)
                artist = self.ax.axhline(y=yy, linewidth=1.5, color="green", alpha=0.95)
                line_hover_targets.append({
                    "y": yy,
                    "artist": artist,
                    "line_width": 1.5,
                    "hover_line_width": 2.2,
                    "alpha": 0.95,
                    "hover_alpha": 1.0,
                    "text": _line_impact_text(
                        "Trail line",
                        yy,
                        "Active trailing sell threshold for the current position.",
                    ),
                })
        except Exception:
            pass

        try:
            if dca_line_price is not None and float(dca_line_price) > 0:
                yy = float(dca_line_price)
                artist = self.ax.axhline(y=yy, linewidth=1.5, color="red", alpha=0.95)
                line_hover_targets.append({
                    "y": yy,
                    "artist": artist,
                    "line_width": 1.5,
                    "hover_line_width": 2.2,
                    "alpha": 0.95,
                    "hover_alpha": 1.0,
                    "text": _line_impact_text(
                        "Next DCA",
                        yy,
                        "Next configured DCA trigger price; touching it makes the next averaging buy eligible.",
                    ),
                })
        except Exception:
            pass

        # Overlay avg cost basis (yellow)
        try:
            if avg_cost_basis is not None and float(avg_cost_basis) > 0:
                yy = float(avg_cost_basis)
                artist = self.ax.axhline(y=yy, linewidth=1.5, color="yellow", alpha=0.95)
                line_hover_targets.append({
                    "y": yy,
                    "artist": artist,
                    "line_width": 1.5,
                    "hover_line_width": 2.2,
                    "alpha": 0.95,
                    "hover_alpha": 1.0,
                    "text": _line_impact_text(
                        "Average cost",
                        yy,
                        "Current blended entry price; near break-even before fees/slippage.",
                    ),
                })
        except Exception:
            pass

        # Overlay current ask/bid prices
        try:
            if current_buy_price is not None and float(current_buy_price) > 0:
                yy = float(current_buy_price)
                artist = self.ax.axhline(y=yy, linewidth=1.5, color="purple", alpha=0.95)
                if show_detailed_levels:
                    line_hover_targets.append({
                        "y": yy,
                        "artist": artist,
                        "line_width": 1.5,
                        "hover_line_width": 2.2,
                        "alpha": 0.95,
                        "hover_alpha": 1.0,
                        "text": _line_impact_text(
                            "Ask",
                            yy,
                            "Current buy-side market reference price.",
                        ),
                    })
        except Exception:
            pass

        try:
            if current_sell_price is not None and float(current_sell_price) > 0:
                yy = float(current_sell_price)
                artist = self.ax.axhline(y=yy, linewidth=1.5, color="teal", alpha=0.95)
                if show_detailed_levels:
                    line_hover_targets.append({
                        "y": yy,
                        "artist": artist,
                        "line_width": 1.5,
                        "hover_line_width": 2.2,
                        "alpha": 0.95,
                        "hover_alpha": 1.0,
                        "text": _line_impact_text(
                            "Bid",
                            yy,
                            "Current sell-side market reference price.",
                        ),
                    })
        except Exception:
            pass

        # Right-side boxed price labels have been removed; line hover now carries the context instead.
        self._hover_text_artists = []

        # Build the chart legend text for the side panel.
        try:
            trade_start_level = int(cfg.get("trade_start_level", 3) or 3)
            dca_levels_cfg = list(cfg.get("dca_levels", []) or [])
            dca_mult = float(cfg.get("dca_multiplier", 2.0) or 2.0)
            max_dca_24h = int(cfg.get("max_dca_buys_per_24h", 2) or 2)
            pm_no_dca = float(cfg.get("pm_start_pct_no_dca", 5.0) or 5.0)
            pm_with_dca = float(cfg.get("pm_start_pct_with_dca", 2.5) or 2.5)
            trail_gap = float(cfg.get("trailing_gap_pct", 0.5) or 0.5)
            level_mode_label = "Detailed" if show_detailed_levels else "Clean"

            def _fmt_level_list(vals: List[float]) -> str:
                try:
                    if not vals:
                        return "N/A"
                    shown_vals = list(vals)
                    extra_count = 0
                    if show_detailed_levels and len(shown_vals) > 6:
                        extra_count = len(shown_vals) - 6
                        shown_vals = shown_vals[:6]
                    txt = ", ".join(_fmt_price(float(v)) for v in shown_vals)
                    if extra_count > 0:
                        txt += f" (+{extra_count} more)"
                    return txt
                except Exception:
                    return "N/A"

            def _fmt_optional_price(v: Optional[float]) -> str:
                try:
                    vv = float(v)
                    if vv > 0 and math.isfinite(vv):
                        return _fmt_price(vv)
                except Exception:
                    pass
                return "N/A"

            def _fmt_delta(anchor_val: Optional[float], target_val: Optional[float]) -> str:
                try:
                    aa = float(anchor_val)
                    tt = float(target_val)
                    if (not math.isfinite(aa)) or (not math.isfinite(tt)) or aa <= 0 or tt <= 0:
                        return "N/A"
                    delta_pct = ((tt - aa) / aa) * 100.0
                    return f"{delta_pct:+.2f}%"
                except Exception:
                    return "N/A"

            ask_text = _fmt_optional_price(current_buy_price)
            bid_text = _fmt_optional_price(current_sell_price)
            avg_text = _fmt_optional_price(avg_cost_basis)
            dca_text = _fmt_optional_price(dca_line_price)
            trail_text = _fmt_optional_price(trail_line)
            anchor_text = _fmt_optional_price(anchor_price)
            dca_delta_text = _fmt_delta(anchor_price, dca_line_price)
            trail_delta_text = _fmt_delta(anchor_price, trail_line)

            try:
                dca_levels_shown = dca_levels_cfg[:4]
                dca_extra = max(0, len(dca_levels_cfg) - len(dca_levels_shown))
                dca_levels_text = ", ".join(str(v) for v in dca_levels_shown) if dca_levels_shown else "N/A"
                if dca_extra > 0:
                    dca_levels_text += f" (+{dca_extra} more)"
            except Exception:
                dca_levels_text = "N/A"

            def _wrap_text_block(text: str, width: int = 60) -> str:
                lines = []
                for raw_line in str(text).splitlines():
                    line = raw_line.strip()
                    if len(line) <= width:
                        lines.append(line)
                        continue
                    current = ""
                    for word in line.split(" "):
                        test = word if not current else f"{current} {word}"
                        if len(test) <= width:
                            current = test
                        else:
                            if current:
                                lines.append(current)
                            current = word
                    if current:
                        lines.append(current)
                return "\n".join(lines)

            legend_lines = [
                f"Mode: {level_mode_label}",
                "Key: ★ Trail | ◆ DCA | ● Avg",
                f"Long: {_fmt_level_list(levels_to_draw_long)}",
                f"Short: {_fmt_level_list(levels_to_draw_short)}",
                f"Px: ● {avg_text} | ◆ {dca_text} | ★ {trail_text}",
                f"Δ: ◆ {dca_delta_text} | ★ {trail_delta_text}",
            ]
            if show_detailed_levels:
                legend_lines = [
                    "Mode: Detailed",
                    "Key: ★ Trail | ◆ DCA | ● Avg | A | B",
                    f"Long: {_fmt_level_list(levels_to_draw_long)}",
                    f"Short: {_fmt_level_list(levels_to_draw_short)}",
                    f"Px: A {ask_text} | B {bid_text} | ● {avg_text} | ★ {trail_text}",
                    f"Δ: ◆ {dca_delta_text} | ★ {trail_delta_text}",
                    "Params:",
                    f"Start L{trade_start_level}",
                    f"DCA%: [{dca_levels_text}]",
                    f"x{dca_mult:g} | Max {max_dca_24h}/coin/24h",
                    f"PM: +{pm_no_dca:g}% / +{pm_with_dca:g}% | Gap {trail_gap:g}%",
                ]

            legend_text = _wrap_text_block("\n".join(legend_lines), width=60)

            self._legend_panel_text = legend_text
            self._legend_mode = level_mode_label
            self._legend_tooltip_text = ""
            self._legend_bbox_px = None
            self._legend_hover_artist = None
            self._legend_needs_scroll = bool(show_detailed_levels)
        except Exception:
            self._legend_panel_text = "Legend unavailable"
            self._legend_mode = "N/A"
            self._legend_tooltip_text = ""
            self._legend_bbox_px = None
            self._legend_hover_artist = None
            self._legend_needs_scroll = False
            pass




        # --- Trade dots (BUY / DCA / SELL) for THIS coin only ---
        try:
            trades = _read_trade_history_jsonl(self.trade_history_path) if self.trade_history_path else []
            plotted_trade_points = []
            if trades:
                candle_ts = [int(c["ts"]) for c in candles]  # oldest->newest
                t_min = float(candle_ts[0])
                t_max = float(candle_ts[-1])

                plotted_trade_points = []
                for tr in trades:
                    sym = str(tr.get("symbol", "")).upper()
                    base = sym.split("-")[0].strip() if sym else ""
                    if base != self.coin.upper().strip():
                        continue

                    side = str(tr.get("side", "")).lower().strip()
                    tag = str(tr.get("tag") or "").upper().strip()

                    if side == "buy":
                        label = "DCA" if tag == "DCA" else "BUY"
                        color = "purple" if tag == "DCA" else "red"
                    elif side == "sell":
                        label = "SELL"
                        color = "green"
                    else:
                        continue

                    tts = tr.get("ts", None)
                    if tts is None:
                        continue
                    try:
                        tts = float(tts)
                    except Exception:
                        continue
                    if tts < t_min or tts > t_max:
                        continue

                    i = bisect.bisect_left(candle_ts, tts)
                    if i <= 0:
                        idx = 0
                    elif i >= len(candle_ts):
                        idx = len(candle_ts) - 1
                    else:
                        idx = i if abs(candle_ts[i] - tts) < abs(tts - candle_ts[i - 1]) else (i - 1)

                    # y = trade price if present, else candle close
                    y = None
                    try:
                        p = tr.get("price", None)
                        if p is not None and float(p) > 0:
                            y = float(p)
                    except Exception:
                        y = None
                    if y is None:
                        try:
                            y = float(candles[idx].get("close", 0.0))
                        except Exception:
                            y = None
                    if y is None:
                        continue

                    x = idx
                    self.ax.scatter([x], [y], s=35, color=color, zorder=6)
                    plotted_trade_points.append((tts, label, x, y))
        except Exception:
            pass

        try:
            if plotted_trade_points:
                plotted_trade_points.sort(key=lambda item: item[0])
                for _, label, x, y in plotted_trade_points[-max_trade_labels:]:
                    self.ax.annotate(
                        label,
                        (x, y),
                        textcoords="offset points",
                        xytext=(0, 10),
                        ha="center",
                        fontsize=8,
                        color=DARK_FG,
                        zorder=7,
                    )
        except Exception:
            pass


        self.ax.set_xlim(-0.5, (len(candles) - 0.5) + 0.6)

        self.ax.set_title(f"{self.coin} ({tf})", color=DARK_FG)



        # x tick labels (date + time) - evenly spaced, never overlapping duplicates
        n = len(candles)
        want = 5  # keep it readable even when the window is narrow
        if n <= want:
            idxs = list(range(n))
        else:
            step = (n - 1) / float(want - 1)
            idxs = []
            last = -1
            for j in range(want):
                i = int(round(j * step))
                if i <= last:
                    i = last + 1
                if i >= n:
                    i = n - 1
                idxs.append(i)
                last = i

        tick_x = [xs[i] for i in idxs]
        tick_lbl = [
            time.strftime("%Y-%m-%d\n%H:%M", time.localtime(int(candles[i].get("ts", 0))))
            for i in idxs
        ]

        try:
            self.ax.minorticks_off()
            self.ax.set_xticks(tick_x)
            self.ax.set_xticklabels(tick_lbl)
            self.ax.tick_params(axis="x", labelsize=8)
        except Exception:
            pass


        self.canvas.draw_idle()
        try:
            try:
                self._line_hover_targets = [
                    {
                        "text": str(item.get("text", "") or "").strip(),
                        "y_disp": float(self.ax.transData.transform((0.0, float(item.get("y", 0.0))))[1]),
                        "artist": item.get("artist"),
                        "line_width": float(item.get("line_width", 1.0)),
                        "hover_line_width": float(item.get("hover_line_width", item.get("line_width", 1.0))),
                        "alpha": float(item.get("alpha", 0.9)),
                        "hover_alpha": float(item.get("hover_alpha", 1.0)),
                    }
                    for item in (line_hover_targets or [])
                    if str(item.get("text", "") or "").strip()
                ]
            except Exception:
                self._line_hover_targets = []

            if getattr(self, "_hover_text_artists", []):
                if getattr(self, "_legend_bbox_after_id", None):
                    self.after_cancel(self._legend_bbox_after_id)

                def _refresh_legend_bbox():
                    try:
                        renderer = self.canvas.get_renderer()
                        hover_regions = []
                        for artist, text in list(getattr(self, "_hover_text_artists", [])):
                            try:
                                bbox = artist.get_window_extent(renderer=renderer)
                                hover_regions.append({"bbox": (bbox.x0, bbox.y0, bbox.x1, bbox.y1), "text": text})
                            except Exception:
                                continue
                        self._hover_regions_px = hover_regions
                    except Exception:
                        self._hover_regions_px = []
                    finally:
                        self._legend_bbox_after_id = None

                self._legend_bbox_after_id = self.after_idle(_refresh_legend_bbox)
            else:
                self._hover_regions_px = []
                tw = getattr(self, "_legend_tooltip_win", None)
                if tw is not None and tw.winfo_exists():
                    tw.destroy()
                    self._legend_tooltip_win = None
                    self._legend_tooltip_label = None
        except Exception:
            pass


        self.neural_status_label.config(text=f"Neural: long={long_sig} short={short_sig} | levels L={len(long_levels)} S={len(short_levels)}")

        # show file update time if possible
        last_ts = None
        try:
            if os.path.isfile(low_path):
                last_ts = os.path.getmtime(low_path)
            elif os.path.isfile(high_path):
                last_ts = os.path.getmtime(high_path)
        except Exception:
            last_ts = None

        if last_ts:
            self.last_update_label.config(text=f"Last: {time.strftime('%H:%M:%S', time.localtime(last_ts))}")
        else:
            self.last_update_label.config(text="Last: N/A")

    def export_png(self, path: str) -> bool:
        try:
            self.fig.savefig(path, dpi=160, facecolor=self.fig.get_facecolor())
            return True
        except Exception:
            return False


# -----------------------------
# Account Value chart widget
# -----------------------------

class AccountValueChart(ttk.Frame):
    def __init__(self, parent: tk.Widget, history_path: str, trade_history_path: str, max_points: int = 250):
        super().__init__(parent)
        self.history_path = history_path
        self.trade_history_path = trade_history_path
        # Hard-cap to 250 points max (account value chart only)
        self.max_points = min(int(max_points or 0) or 250, 250)
        self._last_mtime: Optional[float] = None


        top = ttk.Frame(self)
        top.pack(fill="x", padx=6, pady=6)

        ttk.Label(top, text="Account value").pack(side="left")
        self.last_update_label = ttk.Label(top, text="Last: N/A")
        self.last_update_label.pack(side="right")

        self.fig = Figure(figsize=(6.5, 3.5), dpi=100)
        self.fig.patch.set_facecolor(DARK_BG)

        # Keep a modest buffer for labels/title while maximizing the visible chart area.
        self.fig.subplots_adjust(left=0.05, bottom=0.14, right=0.988, top=0.89)

        self.ax = self.fig.add_subplot(111)
        self._apply_dark_chart_style()
        self.ax.set_title("Account Value", color=DARK_FG)

        self.canvas = FigureCanvasTkAgg(self.fig, master=self)
        canvas_w = self.canvas.get_tk_widget()
        canvas_w.configure(bg=DARK_BG)

        # Remove horizontal padding here so the chart widget truly fills the container.
        canvas_w.pack(fill="both", expand=True, padx=0, pady=(0, 6))

        # Keep the matplotlib figure EXACTLY the same pixel size as the Tk widget.
        # FigureCanvasTkAgg already sizes its backing PhotoImage to e.width/e.height.
        # Multiplying by tk scaling here makes the renderer larger than the PhotoImage,
        # which produces the "blank/covered strip" on the right.
        self._last_canvas_px = (0, 0)
        self._resize_after_id = None

        def _on_canvas_configure(e):
            try:
                w = int(e.width)
                h = int(e.height)
                if w <= 1 or h <= 1:
                    return

                if (w, h) == self._last_canvas_px:
                    return
                self._last_canvas_px = (w, h)

                dpi = float(self.fig.get_dpi() or 100.0)
                self.fig.set_size_inches(w / dpi, h / dpi, forward=True)

                # Debounce redraws during live resize
                if self._resize_after_id:
                    try:
                        self.after_cancel(self._resize_after_id)
                    except Exception:
                        pass
                self._resize_after_id = self.after_idle(self.canvas.draw_idle)
            except Exception:
                pass

        canvas_w.bind("<Configure>", _on_canvas_configure, add="+")








    def _apply_dark_chart_style(self) -> None:
        try:
            self.fig.patch.set_facecolor(DARK_BG)
            self.ax.set_facecolor(DARK_PANEL)
            self.ax.tick_params(colors=DARK_FG)
            for spine in self.ax.spines.values():
                spine.set_color(DARK_BORDER)
            self.ax.grid(True, color=DARK_BORDER, linewidth=0.6, alpha=0.35)
        except Exception:
            pass

    def refresh(self) -> None:
        path = self.history_path

        # mtime cache so we don't redraw if nothing changed (account history OR trade history)
        try:
            m_hist = os.path.getmtime(path)
        except Exception:
            m_hist = None

        try:
            m_trades = os.path.getmtime(self.trade_history_path) if self.trade_history_path else None
        except Exception:
            m_trades = None

        candidates = [m for m in (m_hist, m_trades) if m is not None]
        mtime = max(candidates) if candidates else None

        if mtime is not None and self._last_mtime == mtime:
            return
        self._last_mtime = mtime


        points: List[Tuple[float, float]] = []

        try:
            if os.path.isfile(path):
                # Read the FULL history so the chart shows from the very beginning
                with open(path, "r", encoding="utf-8") as f:
                    lines = f.read().splitlines()

                for ln in lines:
                    try:
                        obj = json.loads(ln)
                        ts = obj.get("ts", None)
                        v = obj.get("total_account_value", None)
                        if ts is None or v is None:
                            continue

                        tsf = float(ts)
                        vf = float(v)

                        # Drop obviously invalid points early
                        if (not math.isfinite(tsf)) or (not math.isfinite(vf)) or (vf <= 0.0):
                            continue

                        points.append((tsf, vf))
                    except Exception:
                        continue
        except Exception:
            points = []

        # ---- Clean up history so single-tick bogus dips/spikes don't render ----
        if points:
            # Ensure chronological order
            points.sort(key=lambda x: x[0])

            # De-dupe identical timestamps (keep the latest occurrence)
            dedup: List[Tuple[float, float]] = []
            for tsf, vf in points:
                if dedup and tsf == dedup[-1][0]:
                    dedup[-1] = (tsf, vf)
                else:
                    dedup.append((tsf, vf))
            points = dedup


        # Downsample to <= 250 points by AVERAGING buckets instead of skipping points.
        # IMPORTANT: never average the VERY FIRST or VERY LAST point.
        # - First point should remain the true first historical value.
        # - Last point should remain the true current/final account value (so the title and chart end match account info).
        max_keep = min(max(2, int(self.max_points or 250)), 250)
        n = len(points)

        if n > max_keep:
            first_pt = points[0]
            last_pt = points[-1]

            mid_points = points[1:-1]
            mid_n = len(mid_points)
            keep_mid = max_keep - 2

            if keep_mid <= 0 or mid_n <= 0:
                points = [first_pt, last_pt]
            elif mid_n <= keep_mid:
                points = [first_pt] + mid_points + [last_pt]
            else:
                bucket_size = mid_n / float(keep_mid)
                new_mid: List[Tuple[float, float]] = []

                for i in range(keep_mid):
                    start = int(i * bucket_size)
                    end = int((i + 1) * bucket_size)
                    if end <= start:
                        end = start + 1
                    if start >= mid_n:
                        break
                    if end > mid_n:
                        end = mid_n

                    bucket = mid_points[start:end]
                    if not bucket:
                        continue

                    # Average timestamp and account value within the bucket (MID ONLY)
                    avg_ts = sum(p[0] for p in bucket) / len(bucket)
                    avg_val = sum(p[1] for p in bucket) / len(bucket)
                    new_mid.append((avg_ts, avg_val))

                points = [first_pt] + new_mid + [last_pt]



        # clear artists (fast) / fallback to cla()
        try:
            self.ax.lines.clear()
            self.ax.patches.clear()
            self.ax.collections.clear()  # scatter dots live here
            self.ax.texts.clear()        # labels/annotations live here
        except Exception:
            self.ax.cla()
            self._apply_dark_chart_style()


        if not points:
            self.ax.set_title("Account Value - no data", color=DARK_FG)
            self.last_update_label.config(text="Last: N/A")
            try:
                self.ax.text(
                    0.5,
                    0.5,
                    "Waiting for account history...\nStart trading or keep runner active to populate this chart.",
                    transform=self.ax.transAxes,
                    ha="center",
                    va="center",
                    color=DARK_MUTED,
                    fontsize=10,
                    bbox={"facecolor": DARK_PANEL, "edgecolor": DARK_BORDER, "pad": 8},
                )
            except Exception:
                pass
            self.canvas.draw_idle()
            return

        xs = list(range(len(points)))
        # Only show cent-level changes (hide sub-cent noise)
        ys = [round(p[1], 2) for p in points]

        self.ax.plot(xs, ys, linewidth=1.5)

        # --- Trade dots (BUY / DCA / SELL) for ALL coins ---
        try:
            trades = _read_trade_history_jsonl(self.trade_history_path) if self.trade_history_path else []
            if trades:
                ts_list = [float(p[0]) for p in points]  # matches xs/ys indices
                t_min = ts_list[0]
                t_max = ts_list[-1]

                for tr in trades:
                    # Determine label/color
                    side = str(tr.get("side", "")).lower().strip()
                    tag = str(tr.get("tag", "")).upper().strip()

                    if side == "buy":
                        action_label = "DCA" if tag == "DCA" else "BUY"
                        color = "purple" if tag == "DCA" else "red"
                    elif side == "sell":
                        action_label = "SELL"
                        color = "green"
                    else:
                        continue

                    # Prefix with coin (so the dot says which coin it is)
                    sym = str(tr.get("symbol", "")).upper().strip()
                    coin_tag = (sym.split("-")[0].split("/")[0].strip() if sym else "") or (sym or "?")
                    label = f"{coin_tag} {action_label}"

                    tts = tr.get("ts")
                    try:
                        tts = float(tts)
                    except Exception:
                        continue
                    if tts < t_min or tts > t_max:
                        continue

                    # nearest account-value point
                    i = bisect.bisect_left(ts_list, tts)
                    if i <= 0:
                        idx = 0
                    elif i >= len(ts_list):
                        idx = len(ts_list) - 1
                    else:
                        idx = i if abs(ts_list[i] - tts) < abs(tts - ts_list[i - 1]) else (i - 1)

                    x = idx
                    y = ys[idx]

                    self.ax.scatter([x], [y], s=30, color=color, zorder=6)
                    plotted_trade_points.append((tts, label, x, y))

                plotted_trade_points.sort(key=lambda item: item[0])
                for _, label, x, y in plotted_trade_points[-3:]:
                    self.ax.annotate(
                        label,
                        (x, y),
                        textcoords="offset points",
                        xytext=(0, 10),
                        ha="center",
                        fontsize=8,
                        color=DARK_FG,
                        zorder=7,
                    )

        except Exception:
            pass

        # Force 2 decimals on the y-axis labels (account value chart only)
        try:
            self.ax.yaxis.set_major_formatter(FuncFormatter(lambda y, _pos: f"${y:,.2f}"))
        except Exception:
            pass


        # x labels: show a few timestamps (date + time) - evenly spaced, never overlapping duplicates
        n = len(points)
        want = 5
        if n <= want:
            idxs = list(range(n))
        else:
            step = (n - 1) / float(want - 1)
            idxs = []
            last = -1
            for j in range(want):
                i = int(round(j * step))
                if i <= last:
                    i = last + 1
                if i >= n:
                    i = n - 1
                idxs.append(i)
                last = i

        tick_x = [xs[i] for i in idxs]
        tick_lbl = [time.strftime("%Y-%m-%d\n%H:%M:%S", time.localtime(points[i][0])) for i in idxs]
        try:
            self.ax.minorticks_off()
            self.ax.set_xticks(tick_x)
            self.ax.set_xticklabels(tick_lbl)
            self.ax.tick_params(axis="x", labelsize=8)
        except Exception:
            pass





        self.ax.set_xlim(-0.5, (len(points) - 0.5) + 0.6)

        try:
            self.ax.set_title(f"Account Value ({_fmt_money(ys[-1])})", color=DARK_FG)
        except Exception:
            self.ax.set_title("Account Value", color=DARK_FG)

        try:
            self.last_update_label.config(
                text=f"Last: {time.strftime('%H:%M:%S', time.localtime(points[-1][0]))}"
            )
        except Exception:
            self.last_update_label.config(text="Last: N/A")

        self.canvas.draw_idle()

    def export_png(self, path: str) -> bool:
        try:
            self.fig.savefig(path, dpi=160, facecolor=self.fig.get_facecolor())
            return True
        except Exception:
            return False



# -----------------------------
# Hub App
# -----------------------------

@dataclass
class ProcInfo:
    name: str
    path: str
    proc: Optional[subprocess.Popen] = None



@dataclass
class LogProc:
    """
    A running process with a live log queue for stdout/stderr lines.
    """
    info: ProcInfo
    log_q: "queue.Queue[str]"
    thread: Optional[threading.Thread] = None
    is_trainer: bool = False
    coin: Optional[str] = None



class PowerTraderHub(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("PowerTrader - Hub")
        self.geometry("1400x820")

        # Hard minimum window size so the UI can't be shrunk to a point where panes vanish.
        # (Keeps things usable even if someone aggressively resizes.)
        self.minsize(980, 640)

        # Debounce map for panedwindow clamp operations
        self._paned_clamp_after_ids: Dict[str, str] = {}

        # Force one and only one theme: dark mode everywhere.
        self._apply_forced_dark_mode()

        self.settings = self._load_settings()
        self._apply_font_scale_preset(str(self.settings.get("ui_font_scale_preset", "normal") or "normal"), persist=False)

        self.project_dir = BASE_DIR

        main_dir = str(self.settings.get("main_neural_dir") or "").strip()
        if main_dir and not os.path.isabs(main_dir):
            main_dir = os.path.abspath(os.path.join(self.project_dir, main_dir))
        if (not main_dir) or (not os.path.isdir(main_dir)):
            main_dir = self.project_dir
        self.settings["main_neural_dir"] = main_dir


        # hub data dir
        hub_dir = str(self.settings.get("hub_data_dir") or "").strip()
        if hub_dir and not os.path.isabs(hub_dir):
            hub_dir = os.path.abspath(os.path.join(self.project_dir, hub_dir))
        if (not hub_dir) or (not os.path.isdir(hub_dir)):
            hub_dir = DEFAULT_HUB_DATA_DIR
        self.hub_dir = os.path.abspath(hub_dir)
        _ensure_dir(self.hub_dir)

        # file paths written by pt_trader.py (after edits below)
        self.trader_status_path = os.path.join(self.hub_dir, "trader_status.json")
        self.trader_data_path = os.path.join(self.hub_dir, "trader_data.json")
        self.trade_history_path = os.path.join(self.hub_dir, "trade_history.jsonl")
        self.pnl_ledger_path = os.path.join(self.hub_dir, "pnl_ledger.json")
        self.account_value_history_path = os.path.join(self.hub_dir, "account_value_history.jsonl")
        self.runner_pid_path = os.path.join(self.hub_dir, "runner.pid")
        self.stop_flag_path = os.path.join(self.hub_dir, "stop_trading.flag")
        self.runner_logs_dir = os.path.join(self.hub_dir, "logs")
        _ensure_dir(self.runner_logs_dir)
        self.runner_log_path = os.path.join(self.runner_logs_dir, "runner.log")
        self.runner_launch_log_path = os.path.join(self.runner_logs_dir, "runner_ui_launch.log")
        self.trader_log_path = os.path.join(self.runner_logs_dir, "trader.log")
        self.market_state_dirs = {
            "stocks": os.path.join(self.hub_dir, "stocks"),
            "forex": os.path.join(self.hub_dir, "forex"),
        }
        for _mk_dir in self.market_state_dirs.values():
            _ensure_dir(_mk_dir)
        self.market_status_paths = {
            "stocks": os.path.join(self.market_state_dirs["stocks"], "alpaca_status.json"),
            "forex": os.path.join(self.market_state_dirs["forex"], "oanda_status.json"),
        }
        self.market_thinker_paths = {
            "stocks": os.path.join(self.market_state_dirs["stocks"], "stock_thinker_status.json"),
            "forex": os.path.join(self.market_state_dirs["forex"], "forex_thinker_status.json"),
        }
        self.market_trader_paths = {
            "stocks": os.path.join(self.market_state_dirs["stocks"], "stock_trader_status.json"),
            "forex": os.path.join(self.market_state_dirs["forex"], "forex_trader_status.json"),
        }
        self.market_scan_diag_paths = {
            "stocks": os.path.join(self.market_state_dirs["stocks"], "scan_diagnostics.json"),
            "forex": os.path.join(self.market_state_dirs["forex"], "scan_diagnostics.json"),
        }
        self.market_panels: Dict[str, Dict[str, Any]] = {}
        self._market_test_busy: Dict[str, bool] = {}
        self._market_refresh_busy: Dict[str, bool] = {}
        self._market_thinker_busy: Dict[str, bool] = {}
        self._market_trader_busy: Dict[str, bool] = {}
        self._last_market_refresh_ts: Dict[str, float] = {}
        self._last_market_thinker_ts: Dict[str, float] = {}
        self._last_market_trader_ts: Dict[str, float] = {}
        self._market_line_caches: Dict[str, Dict[str, Any]] = {}
        self._market_chart_redraw_after: Dict[str, str] = {}

        # file written by pt_thinker.py (runner readiness gate used for Start All)
        self.runner_ready_path = os.path.join(self.hub_dir, "runner_ready.json")
        self.autopilot_status_path = os.path.join(self.hub_dir, "autopilot_status.json")
        self.autofix_status_path = os.path.join(self.hub_dir, "autofix_status.json")
        self.autofix_state_path = os.path.join(self.hub_dir, "autofix_state.json")
        self.autofix_dir = os.path.join(self.hub_dir, "autofix")
        self.autofix_tickets_dir = os.path.join(self.autofix_dir, "tickets")
        self.autofix_patches_dir = os.path.join(self.autofix_dir, "patches")
        self.autofix_script_path = os.path.abspath(os.path.join(self.project_dir, "runtime", "pt_autofix.py"))
        self.user_action_required_path = os.path.join(self.hub_dir, "user_action_required.json")
        self.runtime_startup_checks_path = os.path.join(self.hub_dir, "runtime_startup_checks.json")
        self.onboarding_state_path = os.path.join(self.hub_dir, "onboarding_state.json")
        self.while_you_were_gone_snapshot_path = os.path.join(self.hub_dir, "while_you_were_gone_snapshot.json")
        self._while_you_were_gone_previous = _safe_read_json(self.while_you_were_gone_snapshot_path) or {}
        if not isinstance(self._while_you_were_gone_previous, dict):
            self._while_you_were_gone_previous = {}
        self._while_you_were_gone_shown = False
        self._settings_win: Optional[tk.Toplevel] = None
        self._autofix_win: Optional[tk.Toplevel] = None
        self._autofix_queue_ui: Dict[str, Any] = {}
        self._autofix_apply_busy = False
        self._autofix_request_busy = False
        self._invalid_credentials_route_done = False


        # internal: when Start All is pressed, we start the runner first and only start the trader once ready
        self._auto_start_trader_pending = False


        # cache latest trader status so charts can overlay buy/sell lines
        self._last_positions: Dict[str, dict] = {}

        # account value chart widget (created in _build_layout)
        self.account_chart = None



        # coin folders (neural outputs)
        self.coins = [c.upper().strip() for c in self.settings["coins"]]

        # On startup (like on Settings-save), create missing alt folders and copy the trainer into them.
        self._ensure_alt_coin_folders_and_trainer_on_startup()

        # Rebuild folder map after potential folder creation
        self.coin_folders = build_coin_folders(self.settings["main_neural_dir"], self.coins)


        # scripts
        self.proc_neural = ProcInfo(
            name="Neural Runner",
            path=os.path.abspath(os.path.join(self.project_dir, self.settings["script_neural_runner2"]))
        )
        self.proc_trader = ProcInfo(
            name="Trader",
            path=os.path.abspath(os.path.join(self.project_dir, self.settings["script_trader"]))
        )
        self.proc_runner = ProcInfo(
            name="Trade Supervisor",
            path=os.path.abspath(os.path.join(self.project_dir, "runtime", "pt_runner.py"))
        )

        self.proc_trainer_path = os.path.abspath(os.path.join(self.project_dir, self.settings["script_neural_trainer"]))

        # live log queues
        self.runner_log_q: "queue.Queue[str]" = queue.Queue()
        self.trader_log_q: "queue.Queue[str]" = queue.Queue()

        # trainers: coin -> LogProc
        self.trainers: Dict[str, LogProc] = {}

        self.fetcher = CandleFetcher()

        # Shared fixed-width font used by multiple UI panels (legend + logs).
        # It must exist before _build_layout() because some left-side widgets use it.
        _base = tkfont.nametofont("TkFixedFont")
        _half = max(8, int(round(abs(int(_base.cget("size"))) * 0.82)))
        self._live_log_font = _base.copy()
        self._live_log_font.configure(size=_half)

        self._build_menu()
        self._build_layout()
        self._bind_shortcuts()
        self.after(50, lambda: self._apply_layout_preset(str(self.settings.get("ui_layout_preset", "auto") or "auto"), persist=False))

        # Refresh charts immediately when a timeframe is changed (don't wait for the 10s throttle).
        self.bind_all("<<TimeframeChanged>>", self._on_timeframe_changed)

        self._last_chart_refresh = 0.0

        if bool(self.settings.get("auto_start_scripts", False)):
            self.start_all_scripts()

        self.after(250, self._tick)
        self.after(700, self._maybe_show_onboarding_wizard)
        self.after(1200, self._maybe_route_invalid_credentials)

        self.protocol("WM_DELETE_WINDOW", self._on_close)


    # ---- forced dark mode ----

    def _maybe_show_onboarding_wizard(self) -> None:
        try:
            st = _safe_read_json(self.onboarding_state_path) or {}
            if bool(st.get("completed", False)):
                return
            self._open_onboarding_wizard()
        except Exception:
            pass

    def _startup_invalid_credentials_target(self) -> str:
        warnings: List[str] = []
        try:
            runtime_checks = _safe_read_json(self.runtime_startup_checks_path)
            if isinstance(runtime_checks, dict):
                warnings = [
                    str(x or "").strip().lower()
                    for x in list(runtime_checks.get("warnings", []) or [])
                    if str(x or "").strip()
                ]
        except Exception:
            warnings = []

        alpaca_key, alpaca_secret = get_alpaca_creds(self.settings, base_dir=self.project_dir)
        oanda_account, oanda_token = get_oanda_creds(self.settings, base_dir=self.project_dir)
        rh_key_env, rh_secret_env = get_robinhood_creds_from_env()
        rh_key_file, rh_secret_file = get_robinhood_creds_from_files(self.project_dir)
        rh_ok = bool((rh_key_env and rh_secret_env) or (rh_key_file and rh_secret_file))

        alpaca_missing = (not str(alpaca_key or "").strip()) or (not str(alpaca_secret or "").strip())
        oanda_missing = (not str(oanda_account or "").strip()) or (not str(oanda_token or "").strip())
        if ("alpaca_credentials_missing" in warnings) or alpaca_missing:
            return "stocks_credentials"
        if ("oanda_credentials_missing" in warnings) or oanda_missing:
            return "forex_credentials"
        if not rh_ok:
            return "crypto_credentials"
        return ""

    def _maybe_route_invalid_credentials(self) -> None:
        if bool(getattr(self, "_invalid_credentials_route_done", False)):
            return
        self._invalid_credentials_route_done = True
        target = self._startup_invalid_credentials_target()
        if not target:
            return
        try:
            self.open_settings_dialog(focus_target=target)
        except Exception:
            pass

    def _open_onboarding_wizard(self) -> None:
        win = tk.Toplevel(self)
        win.title("Welcome - First-Time Setup")
        win.geometry("760x480")
        win.transient(self)
        try:
            win.grab_set()
        except Exception:
            pass

        frame = ttk.Frame(win)
        frame.pack(fill="both", expand=True, padx=12, pady=12)
        ttk.Label(frame, text="PowerTrader Quick Start", foreground=DARK_ACCENT2).pack(anchor="w", pady=(0, 8))
        msg = (
            "1. Open Settings and add broker credentials (paper/practice first).\n"
            "2. Test broker connections from Stocks and Forex tabs.\n"
            "3. Run scans and confirm leaders/health.\n"
            "4. Keep live-mode guard enabled until checklist is green.\n"
            "5. Use diagnostics export when investigating issues.\n\n"
            "Shortcuts:\n"
            "  Ctrl+T Toggle trades | Ctrl+, Settings | Ctrl+E Export trades | Ctrl+D Export diagnostics\n"
            "  Ctrl+Shift+A Autofix Queue\n"
            "  Ctrl+1 Crypto | Ctrl+2 Stocks | Ctrl+3 Forex"
        )
        txt = tk.Text(
            frame,
            height=18,
            wrap="word",
            bg=DARK_PANEL,
            fg=DARK_FG,
            relief="flat",
            bd=0,
            padx=8,
            pady=8,
        )
        txt.pack(fill="both", expand=True)
        txt.insert("1.0", msg)
        txt.configure(state="disabled")
        btns = ttk.Frame(frame)
        btns.pack(fill="x", pady=(10, 0))
        ttk.Button(btns, text="Open Settings", command=lambda: self.open_settings_dialog()).pack(side="left")

        def _finish() -> None:
            _safe_write_json(self.onboarding_state_path, {"completed": True, "ts": int(time.time())})
            win.destroy()

        ttk.Button(btns, text="Done", command=_finish).pack(side="right")

    def _apply_forced_dark_mode(self) -> None:
        """Force a single, global, non-optional dark theme."""
        # Root background (handles the areas behind ttk widgets)
        try:
            self.configure(bg=DARK_BG)
        except Exception:
            pass

        # Defaults for classic Tk widgets (Text/Listbox/Menu) created later
        try:
            self.option_add("*Text.background", DARK_PANEL)
            self.option_add("*Text.foreground", DARK_FG)
            self.option_add("*Text.insertBackground", DARK_FG)
            self.option_add("*Text.selectBackground", DARK_SELECT_BG)
            self.option_add("*Text.selectForeground", DARK_SELECT_FG)

            self.option_add("*Listbox.background", DARK_PANEL)
            self.option_add("*Listbox.foreground", DARK_FG)
            self.option_add("*Listbox.selectBackground", DARK_SELECT_BG)
            self.option_add("*Listbox.selectForeground", DARK_SELECT_FG)

            self.option_add("*Menu.background", DARK_BG2)
            self.option_add("*Menu.foreground", DARK_FG)
            self.option_add("*Menu.activeBackground", DARK_SELECT_BG)
            self.option_add("*Menu.activeForeground", DARK_SELECT_FG)
        except Exception:
            pass

        style = ttk.Style(self)

        # Pick a theme that is actually recolorable (Windows 'vista' theme ignores many color configs)
        try:
            style.theme_use("clam")
        except Exception:
            pass

        # Base defaults
        try:
            style.configure(".", background=DARK_BG, foreground=DARK_FG)
        except Exception:
            pass

        # Containers / text
        for name in ("TFrame", "TLabel", "TCheckbutton", "TRadiobutton"):
            try:
                style.configure(name, background=DARK_BG, foreground=DARK_FG)
            except Exception:
                pass
        try:
            style.configure("Toolbar.TFrame", background=DARK_BG2)
            style.configure("ToolbarTitle.TLabel", background=DARK_BG2, foreground=DARK_ACCENT2, font=("TkDefaultFont", 11, "bold"))
            style.configure("Subtle.TLabel", background=DARK_BG2, foreground=DARK_MUTED)
        except Exception:
            pass

        try:
            style.configure(
                "TLabelframe",
                background=DARK_BG,
                foreground=DARK_FG,
                bordercolor=DARK_BORDER,
                relief="solid",
                borderwidth=1,
            )
            style.configure("TLabelframe.Label", background=DARK_BG, foreground=DARK_ACCENT)
        except Exception:
            pass

        try:
            style.configure("TSeparator", background=DARK_BORDER)
        except Exception:
            pass

        # Buttons
        try:
            style.configure(
                "TButton",
                background=DARK_BG2,
                foreground=DARK_FG,
                bordercolor=DARK_BORDER,
                focusthickness=1,
                focuscolor=DARK_ACCENT,
                relief="flat",
                padding=(14, 8),
            )
            style.map(
                "TButton",
                background=[
                    ("active", "#102036"),
                    ("pressed", DARK_PANEL),
                    ("disabled", DARK_BG2),
                ],
                foreground=[
                    ("active", DARK_ACCENT),
                    ("disabled", DARK_MUTED),
                ],
                bordercolor=[
                    ("active", DARK_ACCENT2),
                    ("focus", DARK_ACCENT),
                ],
            )
            style.configure(
                "Accent.TButton",
                background=DARK_PANEL2,
                foreground=DARK_ACCENT2,
                bordercolor=DARK_ACCENT2,
                relief="flat",
                padding=(14, 8),
            )
            style.map(
                "Accent.TButton",
                background=[("active", "#15304D"), ("pressed", DARK_PANEL)],
                foreground=[("active", DARK_ACCENT)],
                bordercolor=[("active", DARK_ACCENT), ("focus", DARK_ACCENT2)],
            )
            style.configure(
                "Compact.TButton",
                background=DARK_BG2,
                foreground=DARK_FG,
                bordercolor=DARK_BORDER,
                relief="flat",
                padding=(10, 6),
            )
            style.map(
                "Compact.TButton",
                background=[("active", DARK_PANEL2), ("pressed", DARK_PANEL)],
                foreground=[("active", DARK_ACCENT2)],
                bordercolor=[("active", DARK_ACCENT2), ("focus", DARK_ACCENT2)],
            )
        except Exception:
            pass

        # Entries / combos
        try:
            style.configure(
                "TEntry",
                fieldbackground=DARK_PANEL,
                foreground=DARK_FG,
                bordercolor=DARK_BORDER,
                insertcolor=DARK_FG,
            )
        except Exception:
            pass

        try:
            style.configure(
                "TCombobox",
                fieldbackground=DARK_PANEL,
                background=DARK_PANEL,
                foreground=DARK_FG,
                bordercolor=DARK_BORDER,
                arrowcolor=DARK_ACCENT,
                padding=4,
            )
            style.map(
                "TCombobox",
                fieldbackground=[
                    ("readonly", DARK_PANEL),
                    ("focus", "#102036"),
                ],
                foreground=[("readonly", DARK_FG)],
                background=[("readonly", DARK_PANEL)],
            )
        except Exception:
            pass

        # Notebooks
        try:
            style.configure("TNotebook", background=DARK_BG, bordercolor=DARK_BORDER)
            style.configure("TNotebook.Tab", background=DARK_BG2, foreground=DARK_FG, padding=(14, 8))
            style.map(
                "TNotebook.Tab",
                background=[
                    ("selected", "#102036"),
                    ("active", DARK_PANEL2),
                ],
                foreground=[
                    ("selected", DARK_ACCENT),
                    ("active", DARK_ACCENT2),
                ],
            )

            # Charts tabs need to wrap to multiple lines. ttk.Notebook can't do that,
            # so we hide the Notebook's native tabs and render our own wrapping tab bar.
            #
            # IMPORTANT: the layout must exclude Notebook.tab entirely, and on some themes
            # you must keep Notebook.padding for proper sizing; otherwise the tab strip
            # can still render.
            style.configure("HiddenTabs.TNotebook", tabmargins=0)
            style.layout(
                "HiddenTabs.TNotebook",
                [
                    (
                        "Notebook.padding",
                        {
                            "sticky": "nswe",
                            "children": [
                                ("Notebook.client", {"sticky": "nswe"}),
                            ],
                        },
                    )
                ],
            )

            # Wrapping chart-tab buttons (normal + selected)
            style.configure(
                "ChartTab.TButton",
                background=DARK_BG2,
                foreground=DARK_FG,
                bordercolor=DARK_BORDER,
                padding=(10, 6),
            )
            style.map(
                "ChartTab.TButton",
                background=[("active", DARK_PANEL2), ("pressed", DARK_PANEL)],
                foreground=[("active", DARK_ACCENT2)],
                bordercolor=[("active", DARK_ACCENT2), ("focus", DARK_ACCENT)],
            )

            style.configure(
                "ChartTabSelected.TButton",
                background=DARK_PANEL,
                foreground=DARK_ACCENT,
                bordercolor=DARK_ACCENT2,
                padding=(10, 6),
            )
        except Exception:
            pass


        # Treeview (Current Trades table)
        try:
            style.configure(
                "Treeview",
                background=DARK_PANEL,
                fieldbackground=DARK_PANEL,
                foreground=DARK_FG,
                bordercolor=DARK_BORDER,
                lightcolor=DARK_BORDER,
                darkcolor=DARK_BORDER,
                rowheight=26,
            )
            style.map(
                "Treeview",
                background=[("selected", DARK_SELECT_BG)],
                foreground=[("selected", DARK_SELECT_FG)],
            )

            style.configure("Treeview.Heading", background=DARK_BG2, foreground=DARK_ACCENT, relief="flat")
            style.map(
                "Treeview.Heading",
                background=[("active", DARK_PANEL2)],
                foreground=[("active", DARK_ACCENT2)],
            )
        except Exception:
            pass

        # Panedwindows / scrollbars
        try:
            style.configure("TPanedwindow", background=DARK_BG)
        except Exception:
            pass

        for sb in ("Vertical.TScrollbar", "Horizontal.TScrollbar"):
            try:
                style.configure(
                    sb,
                    background=DARK_BG2,
                    troughcolor=DARK_BG,
                    bordercolor=DARK_BORDER,
                    arrowcolor=DARK_ACCENT,
                )
            except Exception:
                pass

    # ---- settings ----

    def _load_settings(self) -> dict:
        settings_path = resolve_settings_path(BASE_DIR) or SETTINGS_PATH or os.path.join(BASE_DIR, SETTINGS_FILE)
        data = _safe_read_json(settings_path)
        if not isinstance(data, dict):
            data = {}
        return sanitize_settings(data, defaults=DEFAULT_SETTINGS)

    def _save_settings(self) -> None:
        settings_path = resolve_settings_path(BASE_DIR) or SETTINGS_PATH or os.path.join(BASE_DIR, SETTINGS_FILE)
        self.settings = sanitize_settings(self.settings, defaults=DEFAULT_SETTINGS)
        _safe_write_json(settings_path, self.settings)


    def _settings_getter(self) -> dict:
        return self.settings

    def _ensure_alt_coin_folders_and_trainer_on_startup(self) -> None:
        """
        Startup behavior (mirrors Settings-save behavior):
        - For every configured coin:
            - ensure <main_dir>/<coin> exists
            - copy trainer script into the coin folder if missing
        """
        try:
            coins = [str(c).strip().upper() for c in (self.settings.get("coins") or []) if str(c).strip()]
            main_dir = (self.settings.get("main_neural_dir") or self.project_dir or BASE_DIR).strip()

            trainer_name = os.path.basename(str(self.settings.get("script_neural_trainer", "engines/pt_trainer.py")))

            # Source trainer: project root by default; fallback to BTC folder and configured path.
            src_project_trainer = os.path.join(self.project_dir, trainer_name)
            src_btc_trainer = os.path.join(main_dir, "BTC", trainer_name)

            # Best-effort fallback if the main folder doesn't have it (keeps behavior robust)
            src_cfg_trainer = str(self.settings.get("script_neural_trainer", trainer_name))
            if os.path.isfile(src_project_trainer):
                src_trainer_path = src_project_trainer
            elif os.path.isfile(src_btc_trainer):
                src_trainer_path = src_btc_trainer
            else:
                src_trainer_path = src_cfg_trainer

            for coin in coins:
                coin_dir = os.path.join(main_dir, coin)
                if not os.path.isdir(coin_dir):
                    os.makedirs(coin_dir, exist_ok=True)

                dst_trainer_path = os.path.join(coin_dir, trainer_name)
                if (not os.path.isfile(dst_trainer_path)) and os.path.isfile(src_trainer_path):
                    shutil.copy2(src_trainer_path, dst_trainer_path)
        except Exception:
            pass

    # ---- menu / layout ----

    def _apply_font_scale_preset(self, preset: str, persist: bool = True) -> None:
        mode = str(preset or "normal").strip().lower()
        if mode not in {"small", "normal", "large"}:
            mode = "normal"
        scale_map = {"small": 0.92, "normal": 1.0, "large": 1.12}
        factor = float(scale_map.get(mode, 1.0))
        try:
            self.tk.call("tk", "scaling", max(0.7, min(2.0, factor)))
        except Exception:
            pass
        try:
            base = tkfont.nametofont("TkFixedFont")
            sz = max(7, int(round(abs(int(base.cget("size"))) * 0.82 * factor)))
            self._live_log_font.configure(size=sz)
        except Exception:
            pass
        if persist:
            self.settings["ui_font_scale_preset"] = mode
            try:
                self._save_settings()
            except Exception:
                pass

    def _apply_layout_preset(self, preset: str, persist: bool = True) -> None:
        mode = str(preset or "auto").strip().lower()
        if mode not in {"auto", "compact", "normal", "wide"}:
            mode = "auto"
        if mode == "auto":
            try:
                w = int(self.winfo_width() or 1400)
            except Exception:
                w = 1400
            mode = "compact" if w < 1320 else ("wide" if w >= 1680 else "normal")
        geom = {"compact": "1280x760", "normal": "1400x820", "wide": "1720x940"}.get(mode, "1400x820")
        try:
            self.geometry(geom)
        except Exception:
            pass
        try:
            self._schedule_paned_clamp(getattr(self, "_pw_outer", None))
            self._schedule_paned_clamp(getattr(self, "_pw_left_split", None))
            self._schedule_paned_clamp(getattr(self, "_pw_right_split", None))
        except Exception:
            pass
        if persist:
            self.settings["ui_layout_preset"] = mode
            try:
                self._save_settings()
            except Exception:
                pass

    def _bind_shortcuts(self) -> None:
        try:
            self.bind_all("<Control-t>", lambda _e: self.toggle_all_scripts())
            self.bind_all("<Control-comma>", lambda _e: self.open_settings_dialog())
            self.bind_all("<Control-e>", lambda _e: self._export_trade_history_csv())
            self.bind_all("<Control-Shift-E>", lambda _e: self._export_active_chart_png())
            self.bind_all("<Control-Shift-S>", lambda _e: self._export_market_status_snapshot_json())
            self.bind_all("<Control-Shift-A>", lambda _e: self.open_autofix_queue())
            self.bind_all("<Control-d>", lambda _e: self._export_diagnostics_bundle())
            self.bind_all("<Control-1>", lambda _e: self._select_market_tab("crypto"))
            self.bind_all("<Control-2>", lambda _e: self._select_market_tab("stocks"))
            self.bind_all("<Control-3>", lambda _e: self._select_market_tab("forex"))
        except Exception:
            pass

    def _set_badge_style(self, label: Optional[tk.Label], text: str, tone: str = "muted") -> None:
        if label is None:
            return
        palette = BADGE_STYLES.get(str(tone or "muted").strip().lower(), BADGE_STYLES["muted"])
        bg, fg, border = palette
        try:
            label.configure(
                text=f" {str(text or '').strip()} ",
                bg=bg,
                fg=fg,
                highlightbackground=border,
                highlightcolor=border,
                highlightthickness=1,
                bd=0,
                relief="flat",
            )
        except Exception:
            pass

    def _alert_reason_compact(self, reason: str) -> str:
        key = str(reason or "").strip().lower()
        labels = {
            "scan_reject_pressure": "Reject Pressure",
            "error_incidents": "Runtime Errors",
            "cadence_drift_pressure": "Cadence Drift",
            "startup_checks_failed": "Startup Checks Failed",
            "startup_warnings": "Startup Warnings",
            "api_unstable": "API Unstable",
            "scanner_reject_spike": "Reject Spike",
            "market_loop_stale": "Loop Stale",
            "exposure_concentration": "Exposure Concentration",
            "execution_temporarily_disabled": "Execution Cooldown",
            "key_rotation_due": "Key Rotation",
            "drawdown_guard_triggered": "Drawdown Guard",
            "stop_flag_active": "Stop Flag",
        }
        return str(labels.get(key, key.replace("_", " ").title()) or "Alert")

    def _select_market_tab(self, tab_name: str) -> None:
        name = str(tab_name or "").strip().lower()
        nb = getattr(self, "market_nb", None)
        if nb is None:
            return
        try:
            if name == "crypto":
                nb.select(self.crypto_market_tab)
            elif name == "stocks":
                nb.select(self.stocks_market_tab)
            elif name == "forex":
                nb.select(self.forex_market_tab)
        except Exception:
            pass

    def _build_global_command_bar(self) -> None:
        bar = ttk.Frame(self, style="Toolbar.TFrame")
        bar.pack(fill="x", side="top", padx=8, pady=(8, 0))
        self._global_cmd_bar = bar

        left = ttk.Frame(bar, style="Toolbar.TFrame")
        left.pack(side="left", fill="x", expand=True)
        ttk.Label(left, text="PowerTrader Command Center", style="ToolbarTitle.TLabel").pack(anchor="w")
        self.lbl_toolbar_subtitle = ttk.Label(
            left,
            text="Operator workflow: verify health -> scan leaders -> execute with safety gates.",
            style="Subtle.TLabel",
        )
        self.lbl_toolbar_subtitle.pack(anchor="w", pady=(2, 0))

        center = ttk.Frame(bar, style="Toolbar.TFrame")
        center.pack(side="left", padx=(14, 12))
        self.lbl_toolbar_state_badge = tk.Label(center, text="", padx=8, pady=3)
        self.lbl_toolbar_state_badge.pack(side="left", padx=(0, 6))
        self.lbl_toolbar_api_badge = tk.Label(center, text="", padx=8, pady=3)
        self.lbl_toolbar_api_badge.pack(side="left", padx=(0, 6))
        self.lbl_toolbar_checks_badge = tk.Label(center, text="", padx=8, pady=3)
        self.lbl_toolbar_checks_badge.pack(side="left")

        right = ttk.Frame(bar, style="Toolbar.TFrame")
        right.pack(side="right")

        self.btn_toolbar_toggle = ttk.Button(
            right,
            text="Start Trades",
            style="Accent.TButton",
            command=self.toggle_all_scripts,
        )
        self.btn_toolbar_toggle.pack(side="right", padx=(8, 0))

        ttk.Button(
            right,
            text="Settings",
            style="Compact.TButton",
            command=self.open_settings_dialog,
        ).pack(side="right", padx=(8, 0))
        ttk.Button(
            right,
            text="AI Assist",
            style="Compact.TButton",
            command=self.open_autofix_queue,
        ).pack(side="right", padx=(8, 0))
        ttk.Button(
            right,
            text="Diagnostics",
            style="Compact.TButton",
            command=self._run_quick_diagnostics,
        ).pack(side="right", padx=(8, 0))
        ttk.Button(
            right,
            text="Export Snapshot",
            style="Compact.TButton",
            command=self._export_market_status_snapshot_json,
        ).pack(side="right", padx=(8, 0))
        ttk.Button(
            right,
            text="Quick Start",
            style="Compact.TButton",
            command=self._open_onboarding_wizard,
        ).pack(side="right", padx=(8, 0))
        ttk.Button(
            right,
            text="Crypto",
            style="Compact.TButton",
            command=lambda: self._select_market_tab("crypto"),
        ).pack(side="right", padx=(8, 0))
        ttk.Button(
            right,
            text="Stocks",
            style="Compact.TButton",
            command=lambda: self._select_market_tab("stocks"),
        ).pack(side="right", padx=(8, 0))
        ttk.Button(
            right,
            text="Forex",
            style="Compact.TButton",
            command=lambda: self._select_market_tab("forex"),
        ).pack(side="right", padx=(8, 0))

        self._set_badge_style(self.lbl_toolbar_state_badge, "RUNTIME: STOPPED", tone="muted")
        self._set_badge_style(self.lbl_toolbar_api_badge, "BROKERS: N/A", tone="muted")
        self._set_badge_style(self.lbl_toolbar_checks_badge, "CHECKS: N/A", tone="muted")

    def _update_global_command_bar(
        self,
        runtime_state: str,
        neural_running: bool,
        trader_running: bool,
        runtime_snapshot: Optional[Dict[str, Any]] = None,
        can_toggle: bool = True,
    ) -> None:
        if not hasattr(self, "lbl_toolbar_state_badge"):
            return
        state = str(runtime_state or "STOPPED").strip().upper() or "STOPPED"
        if neural_running or trader_running:
            run_txt = f"RUNTIME: {state}"
            run_tone = "good" if state == "RUNNING" else ("warn" if state == "STOPPING" else ("bad" if state == "ERROR" else "info"))
        else:
            run_txt = "RUNTIME: STOPPED"
            run_tone = "muted"
        self._set_badge_style(self.lbl_toolbar_state_badge, run_txt, tone=run_tone)

        snap = runtime_snapshot if isinstance(runtime_snapshot, dict) else {}
        aq = snap.get("api_quota", {}) if isinstance(snap.get("api_quota", {}), dict) else {}
        autofix_snap = snap.get("autofix", {}) if isinstance(snap.get("autofix", {}), dict) else {}
        quota_state = str(aq.get("status", "n/a") or "n/a").strip().lower()
        q15 = int(aq.get("total_15m", 0) or 0)
        if quota_state in {"ok", "healthy"}:
            api_tone = "good"
        elif quota_state in {"warning", "warn", "degraded"}:
            api_tone = "warn"
        elif quota_state in {"error", "critical"}:
            api_tone = "bad"
        else:
            api_tone = "muted"
        ai_key_missing = bool(autofix_snap.get("enabled", False)) and (not bool(autofix_snap.get("api_key_configured", True)))
        if ai_key_missing and api_tone == "good":
            api_tone = "warn"
        api_txt = f"APIs: {quota_state.upper()} ({q15}/15m)"
        if ai_key_missing:
            api_txt += " | AI KEY MISSING"
        self._set_badge_style(self.lbl_toolbar_api_badge, api_txt, tone=api_tone)

        checks = snap.get("checks", {}) if isinstance(snap.get("checks", {}), dict) else {}
        alerts = snap.get("alerts", {}) if isinstance(snap.get("alerts", {}), dict) else {}
        checks_ok = bool(checks.get("ok", False))
        sev = str(alerts.get("severity", "n/a") or "n/a").strip().lower()
        reasons = [str(x or "").strip() for x in list(alerts.get("reasons", []) or []) if str(x or "").strip()]
        if checks_ok and sev in {"ok", "low", "none"}:
            checks_tone = "good"
        elif sev in {"critical", "high"}:
            checks_tone = "bad"
        elif sev in {"medium", "warn", "warning"}:
            checks_tone = "warn"
        else:
            checks_tone = ("info" if checks_ok else "warn")
        primary_reason = self._alert_reason_compact(reasons[0]) if reasons else ""
        reason_suffix = ""
        if primary_reason:
            extra = max(0, len(reasons) - 1)
            reason_suffix = f" | {primary_reason}" + (f" +{extra}" if extra > 0 else "")
        elif ai_key_missing:
            reason_suffix = " | ai_assist_key_missing"
            if checks_tone == "good":
                checks_tone = "warn"
        checks_txt = f"CHECKS: {'PASS' if checks_ok else 'ATTN'} | ALERTS {sev.upper()}{reason_suffix}"
        if len(checks_txt) > 92:
            checks_txt = checks_txt[:89] + "..."
        self._set_badge_style(self.lbl_toolbar_checks_badge, checks_txt, tone=checks_tone)

        subtitle = (
            f"Stage={str(self.settings.get('market_rollout_stage', 'legacy') or 'legacy')} | "
            f"Auto Stocks={'ON' if bool(self.settings.get('stock_auto_trade_enabled', False)) else 'OFF'} | "
            f"Auto Forex={'ON' if bool(self.settings.get('forex_auto_trade_enabled', False)) else 'OFF'}"
        )
        try:
            self.lbl_toolbar_subtitle.configure(text=subtitle)
        except Exception:
            pass
        try:
            if hasattr(self, "btn_toolbar_toggle"):
                if neural_running or trader_running or bool(getattr(self, "_auto_start_trader_pending", False)):
                    self.btn_toolbar_toggle.configure(text="Stop Trades", state="normal")
                else:
                    self.btn_toolbar_toggle.configure(
                        text=("Start Trades" if can_toggle else "Train All to Start"),
                        state=("normal" if can_toggle else "disabled"),
                    )
        except Exception:
            pass


    def _build_menu(self) -> None:
        menubar = tk.Menu(
            self,
            bg=DARK_BG2,
            fg=DARK_FG,
            activebackground=DARK_SELECT_BG,
            activeforeground=DARK_SELECT_FG,
            bd=0,
            relief="flat",
        )

        m_scripts = tk.Menu(
            menubar,
            tearoff=0,
            bg=DARK_BG2,
            fg=DARK_FG,
            activebackground=DARK_SELECT_BG,
            activeforeground=DARK_SELECT_FG,
        )
        m_scripts.add_command(label="Start Trades", command=self.start_all_scripts)
        m_scripts.add_command(label="Stop Trades", command=self.stop_all_scripts)
        m_scripts.add_separator()
        m_scripts.add_command(label="Start Neural Runner", command=self.start_neural)
        m_scripts.add_command(label="Stop Neural Runner", command=self.stop_neural)
        m_scripts.add_separator()
        m_scripts.add_command(label="Start Trader", command=self.start_trader)
        m_scripts.add_command(label="Stop Trader", command=self.stop_trader)
        menubar.add_cascade(label="Scripts", menu=m_scripts)

        m_settings = tk.Menu(
            menubar,
            tearoff=0,
            bg=DARK_BG2,
            fg=DARK_FG,
            activebackground=DARK_SELECT_BG,
            activeforeground=DARK_SELECT_FG,
        )
        m_settings.add_command(label="Settings...", command=self.open_settings_dialog)
        m_settings.add_command(label="Autofix Queue...", command=self.open_autofix_queue, accelerator="Ctrl+Shift+A")
        m_font = tk.Menu(
            m_settings,
            tearoff=0,
            bg=DARK_BG2,
            fg=DARK_FG,
            activebackground=DARK_SELECT_BG,
            activeforeground=DARK_SELECT_FG,
        )
        m_font.add_command(label="Small", command=lambda: self._apply_font_scale_preset("small"))
        m_font.add_command(label="Normal", command=lambda: self._apply_font_scale_preset("normal"))
        m_font.add_command(label="Large", command=lambda: self._apply_font_scale_preset("large"))
        m_settings.add_cascade(label="Font Scale", menu=m_font)
        m_layout = tk.Menu(
            m_settings,
            tearoff=0,
            bg=DARK_BG2,
            fg=DARK_FG,
            activebackground=DARK_SELECT_BG,
            activeforeground=DARK_SELECT_FG,
        )
        m_layout.add_command(label="Auto", command=lambda: self._apply_layout_preset("auto"))
        m_layout.add_command(label="Compact", command=lambda: self._apply_layout_preset("compact"))
        m_layout.add_command(label="Normal", command=lambda: self._apply_layout_preset("normal"))
        m_layout.add_command(label="Wide", command=lambda: self._apply_layout_preset("wide"))
        m_settings.add_cascade(label="Layout", menu=m_layout)
        menubar.add_cascade(label="Settings", menu=m_settings)

        m_file = tk.Menu(
            menubar,
            tearoff=0,
            bg=DARK_BG2,
            fg=DARK_FG,
            activebackground=DARK_SELECT_BG,
            activeforeground=DARK_SELECT_FG,
        )
        m_file.add_command(label="Export Current Chart PNG", command=self._export_active_chart_png)
        m_file.add_command(label="Export Market Status Snapshot JSON", command=self._export_market_status_snapshot_json)
        m_file.add_command(label="Export Runtime Summary TXT", command=self._export_runtime_summary_txt)
        m_file.add_command(label="Export Scanner Quality JSON", command=self._export_scanner_quality_reports_json)
        m_file.add_command(label="Run Quick Diagnostics", command=self._run_quick_diagnostics)
        m_file.add_command(label="Export Trade History CSV", command=self._export_trade_history_csv)
        m_file.add_command(label="Export Diagnostics Bundle", command=self._export_diagnostics_bundle)
        m_file.add_separator()
        m_file.add_command(label="Exit", command=self._on_close)
        menubar.add_cascade(label="File", menu=m_file)

        self.config(menu=menubar)

    def _export_trade_history_csv(self) -> None:
        try:
            rows = _read_trade_history_jsonl(self.trade_history_path)
            if not rows:
                messagebox.showinfo("Export", "No trade history available to export.")
                return
            default_name = f"trade_history_{time.strftime('%Y%m%d_%H%M%S')}.csv"
            path = filedialog.asksaveasfilename(
                title="Export Trade History CSV",
                defaultextension=".csv",
                initialfile=default_name,
                filetypes=[("CSV", "*.csv"), ("All files", "*.*")],
            )
            if not path:
                return
            cols = [
                "ts", "side", "tag", "symbol", "qty", "price", "avg_cost_basis", "pnl_pct",
                "fees_usd", "realized_profit_usd", "order_id",
                "buying_power_before", "buying_power_after", "buying_power_delta",
                "position_cost_used_usd", "position_cost_after_usd",
            ]
            with open(path, "w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
                w.writeheader()
                for row in rows:
                    if isinstance(row, dict):
                        w.writerow(row)
            messagebox.showinfo("Export", f"Trade history exported:\n{path}")
        except Exception as exc:
            messagebox.showerror("Export failed", f"Could not export trade history CSV:\n{exc}")

    def _export_diagnostics_bundle(self) -> None:
        try:
            default_name = f"diagnostics_{time.strftime('%Y%m%d_%H%M%S')}.zip"
            path = filedialog.asksaveasfilename(
                title="Export Diagnostics Bundle",
                defaultextension=".zip",
                initialfile=default_name,
                filetypes=[("ZIP", "*.zip"), ("All files", "*.*")],
            )
            if not path:
                return
            picks = [
                os.path.join(self.hub_dir, "runtime_state.json"),
                os.path.join(self.hub_dir, "runtime_startup_checks.json"),
                os.path.join(self.hub_dir, "market_sla_metrics.json"),
                os.path.join(self.hub_dir, "market_trends.json"),
                os.path.join(self.hub_dir, "incidents.jsonl"),
                os.path.join(self.hub_dir, "runtime_events.jsonl"),
                os.path.join(self.hub_dir, "smoke_test_report.json"),
                os.path.join(self.hub_dir, "key_rotation_status.json"),
            ]
            logs_dir = os.path.join(self.hub_dir, "logs")
            if os.path.isdir(logs_dir):
                for name in os.listdir(logs_dir):
                    p = os.path.join(logs_dir, name)
                    if os.path.isfile(p):
                        picks.append(p)

            manifest = {"ts": int(time.time()), "hub_dir": self.hub_dir, "files": []}
            with zipfile.ZipFile(path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                for p in picks:
                    if not os.path.isfile(p):
                        continue
                    rel = os.path.relpath(p, self.hub_dir)
                    try:
                        st = os.stat(p)
                        manifest["files"].append({"path": rel, "size": int(st.st_size), "mtime": int(st.st_mtime)})
                    except Exception:
                        manifest["files"].append({"path": rel, "size": 0, "mtime": 0})
                    zf.write(p, arcname=rel)
                zf.writestr("manifest.json", json.dumps(manifest, indent=2))
            messagebox.showinfo("Export", f"Diagnostics bundle exported:\n{path}")
        except Exception as exc:
            messagebox.showerror("Export failed", f"Could not export diagnostics bundle:\n{exc}")

    def _charts_export_dir(self) -> str:
        out_dir = os.path.join(self.hub_dir, "exports", "charts")
        _ensure_dir(out_dir)
        return out_dir

    def _next_chart_export_path(self, prefix: str) -> str:
        safe_prefix = str(prefix or "chart").strip().lower().replace(" ", "_")
        if not safe_prefix:
            safe_prefix = "chart"
        stamp = time.strftime("%Y%m%d_%H%M%S")
        return os.path.join(self._charts_export_dir(), f"{safe_prefix}_{stamp}.png")

    def _export_active_chart_png(self) -> None:
        try:
            current = str(getattr(self, "_current_chart_page", "ACCOUNT") or "ACCOUNT").strip().upper()
            if current == "ACCOUNT":
                if not self.account_chart:
                    messagebox.showinfo("Export", "Account chart is not available yet.")
                    return
                out_path = self._next_chart_export_path("crypto_account")
                ok = bool(self.account_chart.export_png(out_path))
            else:
                chart = self.charts.get(current)
                if chart is None:
                    messagebox.showinfo("Export", f"{current} chart is not available yet.")
                    return
                out_path = self._next_chart_export_path(f"crypto_{current}")
                ok = bool(chart.export_png(out_path))
            if not ok:
                messagebox.showerror("Export failed", "Could not export chart PNG.")
                return
            messagebox.showinfo("Export", f"Chart PNG exported:\n{out_path}")
        except Exception as exc:
            messagebox.showerror("Export failed", f"Could not export chart PNG:\n{exc}")

    def _export_market_status_snapshot_json(self) -> None:
        try:
            stocks = load_market_status_bundle(
                status_path=str(self.market_status_paths.get("stocks", "") or ""),
                trader_path=str(self.market_trader_paths.get("stocks", "") or ""),
                thinker_path=str(self.market_thinker_paths.get("stocks", "") or ""),
                scan_diag_path=str(self.market_scan_diag_paths.get("stocks", "") or ""),
                history_path=os.path.join(self.market_state_dirs.get("stocks", self.hub_dir), "execution_audit.jsonl"),
                history_limit=80,
                market_key="stocks",
            )
            forex = load_market_status_bundle(
                status_path=str(self.market_status_paths.get("forex", "") or ""),
                trader_path=str(self.market_trader_paths.get("forex", "") or ""),
                thinker_path=str(self.market_thinker_paths.get("forex", "") or ""),
                scan_diag_path=str(self.market_scan_diag_paths.get("forex", "") or ""),
                history_path=os.path.join(self.market_state_dirs.get("forex", self.hub_dir), "execution_audit.jsonl"),
                history_limit=80,
                market_key="forex",
            )
            payload = {
                "ts": int(time.time()),
                "runner": _safe_read_json(self.trader_status_path) or {},
                "runtime_state": _safe_read_json(os.path.join(self.hub_dir, "runtime_state.json")) or {},
                "market_loop_status": _safe_read_json(os.path.join(self.hub_dir, "market_loop_status.json")) or {},
                "stocks": stocks,
                "forex": forex,
            }
            out_path = os.path.join(
                self._charts_export_dir(),
                f"market_status_snapshot_{time.strftime('%Y%m%d_%H%M%S')}.json",
            )
            _safe_write_json(out_path, payload)
            messagebox.showinfo("Export", f"Market status snapshot exported:\n{out_path}")
        except Exception as exc:
            messagebox.showerror("Export failed", f"Could not export market status snapshot:\n{exc}")

    def _export_runtime_summary_txt(self) -> None:
        try:
            rt = _safe_read_json(os.path.join(self.hub_dir, "runtime_state.json")) or {}
            checks = rt.get("checks", {}) if isinstance(rt.get("checks", {}), dict) else {}
            alerts = rt.get("alerts", {}) if isinstance(rt.get("alerts", {}), dict) else {}
            incidents = rt.get("incidents_last_200", {}) if isinstance(rt.get("incidents_last_200", {}), dict) else {}
            broker_health = rt.get("broker_health", {}) if isinstance(rt.get("broker_health", {}), dict) else {}
            broker_backoff = rt.get("broker_backoff", {}) if isinstance(rt.get("broker_backoff", {}), dict) else {}
            drawdown = rt.get("drawdown_guard", {}) if isinstance(rt.get("drawdown_guard", {}), dict) else {}
            stop_flag = rt.get("stop_flag", {}) if isinstance(rt.get("stop_flag", {}), dict) else {}
            lines = [
                f"PowerTrader Runtime Summary | generated {time.strftime('%Y-%m-%d %H:%M:%S')}",
                "",
                f"Runner state: {str((rt.get('runner', {}) if isinstance(rt.get('runner', {}), dict) else {}).get('state', 'N/A'))}",
                f"Checks OK: {bool(checks.get('ok', False))}",
                f"Alert severity: {str(alerts.get('severity', 'N/A'))}",
                f"Incidents last 200: {int(incidents.get('count', 0) or 0)}",
                f"Incidents last 1h: {int(incidents.get('count_1h', 0) or 0)}",
                f"Drawdown guard recent: {bool(drawdown.get('triggered_recent', False))}",
                f"Stop flag active: {bool(stop_flag.get('active', False))}",
                f"Broker retry-after events (24h): {int(broker_backoff.get('count_24h', 0) or 0)}",
                f"Broker retry-after avg/max wait s: {float(broker_backoff.get('avg_wait_s', 0.0) or 0.0):.2f}/{float(broker_backoff.get('max_wait_s', 0.0) or 0.0):.2f}",
                "",
                "Broker health:",
            ]
            for key in ("alpaca", "oanda", "kucoin"):
                row = broker_health.get(key, {}) if isinstance(broker_health.get(key, {}), dict) else {}
                lines.append(
                    f"- {key}: state={str(row.get('state', 'n/a'))} quota15m={int(row.get('quota_15m', 0) or 0)} msg={str(row.get('msg', '') or '')}"
                )
            out_path = os.path.join(
                self._charts_export_dir(),
                f"runtime_summary_{time.strftime('%Y%m%d_%H%M%S')}.txt",
            )
            with open(out_path, "w", encoding="utf-8") as f:
                f.write("\n".join(lines) + "\n")
            messagebox.showinfo("Export", f"Runtime summary exported:\n{out_path}")
        except Exception as exc:
            messagebox.showerror("Export failed", f"Could not export runtime summary:\n{exc}")

    def _export_scanner_quality_reports_json(self) -> None:
        try:
            payload = {
                "ts": int(time.time()),
                "stocks": _safe_read_json(os.path.join(self.hub_dir, "stocks", "universe_quality.json")) or {},
                "forex": _safe_read_json(os.path.join(self.hub_dir, "forex", "universe_quality.json")) or {},
                "scanner_cadence_drift": _safe_read_json(os.path.join(self.hub_dir, "scanner_cadence_drift.json")) or {},
            }
            out_path = os.path.join(
                self._charts_export_dir(),
                f"scanner_quality_{time.strftime('%Y%m%d_%H%M%S')}.json",
            )
            _safe_write_json(out_path, payload)
            messagebox.showinfo("Export", f"Scanner quality exported:\n{out_path}")
        except Exception as exc:
            messagebox.showerror("Export failed", f"Could not export scanner quality:\n{exc}")

    def _run_quick_diagnostics(self) -> None:
        if bool(getattr(self, "_diag_busy", False)):
            return
        self._diag_busy = True
        try:
            if getattr(self, "btn_quick_diag", None) is not None:
                self.btn_quick_diag.configure(state="disabled", text="Diagnostics...")
        except Exception:
            pass

        def _worker() -> None:
            rc = 1
            err = ""
            try:
                cmd = [sys.executable, os.path.join(self.project_dir, "runtime", "smoke_test_all.py")]
                env = os.environ.copy()
                env["POWERTRADER_HUB_DIR"] = self.hub_dir
                env["POWERTRADER_PROJECT_DIR"] = self.project_dir
                proc = subprocess.run(
                    cmd,
                    cwd=self.project_dir,
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=180,
                    check=False,
                )
                rc = int(proc.returncode)
                if rc != 0:
                    err = str(proc.stderr or proc.stdout or "").strip()
            except Exception as exc:
                rc = 1
                err = f"{type(exc).__name__}: {exc}"

            def _finish() -> None:
                self._diag_busy = False
                try:
                    if getattr(self, "btn_quick_diag", None) is not None:
                        self.btn_quick_diag.configure(state="normal", text="Quick Diagnostics")
                except Exception:
                    pass
                report_path = os.path.join(self.hub_dir, "smoke_test_report.json")
                if rc == 0:
                    messagebox.showinfo("Diagnostics", f"Quick diagnostics passed.\n\nReport:\n{report_path}")
                else:
                    tail = (err[:300] + "...") if len(err) > 300 else err
                    messagebox.showerror("Diagnostics", f"Quick diagnostics failed.\n\nReport:\n{report_path}\n\n{tail}")

            try:
                self.after(0, _finish)
            except Exception:
                self._diag_busy = False

        threading.Thread(target=_worker, daemon=True).start()

    def _autofix_extract_json_obj(self, text: str) -> Dict[str, Any]:
        raw = str(text or "").strip()
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            pass
        start = raw.find("{")
        end = raw.rfind("}")
        if start < 0 or end <= start:
            return {}
        try:
            parsed = json.loads(raw[start : end + 1])
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

    def _autofix_read_ticket_rows(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        try:
            paths = glob.glob(os.path.join(self.autofix_tickets_dir, "*.json"))
        except Exception:
            paths = []
        for path in paths:
            row = _safe_read_json(path)
            if not isinstance(row, dict):
                continue
            tid = str(row.get("id", "") or "").strip()
            if not tid:
                tid = os.path.splitext(os.path.basename(path))[0]
                row["id"] = tid
            row["_ticket_path"] = path
            try:
                tsv = int(float(row.get("ts", 0) or 0))
            except Exception:
                try:
                    tsv = int(os.path.getmtime(path))
                except Exception:
                    tsv = 0
            row["_sort_ts"] = tsv
            rows.append(row)
        rows.sort(key=lambda r: int(r.get("_sort_ts", 0) or 0), reverse=True)
        return rows

    def _autofix_resolve_patch_path(self, row: Dict[str, Any]) -> str:
        proposal = row.get("proposal", {}) if isinstance(row.get("proposal", {}), dict) else {}
        patch_path = str(proposal.get("patch_diff_path", "") or "").strip()
        if patch_path:
            if not os.path.isabs(patch_path):
                patch_path = os.path.abspath(os.path.join(self.project_dir, patch_path))
            if os.path.isfile(patch_path):
                return patch_path
        tid = str(row.get("id", "") or "").strip()
        if tid:
            fallback = os.path.join(self.autofix_patches_dir, f"{tid}.diff")
            if os.path.isfile(fallback):
                return fallback
        return ""

    def _autofix_set_text(self, widget: tk.Text, text: str) -> None:
        try:
            widget.configure(state="normal")
            widget.delete("1.0", "end")
            widget.insert("1.0", str(text or ""))
            widget.configure(state="disabled")
        except Exception:
            pass

    def _copy_text_to_clipboard(self, text: str, title: str = "Copied") -> None:
        txt = str(text or "").strip()
        if not txt:
            messagebox.showinfo(title, "Nothing to copy.")
            return
        try:
            self.clipboard_clear()
            self.clipboard_append(txt)
            messagebox.showinfo(title, "Copied to clipboard.")
        except Exception as exc:
            messagebox.showerror("Clipboard", f"Unable to copy text.\n{type(exc).__name__}: {exc}")

    def _autofix_mode(self) -> str:
        status_row = _safe_read_json(self.autofix_status_path) or {}
        return str(status_row.get("mode", self.settings.get("autofix_mode", "report_only")) or "report_only").strip().lower()

    def _autofix_filter_rows(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        ui = self._autofix_queue_ui if isinstance(getattr(self, "_autofix_queue_ui", {}), dict) else {}
        status_var = ui.get("ticket_status_filter_var")
        kind_var = ui.get("ticket_kind_filter_var")
        search_var = ui.get("ticket_search_var")
        try:
            status_filter = str(status_var.get() if status_var is not None else "All").strip().lower()
        except Exception:
            status_filter = "all"
        try:
            kind_filter = str(kind_var.get() if kind_var is not None else "All").strip().lower()
        except Exception:
            kind_filter = "all"
        try:
            query = str(search_var.get() if search_var is not None else "").strip().lower()
        except Exception:
            query = ""

        out: List[Dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            status = str(row.get("status", "open") or "open").strip().lower()
            if status_filter not in {"all", ""} and status != status_filter:
                continue
            cls = row.get("classifier", {}) if isinstance(row.get("classifier", {}), dict) else {}
            kind = str(cls.get("kind", "unknown") or "unknown").strip().lower()
            if kind_filter not in {"all", ""} and kind != kind_filter:
                continue
            if query:
                incident = row.get("incident", {}) if isinstance(row.get("incident", {}), dict) else {}
                request_row = row.get("request", {}) if isinstance(row.get("request", {}), dict) else {}
                parts = [
                    str(row.get("id", "") or ""),
                    str(kind),
                    str(status),
                    str(incident.get("component", "") or ""),
                    str(incident.get("event", "") or ""),
                    str(incident.get("msg", "") or ""),
                    str(request_row.get("text", "") or ""),
                ]
                hay = " ".join(parts).lower()
                if query not in hay:
                    continue
            out.append(row)
        return out

    def _autofix_clear_filters(self) -> None:
        ui = self._autofix_queue_ui if isinstance(getattr(self, "_autofix_queue_ui", {}), dict) else {}
        for key, value in (
            ("ticket_status_filter_var", "All"),
            ("ticket_kind_filter_var", "All"),
            ("ticket_search_var", ""),
        ):
            var = ui.get(key)
            if var is None:
                continue
            try:
                var.set(value)
            except Exception:
                pass
        self._refresh_autofix_queue(keep_selection=False)

    def _autofix_on_filter_change(self, _event: Optional[tk.Event] = None) -> None:
        self._refresh_autofix_queue(keep_selection=True)

    def _autofix_set_feedback(self, msg: str, level: str = "info") -> None:
        ui = self._autofix_queue_ui if isinstance(getattr(self, "_autofix_queue_ui", {}), dict) else {}
        var = ui.get("chat_feedback_var")
        label = ui.get("chat_feedback_label")
        text = str(msg or "").strip()
        if var is not None:
            try:
                var.set(text)
            except Exception:
                pass
        if label is not None:
            fg = DARK_MUTED
            lv = str(level or "info").strip().lower()
            if lv in {"ok", "good", "success"}:
                fg = DARK_ACCENT
            elif lv in {"warn", "warning"}:
                fg = "#FFD27A"
            elif lv in {"error", "bad"}:
                fg = "#FF8D80"
            try:
                label.configure(foreground=fg)
            except Exception:
                pass

    def _autofix_human_apply_reason(self, reason: str) -> str:
        key = str(reason or "").strip().lower()
        mapped = {
            "applied_and_tests_passed": "Patch applied and tests passed.",
            "applied_no_tests": "Patch applied (no tests configured).",
            "awaiting_manual_approval": "Awaiting manual approval.",
            "mode_not_shadow_apply": "Mode blocks auto-apply.",
            "live_guarded_blocked": "Live-guard blocked auto-apply.",
            "missing_openai_api_key": "OpenAI key missing.",
            "llm_quota_blocked": "OpenAI quota/billing blocked request.",
            "llm_network_unreachable": "OpenAI endpoint not reachable from runtime.",
            "llm_request_rejected": "OpenAI request payload rejected.",
            "llm_quota_blocked_retries_exhausted": "Blocked after repeated quota failures.",
            "missing_openai_api_key_retries_exhausted": "Blocked after repeated missing-key failures.",
            "llm_request_rejected_retries_exhausted": "Blocked after repeated request-format failures.",
            "missing_patch_file": "Patch was not generated.",
            "dry_run": "Dry-run mode; no apply attempted.",
        }
        return mapped.get(key, str(reason or "").strip())

    def _autofix_update_input_state(self, _event: Optional[tk.Event] = None) -> None:
        ui = self._autofix_queue_ui if isinstance(getattr(self, "_autofix_queue_ui", {}), dict) else {}
        chat_input = ui.get("chat_input")
        send_btn = ui.get("chat_send_btn")
        ticket_btn = ui.get("chat_send_apply_btn")
        chars_var = ui.get("chat_chars_var")
        mode_var = ui.get("chat_mode_hint_var")
        if chat_input is None:
            return
        try:
            raw = str(chat_input.get("1.0", "end-1c") or "")
        except Exception:
            raw = ""
        text = str(raw).strip()
        n_chars = len(raw)
        over = max(0, int(n_chars - AUTOFIX_REQUEST_MAX_CHARS))
        if chars_var is not None:
            try:
                if over > 0:
                    chars_var.set(f"Length: {n_chars}/{AUTOFIX_REQUEST_MAX_CHARS} (+{over} over limit)")
                else:
                    chars_var.set(f"Length: {n_chars}/{AUTOFIX_REQUEST_MAX_CHARS}")
            except Exception:
                pass
        mode = self._autofix_mode()
        if mode_var is not None:
            try:
                if mode in {"manual", "shadow_apply"}:
                    mode_var.set("Mode hint: requests can auto-implement.")
                else:
                    mode_var.set("Mode hint: report_only (implementation may require approval/force).")
            except Exception:
                pass
        can_send = (not self._autofix_request_busy) and bool(text) and (over == 0)
        for btn in (send_btn, ticket_btn):
            if btn is None:
                continue
            try:
                btn.configure(state=("normal" if can_send else "disabled"))
            except Exception:
                pass

    def _autofix_insert_template(self) -> None:
        ui = self._autofix_queue_ui if isinstance(getattr(self, "_autofix_queue_ui", {}), dict) else {}
        chat_input = ui.get("chat_input")
        template_var = ui.get("chat_template_var")
        if chat_input is None or template_var is None:
            return
        try:
            label = str(template_var.get() or "").strip()
        except Exception:
            label = ""
        if not label:
            return
        body = str(AUTOFIX_CHAT_TEMPLATES.get(label, "") or "").strip()
        if not body:
            return
        try:
            cur = str(chat_input.get("1.0", "end-1c") or "")
        except Exception:
            cur = ""
        merged = f"{cur.rstrip()}\n\n{body}\n" if cur.strip() else (body + "\n")
        try:
            chat_input.delete("1.0", "end")
            chat_input.insert("1.0", merged)
            chat_input.focus_set()
        except Exception:
            pass
        self._autofix_update_input_state()

    def _autofix_use_selected_ticket_request(self) -> None:
        row = self._selected_autofix_row()
        if not row:
            return
        request_row = row.get("request", {}) if isinstance(row.get("request", {}), dict) else {}
        incident = row.get("incident", {}) if isinstance(row.get("incident", {}), dict) else {}
        txt = str(request_row.get("text", "") or "").strip()
        if not txt:
            txt = str(incident.get("msg", "") or "").strip()
        if not txt:
            messagebox.showinfo("AI Assist", "Selected ticket has no reusable request text.")
            return
        ui = self._autofix_queue_ui if isinstance(getattr(self, "_autofix_queue_ui", {}), dict) else {}
        chat_input = ui.get("chat_input")
        if chat_input is None:
            return
        try:
            chat_input.delete("1.0", "end")
            chat_input.insert("1.0", txt + "\n")
            chat_input.focus_set()
        except Exception:
            pass
        self._autofix_update_input_state()
        self._autofix_set_feedback("Loaded selected ticket text into prompt box.", level="info")

    def _autofix_copy_selected_ticket_id(self) -> None:
        row = self._selected_autofix_row()
        tid = str((row if isinstance(row, dict) else {}).get("id", "") or "").strip()
        self._copy_text_to_clipboard(tid, title="Ticket ID")

    def _autofix_copy_text_widget(self, widget_key: str, title: str) -> None:
        ui = self._autofix_queue_ui if isinstance(getattr(self, "_autofix_queue_ui", {}), dict) else {}
        widget = ui.get(widget_key)
        if widget is None:
            return
        try:
            txt = str(widget.get("1.0", "end-1c") or "")
        except Exception:
            txt = ""
        self._copy_text_to_clipboard(txt, title=title)

    def _autofix_clear_chat_log(self) -> None:
        ui = self._autofix_queue_ui if isinstance(getattr(self, "_autofix_queue_ui", {}), dict) else {}
        widget = ui.get("chat_text")
        if widget is None:
            return
        try:
            widget.configure(state="normal")
            widget.delete("1.0", "end")
            widget.configure(state="disabled")
        except Exception:
            pass
        self._autofix_set_feedback("Chat log cleared.", level="info")

    def _on_autofix_chat_shortcut(self, auto_apply: bool, _event: Optional[tk.Event] = None) -> str:
        self._submit_autofix_chat_request(auto_apply=bool(auto_apply))
        return "break"

    def _selected_autofix_row(self) -> Dict[str, Any]:
        ui = self._autofix_queue_ui if isinstance(getattr(self, "_autofix_queue_ui", {}), dict) else {}
        lb = ui.get("listbox")
        rows = ui.get("rows", [])
        if not isinstance(rows, list) or lb is None:
            return {}
        try:
            sel = lb.curselection()
            if not sel:
                return {}
            idx = int(sel[0])
            if 0 <= idx < len(rows):
                row = rows[idx]
                return row if isinstance(row, dict) else {}
        except Exception:
            return {}
        return {}

    def _render_autofix_selected_row(self) -> None:
        ui = self._autofix_queue_ui if isinstance(getattr(self, "_autofix_queue_ui", {}), dict) else {}
        details_text = ui.get("details_text")
        patch_text = ui.get("patch_text")
        apply_btn = ui.get("apply_btn")
        force_apply_btn = ui.get("force_apply_btn")
        row = self._selected_autofix_row()

        if details_text is None or patch_text is None:
            return

        if not row:
            self._autofix_set_text(details_text, "No autofix ticket selected.")
            self._autofix_set_text(patch_text, "No patch selected.")
            try:
                apply_btn.configure(state="disabled", text="Approve + Apply")
            except Exception:
                pass
            try:
                if force_apply_btn is not None:
                    force_apply_btn.configure(state="disabled", text="Force Apply")
            except Exception:
                pass
            return

        tid = str(row.get("id", "") or "").strip()
        status = str(row.get("status", "open") or "open").strip().lower()
        incident = row.get("incident", {}) if isinstance(row.get("incident", {}), dict) else {}
        details = incident.get("details", {}) if isinstance(incident.get("details", {}), dict) else {}
        classifier = row.get("classifier", {}) if isinstance(row.get("classifier", {}), dict) else {}
        proposal = row.get("proposal", {}) if isinstance(row.get("proposal", {}), dict) else {}
        evidence = row.get("evidence", {}) if isinstance(row.get("evidence", {}), dict) else {}
        request_row = row.get("request", {}) if isinstance(row.get("request", {}), dict) else {}
        retry_row = row.get("request_retry", {}) if isinstance(row.get("request_retry", {}), dict) else {}
        apply_row = row.get("apply", {}) if isinstance(row.get("apply", {}), dict) else {}
        blocked_row = row.get("blocked", {}) if isinstance(row.get("blocked", {}), dict) else {}
        trace_files = evidence.get("trace_files", []) if isinstance(evidence.get("trace_files", []), list) else []
        log_tail = evidence.get("log_tail", []) if isinstance(evidence.get("log_tail", []), list) else []
        patch_path = self._autofix_resolve_patch_path(row)

        try:
            created_ts = int(float(row.get("ts", 0) or 0))
            created_txt = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(created_ts)) if created_ts > 0 else "N/A"
        except Exception:
            created_txt = "N/A"

        detail_lines: List[str] = []
        detail_lines.append(f"Ticket: {tid}")
        detail_lines.append(f"Status: {status.upper()}")
        detail_lines.append(f"Created: {created_txt}")
        detail_lines.append(f"Mode at create: {str(row.get('mode', 'n/a') or 'n/a')}")
        detail_lines.append(f"Stage at create: {str(row.get('market_rollout_stage', 'n/a') or 'n/a')}")
        detail_lines.append("")
        detail_lines.append("Request")
        detail_lines.append(f"- auto_apply={bool(request_row.get('auto_apply_requested', False))}")
        detail_lines.append(f"- force_apply={bool(request_row.get('force_apply_requested', False))}")
        req_preview = str(request_row.get("text", "") or "").strip()
        if req_preview:
            if len(req_preview) > 180:
                req_preview = req_preview[:180] + "..."
            detail_lines.append(f"- text={req_preview}")
        if retry_row:
            detail_lines.append("- retry")
            detail_lines.append(f"  attempts={int(float(retry_row.get('attempts', 0) or 0))}")
            try:
                retry_last_ts = int(float(retry_row.get("last_ts", 0) or 0))
            except Exception:
                retry_last_ts = 0
            retry_last_txt = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(retry_last_ts)) if retry_last_ts > 0 else "N/A"
            detail_lines.append(f"  last={retry_last_txt}")
            try:
                retry_next_ts = int(float(retry_row.get("next_retry_ts", 0) or 0))
            except Exception:
                retry_next_ts = 0
            retry_next_txt = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(retry_next_ts)) if retry_next_ts > 0 else "N/A"
            detail_lines.append(f"  next={retry_next_txt}")
            retry_err = str(retry_row.get("last_error", "") or "").strip()
            if retry_err:
                if len(retry_err) > 180:
                    retry_err = retry_err[:180] + "..."
                detail_lines.append(f"  last_error={retry_err}")
        detail_lines.append("")
        detail_lines.append("Classifier")
        detail_lines.append(
            f"- kind={str(classifier.get('kind', 'unknown') or 'unknown')} "
            f"confidence={float(classifier.get('confidence', 0.0) or 0.0):.2f}"
        )
        detail_lines.append(f"- match={str(classifier.get('match', '') or '')}")
        detail_lines.append("")
        detail_lines.append("Incident")
        detail_lines.append(f"- severity={str(incident.get('severity', '') or '')}")
        detail_lines.append(f"- component={str(incident.get('component', '') or '')}")
        detail_lines.append(f"- event={str(incident.get('event', '') or '')}")
        detail_lines.append(f"- message={str(incident.get('msg', '') or '')}")
        if details:
            child = str(details.get("child", "") or "").strip()
            if child:
                detail_lines.append(f"- child={child}")
        detail_lines.append("")
        detail_lines.append("Proposal")
        detail_lines.append(f"- summary={str(proposal.get('summary', '') or '')}")
        risk = proposal.get("risk", {}) if isinstance(proposal.get("risk", {}), dict) else {}
        diff_stats = proposal.get("diff_stats", {}) if isinstance(proposal.get("diff_stats", {}), dict) else {}
        target_files = proposal.get("target_files", []) if isinstance(proposal.get("target_files", []), list) else []
        risk_level = str(risk.get("level", "unknown") or "unknown").strip().lower()
        try:
            risk_score = int(float(risk.get("score", 0) or 0))
        except Exception:
            risk_score = 0
        detail_lines.append(f"- risk={risk_level.upper()} (score {risk_score})")
        risk_reasons = list(risk.get("reasons", []) or []) if isinstance(risk.get("reasons", []), list) else []
        if risk_reasons:
            detail_lines.append(f"- risk reasons={', '.join(str(x) for x in risk_reasons[:6])}")
        if diff_stats:
            detail_lines.append(
                "- diff stats="
                f"files={int(diff_stats.get('files', 0) or 0)} "
                f"hunks={int(diff_stats.get('hunks', 0) or 0)} "
                f"changed={int(diff_stats.get('changed', 0) or 0)} "
                f"(+{int(diff_stats.get('added', 0) or 0)}/-{int(diff_stats.get('removed', 0) or 0)})"
            )
        if target_files:
            detail_lines.append("- target files:")
            for path in target_files[:10]:
                detail_lines.append(f"  {str(path or '').strip()}")
        llm = proposal.get("llm", {}) if isinstance(proposal.get("llm", {}), dict) else {}
        detail_lines.append(
            f"- llm used={bool(llm.get('used', False))} ok={bool(llm.get('ok', False))} "
            f"model={str(llm.get('model', '') or '')}"
        )
        llm_err = str(llm.get("error", "") or "").strip()
        llm_detail = str(llm.get("detail", "") or "").strip()
        if llm_err:
            detail_lines.append(f"- llm error={llm_err}")
        if llm_detail:
            d = llm_detail if len(llm_detail) <= 280 else (llm_detail[:280] + "...")
            detail_lines.append(f"- llm detail={d}")
        detail_lines.append(f"- patch={patch_path if patch_path else 'N/A'}")
        tests = proposal.get("recommended_tests", []) if isinstance(proposal.get("recommended_tests", []), list) else []
        if tests:
            detail_lines.append("- recommended tests:")
            for t in tests[:6]:
                detail_lines.append(f"  {str(t or '').strip()}")
        detail_lines.append("")
        detail_lines.append("Apply")
        detail_lines.append(f"- attempted={bool(apply_row.get('attempted', False))}")
        detail_lines.append(f"- ok={bool(apply_row.get('ok', False))}")
        detail_lines.append(f"- reason={str(apply_row.get('reason', '') or '')}")
        if blocked_row:
            detail_lines.append("- blocked")
            detail_lines.append(f"  reason={str(blocked_row.get('reason', '') or '')}")
            try:
                blocked_ts = int(float(blocked_row.get("ts", 0) or 0))
            except Exception:
                blocked_ts = 0
            blocked_txt = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(blocked_ts)) if blocked_ts > 0 else "N/A"
            detail_lines.append(f"  ts={blocked_txt}")
            detail_lines.append(f"  retry_max_attempts={int(float(blocked_row.get('retry_max_attempts', 0) or 0))}")
        detail_lines.append("")
        if trace_files:
            detail_lines.append("Trace files")
            for tf in trace_files[:8]:
                if isinstance(tf, dict):
                    detail_lines.append(
                        f"- {str(tf.get('path', '') or '')}:{str(tf.get('line', '') or '')} "
                        f"{str(tf.get('func', '') or '')}"
                    )
                else:
                    detail_lines.append(f"- {str(tf or '')}")
            detail_lines.append("")
        if log_tail:
            detail_lines.append("Recent log tail")
            for line in log_tail[-30:]:
                detail_lines.append(str(line or ""))

        self._autofix_set_text(details_text, "\n".join(detail_lines).strip() + "\n")

        if patch_path and os.path.isfile(patch_path):
            try:
                with open(patch_path, "r", encoding="utf-8") as f:
                    patch_body = f.read()
            except Exception as exc:
                patch_body = f"[patch read failed] {type(exc).__name__}: {exc}"
        else:
            apply_reason_l = str((apply_row.get("reason", "") if isinstance(apply_row, dict) else "") or "").strip().lower()
            llm_err_l = str(llm_err or "").strip().lower()
            if ("_retries_exhausted" in apply_reason_l) and ("llm_quota_blocked" in apply_reason_l):
                patch_body = (
                    "No patch diff is available yet.\n\n"
                    "Reason: Ticket auto-closed after repeated OpenAI quota failures.\n"
                    "Resolve quota/billing, then submit a new request."
                )
            elif ("_retries_exhausted" in apply_reason_l) and ("missing_openai_api_key" in apply_reason_l):
                patch_body = (
                    "No patch diff is available yet.\n\n"
                    "Reason: Ticket auto-closed after repeated missing API key failures.\n"
                    "Set OPENAI_API_KEY (or keys/openai_api_key.txt), then submit a new request."
                )
            elif ("_retries_exhausted" in apply_reason_l) and ("llm_request_rejected" in apply_reason_l):
                patch_body = (
                    "No patch diff is available yet.\n\n"
                    "Reason: Ticket auto-closed after repeated request-format failures.\n"
                    "Submit a new request from current app version."
                )
            elif "missing_openai_api_key" in llm_err_l:
                patch_body = (
                    "No patch diff is available yet.\n\n"
                    "Reason: OpenAI API key is missing for Autofix runtime.\n"
                    "Set OPENAI_API_KEY or add keys/openai_api_key.txt, relaunch, then retry."
                )
            elif ("http_429" in llm_err_l) or ("insufficient_quota" in str(llm_detail or "").lower()):
                patch_body = (
                    "No patch diff is available yet.\n\n"
                    "Reason: OpenAI API quota/billing is currently blocked (HTTP 429 / insufficient_quota).\n"
                    "Fix account quota, then submit again or wait for auto-retry."
                )
            elif ("urlerror" in llm_err_l) or ("nodename nor servname" in str(llm_detail or "").lower()):
                patch_body = (
                    "No patch diff is available yet.\n\n"
                    "Reason: Autofix runtime could not reach OpenAI (network/DNS issue).\n"
                    "Check internet/DNS connectivity for this process, then retry."
                )
            elif "http_400" in llm_err_l:
                patch_body = (
                    "No patch diff is available yet.\n\n"
                    "Reason: API request payload was rejected (HTTP 400).\n"
                    "Create a new ticket from AI Assist so it uses current request formatting."
                )
            else:
                patch_body = (
                    "No patch diff is available for this ticket yet.\n\n"
                    "If this is a chat improvement request, use 'Send + Implement' or switch Autofix mode to manual/shadow_apply."
                )
        if len(patch_body) > 60000:
            patch_body = patch_body[:60000] + "\n...\n[patch preview truncated]"
        self._autofix_set_text(patch_text, patch_body)

        can_apply = bool(tid) and bool(patch_path) and (status != "applied") and (not self._autofix_apply_busy)
        try:
            if self._autofix_apply_busy:
                apply_btn.configure(state="disabled", text="Applying...")
                if force_apply_btn is not None:
                    force_apply_btn.configure(state="disabled", text="Applying...")
            elif status == "applied":
                apply_btn.configure(state="disabled", text="Already Applied")
                if force_apply_btn is not None:
                    force_apply_btn.configure(state="disabled", text="Already Applied")
            elif status == "blocked":
                apply_btn.configure(state="disabled", text="Blocked")
                if force_apply_btn is not None:
                    force_apply_btn.configure(state="disabled", text="Blocked")
            elif not patch_path:
                apply_btn.configure(state="disabled", text="No Patch")
                if force_apply_btn is not None:
                    force_apply_btn.configure(state="disabled", text="No Patch")
            else:
                apply_btn.configure(state=("normal" if can_apply else "disabled"), text="Approve + Apply")
                if force_apply_btn is not None:
                    force_apply_btn.configure(state=("normal" if can_apply else "disabled"), text="Force Apply")
        except Exception:
            pass

    def _refresh_autofix_queue(self, keep_selection: bool = True) -> None:
        ui = self._autofix_queue_ui if isinstance(getattr(self, "_autofix_queue_ui", {}), dict) else {}
        if not ui:
            return
        lb = ui.get("listbox")
        status_var = ui.get("status_var")
        if lb is None or status_var is None:
            return

        prev_id = str(ui.get("selected_id", "") or "").strip()
        if keep_selection and not prev_id:
            row = self._selected_autofix_row()
            prev_id = str(row.get("id", "") or "").strip()

        all_rows = self._autofix_read_ticket_rows()
        rows = self._autofix_filter_rows(all_rows)
        ui["rows_all"] = all_rows
        ui["rows"] = rows

        try:
            lb.delete(0, "end")
        except Exception:
            pass

        for row in rows:
            tid = str(row.get("id", "") or "").strip()
            incident = row.get("incident", {}) if isinstance(row.get("incident", {}), dict) else {}
            classifier = row.get("classifier", {}) if isinstance(row.get("classifier", {}), dict) else {}
            proposal = row.get("proposal", {}) if isinstance(row.get("proposal", {}), dict) else {}
            risk = proposal.get("risk", {}) if isinstance(proposal.get("risk", {}), dict) else {}
            risk_level = str(risk.get("level", "low") or "low").strip().lower()
            risk_tag = str(risk_level[:1].upper() if risk_level else "L")
            status = str(row.get("status", "open") or "open").strip().upper()
            kind = str(classifier.get("kind", "unknown") or "unknown").strip()
            comp = str(incident.get("component", "") or "").strip()
            msg = str(incident.get("msg", "") or "").replace("\n", " ").strip()
            if len(msg) > 72:
                msg = msg[:72] + "..."
            try:
                tsv = int(float(row.get("ts", 0) or 0))
                ttxt = time.strftime("%m-%d %H:%M:%S", time.localtime(tsv)) if tsv > 0 else "N/A"
            except Exception:
                ttxt = "N/A"
            label = f"{status:<8} {ttxt} | R{risk_tag} | {kind:<14} | {(comp or '-'):<8} | {msg}"
            try:
                lb.insert("end", label)
            except Exception:
                pass
            if not tid:
                continue

        selected_idx = 0 if rows else -1
        if prev_id:
            for i, row in enumerate(rows):
                if str(row.get("id", "") or "").strip() == prev_id:
                    selected_idx = i
                    break
        if selected_idx >= 0 and rows:
            try:
                lb.selection_clear(0, "end")
                lb.selection_set(selected_idx)
                lb.activate(selected_idx)
                lb.see(selected_idx)
                ui["selected_id"] = str(rows[selected_idx].get("id", "") or "").strip()
            except Exception:
                pass
        else:
            ui["selected_id"] = ""

        open_count = sum(1 for r in all_rows if str(r.get("status", "open") or "open").strip().lower() == "open")
        applied_count = sum(1 for r in all_rows if str(r.get("status", "")).strip().lower() == "applied")
        blocked_count = sum(1 for r in all_rows if str(r.get("status", "")).strip().lower() == "blocked")
        open_high_risk = 0
        for r in all_rows:
            if str(r.get("status", "open") or "open").strip().lower() != "open":
                continue
            p = r.get("proposal", {}) if isinstance(r.get("proposal", {}), dict) else {}
            rk = p.get("risk", {}) if isinstance(p.get("risk", {}), dict) else {}
            if str(rk.get("level", "") or "").strip().lower() == "high":
                open_high_risk += 1
        status_row = _safe_read_json(self.autofix_status_path) or {}
        state_row = _safe_read_json(self.autofix_state_path) or {}
        mode = str(status_row.get("mode", self.settings.get("autofix_mode", "report_only")) or "report_only")
        enabled = bool(status_row.get("enabled", self.settings.get("autofix_enabled", True)))
        api_key_cfg = bool(status_row.get("api_key_configured", False))
        retry_attempted = int(float(status_row.get("request_retry_attempted", 0) or 0))
        retry_updated = int(float(status_row.get("request_retry_updated", 0) or 0))
        retry_blocked = int(float(status_row.get("request_retry_blocked", 0) or 0))
        daily = int(float(state_row.get("applied_count_day", 0) or 0))
        tick_ts = status_row.get("last_tick_ts", status_row.get("ts", 0))
        api_hint = "" if api_key_cfg else " (keys/openai_api_key.txt)"
        status_var.set(
            f"Mode={mode} | Enabled={'ON' if enabled else 'OFF'} | Open={open_count} | "
            f"HighRiskOpen={open_high_risk} | Applied={applied_count} | Blocked={blocked_count} | Today={daily} | "
            f"ReqRetry={retry_updated}/{retry_attempted} blocked={retry_blocked} | "
            f"Shown={len(rows)}/{len(all_rows)} | API Key={'YES' if api_key_cfg else 'NO'}{api_hint} | {self._market_age_text(tick_ts)}"
        )

        self._autofix_update_input_state()
        self._render_autofix_selected_row()

    def _on_autofix_ticket_select(self, _event: Optional[tk.Event] = None) -> None:
        ui = self._autofix_queue_ui if isinstance(getattr(self, "_autofix_queue_ui", {}), dict) else {}
        row = self._selected_autofix_row()
        ui["selected_id"] = str(row.get("id", "") or "").strip()
        self._render_autofix_selected_row()

    def _autofix_queue_tick(self) -> None:
        win = self._autofix_win
        if win is None or not win.winfo_exists():
            return
        ui = self._autofix_queue_ui if isinstance(getattr(self, "_autofix_queue_ui", {}), dict) else {}
        auto_refresh_var = ui.get("auto_refresh_var")
        should_refresh = True
        if auto_refresh_var is not None:
            try:
                should_refresh = bool(auto_refresh_var.get())
            except Exception:
                should_refresh = True
        if should_refresh and (not self._autofix_apply_busy) and (not self._autofix_request_busy):
            self._refresh_autofix_queue(keep_selection=True)
        try:
            aft = self.after(8000, self._autofix_queue_tick)
            ui["after_id"] = aft
        except Exception:
            pass

    def _close_autofix_queue(self) -> None:
        ui = self._autofix_queue_ui if isinstance(getattr(self, "_autofix_queue_ui", {}), dict) else {}
        aft = ui.get("after_id")
        if aft:
            try:
                self.after_cancel(aft)
            except Exception:
                pass
        try:
            if self._autofix_win is not None and self._autofix_win.winfo_exists():
                self._autofix_win.destroy()
        except Exception:
            pass
        self._autofix_win = None
        self._autofix_queue_ui = {}
        self._autofix_apply_busy = False
        self._autofix_request_busy = False

    def _apply_selected_autofix_ticket(self, force_apply: bool = False) -> None:
        if self._autofix_apply_busy:
            return
        row = self._selected_autofix_row()
        if not row:
            return
        tid = str(row.get("id", "") or "").strip()
        if not tid:
            return
        patch_path = self._autofix_resolve_patch_path(row)
        if not patch_path:
            messagebox.showerror("Autofix Queue", "No patch file exists for the selected ticket.")
            return
        if not os.path.isfile(self.autofix_script_path):
            messagebox.showerror("Autofix Queue", f"Cannot find autofix script:\n{self.autofix_script_path}")
            return
        if not messagebox.askyesno(
            "Autofix Queue",
            (
                f"{'Force apply' if force_apply else 'Approve and apply'} patch for ticket {tid}?\n\n"
                + "This will edit source files in this project."
                + ("\nLive-guard blocks will be bypassed for this action." if force_apply else "")
            ),
        ):
            return

        self._autofix_apply_busy = True
        self._render_autofix_selected_row()
        ui = self._autofix_queue_ui if isinstance(getattr(self, "_autofix_queue_ui", {}), dict) else {}
        status_var = ui.get("status_var")
        if status_var is not None:
            try:
                status_var.set(f"Applying ticket {tid} ...")
            except Exception:
                pass

        def _worker() -> None:
            rc = 1
            out = ""
            err = ""
            payload: Dict[str, Any] = {}
            try:
                cmd = [sys.executable, self.autofix_script_path, "--apply-ticket", tid]
                if bool(force_apply):
                    cmd.append("--force-apply")
                env = os.environ.copy()
                env["POWERTRADER_HUB_DIR"] = self.hub_dir
                env["POWERTRADER_PROJECT_DIR"] = self.project_dir
                settings_path = resolve_settings_path(BASE_DIR) or SETTINGS_PATH or os.path.join(BASE_DIR, SETTINGS_FILE)
                env["POWERTRADER_GUI_SETTINGS"] = str(settings_path)
                proc = subprocess.run(
                    cmd,
                    cwd=self.project_dir,
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=300,
                    check=False,
                )
                rc = int(proc.returncode)
                out = str(proc.stdout or "").strip()
                err = str(proc.stderr or "").strip()
                payload = self._autofix_extract_json_obj(out)
            except Exception as exc:
                rc = 1
                err = f"{type(exc).__name__}: {exc}"

            def _finish() -> None:
                self._autofix_apply_busy = False
                self._refresh_autofix_queue(keep_selection=True)
                ok = bool(payload.get("ok", False)) and (rc == 0)
                reason = str(payload.get("reason", "") or "").strip()
                if not reason:
                    reason = ("ok" if ok else f"exit_{rc}")
                if ok:
                    messagebox.showinfo("Autofix Queue", f"Ticket {tid} applied.\nReason: {reason}")
                    return
                tail = (err or out or "No output returned.").strip()
                if len(tail) > 1200:
                    tail = tail[-1200:]
                messagebox.showerror(
                    "Autofix Queue",
                    f"Apply failed for ticket {tid}.\nReason: {reason}\n\n{tail}",
                )

            try:
                self.after(0, _finish)
            except Exception:
                self._autofix_apply_busy = False

        threading.Thread(target=_worker, daemon=True).start()

    def _append_autofix_chat_line(self, speaker: str, text: str) -> None:
        ui = self._autofix_queue_ui if isinstance(getattr(self, "_autofix_queue_ui", {}), dict) else {}
        widget = ui.get("chat_text")
        if widget is None:
            return
        stamp = time.strftime("%H:%M:%S")
        line = f"[{stamp}] {str(speaker or 'AI').strip()}: {str(text or '').strip()}\n"
        try:
            widget.configure(state="normal")
            widget.insert("end", line)
            widget.see("end")
            widget.configure(state="disabled")
        except Exception:
            pass

    def _set_autofix_request_controls(self, enabled: bool) -> None:
        ui = self._autofix_queue_ui if isinstance(getattr(self, "_autofix_queue_ui", {}), dict) else {}
        chat_input = ui.get("chat_input")
        send_btn = ui.get("chat_send_btn")
        send_apply_btn = ui.get("chat_send_apply_btn")
        template_btn = ui.get("chat_template_use_btn")
        reuse_btn = ui.get("chat_reuse_btn")
        clear_btn = ui.get("chat_clear_btn")
        state = "normal" if bool(enabled) else "disabled"
        try:
            if chat_input is not None:
                chat_input.configure(state=state)
        except Exception:
            pass
        try:
            if send_btn is not None:
                send_btn.configure(state=state)
        except Exception:
            pass
        try:
            if send_apply_btn is not None:
                send_apply_btn.configure(state=state)
        except Exception:
            pass
        for btn in (template_btn, reuse_btn, clear_btn):
            if btn is None:
                continue
            try:
                btn.configure(state=state)
            except Exception:
                pass
        self._autofix_update_input_state()

    def _submit_autofix_chat_request(self, auto_apply: bool = False) -> None:
        if self._autofix_request_busy:
            return
        ui = self._autofix_queue_ui if isinstance(getattr(self, "_autofix_queue_ui", {}), dict) else {}
        chat_input = ui.get("chat_input")
        status_var = ui.get("status_var")
        if chat_input is None:
            return
        try:
            req_raw = str(chat_input.get("1.0", "end-1c") or "")
        except Exception:
            req_raw = ""
        req = str(req_raw).strip()
        if not req:
            self._autofix_set_feedback("Enter a request before sending.", level="warn")
            self._autofix_update_input_state()
            return
        if len(req_raw) > AUTOFIX_REQUEST_MAX_CHARS:
            self._autofix_set_feedback(
                f"Request exceeds {AUTOFIX_REQUEST_MAX_CHARS} characters. Shorten prompt before sending.",
                level="warn",
            )
            self._autofix_update_input_state()
            return
        status_row = _safe_read_json(self.autofix_status_path) or {}
        mode = str(status_row.get("mode", self.settings.get("autofix_mode", "report_only")) or "report_only").strip().lower()
        request_auto_apply = bool(auto_apply)
        # In non-report modes, a normal request behaves like "send + auto-apply" so the assistant
        # can actually implement approved changes from chat.
        if (not request_auto_apply) and mode in {"manual", "shadow_apply"}:
            request_auto_apply = True
        try:
            chat_input.delete("1.0", "end")
        except Exception:
            pass
        self._append_autofix_chat_line("You", req)
        if request_auto_apply and (not bool(auto_apply)):
            self._append_autofix_chat_line("AI Assist", f"Mode {mode}: request will auto-apply when safe.")
        elif (not request_auto_apply) and mode == "report_only":
            self._append_autofix_chat_line(
                "AI Assist",
                "Ticket-only mode: use 'Send + Implement' for direct implementation.",
            )
        self._autofix_request_busy = True
        self._set_autofix_request_controls(False)
        self._autofix_set_feedback("Request running...", level="info")
        if status_var is not None:
            try:
                status_var.set("AI Assist request running...")
            except Exception:
                pass

        def _worker() -> None:
            rc = 1
            out = ""
            err = ""
            payload: Dict[str, Any] = {}
            req_file = os.path.join(self.hub_dir, f".autofix_request_{int(time.time() * 1000)}.txt")
            try:
                with open(req_file, "w", encoding="utf-8") as f:
                    f.write(req + "\n")
                cmd = [sys.executable, self.autofix_script_path, "--request-file", req_file]
                if bool(request_auto_apply):
                    cmd.append("--request-auto-apply")
                    # User explicitly requested direct implementation from chat.
                    cmd.append("--force-apply")
                env = os.environ.copy()
                env["POWERTRADER_HUB_DIR"] = self.hub_dir
                env["POWERTRADER_PROJECT_DIR"] = self.project_dir
                settings_path = resolve_settings_path(BASE_DIR) or SETTINGS_PATH or os.path.join(BASE_DIR, SETTINGS_FILE)
                env["POWERTRADER_GUI_SETTINGS"] = str(settings_path)
                proc = subprocess.run(
                    cmd,
                    cwd=self.project_dir,
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=420,
                    check=False,
                )
                rc = int(proc.returncode)
                out = str(proc.stdout or "").strip()
                err = str(proc.stderr or "").strip()
                payload = self._autofix_extract_json_obj(out)
            except Exception as exc:
                rc = 1
                err = f"{type(exc).__name__}: {exc}"
            finally:
                try:
                    if os.path.isfile(req_file):
                        os.remove(req_file)
                except Exception:
                    pass

            def _finish() -> None:
                self._autofix_request_busy = False
                self._set_autofix_request_controls(True)
                self._refresh_autofix_queue(keep_selection=True)
                ok = bool(payload.get("ok", False)) and (rc == 0)
                if ok:
                    tid = str(payload.get("ticket_id", "") or "").strip()
                    prop_ok = bool(payload.get("proposal_ok", False))
                    applied = bool(payload.get("applied", False))
                    apply_reason = str(payload.get("apply_reason", "") or "").strip()
                    apply_reason_human = self._autofix_human_apply_reason(apply_reason)
                    llm_error = str(payload.get("llm_error", "") or "").strip()
                    llm_detail = str(payload.get("llm_detail", "") or "").strip()
                    msg = (
                        f"Request accepted. Ticket {tid} created. "
                        f"Patch proposal={'yes' if prop_ok else 'no'}; "
                        f"auto-apply={'yes' if applied else 'no'}"
                    )
                    if apply_reason:
                        msg += f" ({apply_reason_human or apply_reason})"
                    self._append_autofix_chat_line("AI Assist", msg)
                    if str(payload.get("status", "open") or "open").strip().lower() == "blocked":
                        blocked_reason = str(payload.get("blocked_reason", "") or apply_reason).strip()
                        blocked_human = self._autofix_human_apply_reason(blocked_reason)
                        self._append_autofix_chat_line(
                            "AI Assist",
                            f"Ticket closed as BLOCKED ({blocked_human or blocked_reason}). Resolve the root cause and submit again.",
                        )
                        self._autofix_set_feedback("Request blocked; resolve root cause and resubmit.", level="warn")
                    if applied:
                        self._autofix_set_feedback("Request applied successfully.", level="ok")
                    elif prop_ok:
                        self._autofix_set_feedback("Patch proposal created. Awaiting apply path.", level="info")
                    else:
                        self._autofix_set_feedback("Request accepted but proposal failed.", level="warn")
                    if (not applied) and request_auto_apply and str(apply_reason).lower() in {"mode_not_shadow_apply", "awaiting_manual_approval"}:
                        self._append_autofix_chat_line(
                            "AI Assist",
                            "Auto-apply is not active in report_only mode. Switch Autofix mode to manual/shadow_apply for direct implementation.",
                        )
                    if str(apply_reason).strip().lower() == "live_guarded_blocked":
                        self._append_autofix_chat_line(
                            "AI Assist",
                            "Auto-apply is blocked in live_guarded mode. Enable autofix_allow_live_apply in Settings or use a non-live rollout stage.",
                        )
                    if "_retries_exhausted" in str(apply_reason).strip().lower():
                        self._append_autofix_chat_line(
                            "AI Assist",
                            "Ticket was auto-closed as BLOCKED after retry exhaustion. Submit a new request after resolving the root cause.",
                        )
                    if (not prop_ok) and llm_error:
                        self._append_autofix_chat_line("AI Assist", f"Proposal error: {llm_error}")
                    if (not prop_ok) and llm_detail:
                        dt = llm_detail if len(llm_detail) <= 320 else (llm_detail[:320] + "...")
                        self._append_autofix_chat_line("AI Assist", dt)
                    if (not prop_ok) and ("missing_openai_api_key" in str(llm_error).lower()):
                        self._append_autofix_chat_line(
                            "AI Assist",
                            "OpenAI API key is missing. Set OPENAI_API_KEY or save it to keys/openai_api_key.txt, then relaunch.",
                        )
                    if tid:
                        self._autofix_queue_ui["selected_id"] = tid
                        self._refresh_autofix_queue(keep_selection=True)
                else:
                    reason = str(payload.get("reason", "") or "").strip() or f"exit_{rc}"
                    tail = (err or out or "No output returned.").strip()
                    if len(tail) > 900:
                        tail = tail[-900:]
                    self._append_autofix_chat_line("AI Assist", f"Request failed: {reason}")
                    self._autofix_set_feedback(f"Request failed: {reason}", level="error")
                    if tail:
                        self._append_autofix_chat_line("AI Assist", tail)
                self._autofix_update_input_state()

            try:
                self.after(0, _finish)
            except Exception:
                self._autofix_request_busy = False

        threading.Thread(target=_worker, daemon=True).start()

    def open_autofix_queue(self) -> None:
        try:
            if self._autofix_win is not None and self._autofix_win.winfo_exists():
                self._autofix_win.lift()
                self._autofix_win.focus_force()
                self._refresh_autofix_queue(keep_selection=True)
                return
        except Exception:
            pass

        win = tk.Toplevel(self)
        win.title("Autofix Queue")
        win.geometry("1200x760")
        self._autofix_win = win
        win.transient(self)

        def _on_close_queue() -> None:
            self._close_autofix_queue()

        win.protocol("WM_DELETE_WINDOW", _on_close_queue)

        root = ttk.Frame(win)
        root.pack(fill="both", expand=True, padx=10, pady=10)

        top = ttk.Frame(root)
        top.pack(fill="x", pady=(0, 8))
        status_var = tk.StringVar(value="Loading autofix queue...")
        ttk.Label(top, textvariable=status_var, foreground=DARK_MUTED).pack(side="left", padx=(0, 8))
        auto_refresh_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(top, text="Auto refresh", variable=auto_refresh_var).pack(side="right", padx=(6, 0))
        ttk.Button(top, text="Refresh", command=lambda: self._refresh_autofix_queue(keep_selection=True)).pack(side="right", padx=(6, 0))
        apply_btn = ttk.Button(top, text="Approve + Apply", command=lambda: self._apply_selected_autofix_ticket(force_apply=False))
        apply_btn.pack(side="right")
        force_apply_btn = ttk.Button(top, text="Force Apply", command=lambda: self._apply_selected_autofix_ticket(force_apply=True))
        force_apply_btn.pack(side="right", padx=(6, 0))

        tools = ttk.Frame(root)
        tools.pack(fill="x", pady=(0, 8))
        ttk.Label(tools, text="Status:").pack(side="left")
        ticket_status_filter_var = tk.StringVar(value="All")
        ticket_status_filter = ttk.Combobox(
            tools,
            width=10,
            state="readonly",
            values=["All", "Open", "Blocked", "Applied"],
            textvariable=ticket_status_filter_var,
        )
        ticket_status_filter.pack(side="left", padx=(4, 8))
        ttk.Label(tools, text="Type:").pack(side="left")
        ticket_kind_filter_var = tk.StringVar(value="All")
        ticket_kind_filter = ttk.Combobox(
            tools,
            width=14,
            state="readonly",
            values=["All", "user_request", "module_import", "syntax", "type", "attribute", "unknown"],
            textvariable=ticket_kind_filter_var,
        )
        ticket_kind_filter.pack(side="left", padx=(4, 8))
        ttk.Label(tools, text="Search:").pack(side="left")
        ticket_search_var = tk.StringVar(value="")
        ticket_search = ttk.Entry(tools, textvariable=ticket_search_var)
        ticket_search.pack(side="left", fill="x", expand=True, padx=(4, 8))
        ttk.Button(tools, text="Clear Filters", command=self._autofix_clear_filters).pack(side="left")
        ttk.Button(tools, text="Copy Ticket ID", command=self._autofix_copy_selected_ticket_id).pack(side="right")
        ttk.Button(tools, text="Copy Patch", command=lambda: self._autofix_copy_text_widget("patch_text", "Patch Preview")).pack(
            side="right", padx=(6, 0)
        )
        ttk.Button(tools, text="Copy Details", command=lambda: self._autofix_copy_text_widget("details_text", "Ticket Details")).pack(
            side="right", padx=(6, 0)
        )

        chat_frame = ttk.Frame(root)
        chat_frame.pack(fill="x", pady=(0, 8))
        ttk.Label(
            chat_frame,
            text="AI Assist Chat (describe the change in plain language; supports direct implementation from this panel).",
            foreground=DARK_ACCENT2,
        ).pack(anchor="w", pady=(0, 4))
        chat_mode_hint_var = tk.StringVar(value="Mode hint: loading...")
        ttk.Label(chat_frame, textvariable=chat_mode_hint_var, foreground=DARK_MUTED).pack(anchor="w", pady=(0, 4))
        chat_log = tk.Text(
            chat_frame,
            height=4,
            wrap="word",
            bg=DARK_PANEL,
            fg=DARK_FG,
            relief="flat",
            bd=0,
            padx=8,
            pady=6,
            insertbackground=DARK_ACCENT2,
            font=self._live_log_font,
        )
        chat_log.pack(fill="x")
        chat_log.configure(state="disabled")
        template_row = ttk.Frame(chat_frame)
        template_row.pack(fill="x", pady=(4, 2))
        ttk.Label(template_row, text="Template:").pack(side="left")
        chat_template_var = tk.StringVar(value="UI Layout parity")
        chat_template_combo = ttk.Combobox(
            template_row,
            state="readonly",
            values=list(AUTOFIX_CHAT_TEMPLATES.keys()),
            textvariable=chat_template_var,
            width=28,
        )
        chat_template_combo.pack(side="left", padx=(6, 6))
        chat_template_use_btn = ttk.Button(template_row, text="Insert Template", command=self._autofix_insert_template)
        chat_template_use_btn.pack(side="left")
        chat_reuse_btn = ttk.Button(template_row, text="Use Selected Ticket Text", command=self._autofix_use_selected_ticket_request)
        chat_reuse_btn.pack(side="left", padx=(6, 0))
        chat_clear_btn = ttk.Button(template_row, text="Clear Chat", command=self._autofix_clear_chat_log)
        chat_clear_btn.pack(side="right")
        chat_input = tk.Text(
            chat_frame,
            height=3,
            wrap="word",
            bg=DARK_PANEL2,
            fg=DARK_FG,
            relief="flat",
            bd=0,
            padx=8,
            pady=6,
            insertbackground=DARK_ACCENT2,
            font=self._live_log_font,
        )
        chat_input.pack(fill="x", pady=(4, 4))
        chat_meta = ttk.Frame(chat_frame)
        chat_meta.pack(fill="x", pady=(0, 4))
        chat_chars_var = tk.StringVar(value=f"Length: 0/{AUTOFIX_REQUEST_MAX_CHARS}")
        ttk.Label(chat_meta, textvariable=chat_chars_var, foreground=DARK_MUTED).pack(side="left")
        chat_feedback_var = tk.StringVar(value="Ready.")
        chat_feedback_label = ttk.Label(chat_meta, textvariable=chat_feedback_var, foreground=DARK_MUTED)
        chat_feedback_label.pack(side="right")
        chat_btns = ttk.Frame(chat_frame)
        chat_btns.pack(fill="x")
        chat_send_btn = ttk.Button(
            chat_btns,
            text="Send + Implement (Ctrl/Cmd+Enter)",
            command=lambda: self._submit_autofix_chat_request(auto_apply=True),
        )
        chat_send_btn.pack(side="left")
        chat_send_apply_btn = ttk.Button(
            chat_btns,
            text="Send Ticket Only (Ctrl/Cmd+Shift+Enter)",
            command=lambda: self._submit_autofix_chat_request(auto_apply=False),
        )
        chat_send_apply_btn.pack(side="left", padx=(6, 0))
        chat_input.bind("<Control-Return>", lambda e: self._on_autofix_chat_shortcut(True, e))
        chat_input.bind("<Command-Return>", lambda e: self._on_autofix_chat_shortcut(True, e))
        chat_input.bind("<Control-Shift-Return>", lambda e: self._on_autofix_chat_shortcut(False, e))
        chat_input.bind("<Command-Shift-Return>", lambda e: self._on_autofix_chat_shortcut(False, e))
        chat_input.bind("<KeyRelease>", self._autofix_update_input_state)
        ticket_status_filter.bind("<<ComboboxSelected>>", self._autofix_on_filter_change)
        ticket_kind_filter.bind("<<ComboboxSelected>>", self._autofix_on_filter_change)
        ticket_search.bind("<KeyRelease>", self._autofix_on_filter_change)

        body = ttk.Panedwindow(root, orient="horizontal")
        body.pack(fill="both", expand=True)
        left = ttk.Frame(body)
        right = ttk.Frame(body)
        body.add(left, weight=2)
        body.add(right, weight=5)

        ttk.Label(left, text="Tickets", foreground=DARK_ACCENT2).pack(anchor="w", pady=(0, 4))
        list_wrap = ttk.Frame(left)
        list_wrap.pack(fill="both", expand=True)
        lb = tk.Listbox(
            list_wrap,
            activestyle="none",
            selectmode="browse",
            bg=DARK_PANEL2,
            fg=DARK_FG,
            selectbackground=DARK_SELECT_BG,
            selectforeground=DARK_SELECT_FG,
            highlightbackground=DARK_BORDER,
            highlightthickness=1,
            font=self._live_log_font,
        )
        lb.pack(side="left", fill="both", expand=True)
        lb_scroll = ttk.Scrollbar(list_wrap, orient="vertical", command=lb.yview)
        lb_scroll.pack(side="right", fill="y")
        lb.configure(yscrollcommand=lb_scroll.set)
        lb.bind("<<ListboxSelect>>", self._on_autofix_ticket_select)

        right_pane = ttk.Panedwindow(right, orient="vertical")
        right_pane.pack(fill="both", expand=True)

        details_frame = ttk.Frame(right_pane)
        patch_frame = ttk.Frame(right_pane)
        right_pane.add(details_frame, weight=3)
        right_pane.add(patch_frame, weight=4)

        ttk.Label(details_frame, text="Ticket Details", foreground=DARK_ACCENT2).pack(anchor="w", pady=(0, 4))
        details_text = tk.Text(
            details_frame,
            wrap="none",
            bg=DARK_PANEL,
            fg=DARK_FG,
            relief="flat",
            bd=0,
            padx=8,
            pady=8,
            insertbackground=DARK_ACCENT2,
            font=self._live_log_font,
        )
        details_text.pack(fill="both", expand=True)
        details_text.configure(state="disabled")

        ttk.Label(patch_frame, text="Patch Preview", foreground=DARK_ACCENT2).pack(anchor="w", pady=(0, 4))
        patch_wrap = ttk.Frame(patch_frame)
        patch_wrap.pack(fill="both", expand=True)
        patch_text = tk.Text(
            patch_wrap,
            wrap="none",
            bg=DARK_PANEL,
            fg=DARK_FG,
            relief="flat",
            bd=0,
            padx=8,
            pady=8,
            insertbackground=DARK_ACCENT2,
            font=self._live_log_font,
        )
        patch_text.pack(side="left", fill="both", expand=True)
        patch_text.configure(state="disabled")
        patch_y = ttk.Scrollbar(patch_wrap, orient="vertical", command=patch_text.yview)
        patch_y.pack(side="right", fill="y")
        patch_x = ttk.Scrollbar(patch_frame, orient="horizontal", command=patch_text.xview)
        patch_x.pack(fill="x")
        patch_text.configure(yscrollcommand=patch_y.set, xscrollcommand=patch_x.set)

        self._autofix_queue_ui = {
            "win": win,
            "status_var": status_var,
            "auto_refresh_var": auto_refresh_var,
            "ticket_status_filter_var": ticket_status_filter_var,
            "ticket_kind_filter_var": ticket_kind_filter_var,
            "ticket_search_var": ticket_search_var,
            "listbox": lb,
            "details_text": details_text,
            "patch_text": patch_text,
            "apply_btn": apply_btn,
            "force_apply_btn": force_apply_btn,
            "chat_text": chat_log,
            "chat_input": chat_input,
            "chat_send_btn": chat_send_btn,
            "chat_send_apply_btn": chat_send_apply_btn,
            "chat_template_var": chat_template_var,
            "chat_template_use_btn": chat_template_use_btn,
            "chat_reuse_btn": chat_reuse_btn,
            "chat_clear_btn": chat_clear_btn,
            "chat_chars_var": chat_chars_var,
            "chat_mode_hint_var": chat_mode_hint_var,
            "chat_feedback_var": chat_feedback_var,
            "chat_feedback_label": chat_feedback_label,
            "rows": [],
            "rows_all": [],
            "selected_id": "",
            "after_id": None,
        }
        self._append_autofix_chat_line(
            "AI Assist",
            "Ready. Describe a change request, choose implement or ticket-only, and track results in Tickets.",
        )
        self._autofix_update_input_state()

        self._refresh_autofix_queue(keep_selection=False)
        try:
            aft = self.after(8000, self._autofix_queue_tick)
            self._autofix_queue_ui["after_id"] = aft
        except Exception:
            pass

    def _export_market_chart_png(self, market_key: str) -> None:
        try:
            bundle = load_market_status_bundle(
                status_path=str(self.market_status_paths.get(market_key, "") or ""),
                trader_path=str(self.market_trader_paths.get(market_key, "") or ""),
                thinker_path=str(self.market_thinker_paths.get(market_key, "") or ""),
                scan_diag_path=str(self.market_scan_diag_paths.get(market_key, "") or ""),
                history_path=os.path.join(self.market_state_dirs.get(market_key, self.hub_dir), "execution_audit.jsonl"),
                history_limit=80,
                market_key=market_key,
            )
            thinker_data = bundle.get("thinker", {}) if isinstance(bundle.get("thinker", {}), dict) else {}
            focus = self._selected_market_focus_symbol(market_key, thinker_data) or "AUTO"
            chart_map_raw = thinker_data.get("top_chart_map", {}) if isinstance(thinker_data, dict) else {}
            chart_map = chart_map_raw if isinstance(chart_map_raw, dict) else {}
            top_pick = thinker_data.get("top_pick", {}) if isinstance(thinker_data.get("top_pick", {}), dict) else {}
            top_ident = str(top_pick.get("pair") or top_pick.get("symbol") or "").strip().upper()
            top_chart: List[Dict[str, Any]] = []
            chart_source = "top"
            if focus != "AUTO" and isinstance(chart_map.get(focus, None), list):
                top_chart = list(chart_map.get(focus, []) or [])
                chart_source = "focus"
            elif top_ident and isinstance(chart_map.get(top_ident, None), list):
                top_chart = list(chart_map.get(top_ident, []) or [])
                chart_source = "top-map"
            if not top_chart:
                top_chart = list(thinker_data.get("top_chart", []) or [])
                chart_source = "top"
            if (not top_chart) and chart_map:
                for bars in chart_map.values():
                    if isinstance(bars, list) and len(bars) >= 2:
                        top_chart = list(bars)
                        chart_source = "fallback-map"
                        break
            parsed: List[Dict[str, Any]] = []
            for p in top_chart[-180:]:
                if not isinstance(p, dict):
                    continue
                try:
                    c = float(p.get("c", 0.0) or 0.0)
                except Exception:
                    c = 0.0
                if c <= 0.0:
                    continue
                try:
                    o = float(p.get("o", c) or c)
                except Exception:
                    o = c
                try:
                    h = float(p.get("h", max(o, c)) or max(o, c))
                except Exception:
                    h = max(o, c)
                try:
                    l = float(p.get("l", min(o, c)) or min(o, c))
                except Exception:
                    l = min(o, c)
                parsed.append({"t": str(p.get("t", "") or ""), "o": o, "h": max(h, o, c), "l": min(l, o, c), "c": c})
            if len(parsed) < 2:
                messagebox.showinfo("Export", f"No {market_key} chart bars are available to export yet.")
                return

            fig = Figure(figsize=(11.0, 5.2), dpi=110)
            fig.patch.set_facecolor(DARK_BG)
            ax = fig.add_subplot(111)
            ax.set_facecolor(DARK_PANEL)
            ax.tick_params(colors=DARK_FG)
            for sp in ax.spines.values():
                sp.set_color(DARK_BORDER)
            ax.grid(True, color=DARK_BORDER, linewidth=0.7, alpha=0.35)

            closes = [float(r["c"]) for r in parsed]
            lows = [float(r["l"]) for r in parsed]
            highs = [float(r["h"]) for r in parsed]
            xs = list(range(len(parsed)))
            n = len(parsed)

            if n <= 120:
                for i, row in enumerate(parsed):
                    o = float(row["o"])
                    c = float(row["c"])
                    h = float(row["h"])
                    l = float(row["l"])
                    up = c >= o
                    color = DARK_ACCENT if up else "#FF6B57"
                    ax.plot([i, i], [l, h], linewidth=1, color=color)
                    bottom = min(o, c)
                    height = max(1e-12, abs(c - o))
                    ax.add_patch(
                        Rectangle(
                            (i - 0.34, bottom),
                            0.68,
                            height,
                            facecolor=(color if up else DARK_PANEL),
                            edgecolor=color,
                            linewidth=1,
                        )
                    )
            else:
                ax.plot(xs, closes, linewidth=1.8, color=DARK_ACCENT2)

            def _ema(vals: List[float], period: int) -> List[float]:
                if not vals:
                    return []
                alpha = 2.0 / (max(1, int(period)) + 1.0)
                out = [float(vals[0])]
                for v in vals[1:]:
                    out.append((alpha * float(v)) + ((1.0 - alpha) * out[-1]))
                return out

            ema_fast = _ema(closes, 9)
            ema_slow = _ema(closes, 21)
            if len(ema_fast) == n:
                ax.plot(xs, ema_fast, linewidth=1.7, color="#00E5FF")
            if len(ema_slow) == n:
                ax.plot(xs, ema_slow, linewidth=1.7, color="#FFD166")

            delta_pct = 0.0
            try:
                if closes[0] > 0.0:
                    delta_pct = ((closes[-1] - closes[0]) / closes[0]) * 100.0
            except Exception:
                delta_pct = 0.0
            ax.set_title(
                f"{market_key.title()} {focus} | {n} bars | delta {delta_pct:+.2f}% | src {chart_source}",
                color=DARK_FG,
            )
            ax.axhline(closes[-1], color=DARK_ACCENT2, linewidth=1.0, linestyle="--", alpha=0.8)
            ax.set_xlim(-0.5, (n - 0.5) + 0.6)

            vmin = min(lows)
            vmax = max(highs)
            pad = (vmax - vmin) * 0.04
            if pad <= 0.0:
                pad = max(abs(vmax) * 0.001, 1e-6)
            ax.set_ylim(vmin - pad, vmax + pad)

            if n >= 2:
                tick_idxs = sorted(set([0, int((n - 1) * 0.33), int((n - 1) * 0.66), n - 1]))
                tick_x = [xs[i] for i in tick_idxs]
                tick_lbl = []
                for i in tick_idxs:
                    raw_t = str(parsed[i].get("t", "") or "")
                    lbl = raw_t
                    if "T" in raw_t:
                        try:
                            d, t = raw_t.split("T", 1)
                            lbl = f"{d[5:]} {t[:5]}"
                        except Exception:
                            lbl = raw_t[:16]
                    else:
                        lbl = raw_t[5:16] if len(raw_t) >= 16 else raw_t
                    tick_lbl.append(lbl or f"bar {i + 1}")
                ax.set_xticks(tick_x)
                ax.set_xticklabels(tick_lbl, fontsize=8, color=DARK_FG)

            out_path = self._next_chart_export_path(f"{market_key}_chart")
            fig.savefig(out_path, dpi=160, facecolor=fig.get_facecolor())
            messagebox.showinfo("Export", f"{market_key.title()} chart PNG exported:\n{out_path}")
        except Exception as exc:
            messagebox.showerror("Export failed", f"Could not export {market_key} chart PNG:\n{exc}")


    def _build_layout(self) -> None:
        self._build_global_command_bar()
        self.market_nb = ttk.Notebook(self)
        self.market_nb.pack(fill="both", expand=True, padx=0, pady=(6, 0))

        self.crypto_market_tab = ttk.Frame(self.market_nb)
        self.stocks_market_tab = ttk.Frame(self.market_nb)
        self.forex_market_tab = ttk.Frame(self.market_nb)

        self.market_nb.add(self.crypto_market_tab, text="Crypto")
        self.market_nb.add(self.stocks_market_tab, text="Stocks")
        self.market_nb.add(self.forex_market_tab, text="Forex")

        outer = ttk.Panedwindow(self.crypto_market_tab, orient="horizontal")
        outer.pack(fill="both", expand=True)

        # LEFT + RIGHT panes
        left = ttk.Frame(outer)
        right = ttk.Frame(outer)

        outer.add(left, weight=1)
        outer.add(right, weight=2)

        # Prevent the outer (left/right) panes from being collapsible to 0 width
        try:
            outer.paneconfigure(left, minsize=360)
            outer.paneconfigure(right, minsize=520)
        except Exception:
            pass

        # LEFT: vertical split (Controls, Live Output)
        left_split = ttk.Panedwindow(left, orient="vertical")
        left_split.pack(fill="both", expand=True, padx=8, pady=8)


        # RIGHT: vertical split (Charts on top, Trades+History underneath)
        right_split = ttk.Panedwindow(right, orient="vertical")
        right_split.pack(fill="both", expand=True, padx=8, pady=8)

        # Keep references so we can clamp sash positions later
        self._pw_outer = outer
        self._pw_left_split = left_split
        self._pw_right_split = right_split

        # Clamp panes when the user releases a sash or the window resizes
        outer.bind("<Configure>", lambda e: self._schedule_paned_clamp(self._pw_outer))
        outer.bind("<ButtonRelease-1>", lambda e: (
            setattr(self, "_user_moved_outer", True),
            self._schedule_paned_clamp(self._pw_outer),
        ))

        left_split.bind("<Configure>", lambda e: self._schedule_paned_clamp(self._pw_left_split))
        left_split.bind("<ButtonRelease-1>", lambda e: (
            setattr(self, "_user_moved_left_split", True),
            self._schedule_paned_clamp(self._pw_left_split),
        ))

        right_split.bind("<Configure>", lambda e: self._schedule_paned_clamp(self._pw_right_split))
        right_split.bind("<ButtonRelease-1>", lambda e: (
            setattr(self, "_user_moved_right_split", True),
            self._schedule_paned_clamp(self._pw_right_split),
        ))

        # Set a startup default width that matches the screenshot (so left has room for Neural Levels).
        def _init_outer_sash_once():
            try:
                if getattr(self, "_did_init_outer_sash", False):
                    return

                # If the user already moved it, never override it.
                if getattr(self, "_user_moved_outer", False):
                    self._did_init_outer_sash = True
                    return

                total = outer.winfo_width()
                if total <= 2:
                    self.after(10, _init_outer_sash_once)
                    return

                min_left = 360
                min_right = 520
                desired_left = 470  # ~matches your screenshot
                target = max(min_left, min(total - min_right, desired_left))
                outer.sashpos(0, int(target))

                self._did_init_outer_sash = True
            except Exception:
                pass

        self.after_idle(_init_outer_sash_once)

        # Global safety: on some themes/platforms, the mouse events land on the sash element,
        # not the panedwindow widget, so the widget-level binds won't always fire.
        self.bind_all("<ButtonRelease-1>", lambda e: (
            self._schedule_paned_clamp(getattr(self, "_pw_outer", None)),
            self._schedule_paned_clamp(getattr(self, "_pw_left_split", None)),
            self._schedule_paned_clamp(getattr(self, "_pw_right_split", None)),
            self._schedule_paned_clamp(getattr(self, "_pw_right_bottom_split", None)),
        ))


        # ----------------------------
        # LEFT: 1) Dashboard (pane)
        # ----------------------------
        top_controls = ttk.LabelFrame(left_split, text="Dashboard")

        # Layout requirement:
        #   - Buttons (full width) ABOVE
        #   - Dual section BELOW:
        #       LEFT  = Status + Account + Profit
        #       RIGHT = free for future expansion (training now lives in Live Output)
        buttons_bar = ttk.Frame(top_controls)
        buttons_bar.pack(fill="x", expand=False, padx=0, pady=0)

        info_row = ttk.Frame(top_controls)
        info_row.pack(fill="x", expand=False, padx=0, pady=0)

        # LEFT column (status + account/legend)
        controls_left = ttk.Frame(info_row)
        controls_left.pack(side="left", fill="both", expand=True)



        # Fixed controls bar (stable layout; no wrapping/reflow on resize)
        # Wrapped in a scrollable canvas so buttons are never cut off when the window is resized.
        btn_scroll_wrap = ttk.Frame(buttons_bar)
        btn_scroll_wrap.pack(fill="x", expand=False, padx=6, pady=6)

        btn_canvas = tk.Canvas(btn_scroll_wrap, bg=DARK_BG, highlightthickness=0, bd=0, height=1)
        btn_scroll_y = ttk.Scrollbar(btn_scroll_wrap, orient="vertical", command=btn_canvas.yview)
        btn_scroll_x = ttk.Scrollbar(btn_scroll_wrap, orient="horizontal", command=btn_canvas.xview)
        btn_canvas.configure(yscrollcommand=btn_scroll_y.set, xscrollcommand=btn_scroll_x.set)


        btn_scroll_wrap.grid_columnconfigure(0, weight=1)
        btn_scroll_wrap.grid_rowconfigure(0, weight=0)

        btn_canvas.grid(row=0, column=0, sticky="ew")
        btn_scroll_y.grid(row=0, column=1, sticky="ns")
        btn_scroll_x.grid(row=1, column=0, sticky="ew")


        # Start hidden; we only show scrollbars when needed.
        btn_scroll_y.grid_remove()
        btn_scroll_x.grid_remove()

        btn_inner = ttk.Frame(btn_canvas)
        _btn_inner_id = btn_canvas.create_window((0, 0), window=btn_inner, anchor="nw")

        def _btn_update_scrollbars(event=None):
            try:
                # Always keep scrollregion accurate
                btn_canvas.configure(scrollregion=btn_canvas.bbox("all"))
                sr = btn_canvas.bbox("all")
                if not sr:
                    return

                # --- KEY FIX ---
                # Resize the canvas height to the buttons' requested height so there is no
                # dead/empty gap above the horizontal scrollbar.
                try:
                    desired_h = max(1, int(btn_inner.winfo_reqheight()))
                    cur_h = int(btn_canvas.cget("height") or 0)
                    if cur_h != desired_h:
                        btn_canvas.configure(height=desired_h)
                except Exception:
                    pass

                x0, y0, x1, y1 = sr
                cw = btn_canvas.winfo_width()
                ch = btn_canvas.winfo_height()

                need_x = (x1 - x0) > (cw + 1)
                need_y = (y1 - y0) > (ch + 1)

                if need_x:
                    btn_scroll_x.grid()
                else:
                    btn_scroll_x.grid_remove()
                    btn_canvas.xview_moveto(0)

                if need_y:
                    btn_scroll_y.grid()
                else:
                    btn_scroll_y.grid_remove()
                    btn_canvas.yview_moveto(0)
            except Exception:
                pass


        def _btn_canvas_on_configure(event=None):
            try:
                # Keep the inner window pinned to top-left
                btn_canvas.coords(_btn_inner_id, 0, 0)
            except Exception:
                pass
            _btn_update_scrollbars()

        btn_inner.bind("<Configure>", _btn_update_scrollbars)
        btn_canvas.bind("<Configure>", _btn_canvas_on_configure)

        # The original button layout (unchanged), placed inside the scrollable inner frame.
        btn_bar = ttk.Frame(btn_inner)
        btn_bar.pack(fill="x", expand=False)

        # Keep groups left-aligned; the spacer column absorbs extra width.
        btn_bar.grid_columnconfigure(0, weight=0)
        btn_bar.grid_columnconfigure(1, weight=0)
        btn_bar.grid_columnconfigure(2, weight=1)

        BTN_W = 14

        # (Start All button moved into the left-side info section above Account.)
        train_group = ttk.Frame(btn_bar)
        train_group.grid(row=0, column=0, sticky="w", padx=(0, 18), pady=(0, 6))


        # One more pass after layout so scrollbars reflect the true initial size.
        self.after_idle(_btn_update_scrollbars)






        system_box = ttk.LabelFrame(controls_left, text="System")
        system_box.pack(fill="x", padx=6, pady=(0, 6))

        self.lbl_neural = ttk.Label(system_box, text="Neural: stopped")
        self.lbl_neural.pack(anchor="w", padx=6, pady=(2, 2))

        self.lbl_trader = ttk.Label(system_box, text="Trader: stopped")
        self.lbl_trader.pack(anchor="w", padx=6, pady=(0, 6))

        self.lbl_last_status = ttk.Label(system_box, text="Last status: N/A", justify="left", wraplength=430)
        self.lbl_last_status.pack(anchor="w", padx=6, pady=(0, 2))
        self.lbl_broker_health = ttk.Label(
            system_box,
            text="Broker API: Alpaca N/A | OANDA N/A | KuCoin N/A",
            foreground=DARK_MUTED,
            justify="left",
            wraplength=430,
        )
        self.lbl_broker_health.pack(anchor="w", padx=6, pady=(0, 2))
        self.lbl_system_action = ttk.Label(
            system_box,
            text="Next: Train all coins, then start trades.",
            foreground=DARK_MUTED,
            justify="left",
            wraplength=430,
        )
        self.lbl_system_action.pack(anchor="w", padx=6, pady=(0, 6))
        self.lbl_system_checklist = ttk.Label(
            system_box,
            text="Checklist: checks N/A | alerts N/A | quota N/A | guard N/A",
            foreground=DARK_MUTED,
            justify="left",
            wraplength=430,
        )
        self.lbl_system_checklist.pack(anchor="w", padx=6, pady=(0, 6))
        self.lbl_runtime_guard = ttk.Label(
            system_box,
            text="Safety: stop-flag OFF | drawdown guard N/A | loops N/A",
            foreground=DARK_MUTED,
            justify="left",
            wraplength=430,
        )
        self.lbl_runtime_guard.pack(anchor="w", padx=6, pady=(0, 6))
        try:
            system_box.bind(
                "<Configure>",
                lambda e, widgets=(
                    self.lbl_last_status,
                    self.lbl_broker_health,
                    self.lbl_system_action,
                    self.lbl_system_checklist,
                    self.lbl_runtime_guard,
                ): [
                    w.configure(wraplength=max(260, int(getattr(e, "width", 460)) - 24))
                    for w in widgets
                ],
                add="+",
            )
        except Exception:
            pass
        # Start Trades (left control column; does not affect layout elsewhere)
        start_all_row = ttk.Frame(system_box)
        start_all_row.pack(fill="x", padx=6, pady=(0, 6))

        self.btn_toggle_all = ttk.Button(
            start_all_row,
            text="Start Trades",
            width=BTN_W,
            command=self.toggle_all_scripts,
        )
        self.btn_toggle_all.pack(side="left")
        self.btn_quick_diag = ttk.Button(
            start_all_row,
            text="Quick Diagnostics",
            width=BTN_W,
            command=self._run_quick_diagnostics,
        )
        self.btn_quick_diag.pack(side="left", padx=(8, 0))

        acct_box = ttk.LabelFrame(controls_left, text="Portfolio")
        acct_box.pack(fill="x", padx=6, pady=6)
        self.acct_box = acct_box

        portfolio_grid = ttk.Frame(acct_box)
        portfolio_grid.pack(fill="x", padx=6, pady=4)
        portfolio_grid.columnconfigure(0, weight=0)
        portfolio_grid.columnconfigure(1, weight=1)

        def _add_portfolio_metric(row: int, label: str):
            ttk.Label(portfolio_grid, text=label).grid(row=row, column=0, sticky="w", padx=(0, 10), pady=2)
            value_lbl = ttk.Label(portfolio_grid, text="N/A", foreground=DARK_FG)
            value_lbl.grid(row=row, column=1, sticky="e", pady=2)
            return value_lbl

        self.lbl_acct_total_value = _add_portfolio_metric(0, "Total Account Value")
        self.lbl_acct_holdings_value = _add_portfolio_metric(1, "Holdings Value")
        self.lbl_acct_buying_power = _add_portfolio_metric(2, "Buying Power")
        self.lbl_acct_percent_in_trade = _add_portfolio_metric(3, "Percent In Trade")
        self.lbl_acct_dca_spread = _add_portfolio_metric(4, "DCA Levels (spread)")
        self.lbl_acct_dca_single = _add_portfolio_metric(5, "DCA Levels (single)")
        self.lbl_pnl = _add_portfolio_metric(6, "Total realized")

        chart_legend_header = ttk.Frame(controls_left)
        chart_legend_header.pack(fill="x", padx=6, pady=(0, 0))
        self.chart_legend_header = chart_legend_header

        ttk.Label(chart_legend_header, text="Chart Legend", foreground=DARK_ACCENT).pack(side="left")
        self.chart_legend_collapsed = tk.BooleanVar(value=False)

        def _toggle_chart_legend() -> None:
            try:
                self.chart_legend_collapsed.set(not self.chart_legend_collapsed.get())
                self._refresh_chart_legend_panel()
            except Exception:
                pass

        self.btn_chart_legend_toggle = ttk.Button(
            chart_legend_header,
            text="Hide",
            width=6,
            command=_toggle_chart_legend,
        )
        self.btn_chart_legend_toggle.pack(side="right")

        chart_legend_box = ttk.LabelFrame(controls_left, text="")
        chart_legend_box.pack(fill="x", padx=6, pady=(0, 6))
        self.chart_legend_box = chart_legend_box

        chart_legend_body = ttk.Frame(chart_legend_box)
        chart_legend_body.pack(fill="both", expand=True, padx=6, pady=6)

        self.chart_legend_text = tk.Text(
            chart_legend_body,
            height=7,
            wrap="word",
            bg=DARK_PANEL,
            fg=DARK_FG,
            font=self._live_log_font,
            spacing1=2,
            spacing3=1,
            insertbackground=DARK_FG,
            selectbackground=DARK_SELECT_BG,
            selectforeground=DARK_SELECT_FG,
            highlightbackground=DARK_BORDER,
            highlightcolor=DARK_ACCENT,
            relief="flat",
            bd=0,
        )
        self.chart_legend_scroll = ttk.Scrollbar(chart_legend_body, orient="vertical", command=self.chart_legend_text.yview)
        self.chart_legend_text.configure(yscrollcommand=self.chart_legend_scroll.set)
        self.chart_legend_text.pack(side="left", fill="both", expand=True)
        self.chart_legend_scroll.pack(side="right", fill="y")
        self.chart_legend_scroll.pack_forget()
        try:
            self.chart_legend_text.tag_configure("legend_head", foreground=DARK_ACCENT2, font=(self._live_log_font.cget("family"), int(self._live_log_font.cget("size")), "bold"))
            self.chart_legend_text.tag_configure("legend_label", foreground="#A9B7C6")
            self.chart_legend_text.tag_configure("legend_value", foreground=DARK_FG)
        except Exception:
            pass
        self.chart_legend_text.configure(state="disabled")



        # Neural levels overview (spans FULL width under the dual section)
        # Shows the current LONG/SHORT level (0..7) for every coin at once.
        neural_box = ttk.LabelFrame(top_controls, text="Neural Levels (0–7)")
        neural_box.pack(fill="both", expand=True, padx=6, pady=(0, 6))
        self.neural_box = neural_box

        legend = ttk.Frame(neural_box)
        legend.pack(fill="x", padx=6, pady=(4, 0))

        ttk.Label(legend, text="Level bars: 0 = bottom, 7 = top").pack(side="left")
        ttk.Label(legend, text="   ").pack(side="left")
        ttk.Label(legend, text="Blue = Long").pack(side="left")
        ttk.Label(legend, text="  ").pack(side="left")
        ttk.Label(legend, text="Orange = Short").pack(side="left")

        self.lbl_neural_overview_last = ttk.Label(legend, text="Last: N/A")
        self.lbl_neural_overview_last.pack(side="right")

        # Scrollable area for tiles (auto-hides the scrollbar if everything fits)
        neural_viewport = ttk.Frame(neural_box)
        neural_viewport.pack(fill="both", expand=True, padx=6, pady=(4, 6))
        neural_viewport.grid_rowconfigure(0, weight=1)
        neural_viewport.grid_columnconfigure(0, weight=1)

        self._neural_overview_canvas = tk.Canvas(
            neural_viewport,
            bg=DARK_PANEL2,
            highlightthickness=1,
            highlightbackground=DARK_BORDER,
            bd=0,
        )
        self._neural_overview_canvas.grid(row=0, column=0, sticky="nsew")

        self._neural_overview_scroll = ttk.Scrollbar(
            neural_viewport,
            orient="vertical",
            command=self._neural_overview_canvas.yview,
        )
        self._neural_overview_scroll.grid(row=0, column=1, sticky="ns")

        self._neural_overview_canvas.configure(yscrollcommand=self._neural_overview_scroll.set)

        self.neural_wrap = WrapFrame(self._neural_overview_canvas)
        self._neural_overview_window = self._neural_overview_canvas.create_window(
            (0, 0),
            window=self.neural_wrap,
            anchor="nw",
        )

        def _update_neural_overview_scrollbars(event=None) -> None:
            """Update scrollregion + hide/show the scrollbar depending on overflow."""
            try:
                c = self._neural_overview_canvas
                win = self._neural_overview_window

                c.update_idletasks()
                bbox = c.bbox(win)
                if not bbox:
                    self._neural_overview_scroll.grid_remove()
                    return

                c.configure(scrollregion=bbox)
                content_h = int(bbox[3] - bbox[1])
                view_h = int(c.winfo_height())

                if content_h > (view_h + 1):
                    self._neural_overview_scroll.grid()
                else:
                    self._neural_overview_scroll.grid_remove()
                    try:
                        c.yview_moveto(0)
                    except Exception:
                        pass
            except Exception:
                pass

        def _on_neural_canvas_configure(e) -> None:
            # Keep the inner wrap frame exactly the canvas width so wrapping is correct.
            try:
                self._neural_overview_canvas.itemconfigure(self._neural_overview_window, width=int(e.width))
            except Exception:
                pass
            _update_neural_overview_scrollbars()

        self._neural_overview_canvas.bind("<Configure>", _on_neural_canvas_configure, add="+")
        self.neural_wrap.bind("<Configure>", _update_neural_overview_scrollbars, add="+")
        self._update_neural_overview_scrollbars = _update_neural_overview_scrollbars

        # Mousewheel scroll inside the tiles area
        def _wheel(e):
            try:
                if self._neural_overview_scroll.winfo_ismapped():
                    self._neural_overview_canvas.yview_scroll(int(-1 * (e.delta / 120)), "units")
            except Exception:
                pass

        self._neural_overview_canvas.bind("<Enter>", lambda _e: self._neural_overview_canvas.focus_set(), add="+")
        self._neural_overview_canvas.bind("<MouseWheel>", _wheel, add="+")

        # tiles by coin
        self.neural_tiles: Dict[str, NeuralSignalTile] = {}
        # small cache: path -> (mtime, value)
        self._neural_overview_cache: Dict[str, Tuple[float, Any]] = {}

        self._rebuild_neural_overview()
        try:
            self.after_idle(self._update_neural_overview_scrollbars)
        except Exception:
            pass








        # ----------------------------
        # LEFT: 3) Live Output (pane)
        # ----------------------------

        # Half-size fixed-width font for live logs (Runner/Trader/Trainers)
        _base = tkfont.nametofont("TkFixedFont")
        _half = max(8, int(round(abs(int(_base.cget("size"))) * 0.82)))
        self._live_log_font = _base.copy()
        self._live_log_font.configure(size=_half)

        logs_frame = ttk.LabelFrame(left_split, text="Live Output")
        self.logs_nb = ttk.Notebook(logs_frame)
        self.logs_nb.pack(fill="both", expand=True, padx=6, pady=6)


        # Runner tab
        runner_tab = ttk.Frame(self.logs_nb)
        self.logs_nb.add(runner_tab, text="Runner")
        self.runner_text = tk.Text(
            runner_tab,
            height=8,
            wrap="none",
            font=self._live_log_font,
            bg=DARK_PANEL,
            fg=DARK_FG,
            padx=8,
            pady=6,
            spacing1=1,
            spacing3=1,
            insertbackground=DARK_FG,
            selectbackground=DARK_SELECT_BG,
            selectforeground=DARK_SELECT_FG,
            highlightbackground=DARK_BORDER,
            highlightcolor=DARK_ACCENT,
        )

        runner_scroll = ttk.Scrollbar(runner_tab, orient="vertical", command=self.runner_text.yview)
        self.runner_text.configure(yscrollcommand=runner_scroll.set)
        self.runner_text.pack(side="left", fill="both", expand=True)
        runner_scroll.pack(side="right", fill="y")
        try:
            self.runner_text.tag_configure("log_ts", foreground="#8FA5B8")
            self.runner_text.tag_configure("log_warn", foreground="#FFCC66")
            self.runner_text.tag_configure("log_err", foreground="#FF6B57")
            self.runner_text.tag_configure("log_launch", foreground=DARK_ACCENT2)
        except Exception:
            pass

        # Trader tab
        trader_tab = ttk.Frame(self.logs_nb)
        self.logs_nb.add(trader_tab, text="Trader")
        self.trader_text = tk.Text(
            trader_tab,
            height=8,
            wrap="none",
            font=self._live_log_font,
            bg=DARK_PANEL,
            fg=DARK_FG,
            padx=8,
            pady=6,
            spacing1=1,
            spacing3=1,
            insertbackground=DARK_FG,
            selectbackground=DARK_SELECT_BG,
            selectforeground=DARK_SELECT_FG,
            highlightbackground=DARK_BORDER,
            highlightcolor=DARK_ACCENT,
        )

        trader_scroll = ttk.Scrollbar(trader_tab, orient="vertical", command=self.trader_text.yview)
        self.trader_text.configure(yscrollcommand=trader_scroll.set)
        self.trader_text.pack(side="left", fill="both", expand=True)
        trader_scroll.pack(side="right", fill="y")
        try:
            self.trader_text.tag_configure("log_ts", foreground="#8FA5B8")
            self.trader_text.tag_configure("log_warn", foreground="#FFCC66")
            self.trader_text.tag_configure("log_err", foreground="#FF6B57")
            self.trader_text.tag_configure("log_launch", foreground=DARK_ACCENT2)
        except Exception:
            pass

        # Training tab (statuses + trainer controls/logs)
        training_tab = ttk.Frame(self.logs_nb)
        self.logs_nb.add(training_tab, text="Training")

        train_status_wrap = ttk.LabelFrame(training_tab, text="Training Status")
        train_status_wrap.pack(fill="x", padx=6, pady=(6, 0))

        train_row = ttk.Frame(train_status_wrap)
        train_row.pack(fill="x", pady=(0, 6))

        self.train_coin_var = tk.StringVar(value=(self.coins[0] if self.coins else ""))
        ttk.Label(train_row, text="Train coin:").pack(side="left")
        self.train_coin_combo = ttk.Combobox(
            train_row,
            textvariable=self.train_coin_var,
            values=self.coins,
            width=8,
            state="readonly",
        )
        self.train_coin_combo.pack(side="left", padx=(6, 0))

        train_buttons_row = ttk.Frame(train_status_wrap)
        train_buttons_row.pack(fill="x", pady=(0, 6))
        ttk.Button(train_buttons_row, text="Train Selected", width=BTN_W, command=self.train_selected_coin).pack(side="left")
        ttk.Button(train_buttons_row, text="Train All", width=BTN_W, command=self.train_all_coins).pack(side="left", padx=(6, 0))

        self.lbl_training_overview = ttk.Label(train_status_wrap, text="Training: N/A")
        self.lbl_training_overview.pack(anchor="w", pady=(0, 2))

        self.lbl_training_progress = ttk.Label(train_status_wrap, text="Progress: 0% (0 / 0)")
        self.lbl_training_progress.pack(anchor="w", pady=(0, 2))

        self.lbl_flow_hint = ttk.Label(train_status_wrap, text="Flow: Train → Start Trades")
        self.lbl_flow_hint.pack(anchor="w", pady=(0, 6))

        self.training_list = tk.Listbox(
            train_status_wrap,
            height=5,
            bg=DARK_PANEL,
            fg=DARK_FG,
            selectbackground=DARK_SELECT_BG,
            selectforeground=DARK_SELECT_FG,
            highlightbackground=DARK_BORDER,
            highlightcolor=DARK_ACCENT,
            activestyle="none",
        )
        self.training_list.pack(fill="x", pady=(0, 6))

        ttk.Separator(training_tab, orient="horizontal").pack(fill="x", padx=6, pady=(0, 6))

        top_bar = ttk.Frame(training_tab)
        top_bar.pack(fill="x", padx=6, pady=6)

        self.trainer_coin_var = tk.StringVar(value=(self.coins[0] if self.coins else "BTC"))
        ttk.Label(top_bar, text="Coin:").pack(side="left")
        self.trainer_coin_combo = ttk.Combobox(
            top_bar,
            textvariable=self.trainer_coin_var,
            values=self.coins,
            state="readonly",
            width=8
        )
        self.trainer_coin_combo.pack(side="left", padx=(6, 12))

        ttk.Button(top_bar, text="Start Trainer", command=self.start_trainer_for_selected_coin).pack(side="left")
        ttk.Button(top_bar, text="Stop Trainer", command=self.stop_trainer_for_selected_coin).pack(side="left", padx=(6, 0))

        self.trainer_status_lbl = ttk.Label(top_bar, text="(no trainers running)")
        self.trainer_status_lbl.pack(side="left", padx=(12, 0))

        def _sync_train_coin(*_):
            try:
                self.trainer_coin_var.set(self.train_coin_var.get())
            except Exception:
                pass

        def _sync_trainer_coin(*_):
            try:
                self.train_coin_var.set(self.trainer_coin_var.get())
            except Exception:
                pass

        self.train_coin_combo.bind("<<ComboboxSelected>>", _sync_train_coin)
        self.trainer_coin_combo.bind("<<ComboboxSelected>>", _sync_trainer_coin)
        _sync_train_coin()

        trainer_log_box = ttk.LabelFrame(training_tab, text="Trainer Log")
        trainer_log_box.pack(fill="both", expand=True, padx=6, pady=(0, 6))

        self.trainer_text = tk.Text(
            trainer_log_box,
            height=8,
            wrap="none",
            font=self._live_log_font,
            bg=DARK_PANEL,
            fg=DARK_FG,
            padx=8,
            pady=6,
            spacing1=1,
            spacing3=1,
            insertbackground=DARK_FG,
            selectbackground=DARK_SELECT_BG,
            selectforeground=DARK_SELECT_FG,
            highlightbackground=DARK_BORDER,
            highlightcolor=DARK_ACCENT,
        )

        trainer_scroll = ttk.Scrollbar(trainer_log_box, orient="vertical", command=self.trainer_text.yview)
        self.trainer_text.configure(yscrollcommand=trainer_scroll.set)
        self.trainer_text.pack(side="left", fill="both", expand=True, padx=(6, 0), pady=(0, 6))
        trainer_scroll.pack(side="right", fill="y", padx=(0, 6), pady=(0, 6))
        try:
            self.trainer_text.tag_configure("log_ts", foreground="#8FA5B8")
            self.trainer_text.tag_configure("log_warn", foreground="#FFCC66")
            self.trainer_text.tag_configure("log_err", foreground="#FF6B57")
            self.trainer_text.tag_configure("log_launch", foreground=DARK_ACCENT2)
        except Exception:
            pass


        # Add left panes (no trades/history on the left anymore)
        # Default should match the screenshot: more room for Controls/Health + Neural Levels.
        left_split.add(top_controls, weight=1)
        left_split.add(logs_frame, weight=1)

        try:
            # Ensure the top pane can't start (or be clamped) too small to show Neural Levels.
            left_split.paneconfigure(top_controls, minsize=360)
            left_split.paneconfigure(logs_frame, minsize=220)
        except Exception:
            pass

        def _init_left_split_sash_once():
            try:
                if getattr(self, "_did_init_left_split_sash", False):
                    return

                # If the user already moved the sash, never override it.
                if getattr(self, "_user_moved_left_split", False):
                    self._did_init_left_split_sash = True
                    return

                total = left_split.winfo_height()
                if total <= 2:
                    self.after(10, _init_left_split_sash_once)
                    return

                min_top = 360
                min_bottom = 220

                # Match screenshot feel: keep Live Output ~260px high, give the rest to top.
                desired_bottom = 260
                target = total - max(min_bottom, desired_bottom)
                target = max(min_top, min(total - min_bottom, target))

                left_split.sashpos(0, int(target))
                self._did_init_left_split_sash = True
            except Exception:
                pass

        self.after_idle(_init_left_split_sash_once)






        # ----------------------------
        # RIGHT TOP: Charts (tabs)
        # ----------------------------
        charts_frame = ttk.LabelFrame(right_split, text="Charts (Neural lines overlaid)")
        self._charts_frame = charts_frame

        charts_top_bar = ttk.Frame(charts_frame)
        charts_top_bar.pack(fill="x", padx=6, pady=(6, 0))

        ttk.Label(charts_top_bar, text="Chart:").pack(side="left")
        self.chart_search_var = tk.StringVar(value="ACCOUNT")
        self.chart_search_combo = ttk.Combobox(
            charts_top_bar,
            textvariable=self.chart_search_var,
            values=["ACCOUNT"] + list(self.coins),
            width=18,
            state="readonly",
        )
        self.chart_search_combo.pack(side="left", padx=(6, 12))

        def _activate_chart_search(_e=None):
            try:
                target = (self.chart_search_var.get() or "").strip().upper()
                options = ["ACCOUNT"] + list(self.coins)
                self.chart_search_combo["values"] = options
                if target in options:
                    self._show_chart_page(target)
                elif options:
                    self._show_chart_page(options[0])
            except Exception:
                pass

        self.chart_search_combo.bind("<<ComboboxSelected>>", _activate_chart_search)
        self.chart_search_combo.bind("<Return>", _activate_chart_search)

        def _open_tradingview() -> None:
            try:
                import webbrowser
                sym = str(getattr(self, "_current_chart_page", "ACCOUNT") or "ACCOUNT").upper().strip()
                if sym == "ACCOUNT" or sym not in (self.coins or []):
                    sym = (self.coins[0] if self.coins else "BTC")
                webbrowser.open(f"https://www.tradingview.com/chart/?symbol=KUCOIN:{sym}USDT")
            except Exception:
                pass

        ttk.Button(charts_top_bar, text="Open TradingView", style="Accent.TButton", command=_open_tradingview).pack(side="right")
        ttk.Button(charts_top_bar, text="Export PNG", command=self._export_active_chart_png).pack(side="right", padx=(0, 6))

        # Navigation is now handled by the dropdown only; keep a hidden placeholder for rebuild logic.
        self.chart_tabs_bar = ttk.Frame(charts_frame)

        # Page container (no ttk.Notebook, so there are NO native tabs to show)
        self.chart_pages_container = ttk.Frame(charts_frame)
        # Keep left padding, remove right padding so charts fill to the edge
        self.chart_pages_container.pack(fill="both", expand=True, padx=(6, 0), pady=(0, 6))


        self._chart_tab_buttons: Dict[str, ttk.Button] = {}
        self.chart_pages: Dict[str, ttk.Frame] = {}
        self._current_chart_page: str = "ACCOUNT"

        def _show_page(name: str) -> None:
            self._current_chart_page = name
            # hide all pages
            for f in self.chart_pages.values():
                try:
                    f.pack_forget()
                except Exception:
                    pass
            # show selected
            f = self.chart_pages.get(name)
            if f is not None:
                f.pack(fill="both", expand=True)
            try:
                self.chart_search_var.set(name)
            except Exception:
                pass
            try:
                self._refresh_chart_legend_panel()
            except Exception:
                pass
            try:
                self._refresh_neural_overview_visibility()
            except Exception:
                pass

            # style selected tab
            for txt, b in self._chart_tab_buttons.items():
                try:
                    b.configure(style=("ChartTabSelected.TButton" if txt == name else "ChartTab.TButton"))
                except Exception:
                    pass

            # Immediately refresh the newly shown coin chart so candles appear right away
            # (even if trader/neural scripts are not running yet).
            try:
                tab = str(name or "").strip().upper()
                if tab and tab != "ACCOUNT":
                    coin = tab
                    chart = self.charts.get(coin)
                    if chart:
                        def _do_refresh_visible():
                            try:
                                # Ensure coin folders exist (best-effort; fast)
                                try:
                                    cf_sig = (self.settings.get("main_neural_dir"), tuple(self.coins))
                                    if getattr(self, "_coin_folders_sig", None) != cf_sig:
                                        self._coin_folders_sig = cf_sig
                                        self.coin_folders = build_coin_folders(self.settings["main_neural_dir"], self.coins)
                                except Exception:
                                    pass

                                pos = self._last_positions.get(coin, {}) if isinstance(self._last_positions, dict) else {}
                                buy_px = pos.get("current_buy_price", None)
                                sell_px = pos.get("current_sell_price", None)
                                trail_line = pos.get("trail_line", None)
                                dca_line_price = pos.get("dca_line_price", None)
                                avg_cost_basis = pos.get("avg_cost_basis", None)
                                qty = pos.get("quantity", None)

                                chart.refresh(
                                    self.coin_folders,
                                    current_buy_price=buy_px,
                                    current_sell_price=sell_px,
                                    trail_line=trail_line,
                                    dca_line_price=dca_line_price,
                                    avg_cost_basis=avg_cost_basis,
                                    quantity=qty,
                                )

                            except Exception:
                                pass

                        self.after(1, _do_refresh_visible)
            except Exception:
                pass


        self._show_chart_page = _show_page  # used by _rebuild_coin_chart_tabs()

        # ACCOUNT page
        acct_page = ttk.Frame(self.chart_pages_container)
        self.chart_pages["ACCOUNT"] = acct_page

        self.account_chart = AccountValueChart(
            acct_page,
            self.account_value_history_path,
            self.trade_history_path,
        )
        self.account_chart.pack(fill="both", expand=True)

        # Coin pages
        self.charts: Dict[str, CandleChart] = {}
        for coin in self.coins:
            page = ttk.Frame(self.chart_pages_container)
            self.chart_pages[coin] = page

            chart = CandleChart(page, self.fetcher, coin, self._settings_getter, self.trade_history_path)
            chart.pack(fill="both", expand=True)
            self.charts[coin] = chart

        # show initial page
        self._show_chart_page("ACCOUNT")





        # ----------------------------
        # RIGHT BOTTOM: Current Trades + Trade History (stacked)
        # ----------------------------
        right_bottom_split = ttk.Panedwindow(right_split, orient="vertical")
        self._pw_right_bottom_split = right_bottom_split

        right_bottom_split.bind("<Configure>", lambda e: self._schedule_paned_clamp(self._pw_right_bottom_split))
        right_bottom_split.bind("<ButtonRelease-1>", lambda e: (
            setattr(self, "_user_moved_right_bottom_split", True),
            self._schedule_paned_clamp(self._pw_right_bottom_split),
        ))

        # Current trades (top)
        trades_frame = ttk.LabelFrame(right_bottom_split, text="Current Trades")

        self.lbl_selected_coin_summary = tk.Label(
            trades_frame,
            text="Selected: ACCOUNT",
            bg=DARK_PANEL2,
            fg=DARK_ACCENT2,
            anchor="w",
            padx=8,
            pady=4,
        )
        self.lbl_selected_coin_summary.pack(fill="x", padx=6, pady=(4, 0))

        cols = (
            "coin",
            "qty",
            "value",          # <-- right after qty
            "unrealized_usd",
            "realized_usd",
            "avg_cost",
            "buy_price",
            "buy_pnl",
            "sell_price",
            "sell_pnl",
            "dca_stages",
            "dca_24h",
            "next_dca",
            "trail_line",     # keep trail line column
        )

        header_labels = {
            "coin": "Coin",
            "qty": "Qty",
            "value": "Value",
            "unrealized_usd": "Unrlzd $",
            "realized_usd": "Rlz $",
            "avg_cost": "Avg Cost",
            "buy_price": "Ask Price",
            "buy_pnl": "DCA PnL",
            "sell_price": "Bid Price",
            "sell_pnl": "Sell PnL",
            "dca_stages": "Stage",
            "dca_24h": "24h DCA",
            "next_dca": "Next DCA",
            "trail_line": "Trail Line",
        }

        trades_table_wrap = ttk.Frame(trades_frame)
        trades_table_wrap.pack(fill="both", expand=True, padx=6, pady=6)
        self.trades_cols = cols
        self.trades_header_labels = dict(header_labels)
        self.trades_numeric_cols = {
            "qty", "value", "unrealized_usd", "realized_usd", "avg_cost",
            "buy_price", "buy_pnl", "sell_price", "sell_pnl", "next_dca", "trail_line",
        }
        self.trades_center_cols = {"coin", "dca_stages", "dca_24h"}
        self._trades_base_widths = {
            "coin": 76,
            "qty": 102,
            "value": 104,
            "unrealized_usd": 118,
            "realized_usd": 104,
            "avg_cost": 112,
            "buy_price": 112,
            "buy_pnl": 88,
            "sell_price": 112,
            "sell_pnl": 88,
            "dca_stages": 82,
            "dca_24h": 86,
            "next_dca": 138,
            "trail_line": 112,
        }
        self._trades_table_rows = []
        self._trades_header_height = 28
        self._trades_row_height = 28

        self.trades_canvas = tk.Canvas(
            trades_table_wrap,
            bg=DARK_PANEL,
            highlightthickness=1,
            highlightbackground=DARK_BORDER,
            bd=0,
        )
        ysb = ttk.Scrollbar(trades_table_wrap, orient="vertical", command=self.trades_canvas.yview)
        xsb = ttk.Scrollbar(trades_table_wrap, orient="horizontal", command=self.trades_canvas.xview)
        self.trades_canvas.configure(yscrollcommand=ysb.set, xscrollcommand=xsb.set)

        self.trades_canvas.pack(side="top", fill="both", expand=True)
        xsb.pack(side="bottom", fill="x")
        ysb.pack(side="right", fill="y")

        self.trades_canvas.bind("<Configure>", lambda e: self.after_idle(self._draw_trades_table))


        # Trade history (bottom)
        hist_frame = ttk.LabelFrame(right_bottom_split, text="Trade History (scroll)")

        hist_wrap = ttk.Frame(hist_frame)
        hist_wrap.pack(fill="both", expand=True, padx=6, pady=6)

        self.hist_list = tk.Listbox(
            hist_wrap,
            height=10,
            font=self._live_log_font,
            bg=DARK_PANEL,
            fg=DARK_FG,
            selectbackground=DARK_SELECT_BG,
            selectforeground=DARK_SELECT_FG,
            highlightbackground=DARK_BORDER,
            highlightcolor=DARK_ACCENT,
            activestyle="none",
        )
        ysb2 = ttk.Scrollbar(hist_wrap, orient="vertical", command=self.hist_list.yview)
        xsb2 = ttk.Scrollbar(hist_wrap, orient="horizontal", command=self.hist_list.xview)
        self.hist_list.configure(yscrollcommand=ysb2.set, xscrollcommand=xsb2.set)

        self.hist_list.pack(side="left", fill="both", expand=True)
        ysb2.pack(side="right", fill="y")
        xsb2.pack(side="bottom", fill="x")


        # Assemble right side
        right_split.add(charts_frame, weight=3)
        right_split.add(right_bottom_split, weight=2)

        right_bottom_split.add(trades_frame, weight=2)
        right_bottom_split.add(hist_frame, weight=1)

        try:
            # Screenshot-style sizing: don't force Charts to be enormous by default.
            right_split.paneconfigure(charts_frame, minsize=360)
            right_split.paneconfigure(right_bottom_split, minsize=220)
        except Exception:
            pass

        try:
            right_bottom_split.paneconfigure(trades_frame, minsize=140)
            right_bottom_split.paneconfigure(hist_frame, minsize=120)
        except Exception:
            pass

        # Startup defaults to match the screenshot (but never override if user already dragged).
        def _init_right_split_sash_once():
            try:
                if getattr(self, "_did_init_right_split_sash", False):
                    return

                if getattr(self, "_user_moved_right_split", False):
                    self._did_init_right_split_sash = True
                    return

                total = right_split.winfo_height()
                if total <= 2:
                    self.after(10, _init_right_split_sash_once)
                    return

                min_top = 360
                min_bottom = 220
                desired_top = 455  # favor more height for the active chart
                target = max(min_top, min(total - min_bottom, desired_top))

                right_split.sashpos(0, int(target))
                self._did_init_right_split_sash = True
            except Exception:
                pass

        def _init_right_bottom_split_sash_once():
            try:
                if getattr(self, "_did_init_right_bottom_split_sash", False):
                    return

                if getattr(self, "_user_moved_right_bottom_split", False):
                    self._did_init_right_bottom_split_sash = True
                    return

                total = right_bottom_split.winfo_height()
                if total <= 2:
                    self.after(10, _init_right_bottom_split_sash_once)
                    return

                min_top = 140
                min_bottom = 120
                desired_top = 240  # give the chart more room by default
                target = max(min_top, min(total - min_bottom, desired_top))

                right_bottom_split.sashpos(0, int(target))
                self._did_init_right_bottom_split_sash = True
            except Exception:
                pass

        self.after_idle(_init_right_split_sash_once)
        self.after_idle(_init_right_bottom_split_sash_once)

        # Initial clamp once everything is laid out
        self.after_idle(lambda: (
            self._schedule_paned_clamp(getattr(self, "_pw_outer", None)),
            self._schedule_paned_clamp(getattr(self, "_pw_left_split", None)),
            self._schedule_paned_clamp(getattr(self, "_pw_right_split", None)),
            self._schedule_paned_clamp(getattr(self, "_pw_right_bottom_split", None)),
        ))

        self._build_parallel_market_placeholder(
            self.stocks_market_tab,
            market_key="stocks",
            market_name="Stocks",
            broker_name="Alpaca",
            subtitle="Alpaca-backed stock AI (paper/live later)",
            notes=(
                "Status: UI scaffold ready\n"
                "Broker: Alpaca (paper-first)\n"
                "Focus: small equity trades, profit target, trailing exits\n"
                "Next: account auth, market hours, stock scanner, paper executor"
            ),
        )
        self._build_parallel_market_placeholder(
            self.forex_market_tab,
            market_key="forex",
            market_name="Forex",
            broker_name="OANDA",
            subtitle="OANDA-backed forex AI (practice/live later)",
            notes=(
                "Status: UI scaffold ready\n"
                "Broker: OANDA (practice-first)\n"
                "Focus: short-horizon FX trades, profit target, trailing exits\n"
                "Next: account auth, pair universe, pricing feed, practice executor"
            ),
        )


        # status bar
        self.status = ttk.Label(self, text="Ready", anchor="w")
        self.status.pack(fill="x", side="bottom")



    def _build_parallel_market_placeholder(
        self,
        parent: ttk.Frame,
        market_key: str,
        market_name: str,
        broker_name: str,
        subtitle: str,
        notes: str,
    ) -> None:
        outer = ttk.Panedwindow(parent, orient="horizontal")
        outer.pack(fill="both", expand=True, padx=8, pady=8)

        left = ttk.Frame(outer)
        right = ttk.Frame(outer)
        outer.add(left, weight=1)
        outer.add(right, weight=2)

        left_split = ttk.Panedwindow(left, orient="vertical")
        left_split.pack(fill="both", expand=True)
        right_split = ttk.Panedwindow(right, orient="vertical")
        right_split.pack(fill="both", expand=True)

        dashboard = ttk.LabelFrame(left_split, text=f"{market_name} Dashboard")

        system_box = ttk.LabelFrame(dashboard, text="System")
        system_box.pack(fill="x", padx=6, pady=(6, 6))
        ai_var = tk.StringVar(value=f"{market_name} AI: not configured")
        trader_var = tk.StringVar(value=f"{market_name} Trader: not configured")
        state_var = tk.StringVar(value="Trade State: NOT STARTED")
        endpoint_var = tk.StringVar(value=f"Broker: {broker_name} | endpoint not set")
        ttk.Label(system_box, textvariable=ai_var).pack(anchor="w", padx=6, pady=(4, 2))
        ttk.Label(system_box, textvariable=trader_var).pack(anchor="w", padx=6, pady=(0, 2))
        state_lbl = ttk.Label(system_box, textvariable=state_var, justify="left", wraplength=520)
        state_lbl.pack(anchor="w", padx=6, pady=(0, 2), fill="x")
        endpoint_lbl = ttk.Label(system_box, textvariable=endpoint_var, foreground=DARK_MUTED, justify="left", wraplength=520)
        endpoint_lbl.pack(anchor="w", padx=6, pady=(0, 4), fill="x")
        health_chip_row = tk.Frame(system_box, bg=DARK_BG)
        health_chip_row.pack(fill="x", padx=6, pady=(0, 6))
        chip_data = tk.Label(health_chip_row, text=" Data: N/A ", padx=8, pady=3)
        chip_data.pack(side="left", padx=(0, 6))
        chip_broker = tk.Label(health_chip_row, text=" Broker: N/A ", padx=8, pady=3)
        chip_broker.pack(side="left", padx=(0, 6))
        chip_orders = tk.Label(health_chip_row, text=" Orders: N/A ", padx=8, pady=3)
        chip_orders.pack(side="left", padx=(0, 6))
        chip_cycle = tk.Label(health_chip_row, text=" Cycle: N/A ", padx=8, pady=3)
        chip_cycle.pack(side="left")
        self._set_badge_style(chip_data, "Data: N/A", tone="muted")
        self._set_badge_style(chip_broker, "Broker: N/A", tone="muted")
        self._set_badge_style(chip_orders, "Orders: N/A", tone="muted")
        self._set_badge_style(chip_cycle, "Cycle: N/A", tone="muted")
        try:
            system_box.bind(
                "<Configure>",
                lambda e, lbls=(state_lbl, endpoint_lbl): [
                    w.configure(wraplength=max(260, int(getattr(e, "width", 560)) - 28))
                    for w in lbls
                ],
                add="+",
            )
        except Exception:
            pass
        test_btn = ttk.Button(
            system_box,
            text=f"Test {broker_name} Connection",
            style="Compact.TButton",
            command=lambda mk=market_key: self._run_market_connection_test(mk),
        )
        test_btn.pack(anchor="w", padx=6, pady=(0, 6))

        action_box = ttk.LabelFrame(dashboard, text="Action Center")
        action_box.pack(fill="x", padx=6, pady=(0, 6))
        action_buttons = ttk.Frame(action_box)
        action_buttons.pack(fill="x", padx=6, pady=(6, 4))
        run_btn = ttk.Button(
            action_buttons,
            text="Run Scan",
            width=14,
            style="Accent.TButton",
            command=lambda mk=market_key: self._run_market_thinker_scan(mk, force=True, min_interval_s=0.0),
        )
        run_btn.pack(side="left")
        if market_key == "stocks":
            trader_step_btn = ttk.Button(
                action_buttons,
                text="Run Stocks Step",
                width=16,
                style="Compact.TButton",
                command=lambda: self._run_stock_trader_step(force=True, min_interval_s=0.0),
            )
        else:
            trader_step_btn = ttk.Button(
                action_buttons,
                text="Run Forex Step",
                width=16,
                style="Compact.TButton",
                command=lambda: self._run_forex_trader_step(force=True, min_interval_s=0.0),
            )
        trader_step_btn.pack(side="left", padx=(6, 0))
        ttk.Button(
            action_buttons,
            text="Refresh Snapshot",
            width=16,
            style="Compact.TButton",
            command=lambda mk=market_key: self._schedule_market_snapshot_refresh(mk, every_s=0.0),
        ).pack(side="left", padx=(6, 0))
        trader_step_market_key = market_key

        action_status_var = tk.StringVar(value="Next: configure broker credentials, then test connection.")
        ttk.Label(action_box, textvariable=action_status_var, foreground=DARK_MUTED, wraplength=500, justify="left").pack(
            anchor="w", padx=6, pady=(0, 6)
        )
        action_auto_row = ttk.Frame(action_box)
        action_auto_row.pack(fill="x", padx=6, pady=(0, 6))
        auto_scan_var = tk.BooleanVar(value=True)
        auto_step_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(action_auto_row, text="Auto scan", variable=auto_scan_var).pack(side="left")
        ttk.Checkbutton(action_auto_row, text="Auto trader step", variable=auto_step_var).pack(side="left", padx=(10, 0))

        portfolio_box = ttk.LabelFrame(dashboard, text="Portfolio")
        portfolio_box.pack(fill="x", padx=6, pady=(0, 6))
        metric_grid = ttk.Frame(portfolio_box)
        metric_grid.pack(fill="x", padx=6, pady=6)
        metric_grid.columnconfigure(1, weight=1)
        portfolio_vars = {
            "buying_power": tk.StringVar(value="Pending account link"),
            "open_positions": tk.StringVar(value="0"),
            "realized_pnl": tk.StringVar(value="N/A"),
            "mode": tk.StringVar(value="Paper first"),
            "daily_guard": tk.StringVar(value="Armed"),
        }
        for idx, (label, key) in enumerate((
            ("Buying Power", "buying_power"),
            ("Open Positions", "open_positions"),
            ("Realized PnL", "realized_pnl"),
            ("Mode", "mode"),
            ("Daily Loss Guardrail", "daily_guard"),
        )):
            ttk.Label(metric_grid, text=label).grid(row=idx, column=0, sticky="w", padx=(0, 10), pady=2)
            ttk.Label(metric_grid, textvariable=portfolio_vars[key]).grid(row=idx, column=1, sticky="e", pady=2)

        notes_box = ttk.LabelFrame(dashboard, text="Market Notes")
        notes_box.pack(fill="both", expand=True, padx=6, pady=(0, 6))
        notes_hdr = ttk.Frame(notes_box)
        notes_hdr.pack(fill="x", padx=6, pady=(6, 0))
        notes_collapsed_var = tk.BooleanVar(value=True)
        notes_toggle_btn = ttk.Button(notes_hdr, text="Show")
        notes_toggle_btn.pack(side="right")
        notes_body = ttk.Frame(notes_box)
        notes_body.pack(fill="both", expand=True, padx=6, pady=6)
        notes_text = tk.Text(
            notes_body,
            height=8,
            wrap="word",
            font=self._live_log_font,
            bg=DARK_PANEL,
            fg=DARK_FG,
            padx=8,
            pady=6,
            spacing1=2,
            spacing3=1,
            relief="flat",
            bd=0,
            highlightbackground=DARK_BORDER,
            highlightcolor=DARK_ACCENT,
        )
        notes_text.pack(fill="both", expand=True, padx=6, pady=6)
        notes_text.insert("1.0", notes)
        notes_text.configure(state="disabled")

        def _toggle_notes() -> None:
            try:
                collapsed = bool(notes_collapsed_var.get())
            except Exception:
                collapsed = False
            if collapsed:
                try:
                    notes_body.pack(fill="both", expand=True, padx=6, pady=6)
                except Exception:
                    pass
                notes_toggle_btn.configure(text="Hide")
                notes_collapsed_var.set(False)
            else:
                try:
                    notes_body.pack_forget()
                except Exception:
                    pass
                notes_toggle_btn.configure(text="Show")
                notes_collapsed_var.set(True)

        notes_toggle_btn.configure(command=_toggle_notes)
        try:
            notes_body.pack_forget()
        except Exception:
            pass

        left_split.add(dashboard, weight=1)

        charts_frame = ttk.LabelFrame(right_split, text=f"{market_name} Charts")
        charts_top = ttk.Frame(charts_frame)
        charts_top.pack(fill="x", padx=6, pady=(6, 0))
        charts_top_row1 = ttk.Frame(charts_top)
        charts_top_row1.pack(fill="x")
        charts_top_row2 = ttk.Frame(charts_top)
        charts_top_row2.pack(fill="x", pady=(6, 0))

        ttk.Label(charts_top_row1, text=f"{market_name} View:").pack(side="left")
        view_options = ("Overview", "Scanner", "Leaders", "Positions", "Execution", "Health")
        market_view_var = tk.StringVar(value="Overview")
        view_tabs = ttk.Frame(charts_top_row1)
        view_tabs.pack(side="left", padx=(6, 12))
        view_buttons: Dict[str, ttk.Button] = {}

        def _set_market_view(view_name: str) -> None:
            market_view_var.set(view_name)
            for vname, btn in view_buttons.items():
                try:
                    btn.configure(style=("ChartTabSelected.TButton" if vname == view_name else "ChartTab.TButton"))
                except Exception:
                    pass
            self._refresh_parallel_market_panels()

        for v in view_options:
            btn = ttk.Button(
                view_tabs,
                text=v,
                style=("ChartTabSelected.TButton" if v == "Overview" else "ChartTab.TButton"),
                command=lambda name=v: _set_market_view(name),
                width=10,
            )
            btn.pack(side="left", padx=(0, 4))
            view_buttons[v] = btn

        ttk.Label(charts_top_row2, text=subtitle, foreground=DARK_MUTED).pack(side="left")
        top_pick_var = tk.StringVar(value="")
        ttk.Label(charts_top_row2, textvariable=top_pick_var, foreground=DARK_ACCENT2).pack(side="left", padx=(10, 0))
        signal_var = tk.StringVar(value="")
        signal_lbl = ttk.Label(charts_top_row2, textvariable=signal_var, foreground=DARK_FG, justify="left", wraplength=420)
        signal_lbl.pack(side="left", padx=(10, 0), fill="x", expand=True)
        try:
            charts_top_row2.bind(
                "<Configure>",
                lambda e, lbl=signal_lbl: lbl.configure(wraplength=max(260, int(getattr(e, "width", 600)) - 320)),
                add="+",
            )
        except Exception:
            pass
        charts_age_var = tk.StringVar(value="Updated: N/A")
        ttk.Label(charts_top_row2, textvariable=charts_age_var, foreground=DARK_MUTED).pack(side="right", padx=(0, 10))
        ttk.Label(charts_top_row2, text="Auto scan (background)", foreground=DARK_MUTED).pack(side="right")
        market_view_hint_var = tk.StringVar(value="Overview: ranked leaders, focus chart, and quality signals.")
        ttk.Label(
            charts_top,
            textvariable=market_view_hint_var,
            foreground=DARK_MUTED,
            justify="left",
            wraplength=820,
        ).pack(fill="x", padx=(0, 0), pady=(4, 0))

        charts_top_row3 = ttk.Frame(charts_top)
        charts_top_row3.pack(fill="x", pady=(6, 0))
        ttk.Label(charts_top_row3, text="Chart Focus:").pack(side="left")
        instrument_var = tk.StringVar(value="AUTO")
        instrument_combo = ttk.Combobox(charts_top_row3, textvariable=instrument_var, values=["AUTO"], width=18, state="readonly")
        instrument_combo.pack(side="left", padx=(6, 10))
        ttk.Button(
            charts_top_row3,
            text="Reset to Top",
            width=12,
            style="Compact.TButton",
            command=lambda mk=market_key: self._reset_market_chart_focus(mk),
        ).pack(side="left")
        ttk.Button(
            charts_top_row3,
            text="Open TradingView",
            style="Accent.TButton",
            command=lambda mk=market_key: self._open_market_tradingview(mk),
        ).pack(side="right")
        ttk.Button(
            charts_top_row3,
            text="Export PNG",
            style="Compact.TButton",
            command=lambda mk=market_key: self._export_market_chart_png(mk),
        ).pack(side="right", padx=(0, 6))
        instrument_combo.bind("<<ComboboxSelected>>", lambda _e: self._refresh_parallel_market_panels())

        center = ttk.Frame(charts_frame)
        center.pack(fill="both", expand=True, padx=6, pady=6)
        center.columnconfigure(0, weight=1)
        center.rowconfigure(0, weight=1)
        center.bind("<Configure>", lambda _e, mk=market_key: self._schedule_market_chart_redraw(mk), add="+")
        placeholder = tk.Canvas(
            center,
            bg=DARK_PANEL2,
            highlightthickness=1,
            highlightbackground=DARK_BORDER,
            bd=0,
        )
        placeholder.grid(row=0, column=0, sticky="nsew")
        placeholder.bind("<Configure>", lambda _e, mk=market_key: self._schedule_market_chart_redraw(mk), add="+")
        placeholder.bind("<Motion>", lambda e, mk=market_key: self._on_market_chart_hover(mk, e), add="+")
        placeholder.bind("<Leave>", lambda _e, mk=market_key: self._clear_market_chart_hover(mk), add="+")
        placeholder.create_text(
            24,
            24,
            anchor="nw",
            text=(
                f"{market_name} market overview\n\n"
                "Scanner and trader run in background loops.\n"
                "Use tabs to inspect:\n"
                "• status + account summary\n"
                "• symbol/pair charts\n"
                "• current positions\n"
                "• trade history\n"
                "• logs / health"
            ),
            fill=DARK_FG,
            font=(self._live_log_font.cget("family"), max(9, int(self._live_log_font.cget("size")) + 1)),
        )
        chart_table_wrap = ttk.Frame(center)
        chart_table_wrap.grid(row=0, column=0, sticky="nsew")
        chart_table_wrap.columnconfigure(0, weight=1)
        chart_table_wrap.rowconfigure(0, weight=1)
        chart_table_wrap.bind("<Configure>", lambda _e, mk=market_key: self._schedule_market_chart_redraw(mk), add="+")
        chart_table = ttk.Treeview(chart_table_wrap, show="headings", height=9, selectmode="browse")
        chart_table_y = ttk.Scrollbar(chart_table_wrap, orient="vertical", command=chart_table.yview)
        chart_table_x = ttk.Scrollbar(chart_table_wrap, orient="horizontal", command=chart_table.xview)
        chart_table.configure(yscrollcommand=chart_table_y.set, xscrollcommand=chart_table_x.set)
        chart_table.grid(row=0, column=0, sticky="nsew")
        chart_table_y.grid(row=0, column=1, sticky="ns")
        chart_table_x.grid(row=1, column=0, sticky="ew")
        chart_table.bind("<Motion>", lambda e, mk=market_key: self._on_market_table_hover(mk, e), add="+")
        chart_table.bind("<Leave>", lambda _e, mk=market_key: self._hide_market_table_tooltip(mk), add="+")
        chart_table_wrap.grid_remove()
        lower = ttk.Panedwindow(right_split, orient="vertical")
        positions_box = ttk.LabelFrame(lower, text=f"{market_name} Positions")
        pos_header = ttk.Frame(positions_box)
        pos_header.pack(fill="x", padx=6, pady=(6, 0))
        positions_summary_var = tk.StringVar(value="No open positions.")
        ttk.Label(pos_header, textvariable=positions_summary_var, foreground=DARK_MUTED).pack(side="left")
        positions_age_var = tk.StringVar(value="Updated: N/A")
        ttk.Label(pos_header, textvariable=positions_age_var, foreground=DARK_MUTED).pack(side="right")

        pos_table_wrap = ttk.Frame(positions_box)
        pos_table_wrap.pack(fill="both", expand=True, padx=6, pady=6)
        pos_table_wrap.columnconfigure(0, weight=1)
        pos_table_wrap.rowconfigure(0, weight=1)
        if market_key == "stocks":
            pos_columns = ("symbol", "qty", "value", "upl")
            pos_headings = {
                "symbol": "Symbol",
                "qty": "Qty",
                "value": "Value",
                "upl": "uPnL",
            }
            pos_widths = {"symbol": 120, "qty": 110, "value": 140, "upl": 120}
        else:
            pos_columns = ("pair", "side", "units", "upl")
            pos_headings = {
                "pair": "Pair",
                "side": "Side",
                "units": "Units",
                "upl": "uPnL",
            }
            pos_widths = {"pair": 120, "side": 90, "units": 120, "upl": 120}
        positions_tree = ttk.Treeview(
            pos_table_wrap,
            columns=pos_columns,
            show="headings",
            height=6,
            selectmode="browse",
        )
        for col in pos_columns:
            positions_tree.heading(col, text=pos_headings.get(col, col.title()))
            positions_tree.column(col, anchor=("w" if col in ("symbol", "pair") else "e"), width=pos_widths.get(col, 110), stretch=True)
        try:
            positions_tree.tag_configure("upl_pos", foreground="#00FF99")
            positions_tree.tag_configure("upl_neg", foreground="#FF6B57")
            positions_tree.tag_configure("upl_neu", foreground=DARK_FG)
            positions_tree.tag_configure("placeholder", foreground=DARK_MUTED)
        except Exception:
            pass
        positions_scroll_y = ttk.Scrollbar(pos_table_wrap, orient="vertical", command=positions_tree.yview)
        positions_scroll_x = ttk.Scrollbar(pos_table_wrap, orient="horizontal", command=positions_tree.xview)
        positions_tree.configure(yscrollcommand=positions_scroll_y.set, xscrollcommand=positions_scroll_x.set)
        positions_tree.grid(row=0, column=0, sticky="nsew")
        positions_scroll_y.grid(row=0, column=1, sticky="ns")
        positions_scroll_x.grid(row=1, column=0, sticky="ew")

        history_logs_row = ttk.Frame(lower)
        history_logs_row.columnconfigure(0, weight=1)
        history_logs_row.columnconfigure(1, weight=1)
        history_logs_row.rowconfigure(0, weight=1)

        history_box = ttk.LabelFrame(history_logs_row, text=f"{market_name} History")
        history_box.grid(row=0, column=0, sticky="nsew", padx=(0, 4))
        history_header = ttk.Frame(history_box)
        history_header.pack(fill="x", padx=6, pady=(6, 0))
        history_age_var = tk.StringVar(value="Updated: N/A")
        ttk.Label(history_header, textvariable=history_age_var, foreground=DARK_MUTED).pack(side="left")
        history_autoscroll_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(history_header, text="Auto-scroll", variable=history_autoscroll_var).pack(side="right")
        history_text = tk.Text(
            history_box,
            height=5,
            wrap="none",
            font=self._live_log_font,
            bg=DARK_PANEL,
            fg=DARK_FG,
            padx=8,
            pady=6,
            spacing1=1,
            spacing3=1,
            relief="flat",
            bd=0,
            highlightbackground=DARK_BORDER,
            highlightcolor=DARK_ACCENT,
        )
        history_scroll = ttk.Scrollbar(history_box, orient="vertical", command=history_text.yview)
        history_text.configure(yscrollcommand=history_scroll.set)
        history_text.pack(side="left", fill="both", expand=True, padx=(6, 0), pady=(4, 6))
        history_scroll.pack(side="right", fill="y", padx=(0, 6), pady=(4, 6))
        history_text.insert("1.0", "No trader actions yet.\n")
        history_text.configure(state="disabled")

        logs_box = ttk.LabelFrame(history_logs_row, text=f"{market_name} Logs")
        logs_box.grid(row=0, column=1, sticky="nsew", padx=(4, 0))
        logs_header = ttk.Frame(logs_box)
        logs_header.pack(fill="x", padx=6, pady=(6, 0))
        logs_age_var = tk.StringVar(value="Updated: N/A")
        ttk.Label(logs_header, textvariable=logs_age_var, foreground=DARK_MUTED).pack(side="left")
        log_filter_var = tk.StringVar(value="All")
        ttk.Label(logs_header, text="Filter:", foreground=DARK_MUTED).pack(side="left", padx=(12, 4))
        log_filter_combo = ttk.Combobox(logs_header, values=["All", "Thinker", "Trader", "Broker"], state="readonly", width=9, textvariable=log_filter_var)
        log_filter_combo.pack(side="left")
        logs_autoscroll_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(logs_header, text="Auto-scroll", variable=logs_autoscroll_var).pack(side="right")
        log_text = tk.Text(
            logs_box,
            height=8,
            wrap="none",
            font=self._live_log_font,
            bg=DARK_PANEL,
            fg=DARK_FG,
            padx=8,
            pady=6,
            spacing1=1,
            spacing3=1,
            relief="flat",
            bd=0,
            highlightbackground=DARK_BORDER,
            highlightcolor=DARK_ACCENT,
        )
        log_scroll = ttk.Scrollbar(logs_box, orient="vertical", command=log_text.yview)
        log_text.configure(yscrollcommand=log_scroll.set)
        log_text.pack(side="left", fill="both", expand=True, padx=(6, 0), pady=(4, 6))
        log_scroll.pack(side="right", fill="y", padx=(0, 6), pady=(4, 6))
        log_text.configure(state="disabled")
        log_filter_combo.bind("<<ComboboxSelected>>", lambda _e, mk=market_key: self._render_market_log(mk))

        self.market_panels[market_key] = {
            "market_name": market_name,
            "broker_name": broker_name,
            "status_path": self.market_status_paths.get(market_key, ""),
            "ai_var": ai_var,
            "trader_var": trader_var,
            "state_var": state_var,
            "endpoint_var": endpoint_var,
            "portfolio_vars": portfolio_vars,
            "notes_text": notes_text,
            "notes_toggle_btn": notes_toggle_btn,
            "notes_collapsed_var": notes_collapsed_var,
            "log_text": log_text,
            "logs_age_var": logs_age_var,
            "log_filter_var": log_filter_var,
            "logs_autoscroll_var": logs_autoscroll_var,
            "log_lines": [
                f"[{market_name.upper()}] UI scaffold initialized",
                f"[{market_name.upper()}] Waiting for broker credentials and engine wiring",
            ],
            "positions_tree": positions_tree,
            "positions_summary_var": positions_summary_var,
            "positions_age_var": positions_age_var,
            "history_text": history_text,
            "history_age_var": history_age_var,
            "history_autoscroll_var": history_autoscroll_var,
            "history_lines": [],
            "test_btn": test_btn,
            "trader_step_btn": trader_step_btn,
            "trader_step_market_key": trader_step_market_key,
            "run_btn": run_btn,
            "action_status_var": action_status_var,
            "auto_scan_var": auto_scan_var,
            "auto_step_var": auto_step_var,
            "market_view_var": market_view_var,
            "view_buttons": view_buttons,
            "chart_canvas": placeholder,
            "chart_table_wrap": chart_table_wrap,
            "chart_table": chart_table,
            "top_pick_var": top_pick_var,
            "signal_var": signal_var,
            "signal_lbl": signal_lbl,
            "instrument_var": instrument_var,
            "instrument_combo": instrument_combo,
            "market_view_hint_var": market_view_hint_var,
            "charts_age_var": charts_age_var,
            "chart_hover_data": {},
            "chart_hover_idx": -1,
            "chart_table_tooltips": {},
            "chart_table_note_col_id": "",
            "chart_table_tooltip_key": None,
            "chart_table_tooltip_win": None,
            "chart_table_tooltip_label": None,
            "chart_table_sort_col": "",
            "chart_table_sort_reverse": False,
            "chart_table_headings": {},
            "last_log_sig": None,
            "last_history_sig": None,
            "chip_data": chip_data,
            "chip_broker": chip_broker,
            "chip_orders": chip_orders,
            "chip_cycle": chip_cycle,
        }
        self._render_market_log(market_key)

        right_split.add(charts_frame, weight=4)
        right_split.add(lower, weight=2)
        lower.add(positions_box, weight=2)
        lower.add(history_logs_row, weight=1)

    def _mask_secret(self, value: str, keep: int = 4) -> str:
        raw = str(value or "").strip()
        if not raw:
            return "not set"
        if len(raw) <= keep:
            return "*" * len(raw)
        return ("*" * max(4, len(raw) - keep)) + raw[-keep:]

    def _set_market_notes(self, market_key: str, text: str) -> None:
        panel = self.market_panels.get(market_key, {})
        widget = panel.get("notes_text")
        if not widget:
            return
        try:
            widget.configure(state="normal")
            widget.delete("1.0", "end")
            widget.insert("1.0", str(text or "").strip() + "\n")
            widget.configure(state="disabled")
        except Exception:
            pass

    def _append_market_log(self, market_key: str, line: str) -> None:
        panel = self.market_panels.get(market_key, {})
        lines = panel.get("log_lines")
        if not isinstance(lines, list):
            lines = []
            panel["log_lines"] = lines
        txt = str(line or "").rstrip()
        if not txt:
            return
        lines.append(txt)
        if len(lines) > 1200:
            panel["log_lines"] = lines[-1200:]
        self._render_market_log(market_key)

    def _render_market_log(self, market_key: str) -> None:
        panel = self.market_panels.get(market_key, {})
        widget = panel.get("log_text")
        if not widget:
            return
        all_lines = list(panel.get("log_lines", []) or [])
        mode = str((panel.get("log_filter_var").get() if panel.get("log_filter_var") else "All") or "All").strip().lower()

        def _match(line: str) -> bool:
            up = str(line or "").upper()
            if mode == "thinker":
                return "[THINKER]" in up
            if mode == "trader":
                return "[TRADER]" in up
            if mode == "broker":
                return any(tok in up for tok in ("[OANDA]", "[ALPACA]", "[TEST]", "[OK]", "[FAIL]"))
            return True

        payload = [ln for ln in all_lines if _match(ln)]
        if not payload:
            payload = ["(no log lines for selected filter)"]
        try:
            widget.configure(state="normal")
            widget.delete("1.0", "end")
            widget.insert("1.0", "\n".join(str(x) for x in payload[-500:]) + "\n")
            widget.configure(state="disabled")
            try:
                do_scroll = bool(panel.get("logs_autoscroll_var").get()) if panel.get("logs_autoscroll_var") else True
            except Exception:
                do_scroll = True
            if do_scroll:
                widget.see("end")
        except Exception:
            pass

    def _market_fmt_num(self, value: Any, digits: int = 2) -> str:
        try:
            return f"{float(value):.{int(digits)}f}"
        except Exception:
            return str(value if value is not None else "0")

    def _market_fmt_money(self, value: Any, digits: int = 2) -> str:
        try:
            return f"${float(value):,.{int(digits)}f}"
        except Exception:
            return str(value if value is not None else "0")

    def _market_fmt_signed_money(self, value: Any, digits: int = 2) -> str:
        try:
            v = float(value)
            return f"{v:+,.{int(digits)}f}"
        except Exception:
            return str(value if value is not None else "0")

    def _market_age_text(self, ts: Any) -> str:
        try:
            tsv = float(ts or 0.0)
        except Exception:
            tsv = 0.0
        if tsv <= 0.0:
            return "Updated: N/A"
        delta = max(0, int(time.time() - tsv))
        return f"Updated: {delta}s ago"

    def _market_eta_or_age(self, ts: Any) -> str:
        try:
            tsv = float(ts or 0.0)
        except Exception:
            tsv = 0.0
        if tsv <= 0.0:
            return "N/A"
        diff = int(tsv - time.time())
        if diff >= 0:
            return f"in {diff}s"
        return f"{abs(diff)}s ago"

    def _forex_reason_metric(self, raw_reason: str, key: str) -> Optional[float]:
        txt = str(raw_reason or "")
        if not txt:
            return None
        try:
            m = re.search(rf"{re.escape(key)}\s*([+-]?\d+(?:\.\d+)?)%", txt, flags=re.IGNORECASE)
            if not m:
                return None
            return float(m.group(1))
        except Exception:
            return None

    def _market_reason_parts(self, market_key: str, row: Dict[str, Any]) -> Tuple[str, str]:
        if not isinstance(row, dict):
            return "", ""
        logic = str(row.get("reason_logic", "") or "").strip()
        data = str(row.get("reason_data", "") or "").strip()
        raw = str(row.get("reason", "") or "").strip()

        def _looks_metric_blob(txt: str) -> bool:
            low = str(txt or "").strip().lower()
            if not low:
                return False
            metric_hits = sum(
                1
                for tok in ("6h", "24h", "vol", "spr", "bps", "score", "%", "range", "bars")
                if tok in low
            )
            return (metric_hits >= 3) and ("|" in low or "%" in low)

        def _metric_bps(txt: str, key: str) -> Optional[float]:
            blob = str(txt or "")
            if not blob:
                return None
            try:
                m = re.search(rf"{re.escape(key)}\s*([+-]?\d+(?:\.\d+)?)\s*bps", blob, flags=re.IGNORECASE)
                if not m:
                    return None
                return float(m.group(1))
            except Exception:
                return None

        # If engine provided metric-only blobs in reason_logic/reason, treat that as hover data
        # and synthesize human-readable logic.
        if logic and _looks_metric_blob(logic):
            if (not data) or (data == logic):
                data = logic
            logic = ""
        if logic and (not data) and _looks_metric_blob(logic):
            data = logic
            logic = ""
        if raw and (not data) and _looks_metric_blob(raw):
            data = raw
        if data and logic and (not _looks_metric_blob(logic)):
            return logic, data
        if data and (not logic) and raw and (not _looks_metric_blob(raw)):
            return raw, data
        if logic:
            if raw and raw != logic:
                return logic, raw
            if data and data != logic:
                return logic, data
            return logic, data if data else ""

        side = str(row.get("side", "watch") or "watch").strip().lower()
        try:
            score = float(row.get("score", 0.0) or 0.0)
        except Exception:
            score = 0.0

        c6 = row.get("change_6h_pct")
        c24 = row.get("change_24h_pct")
        vol = row.get("volatility_pct")
        spr = row.get("spread_bps")
        try:
            c6v = float(c6) if c6 is not None else None
        except Exception:
            c6v = None
        try:
            c24v = float(c24) if c24 is not None else None
        except Exception:
            c24v = None
        try:
            volv = float(vol) if vol is not None else None
        except Exception:
            volv = None
        try:
            sprv = float(spr) if spr is not None else None
        except Exception:
            sprv = None

        if c6v is None:
            c6v = self._forex_reason_metric(raw, "6h")
        if c24v is None:
            c24v = self._forex_reason_metric(raw, "24h")
        if volv is None:
            volv = self._forex_reason_metric(raw, "vol")
        if sprv is None:
            sprv = _metric_bps(raw, "spr")

        logic_txt = ""
        if side == "short":
            if (c6v is not None) and (c24v is not None) and (c6v <= 0.0) and (c24v <= 0.0):
                logic_txt = "Downtrend signal: both short-term and daily momentum are negative."
            elif (c6v is not None) and (c6v <= 0.0):
                logic_txt = "Short-biased setup: near-term momentum has turned lower."
            else:
                logic_txt = "Short-biased setup with mixed momentum confirmation."
        elif side == "long":
            if (c6v is not None) and (c24v is not None) and (c6v >= 0.0) and (c24v >= 0.0):
                logic_txt = "Uptrend signal: both short-term and daily momentum are positive."
            elif (c6v is not None) and (c6v >= 0.0):
                logic_txt = "Long-biased setup: near-term momentum has turned higher."
            else:
                logic_txt = "Long-biased setup with mixed momentum confirmation."
        else:
            if (c6v is not None) and (c24v is not None):
                if c6v >= 0.0 and c24v >= 0.0:
                    logic_txt = "Watchlist long-bias: momentum is positive but entry confidence is not strong enough yet."
                elif c6v <= 0.0 and c24v <= 0.0:
                    logic_txt = "Watchlist short-bias: momentum is negative but entry confidence is not strong enough yet."
                else:
                    logic_txt = "Watch bias: mixed momentum across timeframes."
            else:
                logic_txt = "Watch bias: directional edge is limited versus peers."

        if abs(float(score)) < 0.06:
            logic_txt = "Weak directional edge; ranked as relative leader among current symbols."

        extra_bits: List[str] = []
        low_vol_floor = 0.05 if market_key == "forex" else 0.25
        if (volv is not None) and (volv < low_vol_floor):
            extra_bits.append("low-volatility regime")
        spread_cap_key = "forex_max_spread_bps" if market_key == "forex" else "stock_max_spread_bps"
        try:
            spread_cap = float(self.settings.get(spread_cap_key, 0.0) or 0.0)
        except Exception:
            spread_cap = 0.0
        if (sprv is not None) and spread_cap > 0.0 and sprv > spread_cap:
            extra_bits.append(f"spread above gate ({sprv:.2f}bps > {spread_cap:.2f}bps)")
        if bool(row.get("event_risk_active", False)):
            extra_bits.append("macro event risk nearby")
        if row.get("mtf_confirmed", True) is False:
            extra_bits.append("multi-timeframe mismatch")
        if row.get("data_quality_ok", True) is False:
            extra_bits.append("data quality gate soft-failed")
        if bool(row.get("eligible_for_entry", True)) is False:
            extra_bits.append("watch-only after execution filters")
        if extra_bits:
            logic_txt = f"{logic_txt} ({', '.join(extra_bits)})"

        data_txt = str(data or "").strip()
        if (not data_txt) and raw and _looks_metric_blob(raw):
            data_txt = raw
        if data_txt and (data_txt != logic_txt):
            return logic_txt, data_txt
        return logic_txt, ""

    def _format_market_state_line(self, raw_line: str, max_lines: int = 9) -> str:
        text = str(raw_line or "").strip()
        if not text:
            return "Trade State: N/A"
        chunks = [str(x).strip() for x in re.split(r"[|•]+", text) if str(x).strip()]
        if not chunks:
            chunks = [text]
        lines: List[str] = []
        for idx, chunk in enumerate(chunks):
            prefix = "" if idx == 0 else "• "
            wrapped = textwrap.wrap(chunk, width=(78 - len(prefix)))
            if wrapped:
                for w_idx, seg in enumerate(wrapped):
                    if w_idx == 0:
                        lines.append(f"{prefix}{seg}")
                    else:
                        lines.append(f"  {seg}")
            else:
                lines.append(f"{prefix}{chunk}")
        if len(lines) > max(2, int(max_lines)):
            extra = len(lines) - int(max_lines)
            lines = lines[: int(max_lines)]
            lines[-1] = f"{lines[-1]} (+{extra} more)"
        return "\n".join(lines)

    def _clear_market_chart_hover(self, market_key: str) -> None:
        panel = self.market_panels.get(market_key, {})
        canvas = panel.get("chart_canvas")
        if not canvas:
            return
        try:
            canvas.delete("hover_layer")
            panel["chart_hover_idx"] = -1
        except Exception:
            pass

    def _on_market_chart_hover(self, market_key: str, event: tk.Event) -> None:
        panel = self.market_panels.get(market_key, {})
        canvas = panel.get("chart_canvas")
        hover_data = panel.get("chart_hover_data", {})
        if (not canvas) or (not isinstance(hover_data, dict)) or (not hover_data):
            self._clear_market_chart_hover(market_key)
            return
        try:
            x = float(getattr(event, "x", -1))
            y = float(getattr(event, "y", -1))
            plot_left = float(hover_data.get("plot_left", 0.0) or 0.0)
            plot_right = float(hover_data.get("plot_right", 0.0) or 0.0)
            plot_top = float(hover_data.get("plot_top", 0.0) or 0.0)
            plot_bot = float(hover_data.get("plot_bot", 0.0) or 0.0)
            x_points = list(hover_data.get("x_points", []) or [])
            rows = list(hover_data.get("rows", []) or [])
            if (not x_points) or (not rows):
                self._clear_market_chart_hover(market_key)
                return
            if x < plot_left or x > plot_right or y < plot_top or y > plot_bot:
                self._clear_market_chart_hover(market_key)
                return
            idx = min(range(len(x_points)), key=lambda i: abs(float(x_points[i]) - x))
            if idx < 0 or idx >= len(rows):
                self._clear_market_chart_hover(market_key)
                return
            row = rows[idx] if isinstance(rows[idx], dict) else {}
            hx = float(x_points[idx])
            o = float(row.get("o", 0.0) or 0.0)
            h = float(row.get("h", 0.0) or 0.0)
            l = float(row.get("l", 0.0) or 0.0)
            c = float(row.get("c", 0.0) or 0.0)
            t = str(row.get("t", "") or "")
            delta_pct = (((c - o) / o) * 100.0) if o > 0 else 0.0
            tip = (
                f"{t or f'bar {idx + 1}'}\n"
                f"O {o:.5f}  H {h:.5f}\n"
                f"L {l:.5f}  C {c:.5f}\n"
                f"Delta {delta_pct:+.2f}%"
            )
            tip_w = 194
            tip_h = 70
            tx = min(max(plot_left + 8.0, x + 14.0), max(plot_left + 8.0, plot_right - tip_w - 8.0))
            ty = max(plot_top + 8.0, min(y + 12.0, max(plot_top + 8.0, plot_bot - tip_h - 8.0)))
            panel["chart_hover_idx"] = int(idx)
            canvas.delete("hover_layer")
            canvas.create_line(hx, plot_top, hx, plot_bot, fill=DARK_ACCENT2, dash=(4, 3), tags=("hover_layer",))
            canvas.create_rectangle(
                tx,
                ty,
                tx + tip_w,
                ty + tip_h,
                fill=DARK_BG2,
                outline=DARK_ACCENT2,
                width=1,
                tags=("hover_layer",),
            )
            canvas.create_text(
                tx + 8,
                ty + 6,
                anchor="nw",
                text=tip,
                fill=DARK_FG,
                font=(self._live_log_font.cget("family"), max(8, int(self._live_log_font.cget("size")))),
                tags=("hover_layer",),
            )
        except Exception:
            self._clear_market_chart_hover(market_key)

    def _hide_market_table_tooltip(self, market_key: str) -> None:
        panel = self.market_panels.get(market_key, {})
        tw = panel.get("chart_table_tooltip_win")
        table = panel.get("chart_table")
        if tw:
            try:
                tw.destroy()
            except Exception:
                pass
        panel["chart_table_tooltip_win"] = None
        panel["chart_table_tooltip_label"] = None
        panel["chart_table_tooltip_key"] = None
        try:
            if table is not None:
                table.configure(cursor="")
        except Exception:
            pass

    def _on_market_table_hover(self, market_key: str, event: tk.Event) -> None:
        panel = self.market_panels.get(market_key, {})
        table = panel.get("chart_table")
        if not table:
            return
        tooltips = panel.get("chart_table_tooltips", {})
        if not isinstance(tooltips, dict) or not tooltips:
            self._hide_market_table_tooltip(market_key)
            return
        try:
            row_id = str(table.identify_row(int(getattr(event, "y", -1)))).strip()
            col_id = str(table.identify_column(int(getattr(event, "x", -1)))).strip()
        except Exception:
            row_id = ""
            col_id = ""
        if (not row_id) or (not col_id):
            self._hide_market_table_tooltip(market_key)
            return
        tip = str(tooltips.get((row_id, col_id), "") or "").strip()
        if not tip:
            self._hide_market_table_tooltip(market_key)
            return
        tip_key = f"{row_id}:{col_id}:{tip}"
        tw = panel.get("chart_table_tooltip_win")
        lbl = panel.get("chart_table_tooltip_label")
        try:
            if (not tw) or (not bool(tw.winfo_exists())) or (not lbl):
                tw = tk.Toplevel(self)
                tw.wm_overrideredirect(True)
                try:
                    tw.attributes("-topmost", True)
                except Exception:
                    pass
                lbl = tk.Label(
                    tw,
                    text=tip,
                    justify="left",
                    background=DARK_BG2,
                    foreground=DARK_FG,
                    borderwidth=1,
                    relief="solid",
                    padx=8,
                    pady=5,
                    wraplength=520,
                )
                lbl.pack(fill="both", expand=True)
                panel["chart_table_tooltip_win"] = tw
                panel["chart_table_tooltip_label"] = lbl
            else:
                if panel.get("chart_table_tooltip_key") != tip_key:
                    lbl.configure(text=tip)
            panel["chart_table_tooltip_key"] = tip_key
            tw.update_idletasks()
            tip_w = max(120, int(tw.winfo_reqwidth() or 0))
            tip_h = max(30, int(tw.winfo_reqheight() or 0))
            px = int(getattr(event, "x_root", 0) or 0)
            py = int(getattr(event, "y_root", 0) or 0)
            sx = int(self.winfo_screenwidth() or 1600)
            sy = int(self.winfo_screenheight() or 900)
            tx = max(8, min(sx - tip_w - 8, px + 14))
            ty = max(8, min(sy - tip_h - 8, py + 12))
            tw.wm_geometry(f"+{tx}+{ty}")
            try:
                table.configure(cursor="hand2")
            except Exception:
                pass
        except Exception:
            self._hide_market_table_tooltip(market_key)
            try:
                table.configure(cursor="")
            except Exception:
                pass

    def _market_sort_value(self, value: Any) -> Tuple[int, Any]:
        raw = str(value if value is not None else "").strip()
        if not raw:
            return (3, "")
        upper = raw.upper()
        if upper in {"Y", "YES", "TRUE", "PASS", "OK"}:
            return (0, 1.0)
        if upper in {"N", "NO", "FALSE", "FAIL", "BLOCK"}:
            return (0, 0.0)
        cleaned = raw.replace(",", "").replace("$", "").replace("%", "").replace("x", "")
        try:
            return (1, float(cleaned))
        except Exception:
            pass
        return (2, raw.lower())

    def _sort_market_tree_table(self, market_key: str, col: str, toggle: bool = True) -> None:
        panel = self.market_panels.get(market_key, {})
        tree = panel.get("chart_table")
        if tree is None:
            return
        columns = list(tree["columns"]) if "columns" in tree.keys() else []
        if col not in columns:
            return
        cur_col = str(panel.get("chart_table_sort_col", "") or "")
        cur_rev = bool(panel.get("chart_table_sort_reverse", False))
        if toggle:
            reverse = (not cur_rev) if (cur_col == col) else False
        else:
            reverse = (cur_rev if (cur_col == col) else False)
        rows: List[Tuple[Tuple[int, Any], str]] = []
        for iid in tree.get_children(""):
            try:
                val = tree.set(iid, col)
            except Exception:
                val = ""
            rows.append((self._market_sort_value(val), str(iid)))
        rows.sort(key=lambda item: item[0], reverse=bool(reverse))
        for idx, (_key, iid) in enumerate(rows):
            try:
                tree.move(iid, "", idx)
            except Exception:
                pass
            try:
                tags = list(tree.item(iid, "tags") or [])
                tags = [t for t in tags if t not in {"row_even", "row_odd", "row_top"}]
                tags.append("row_even" if (idx % 2) == 0 else "row_odd")
                if (idx == 0) and ("row_muted" not in tags):
                    tags.append("row_top")
                tree.item(iid, tags=tuple(tags))
            except Exception:
                pass
        panel["chart_table_sort_col"] = col
        panel["chart_table_sort_reverse"] = bool(reverse)
        base_headings = panel.get("chart_table_headings", {}) if isinstance(panel.get("chart_table_headings", {}), dict) else {}
        arrow_up = " ^"
        arrow_dn = " v"
        for c in columns:
            base_label = str(base_headings.get(c, c.title()) or c.title())
            label = base_label
            if c == col:
                label = f"{base_label}{arrow_dn if reverse else arrow_up}"
            try:
                tree.heading(c, text=label, command=lambda c_name=c: self._sort_market_tree_table(market_key, c_name, toggle=True))
            except Exception:
                pass

    def _request_market_chart_hydrate(self, market_key: str) -> None:
        self._append_market_log(market_key, "[THINKER] Manual chart hydrate requested.")
        self._run_market_thinker_scan(market_key, force=True, min_interval_s=0.0)

    def _schedule_market_chart_redraw(self, market_key: str, delay_ms: int = 120) -> None:
        mk = str(market_key or "").strip().lower()
        if mk not in self.market_panels:
            return
        prev_id = str(self._market_chart_redraw_after.get(mk, "") or "").strip()
        if prev_id:
            try:
                self.after_cancel(prev_id)
            except Exception:
                pass

        def _run() -> None:
            self._market_chart_redraw_after[mk] = ""
            self._refresh_parallel_market_panels()

        try:
            aft = self.after(max(40, int(delay_ms or 120)), _run)
            self._market_chart_redraw_after[mk] = str(aft)
        except Exception:
            pass

    def _switch_market_view(self, market_key: str, view_name: str) -> None:
        panel = self.market_panels.get(market_key, {})
        mv = panel.get("market_view_var")
        if isinstance(mv, tk.StringVar):
            try:
                mv.set(str(view_name or "Overview"))
            except Exception:
                pass
        self._refresh_parallel_market_panels()

    def _set_market_positions(self, market_key: str, lines: List[str], raw_positions: Optional[List[Dict[str, Any]]] = None) -> None:
        panel = self.market_panels.get(market_key, {})
        tree = panel.get("positions_tree")
        summary_var = panel.get("positions_summary_var")
        if not tree:
            return
        rows = list(raw_positions or [])
        if not rows:
            rows = []
        try:
            for iid in tree.get_children():
                tree.delete(iid)

            inserted = 0
            if market_key == "stocks":
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    sym = str(row.get("symbol", "") or "").strip().upper()
                    if not sym:
                        continue
                    try:
                        qty_f = float(row.get("qty", 0.0) or 0.0)
                    except Exception:
                        qty_f = 0.0
                    try:
                        value_f = float(row.get("market_value", 0.0) or 0.0)
                    except Exception:
                        value_f = 0.0
                    try:
                        upl_f = float(row.get("unrealized_pl", 0.0) or 0.0)
                    except Exception:
                        upl_f = 0.0
                    tag = "upl_pos" if upl_f > 0 else ("upl_neg" if upl_f < 0 else "upl_neu")
                    tree.insert(
                        "",
                        "end",
                        values=(
                            sym,
                            self._market_fmt_num(qty_f, 6),
                            self._market_fmt_money(value_f, 2),
                            self._market_fmt_signed_money(upl_f, 4),
                        ),
                        tags=(tag,),
                    )
                    inserted += 1
            else:
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    inst = str(row.get("instrument", "") or "").strip().upper()
                    if not inst:
                        continue
                    long_leg = row.get("long", {}) or {}
                    short_leg = row.get("short", {}) or {}
                    long_units = str(long_leg.get("units", "0") or "0")
                    short_units = str(short_leg.get("units", "0") or "0")
                    if long_units not in ("", "0", "0.0"):
                        side = "LONG"
                        try:
                            units_f = float(long_units or 0.0)
                        except Exception:
                            units_f = 0.0
                        try:
                            upl_f = float(long_leg.get("unrealizedPL", 0.0) or 0.0)
                        except Exception:
                            upl_f = 0.0
                    elif short_units not in ("", "0", "0.0"):
                        side = "SHORT"
                        try:
                            units_f = float(short_units or 0.0)
                        except Exception:
                            units_f = 0.0
                        try:
                            upl_f = float(short_leg.get("unrealizedPL", 0.0) or 0.0)
                        except Exception:
                            upl_f = 0.0
                    else:
                        side = "FLAT"
                        units_f = 0.0
                        upl_f = 0.0
                    tag = "upl_pos" if upl_f > 0 else ("upl_neg" if upl_f < 0 else "upl_neu")
                    tree.insert(
                        "",
                        "end",
                        values=(inst, side, self._market_fmt_num(units_f, 0), self._market_fmt_signed_money(upl_f, 4)),
                        tags=(tag,),
                    )
                    inserted += 1

            if inserted == 0:
                tree.insert("", "end", values=("No open positions", "-", "-", "-"), tags=("placeholder",))
                if isinstance(summary_var, tk.StringVar):
                    summary_var.set("No open positions.")
            else:
                if isinstance(summary_var, tk.StringVar):
                    summary_var.set(f"Open positions: {inserted}")

            if (inserted == 0) and lines:
                if isinstance(summary_var, tk.StringVar):
                    summary_var.set(str(lines[0]).strip() or "No open positions.")
        except Exception:
            pass

    def _set_market_history(self, market_key: str, lines: List[str]) -> None:
        panel = self.market_panels.get(market_key, {})
        widget = panel.get("history_text")
        if not widget:
            return
        payload = list(lines or [])
        if not payload:
            payload = ["No trader actions yet."]
        try:
            widget.configure(state="normal")
            widget.delete("1.0", "end")
            widget.insert("1.0", "\n".join(str(x) for x in payload[-80:]) + "\n")
            widget.configure(state="disabled")
            try:
                do_scroll = bool(panel.get("history_autoscroll_var").get()) if panel.get("history_autoscroll_var") else True
            except Exception:
                do_scroll = True
            if do_scroll:
                widget.see("end")
        except Exception:
            pass

    def _selected_market_focus_symbol(self, market_key: str, thinker_data: Optional[Dict[str, Any]] = None) -> str:
        panel = self.market_panels.get(market_key, {})
        focus_var = panel.get("instrument_var")
        selected = str((focus_var.get() if focus_var else "AUTO") or "AUTO").strip().upper()
        if selected and selected not in {"AUTO", "TOP", "TOP_PICK"}:
            return selected
        data = thinker_data if isinstance(thinker_data, dict) else self._read_market_thinker_status(market_key)
        if isinstance(data, dict):
            top = data.get("top_pick") or {}
            if isinstance(top, dict):
                ident = str(top.get("pair") or top.get("symbol") or "").strip().upper()
                if ident:
                    return ident
            leaders = list(data.get("leaders", []) or [])
            if leaders and isinstance(leaders[0], dict):
                ident = str(leaders[0].get("pair") or leaders[0].get("symbol") or "").strip().upper()
                if ident:
                    return ident
        return ""

    def _reset_market_chart_focus(self, market_key: str) -> None:
        panel = self.market_panels.get(market_key, {})
        focus_var = panel.get("instrument_var")
        if focus_var is not None:
            try:
                focus_var.set("AUTO")
            except Exception:
                pass
        try:
            self._refresh_parallel_market_panels()
        except Exception:
            pass

    def _open_market_tradingview(self, market_key: str) -> None:
        try:
            import webbrowser

            sym = self._selected_market_focus_symbol(market_key)
            if not sym:
                return
            if market_key == "forex":
                tv_sym = f"OANDA:{sym.replace('_', '')}"
            else:
                tv_sym = sym
            webbrowser.open(f"https://www.tradingview.com/chart/?symbol={tv_sym}")
        except Exception:
            pass

    def _make_alpaca_client(self) -> AlpacaBrokerClient:
        key_id, secret = get_alpaca_creds(self.settings, base_dir=self.project_dir)
        return AlpacaBrokerClient(
            api_key_id=key_id,
            secret_key=secret,
            base_url=str(self.settings.get("alpaca_base_url", DEFAULT_SETTINGS.get("alpaca_base_url", "")) or ""),
            data_url=str(self.settings.get("alpaca_data_url", DEFAULT_SETTINGS.get("alpaca_data_url", "")) or ""),
        )

    def _make_oanda_client(self) -> OandaBrokerClient:
        account_id, token = get_oanda_creds(self.settings, base_dir=self.project_dir)
        return OandaBrokerClient(
            account_id=account_id,
            api_token=token,
            rest_url=str(self.settings.get("oanda_rest_url", DEFAULT_SETTINGS.get("oanda_rest_url", "")) or ""),
        )

    def _read_market_thinker_status(self, market_key: str) -> Dict[str, Any]:
        path = self.market_thinker_paths.get(market_key, "")
        data = _safe_read_json(path) if path else None
        return data if isinstance(data, dict) else {}

    def _write_market_thinker_status(self, market_key: str, payload: Dict[str, Any]) -> None:
        path = self.market_thinker_paths.get(market_key, "")
        if not path:
            return
        try:
            _safe_write_json(path, payload)
        except Exception:
            pass

    def _render_market_canvas(
        self,
        market_key: str,
        thinker_data: Dict[str, Any],
        status_data: Optional[Dict[str, Any]] = None,
        diag_data: Optional[Dict[str, Any]] = None,
    ) -> None:
        panel = self.market_panels.get(market_key, {})
        canvas = panel.get("chart_canvas")
        table_wrap = panel.get("chart_table_wrap")
        table = panel.get("chart_table")
        if not canvas:
            return
        try:
            width = max(320, int(canvas.winfo_width() or 0))
            height = max(220, int(canvas.winfo_height() or 0))
        except Exception:
            width = 720
            height = 320
        try:
            canvas.delete("all")
            canvas.create_rectangle(0, 0, width, height, fill=DARK_PANEL2, outline=DARK_BORDER)
            panel["chart_hover_data"] = {}
            panel["chart_hover_idx"] = -1
            panel["chart_table_tooltips"] = {}
            self._hide_market_table_tooltip(market_key)
            try:
                canvas.configure(cursor="")
            except Exception:
                pass
        except Exception:
            return

        view_var = panel.get("market_view_var")
        view = str((view_var.get() if view_var else "Overview") or "Overview").strip() or "Overview"
        leaders = list(thinker_data.get("leaders", []) or [])
        top_pick = thinker_data.get("top_pick") or (leaders[0] if leaders else None)
        if not isinstance(status_data, dict):
            status_data = {}
        if not isinstance(diag_data, dict):
            diag_data = {}
        updated_at = thinker_data.get("updated_at")
        updated_txt = ""
        try:
            if updated_at:
                updated_txt = time.strftime("%H:%M:%S", time.localtime(float(updated_at)))
        except Exception:
            updated_txt = ""

        def _table_mode(
            columns: Tuple[str, ...],
            rows: List[Tuple[Any, ...]],
            headings: Optional[Dict[str, str]] = None,
            empty_message: str = "",
            tooltips: Optional[List[str]] = None,
            tooltip_col: str = "note",
        ) -> None:
            if not table or not table_wrap:
                return
            try:
                canvas.grid_remove()
                table_wrap.grid(row=0, column=0, sticky="nsew")
            except Exception:
                pass
            try:
                tooltip_map: Dict[Tuple[str, str], str] = {}
                table["columns"] = columns
                for iid in table.get_children():
                    table.delete(iid)
                headings_map = {col: (headings or {}).get(col, col.title()) for col in columns}
                panel["chart_table_headings"] = dict(headings_map)
                for col in columns:
                    title = headings_map.get(col, col.title())
                    table.heading(
                        col,
                        text=title,
                        command=lambda c_name=col: self._sort_market_tree_table(market_key, c_name, toggle=True),
                    )
                    anchor = "w" if col in ("symbol", "pair", "side", "conf", "note") else "e"
                    base_w = 120
                    if col in ("rank",):
                        base_w = 56
                    elif col in ("score",):
                        base_w = 110
                    elif col in ("note",):
                        base_w = 280
                    table.column(col, anchor=anchor, width=base_w, stretch=True)
                try:
                    table.tag_configure("row_long", foreground=DARK_ACCENT)
                    table.tag_configure("row_short", foreground="#FF6B57")
                    table.tag_configure("row_muted", foreground=DARK_MUTED)
                    table.tag_configure("row_even", background=DARK_PANEL)
                    table.tag_configure("row_odd", background="#0C1827")
                    table.tag_configure("row_top", background="#10253A")
                    table.tag_configure("row_conf_high", foreground=DARK_ACCENT2)
                    table.tag_configure("row_conf_med", foreground="#FFD27A")
                    table.tag_configure("row_conf_low", foreground=DARK_MUTED)
                except Exception:
                    pass
                if not rows:
                    lead = "No data yet"
                    detail = str(empty_message or "Waiting for next scanner/trader update.").strip()
                    if len(columns) >= 2:
                        rows = [(lead, detail) + tuple("" for _ in range(max(0, len(columns) - 2)))]
                    else:
                        rows = [(lead,)]
                note_col_id = ""
                if tooltip_col in columns:
                    note_col_id = f"#{int(columns.index(tooltip_col)) + 1}"
                for idx, row in enumerate(rows):
                    tag = ""
                    tags: List[str] = []
                    up_vals = [str(x or "").strip().upper() for x in row]
                    joined = " | ".join(up_vals)
                    if "SHORT" in joined or "REJECT" in joined:
                        tag = "row_short"
                    elif "LONG" in joined:
                        tag = "row_long"
                    elif "NO DATA YET" in joined:
                        tag = "row_muted"
                    if tag:
                        tags.append(tag)
                    conf_txt = ""
                    if "conf" in columns:
                        try:
                            c_idx = int(columns.index("conf"))
                            if c_idx < len(row):
                                conf_txt = str(row[c_idx] or "").strip().upper()
                        except Exception:
                            conf_txt = ""
                    if conf_txt.startswith("HIGH"):
                        tags.append("row_conf_high")
                    elif conf_txt.startswith("MED"):
                        tags.append("row_conf_med")
                    elif conf_txt.startswith("LOW"):
                        tags.append("row_conf_low")
                    tags.append("row_even" if (idx % 2) == 0 else "row_odd")
                    if (idx == 0) and ("NO DATA YET" not in joined):
                        tags.append("row_top")
                    iid = table.insert("", "end", values=row, tags=tuple(tags))
                    if note_col_id and isinstance(tooltips, list):
                        if 0 <= idx < len(tooltips):
                            tip = str(tooltips[idx] or "").strip()
                            if tip:
                                tooltip_map[(str(iid), note_col_id)] = tip
                panel["chart_table_tooltips"] = tooltip_map
                panel["chart_table_note_col_id"] = note_col_id
                sort_col = str(panel.get("chart_table_sort_col", "") or "")
                if sort_col and (sort_col in columns):
                    self._sort_market_tree_table(market_key, sort_col, toggle=False)
                if not tooltip_map:
                    self._hide_market_table_tooltip(market_key)
            except Exception:
                pass

        def _canvas_mode() -> None:
            try:
                if table_wrap:
                    table_wrap.grid_remove()
            except Exception:
                pass
            self._hide_market_table_tooltip(market_key)
            try:
                canvas.grid(row=0, column=0, sticky="nsew")
            except Exception:
                pass

        if view == "Scanner":
            all_scores = list(thinker_data.get("all_scores", leaders) or [])
            show_rejected_key = "stock_show_rejected_rows" if market_key == "stocks" else "forex_show_rejected_rows"
            show_rejected = bool(self.settings.get(show_rejected_key, False))
            rows: List[Tuple[Any, ...]] = []
            tooltip_rows: List[str] = []
            filtered_scores: List[Dict[str, Any]] = []
            for row in all_scores:
                try:
                    if float(row.get("score", -9999.0) or -9999.0) <= -9999.0:
                        continue
                except Exception:
                    pass
                filtered_scores.append(row)
            display_scores = all_scores if show_rejected else filtered_scores
            for idx, row in enumerate(display_scores[:40], start=1):
                ident = str(row.get("pair") or row.get("symbol") or "N/A")
                side = str(row.get("side", "watch") or "watch").upper()
                try:
                    score_txt = f"{float(row.get('score', 0.0)):+.6f}"
                except Exception:
                    score_txt = str(row.get("score", "N/A"))
                conf = str(row.get("confidence", "N/A") or "N/A")
                note_logic, note_data = self._market_reason_parts(market_key, row if isinstance(row, dict) else {})
                note = note_logic if note_logic else str(row.get("reason", "") or "").strip()
                bars_count = str(row.get("bars_count", "") or "")
                src = str(row.get("data_source", "") or "")
                eligible = "Y" if bool(row.get("eligible_for_entry", False)) else "N"
                rows.append((idx, ident, side, score_txt, conf, bars_count, src, eligible, note))
                tooltip_rows.append(note_data if note_data else "")
            if show_rejected:
                rejected_rows = list(thinker_data.get("rejected", []) or [])
                base = len(rows)
                for j, rej in enumerate(rejected_rows[:30], start=1):
                    ident = str(rej.get("pair") or rej.get("symbol") or "N/A")
                    reason = str(rej.get("reason", "rejected") or "rejected")
                    bars_count = str(rej.get("bars_count", "") or "")
                    src = str(rej.get("source", "") or "")
                    note = reason
                    rows.append((base + j, ident, "REJECT", "-", "-", bars_count, src, "N", note))
                    rej_parts: List[str] = []
                    for key in ("spread_bps", "volatility_pct", "valid_ratio", "stale_hours", "cooldown_until"):
                        if key in rej:
                            rej_parts.append(f"{key}={rej.get(key)}")
                    if bars_count:
                        rej_parts.append(f"bars_count={bars_count}")
                    if src:
                        rej_parts.append(f"source={src}")
                    tooltip_rows.append(" | ".join(rej_parts))
            elif (not rows) and thinker_data.get("rejected"):
                rows.append(
                    (
                        1,
                        "No eligible rows",
                        "INFO",
                        "-",
                        "-",
                        "",
                        "",
                        "N",
                        "All candidates rejected by gates. Enable 'show rejected rows' in Settings for details.",
                    )
                )
                tooltip_rows.append("")
            _table_mode(
                ("rank", "symbol", "side", "score", "conf", "bars", "src", "eligible", "note"),
                rows,
                headings={
                    "rank": "#",
                    "symbol": "Symbol",
                    "side": "Side",
                    "score": "Score",
                    "conf": "Conf",
                    "bars": "Bars",
                    "src": "Source",
                    "eligible": "Exec",
                    "note": "Reason",
                },
                empty_message="No scan rows yet. Run scan or loosen filters in Settings.",
                tooltips=tooltip_rows,
                tooltip_col="note",
            )
            return

        if view == "Leaders":
            rows = []
            tooltip_rows = []
            for idx, row in enumerate(leaders[:20], start=1):
                ident = str(row.get("pair") or row.get("symbol") or "N/A")
                side = str(row.get("side", "watch") or "watch").upper()
                try:
                    score_txt = f"{float(row.get('score', 0.0)):+.6f}"
                except Exception:
                    score_txt = str(row.get("score", "N/A"))
                conf = str(row.get("confidence", "N/A") or "N/A")
                note_logic, note_data = self._market_reason_parts(market_key, row if isinstance(row, dict) else {})
                note = note_logic if note_logic else str(row.get("reason", "") or "").strip()
                rows.append((idx, ident, side, score_txt, conf, note))
                tooltip_rows.append(note_data if note_data else "")
            _table_mode(
                ("rank", "symbol", "side", "score", "conf", "note"),
                rows,
                headings={"rank": "#", "symbol": ("Pair" if market_key == "forex" else "Symbol"), "side": "Side", "score": "Score", "conf": "Conf", "note": "Reason"},
                empty_message="No leaders ranked yet. Wait for next scan cycle.",
                tooltips=tooltip_rows,
                tooltip_col="note",
            )
            return

        if view == "Positions":
            raw_positions = list(status_data.get("raw_positions", []) or [])
            rows = []
            if market_key == "stocks":
                for row in raw_positions:
                    if not isinstance(row, dict):
                        continue
                    sym = str(row.get("symbol", "") or "").strip().upper()
                    if not sym:
                        continue
                    qty = self._market_fmt_num(row.get("qty", 0.0), 6)
                    val = self._market_fmt_money(row.get("market_value", 0.0), 2)
                    upl = self._market_fmt_signed_money(row.get("unrealized_pl", 0.0), 4)
                    rows.append((sym, qty, val, upl))
                _table_mode(
                    ("symbol", "qty", "value", "upl"),
                    rows,
                    headings={"symbol": "Symbol", "qty": "Qty", "value": "Value", "upl": "uPnL"},
                    empty_message="No open positions from broker.",
                )
            else:
                for row in raw_positions:
                    if not isinstance(row, dict):
                        continue
                    inst = str(row.get("instrument", "") or "").strip().upper()
                    if not inst:
                        continue
                    long_leg = row.get("long", {}) or {}
                    short_leg = row.get("short", {}) or {}
                    long_units = str(long_leg.get("units", "0") or "0")
                    short_units = str(short_leg.get("units", "0") or "0")
                    if long_units not in ("", "0", "0.0"):
                        side = "LONG"
                        units = self._market_fmt_num(long_units, 0)
                        upl = self._market_fmt_signed_money(long_leg.get("unrealizedPL", 0.0), 4)
                    elif short_units not in ("", "0", "0.0"):
                        side = "SHORT"
                        units = self._market_fmt_num(short_units, 0)
                        upl = self._market_fmt_signed_money(short_leg.get("unrealizedPL", 0.0), 4)
                    else:
                        side = "FLAT"
                        units = "0"
                        upl = "0.0000"
                    rows.append((inst, side, units, upl))
                _table_mode(
                    ("pair", "side", "units", "upl"),
                    rows,
                    headings={"pair": "Pair", "side": "Side", "units": "Units", "upl": "uPnL"},
                    empty_message="No open positions from broker.",
                )
            return

        if view == "Execution":
            audit_path = os.path.join(self.market_state_dirs.get(market_key, self.hub_dir), "execution_audit.jsonl")
            rows: List[Tuple[Any, ...]] = []
            try:
                now_ts = float(time.time())
                with open(audit_path, "r", encoding="utf-8") as f:
                    data = [ln.strip() for ln in f if ln.strip()]
                for ln in data[-80:]:
                    try:
                        row = json.loads(ln)
                    except Exception:
                        continue
                    when = ""
                    try:
                        row_ts = float(row.get("ts", 0) or 0)
                        if row_ts > 0 and abs(now_ts - row_ts) >= 86400.0:
                            when = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(row_ts))
                        else:
                            when = time.strftime("%H:%M:%S", time.localtime(row_ts))
                    except Exception:
                        when = ""
                    ident = str(row.get("symbol", "") or row.get("instrument", "") or "").strip().upper()
                    evt = str(row.get("event", "") or "").strip()
                    side = str(row.get("side", "") or "").strip().upper()
                    ok = str(row.get("ok", "")).strip()
                    msg = str(row.get("msg", "") or "").strip()
                    if "max open positions reached" in msg.lower():
                        cap_m = re.search(r"\((\d+)\s*/\s*(\d+)\)", msg)
                        if cap_m:
                            try:
                                shown_cap = int(cap_m.group(2))
                            except Exception:
                                shown_cap = 0
                            cfg_key = "stock_max_open_positions" if market_key == "stocks" else "forex_max_open_positions"
                            try:
                                cfg_cap = max(1, int(float(self.settings.get(cfg_key, 1) or 1)))
                            except Exception:
                                cfg_cap = 1
                            if shown_cap > 0 and cfg_cap != shown_cap:
                                msg = f"{msg} | config max now {cfg_cap}"
                    rows.append((when, ident, evt, side, ok, msg[:120]))
            except Exception:
                rows = []
            _table_mode(
                ("time", "symbol", "event", "side", "ok", "msg"),
                rows,
                headings={"time": "When", "symbol": ("Pair" if market_key == "forex" else "Symbol"), "event": "Event", "side": "Side", "ok": "OK", "msg": "Message"},
                empty_message="No execution audit rows yet.",
            )
            return

        if view == "Health":
            health = {}
            try:
                health = _safe_read_json(os.path.join(self.market_state_dirs.get(market_key, self.hub_dir), "health_status.json")) or {}
            except Exception:
                health = {}
            row = (
                "YES" if bool(health.get("data_ok", True)) else "NO",
                "YES" if bool(health.get("broker_ok", True)) else "NO",
                "YES" if bool(health.get("orders_ok", True)) else "NO",
                "YES" if bool(health.get("drift_warning", False)) else "NO",
                self._market_age_text(health.get("ts", 0)),
            )
            _table_mode(
                ("data", "broker", "orders", "drift", "updated"),
                [row],
                headings={"data": "Data OK", "broker": "Broker OK", "orders": "Orders OK", "drift": "Drift Warning", "updated": "Updated"},
            )
            return

        if view == "Overview":
            _canvas_mode()
            title = f"{panel.get('market_name', market_key.title())} Thinker Overview"
            body_lines = []
            focus_symbol = self._selected_market_focus_symbol(market_key, thinker_data)
            focus_row: Dict[str, Any] = {}
            if focus_symbol:
                for row in list(thinker_data.get("all_scores", []) or []):
                    if not isinstance(row, dict):
                        continue
                    ident = str(row.get("pair") or row.get("symbol") or "").strip().upper()
                    if ident == focus_symbol:
                        focus_row = row
                        break
                if (not focus_row) and isinstance(top_pick, dict):
                    top_ident = str(top_pick.get("pair") or top_pick.get("symbol") or "").strip().upper()
                    if top_ident == focus_symbol:
                        focus_row = dict(top_pick)
            diag_reject_summary = diag_data.get("reject_summary", {}) if isinstance(diag_data, dict) else {}
            reject_summary = (
                dict(diag_reject_summary)
                if isinstance(diag_reject_summary, dict) and diag_reject_summary
                else (thinker_data.get("reject_summary", {}) if isinstance(thinker_data, dict) else {})
            )
            hints = list(thinker_data.get("hints", []) or []) if isinstance(thinker_data, dict) else []
            try:
                universe_n = int(len(list(thinker_data.get("universe", []) or [])))
            except Exception:
                universe_n = 0
            body_lines.append(
                f"Universe {universe_n} | leaders {len(leaders)} | "
                f"adaptive threshold {thinker_data.get('adaptive_threshold', 'N/A')}"
            )
            leader_mode = str(thinker_data.get("leader_mode", "") or "").strip().lower()
            if leader_mode == "watch_fallback":
                body_lines.append("Leader mode: watch fallback (no long setups).")
            elif leader_mode and leader_mode not in {"long", "none"}:
                body_lines.append(f"Leader mode: {leader_mode}.")
            if isinstance(diag_data, dict) and diag_data:
                try:
                    cand_total = int(diag_data.get("candidates_total", 0) or 0)
                    score_total = int(diag_data.get("scores_total", 0) or 0)
                    mode = str(diag_data.get("mode", "intraday") or "intraday")
                    body_lines.append(f"Scan mode: {mode} | candidates {cand_total} | scored {score_total}")
                except Exception:
                    pass
            if bool(thinker_data.get("fallback_cached", False)):
                body_lines.append("Status: network degraded, showing cached scanner leaders.")
            if top_pick:
                ident = top_pick.get("pair") or top_pick.get("symbol") or "N/A"
                side = str(top_pick.get("side", "watch") or "watch").upper()
                score = top_pick.get("score", "N/A")
                confidence = str(top_pick.get("confidence", "N/A") or "N/A")
                body_lines.append(f"Top pick: {ident} | {side} | score {score} | {confidence}")
                logic_reason, _raw_reason = self._market_reason_parts(market_key, top_pick if isinstance(top_pick, dict) else {})
                body_lines.append(logic_reason or str(top_pick.get("reason", "") or "").strip())
            else:
                body_lines.append("No ranked candidates yet.")
            if focus_row:
                try:
                    bars_n = int(focus_row.get("bars_count", 0) or 0)
                except Exception:
                    bars_n = 0
                try:
                    valid_ratio = float(focus_row.get("valid_ratio", 0.0) or 0.0)
                except Exception:
                    valid_ratio = 0.0
                try:
                    stale_h = float(focus_row.get("stale_hours", 0.0) or 0.0)
                except Exception:
                    stale_h = 0.0
                source = str(focus_row.get("data_source", "") or "").strip()
                if bars_n > 0 or source:
                    body_lines.append(
                        f"Focus quality: bars={bars_n} valid={valid_ratio:.2f} stale={stale_h:.1f}h"
                        + (f" src={source}" if source else "")
                    )
            leaders_preview = []
            for idx, row in enumerate(leaders[:4], start=1):
                ident = row.get("pair") or row.get("symbol") or "N/A"
                side = str(row.get("side", "watch") or "watch").upper()
                score = row.get("score", "N/A")
                leaders_preview.append(f"{idx}. {ident:<10} {side:<5} score {score}")
            if leaders_preview:
                body_lines.append("")
                body_lines.append("Leaders:")
                body_lines.extend(leaders_preview)
            if isinstance(reject_summary, dict) and reject_summary:
                try:
                    rejected_total = int(reject_summary.get("total_rejected", 0) or 0)
                except Exception:
                    rejected_total = 0
                try:
                    reject_rate_pct = max(0.0, min(100.0, float(reject_summary.get("reject_rate_pct", 0.0) or 0.0)))
                except Exception:
                    reject_rate_pct = 0.0
                dominant_reason = str(reject_summary.get("dominant_reason", "n/a") or "n/a")
                body_lines.append("")
                body_lines.append(
                    "Reject summary: "
                    f"{rejected_total} rejected "
                    f"({reject_rate_pct:.1f}%) "
                    f"| dominant: {dominant_reason}"
                )
            if hints:
                body_lines.append("Hints:")
                for h in hints[:3]:
                    body_lines.append(f"- {str(h)}")
            if updated_txt:
                body_lines.append(f"Last scan: {updated_txt}")
            if focus_symbol:
                top_ident_u = str(top_pick.get("pair") or top_pick.get("symbol") or "").strip().upper() if isinstance(top_pick, dict) else ""
                if focus_symbol != top_ident_u:
                    body_lines.append(f"Chart focus: {focus_symbol} (showing top-pick bars: {top_ident_u or 'N/A'})")
        else:
            _canvas_mode()
            title = "Positions-Aware View"
            body_lines = ["Use the Positions panel below for linked broker positions."]
            if top_pick:
                ident = top_pick.get("pair") or top_pick.get("symbol") or "N/A"
                body_lines.append(f"Current strongest candidate: {ident}")

        try:
            chart_map_raw = thinker_data.get("top_chart_map", {}) if isinstance(thinker_data, dict) else {}
            chart_map = chart_map_raw if isinstance(chart_map_raw, dict) else {}
            top_chart = []
            chart_source = "top"
            top_ident = ""
            if isinstance(top_pick, dict):
                top_ident = str(top_pick.get("pair") or top_pick.get("symbol") or "").strip().upper()
            focus_ident = self._selected_market_focus_symbol(market_key, thinker_data)
            if focus_ident and isinstance(chart_map.get(focus_ident, None), list):
                top_chart = list(chart_map.get(focus_ident, []) or [])
                chart_source = "focus"
            elif top_ident and isinstance(chart_map.get(top_ident, None), list):
                top_chart = list(chart_map.get(top_ident, []) or [])
                chart_source = "top-map"
            if not top_chart:
                top_chart = list(thinker_data.get("top_chart", []) or [])
                chart_source = "top"
            if (not top_chart) and chart_map:
                for v in chart_map.values():
                    if isinstance(v, list) and len(v) >= 2:
                        top_chart = list(v)
                        chart_source = "fallback-map"
                        break
            parsed: List[Dict[str, Any]] = []
            for p in top_chart[-140:]:
                if not isinstance(p, dict):
                    continue
                try:
                    c = float(p.get("c", 0.0) or 0.0)
                except Exception:
                    c = 0.0
                if c <= 0.0:
                    continue
                try:
                    o = float(p.get("o", c) or c)
                except Exception:
                    o = c
                try:
                    h = float(p.get("h", max(o, c)) or max(o, c))
                except Exception:
                    h = max(o, c)
                try:
                    l = float(p.get("l", min(o, c)) or min(o, c))
                except Exception:
                    l = min(o, c)
                if h <= 0.0:
                    h = max(o, c)
                if l <= 0.0:
                    l = min(o, c)
                parsed.append(
                    {
                        "t": str(p.get("t", "") or ""),
                        "o": o,
                        "h": max(h, o, c),
                        "l": min(l, o, c),
                        "c": c,
                    }
                )

            def _fmt_px(v: float) -> str:
                av = abs(float(v))
                if av >= 1000.0:
                    return f"{v:,.2f}"
                if av >= 100.0:
                    return f"{v:,.3f}"
                if av >= 1.0:
                    return f"{v:,.4f}"
                return f"{v:,.6f}"

            def _ema(values: List[float], period: int) -> List[float]:
                if not values:
                    return []
                p = max(1, int(period))
                alpha = 2.0 / (float(p) + 1.0)
                out: List[float] = [float(values[0])]
                for val in values[1:]:
                    out.append((alpha * float(val)) + ((1.0 - alpha) * out[-1]))
                return out

            canvas.create_text(
                18,
                16,
                anchor="nw",
                text=title,
                fill=DARK_ACCENT,
                font=(self._live_log_font.cget("family"), max(10, int(self._live_log_font.cget("size")) + 3), "bold"),
            )

            body = "\n".join(str(x) for x in body_lines)
            text_right = max(230, int(width * 0.36))
            canvas.create_text(
                18,
                48,
                anchor="nw",
                text=body,
                fill=DARK_FG,
                width=max(210, text_right - 24),
                font=(self._live_log_font.cget("family"), max(9, int(self._live_log_font.cget("size")) + 1)),
            )

            if len(parsed) >= 2:
                closes = [float(r["c"]) for r in parsed]
                lows = [float(r["l"]) for r in parsed]
                highs = [float(r["h"]) for r in parsed]
                chart_times = [str(r.get("t", "") or "") for r in parsed]

                plot_left = min(width - 190, text_right + 12)
                plot_right = width - 18
                plot_top = 36
                plot_bot = max(plot_top + 100, height - 28)
                if plot_right > plot_left + 140 and plot_bot > plot_top + 90:
                    vmin = min(lows)
                    vmax = max(highs)
                    first_v = closes[0]
                    last_v = closes[-1]
                    delta_pct = (((last_v - first_v) / first_v) * 100.0) if first_v > 0 else 0.0
                    pad = max((vmax - vmin) * 0.08, max(vmax, 1.0) * 0.002)
                    y_min = max(1e-12, vmin - pad)
                    y_max = vmax + pad
                    yr = max(1e-9, y_max - y_min)

                    canvas.create_rectangle(plot_left, plot_top, plot_right, plot_bot, outline=DARK_BORDER, fill=DARK_PANEL2)

                    def _x_for(i: int, total: int) -> float:
                        if total <= 1:
                            return float(plot_left)
                        return plot_left + (float(i) / float(total - 1)) * (plot_right - plot_left)

                    def _y_for(v: float) -> float:
                        return plot_bot - ((float(v) - y_min) / yr) * (plot_bot - plot_top)

                    # Grid + axis labels
                    for gy in range(5):
                        frac = float(gy) / 4.0
                        y = plot_top + frac * (plot_bot - plot_top)
                        canvas.create_line(plot_left, y, plot_right, y, fill=DARK_BORDER)
                        price = y_max - (frac * yr)
                        canvas.create_text(
                            plot_right - 4,
                            y - 1,
                            anchor="ne",
                            text=_fmt_px(price),
                            fill=DARK_MUTED,
                            font=(self._live_log_font.cget("family"), max(8, int(self._live_log_font.cget("size")))),
                        )
                    for gx in range(6):
                        frac = float(gx) / 5.0
                        x = plot_left + frac * (plot_right - plot_left)
                        canvas.create_line(x, plot_top, x, plot_bot, fill=DARK_BORDER)

                    n = len(parsed)
                    dx = (plot_right - plot_left) / float(max(1, n))
                    candle_w = max(1.0, min(10.0, dx * 0.55))
                    use_candles = n <= 90

                    if use_candles:
                        for i, row in enumerate(parsed):
                            x = _x_for(i, n)
                            yo = _y_for(float(row["o"]))
                            yc = _y_for(float(row["c"]))
                            yh = _y_for(float(row["h"]))
                            yl = _y_for(float(row["l"]))
                            up = float(row["c"]) >= float(row["o"])
                            color = DARK_ACCENT if up else "#FF6B57"
                            canvas.create_line(x, yh, x, yl, fill=color, width=1)
                            y1 = min(yo, yc)
                            y2 = max(yo, yc)
                            if abs(y2 - y1) < 1.0:
                                y2 = y1 + 1.0
                            canvas.create_rectangle(
                                x - candle_w,
                                y1,
                                x + candle_w,
                                y2,
                                outline=color,
                                fill=(color if up else DARK_PANEL),
                            )
                    else:
                        line_pts: List[float] = []
                        for i, v in enumerate(closes):
                            line_pts.extend([_x_for(i, n), _y_for(v)])
                        canvas.create_line(*line_pts, fill=DARK_ACCENT2, width=2, smooth=True)

                    # Fast/slow EMA overlays for trend context.
                    ema_fast = _ema(closes, 9)
                    ema_slow = _ema(closes, 21)
                    fast_pts: List[float] = []
                    slow_pts: List[float] = []
                    for i in range(n):
                        x = _x_for(i, n)
                        fast_pts.extend([x, _y_for(ema_fast[i])])
                        slow_pts.extend([x, _y_for(ema_slow[i])])
                    if len(fast_pts) >= 4:
                        canvas.create_line(*fast_pts, fill="#00E5FF", width=2, smooth=True)
                    if len(slow_pts) >= 4:
                        canvas.create_line(*slow_pts, fill="#FFD166", width=2, smooth=True)

                    # Compact legend for market charts (stocks/forex).
                    legend_w = 186
                    legend_h = 74
                    legend_x1 = max(plot_left + 10, plot_right - legend_w - 10)
                    legend_y1 = plot_top + 10
                    legend_x2 = legend_x1 + legend_w
                    legend_y2 = legend_y1 + legend_h
                    canvas.create_rectangle(
                        legend_x1,
                        legend_y1,
                        legend_x2,
                        legend_y2,
                        fill=DARK_BG2,
                        outline=DARK_BORDER,
                    )
                    lx = legend_x1 + 10
                    ly = legend_y1 + 10
                    canvas.create_line(lx, ly, lx + 18, ly, fill=DARK_ACCENT2, width=2)
                    canvas.create_text(lx + 24, ly, anchor="w", text="Price", fill=DARK_FG, font=(self._live_log_font.cget("family"), max(8, int(self._live_log_font.cget("size")))))
                    ly += 16
                    canvas.create_line(lx, ly, lx + 18, ly, fill="#00E5FF", width=2)
                    canvas.create_text(lx + 24, ly, anchor="w", text="EMA 9", fill=DARK_FG, font=(self._live_log_font.cget("family"), max(8, int(self._live_log_font.cget("size")))))
                    ly += 16
                    canvas.create_line(lx, ly, lx + 18, ly, fill="#FFD166", width=2)
                    canvas.create_text(lx + 24, ly, anchor="w", text="EMA 21", fill=DARK_FG, font=(self._live_log_font.cget("family"), max(8, int(self._live_log_font.cget("size")))))
                    ly += 16
                    canvas.create_line(lx, ly, lx + 18, ly, fill=DARK_ACCENT2, dash=(4, 3))
                    canvas.create_text(
                        lx + 24,
                        ly,
                        anchor="w",
                        text=f"Last {_fmt_px(last_v)} | hover for OHLC",
                        fill=DARK_MUTED,
                        font=(self._live_log_font.cget("family"), max(8, int(self._live_log_font.cget("size")))),
                    )

                    # Last price guide
                    last_y = _y_for(last_v)
                    canvas.create_line(plot_left, last_y, plot_right, last_y, fill=DARK_ACCENT2, dash=(4, 3))
                    canvas.create_text(
                        plot_right - 4,
                        max(plot_top + 10, min(plot_bot - 10, last_y - 2)),
                        anchor="ne",
                        text=f"last {_fmt_px(last_v)}",
                        fill=DARK_ACCENT2,
                        font=(self._live_log_font.cget("family"), max(9, int(self._live_log_font.cget("size")))),
                    )

                    # Bottom x-axis labels
                    if n >= 2:
                        tick_idxs = sorted(set([0, int((n - 1) * 0.33), int((n - 1) * 0.66), n - 1]))
                        for idx in tick_idxs:
                            raw_t = chart_times[idx]
                            lbl = raw_t
                            if raw_t:
                                if "T" in raw_t:
                                    try:
                                        d, t = raw_t.split("T", 1)
                                        lbl = f"{d[5:]} {t[:5]}"
                                    except Exception:
                                        lbl = raw_t[:16]
                                else:
                                    lbl = raw_t[5:16] if len(raw_t) >= 16 else raw_t
                            else:
                                lbl = f"bar {idx + 1}"
                            canvas.create_text(
                                _x_for(idx, n),
                                plot_bot + 2,
                                anchor="n",
                                text=lbl,
                                fill=DARK_MUTED,
                                font=(self._live_log_font.cget("family"), max(8, int(self._live_log_font.cget("size")))),
                            )

                    panel["chart_hover_data"] = {
                        "plot_left": float(plot_left),
                        "plot_right": float(plot_right),
                        "plot_top": float(plot_top),
                        "plot_bot": float(plot_bot),
                        "x_points": [_x_for(i, n) for i in range(n)],
                        "rows": list(parsed),
                    }
                    trend_color = DARK_ACCENT if delta_pct >= 0 else "#FF6B57"
                    focus_display = self._selected_market_focus_symbol(market_key, thinker_data) or "AUTO"
                    canvas.create_text(
                        plot_left,
                        plot_top - 2,
                        anchor="sw",
                        text=(
                            f"{focus_display} | {n} bars | delta {delta_pct:+.2f}% | "
                            f"range {_fmt_px(vmax - vmin)} | src {chart_source}"
                        ),
                        fill=trend_color,
                        font=(self._live_log_font.cget("family"), max(8, int(self._live_log_font.cget("size")))),
                    )
            else:
                panel_left = min(width - 220, max(250, int(width * 0.42)))
                panel_right = width - 20
                panel_top = 42
                panel_bottom = max(panel_top + 120, height - 28)
                canvas.create_rectangle(
                    panel_left,
                    panel_top,
                    panel_right,
                    panel_bottom,
                    outline=DARK_BORDER,
                    fill=DARK_PANEL,
                )
                focus_symbol = self._selected_market_focus_symbol(market_key, thinker_data) or "AUTO"
                rejected_rows = list(thinker_data.get("rejected", []) or [])
                warmup_pending = sum(1 for row in rejected_rows if str((row or {}).get("reason", "") or "").strip().lower() in {"warmup_pending", "insufficient_bars"})
                updated_age_s = 0
                try:
                    updated_age_s = max(0, int(time.time() - float(thinker_data.get("updated_at", 0) or 0)))
                except Exception:
                    updated_age_s = 0
                busy_scan = bool(self._market_thinker_busy.get(market_key, False))
                msg_lower = str(thinker_data.get("msg", "") or "").strip().lower()
                thinker_state = str(thinker_data.get("state", "") or "").strip().lower()
                has_cached_chart = bool(isinstance(chart_map, dict) and chart_map)
                likely_loading = bool(
                    busy_scan
                    or ("starting scan" in msg_lower)
                    or ("using cached scan" in msg_lower and updated_age_s <= 45)
                    or (warmup_pending > 0 and updated_age_s <= 240)
                    or (thinker_state in {"starting", "running", "scanning", "hydrating"})
                    or ((not has_cached_chart) and updated_age_s <= 35)
                )

                if likely_loading:
                    canvas.create_text(
                        panel_left + 14,
                        panel_top + 14,
                        anchor="nw",
                        text=f"{market_key.title()} chart loading",
                        fill=DARK_ACCENT,
                        font=(self._live_log_font.cget("family"), max(10, int(self._live_log_font.cget("size")) + 2), "bold"),
                    )
                    # Lightweight spinner (advances each UI tick).
                    spin_cx = panel_left + 34
                    spin_cy = panel_top + 74
                    spin_r = 16
                    spin_phase = int(time.time() * 5.0) % 360
                    canvas.create_oval(
                        spin_cx - spin_r,
                        spin_cy - spin_r,
                        spin_cx + spin_r,
                        spin_cy + spin_r,
                        outline=DARK_BORDER,
                        width=2,
                    )
                    canvas.create_arc(
                        spin_cx - spin_r,
                        spin_cy - spin_r,
                        spin_cx + spin_r,
                        spin_cy + spin_r,
                        start=spin_phase,
                        extent=110,
                        style="arc",
                        outline=DARK_ACCENT2,
                        width=3,
                    )
                    spinner_char = ["|", "/", "-", "\\"][int(time.time() * 6.0) % 4]
                    canvas.create_text(
                        spin_cx,
                        spin_cy,
                        text=spinner_char,
                        fill=DARK_ACCENT2,
                        font=(self._live_log_font.cget("family"), max(9, int(self._live_log_font.cget("size")) + 1), "bold"),
                    )
                    canvas.create_text(
                        panel_left + 62,
                        panel_top + 58,
                        anchor="nw",
                        width=max(180, (panel_right - panel_left) - 76),
                        text=(
                            f"Hydrating bars for {focus_symbol}.\n"
                            f"Scanner status: {'running' if busy_scan else 'waiting for bars'}."
                            + (f"\nWarmup queue: {warmup_pending} symbols." if warmup_pending > 0 else "")
                            + (f"\nCached chart symbols: {len(chart_map)}." if isinstance(chart_map, dict) and chart_map else "")
                        ),
                        fill=DARK_MUTED,
                        font=(self._live_log_font.cget("family"), max(9, int(self._live_log_font.cget("size")))),
                    )
                else:
                    canvas.create_text(
                        panel_left + 14,
                        panel_top + 14,
                        anchor="nw",
                        text=f"{market_key.title()} chart has no data",
                        fill="#FFCC66",
                        font=(self._live_log_font.cget("family"), max(10, int(self._live_log_font.cget("size")) + 2), "bold"),
                    )
                    no_data_msg = (
                        f"No hydrated candles for {focus_symbol}.\n"
                        "Run scan/hydrate to download chart data, then review Scanner for eligibility reasons."
                    )
                    extra_msg = str(thinker_data.get("msg", "") or "").strip()
                    if extra_msg:
                        no_data_msg += f"\nStatus: {extra_msg}"
                    if isinstance(chart_map, dict) and chart_map:
                        no_data_msg += f"\nCached chart symbols: {len(chart_map)}"
                    canvas.create_text(
                        panel_left + 14,
                        panel_top + 46,
                        anchor="nw",
                        width=max(180, (panel_right - panel_left) - 24),
                        text=no_data_msg,
                        fill=DARK_MUTED,
                        font=(self._live_log_font.cget("family"), max(9, int(self._live_log_font.cget("size")))),
                    )

                    btn_y = panel_top + 118
                    scan_tag = f"chart_scan_{market_key}"
                    hydrate_tag = f"chart_hydrate_{market_key}"
                    scanner_tag = f"chart_scanner_{market_key}"
                    btn_x = panel_left + 14
                    btn_gap = 12
                    btn_h = 30
                    run_w = 110
                    hyd_w = 156
                    scan_x1 = btn_x
                    scan_x2 = scan_x1 + run_w
                    hyd_x1 = scan_x2 + btn_gap
                    hyd_x2 = hyd_x1 + hyd_w
                    if hyd_x2 > (panel_right - 10):
                        hyd_x2 = panel_right - 10
                        hyd_x1 = max(scan_x2 + 6, hyd_x2 - hyd_w)
                    canvas.create_rectangle(scan_x1, btn_y, scan_x2, btn_y + btn_h, fill=DARK_BG2, outline=DARK_ACCENT, tags=(scan_tag,))
                    canvas.create_text((scan_x1 + scan_x2) / 2.0, btn_y + 15, text="Run Scan", fill=DARK_ACCENT, tags=(scan_tag,))
                    canvas.create_rectangle(hyd_x1, btn_y, hyd_x2, btn_y + btn_h, fill=DARK_BG2, outline=DARK_ACCENT2, tags=(hydrate_tag,))
                    canvas.create_text((hyd_x1 + hyd_x2) / 2.0, btn_y + 15, text="Download/Hydrate", fill=DARK_ACCENT2, tags=(hydrate_tag,))
                    sc_y1 = btn_y + btn_h + 8
                    sc_y2 = sc_y1 + btn_h
                    sc_x1 = btn_x
                    sc_x2 = min(panel_right - 10, sc_x1 + 156)
                    canvas.create_rectangle(sc_x1, sc_y1, sc_x2, sc_y2, fill=DARK_BG2, outline="#7EC8FF", tags=(scanner_tag,))
                    canvas.create_text((sc_x1 + sc_x2) / 2.0, sc_y1 + 15, text="Open Scanner", fill="#7EC8FF", tags=(scanner_tag,))
                    canvas.tag_bind(scan_tag, "<Button-1>", lambda _e, mk=market_key: self._run_market_thinker_scan(mk, force=True, min_interval_s=0.0))
                    canvas.tag_bind(hydrate_tag, "<Button-1>", lambda _e, mk=market_key: self._request_market_chart_hydrate(mk))
                    canvas.tag_bind(scanner_tag, "<Button-1>", lambda _e, mk=market_key: self._switch_market_view(mk, "Scanner"))
                    canvas.tag_bind(scan_tag, "<Enter>", lambda _e, cv=canvas: cv.configure(cursor="hand2"))
                    canvas.tag_bind(scan_tag, "<Leave>", lambda _e, cv=canvas: cv.configure(cursor=""))
                    canvas.tag_bind(hydrate_tag, "<Enter>", lambda _e, cv=canvas: cv.configure(cursor="hand2"))
                    canvas.tag_bind(hydrate_tag, "<Leave>", lambda _e, cv=canvas: cv.configure(cursor=""))
                    canvas.tag_bind(scanner_tag, "<Enter>", lambda _e, cv=canvas: cv.configure(cursor="hand2"))
                    canvas.tag_bind(scanner_tag, "<Leave>", lambda _e, cv=canvas: cv.configure(cursor=""))
        except Exception:
            pass

    def _market_settings_snapshot(self, market_key: str) -> Dict[str, Any]:
        alpaca_key, alpaca_secret = get_alpaca_creds(self.settings, base_dir=self.project_dir)
        oanda_account, oanda_token = get_oanda_creds(self.settings, base_dir=self.project_dir)
        if market_key == "stocks":
            key_ok = bool(str(alpaca_key).strip())
            secret_ok = bool(str(alpaca_secret).strip())
            if key_ok and secret_ok:
                detail = f"API Key {self._mask_secret(alpaca_key)} | Secret present"
            elif key_ok and (not secret_ok):
                detail = f"API Key {self._mask_secret(alpaca_key)} | Secret missing"
            elif (not key_ok) and secret_ok:
                detail = "API key ID missing | Secret present"
            else:
                detail = "API key ID missing | Secret missing"
            return {
                "broker": "Alpaca",
                "configured": bool(key_ok and secret_ok),
                "mode": ("Paper" if bool(self.settings.get("alpaca_paper_mode", True)) else "Live"),
                "endpoint": str(self.settings.get("alpaca_base_url", DEFAULT_SETTINGS.get("alpaca_base_url", "")) or "").strip(),
                "detail": detail,
            }
        account_ok = bool(str(oanda_account).strip())
        token_ok = bool(str(oanda_token).strip())
        if account_ok and token_ok:
            oanda_detail = f"Account {str(oanda_account or '').strip()} | Token {self._mask_secret(oanda_token)}"
        elif account_ok and (not token_ok):
            oanda_detail = f"Account {str(oanda_account or '').strip()} | Token missing"
        elif (not account_ok) and token_ok:
            oanda_detail = f"Account missing | Token {self._mask_secret(oanda_token)}"
        else:
            oanda_detail = "Account missing | Token missing"
        return {
            "broker": "OANDA",
            "configured": bool(account_ok and token_ok),
            "mode": ("Practice" if bool(self.settings.get("oanda_practice_mode", True)) else "Live"),
            "endpoint": str(self.settings.get("oanda_rest_url", DEFAULT_SETTINGS.get("oanda_rest_url", "")) or "").strip(),
            "detail": oanda_detail,
        }

    def _refresh_parallel_market_panels(self) -> None:
        awareness = build_awareness_payload()
        broker_awareness = awareness.get("brokers", {}) if isinstance(awareness.get("brokers", {}), dict) else {}
        loop_status = _safe_read_json(os.path.join(self.hub_dir, "market_loop_status.json")) or {}
        if not isinstance(loop_status, dict):
            loop_status = {}
        for market_key, panel in self.market_panels.items():
            snap = self._market_settings_snapshot(market_key)
            configured = bool(snap.get("configured"))
            mode_txt = str(snap.get("mode", "") or "")
            endpoint = str(snap.get("endpoint", "") or "").strip()
            broker = str(snap.get("broker", market_key.title()) or market_key.title())
            state_txt = "Configured" if configured else "Credentials missing"

            status_path = str(panel.get("status_path", "") or "")
            trader_status_path = self.market_trader_paths.get(market_key, "")
            diag_path = self.market_scan_diag_paths.get(market_key, "")
            thinker_path = self.market_thinker_paths.get(market_key, "")
            history_path = os.path.join(self.market_state_dirs.get(market_key, self.hub_dir), "execution_audit.jsonl")
            bundle = load_market_status_bundle(
                status_path=status_path,
                trader_path=str(trader_status_path or ""),
                thinker_path=str(thinker_path or ""),
                scan_diag_path=str(diag_path or ""),
                history_path=history_path,
                history_limit=120,
                market_key=market_key,
            )
            status_data = bundle.get("status", {}) if isinstance(bundle.get("status", {}), dict) else {}
            trader_data = bundle.get("trader", {}) if isinstance(bundle.get("trader", {}), dict) else {}
            thinker_data = bundle.get("thinker", {}) if isinstance(bundle.get("thinker", {}), dict) else {}
            diag_data = bundle.get("scan_diagnostics", {}) if isinstance(bundle.get("scan_diagnostics", {}), dict) else {}

            ai_state = str(thinker_data.get("ai_state", status_data.get("ai_state", state_txt)) or state_txt)
            trader_state = str(trader_data.get("trader_state", status_data.get("trader_state", "Idle")) or "Idle")
            msg = str(trader_data.get("msg", "") or thinker_data.get("msg", "") or status_data.get("msg", "") or "").strip()
            panel["ai_var"].set(f"{panel['market_name']} AI: {ai_state}")
            panel["trader_var"].set(f"{panel['market_name']} Trader: {trader_state}")
            state_line = f"Trade State: {str(thinker_data.get('state', status_data.get('state', state_txt)) or state_txt)}"
            if msg:
                state_line += f" | {msg}"
            if market_key == "stocks":
                sm = awareness.get("stocks", {}) if isinstance(awareness.get("stocks", {}), dict) else {}
                state_line += f" | MarketHours={'OPEN' if bool(sm.get('is_open', False)) else 'CLOSED'}"
                try:
                    cdown = int(sm.get("countdown_s", 0) or 0)
                except Exception:
                    cdown = 0
                if cdown > 0:
                    state_line += f" | T- {max(1, cdown // 60)}m"
                wpol = diag_data.get("window_policy", {}) if isinstance(diag_data.get("window_policy", {}), dict) else {}
                if bool(wpol.get("active", False)):
                    try:
                        wmult = float(wpol.get("score_mult", 1.0) or 1.0)
                        wname = str(wpol.get("window", "window") or "window").strip().lower()
                        state_line += f" | {wname} x{wmult:.2f}"
                    except Exception:
                        pass
                last_loop_ts = int(loop_status.get("stocks_last_scan_ts", 0) or 0)
            else:
                fx = awareness.get("forex", {}) if isinstance(awareness.get("forex", {}), dict) else {}
                state_line += f" | Session={str(fx.get('session', 'N/A') or 'N/A')}"
                next_sess = str(fx.get("next_session", "") or "").strip()
                if next_sess:
                    state_line += f" -> {next_sess}"
                try:
                    sess_weighted = int(diag_data.get("session_weighted_candidates", 0) or 0)
                    if sess_weighted > 0:
                        state_line += f" | SessW={sess_weighted}"
                except Exception:
                    pass
                last_loop_ts = int(loop_status.get("forex_last_scan_ts", 0) or 0)
            if last_loop_ts > 0:
                loop_age = max(0, int(time.time()) - int(last_loop_ts))
                state_line += f" | LoopAge={loop_age}s"
            cycle_meta = loop_status.get(f"{market_key}_cycle", {}) if isinstance(loop_status.get(f"{market_key}_cycle", {}), dict) else {}
            cadence_meta = cycle_meta.get("cadence", {}) if isinstance(cycle_meta.get("cadence", {}), dict) else {}
            if cadence_meta:
                try:
                    obs_s = float(cadence_meta.get("observed_s", 0.0) or 0.0)
                    exp_s = float(cadence_meta.get("expected_s", 0.0) or 0.0)
                    late_pct = float(cadence_meta.get("late_pct", 0.0) or 0.0)
                    level = str(cadence_meta.get("level", "ok") or "ok").strip().lower()
                    if obs_s > 0.0 and exp_s > 0.0:
                        state_line += f" | Cadence {obs_s:.0f}/{exp_s:.0f}s"
                        if level != "ok":
                            state_line += f" ({late_pct:.0f}% late)"
                except Exception:
                    pass
            try:
                uni_n = int(len(list(thinker_data.get("universe", []) or [])))
            except Exception:
                uni_n = 0
            leaders_n = int(len(list(thinker_data.get("leaders", []) or []))) if isinstance(thinker_data, dict) else 0
            if uni_n or leaders_n:
                state_line += f" | Uni={uni_n} Leaders={leaders_n}"
            leader_mode = str(thinker_data.get("leader_mode", "") or "").strip().lower()
            if leader_mode == "watch_fallback":
                state_line += " | LeaderMode=WATCH"
            if isinstance(diag_data, dict) and diag_data:
                try:
                    c_churn = float(diag_data.get("candidate_churn_pct", 0.0) or 0.0)
                    l_churn = float(diag_data.get("leader_churn_pct", 0.0) or 0.0)
                    if c_churn > 0.0:
                        state_line += f" | Churn={c_churn:.1f}%"
                    if l_churn > 0.0:
                        state_line += f" Ldr={l_churn:.1f}%"
                except Exception:
                    pass
            health = {}
            if isinstance(thinker_data.get("health"), dict):
                health = thinker_data.get("health") or {}
            elif isinstance(trader_data.get("health"), dict):
                health = trader_data.get("health") or {}
            if health:
                hb = (
                    f"Data={'OK' if bool(health.get('data_ok', True)) else 'NO'} "
                    f"Broker={'OK' if bool(health.get('broker_ok', True)) else 'NO'} "
                    f"Orders={'OK' if bool(health.get('orders_ok', True)) else 'NO'} "
                    f"Drift={'YES' if bool(health.get('drift_warning', False)) else 'NO'}"
                )
                state_line += f" | {hb}"
            if bool(thinker_data.get("fallback_cached", False)):
                try:
                    fb_age = int(float(thinker_data.get("fallback_age_s", 0) or 0))
                except Exception:
                    fb_age = 0
                state_line += (f" | CachedFallback=ON({fb_age}s)" if fb_age > 0 else " | CachedFallback=ON")
            gate_reason = str(trader_data.get("entry_eval_top_reason", "") or "").strip()
            if gate_reason:
                state_line += f" | Gate={gate_reason[:56]}"
            gate_flags = trader_data.get("entry_gate_flags", {}) if isinstance(trader_data.get("entry_gate_flags", {}), dict) else {}
            if gate_flags:
                try:
                    rej = float(gate_flags.get("reject_rate_pct", 0.0) or 0.0)
                    rej_max = float(gate_flags.get("reject_rate_max_pct", 0.0) or 0.0)
                    if rej_max > 0.0:
                        state_line += f" | Reject={rej:.1f}/{rej_max:.1f}%"
                except Exception:
                    pass
                if bool(gate_flags.get("data_quality_required", False)) and (not bool(gate_flags.get("data_quality_ok", True))):
                    state_line += " | DataGate=BLOCK"
            try:
                entry_size_scale = float(trader_data.get("entry_size_scale", 1.0) or 1.0)
                if entry_size_scale < 0.999:
                    state_line += f" | EntrySize x{entry_size_scale:.2f}"
            except Exception:
                pass
            if bool(trader_data.get("guard_active", False)):
                try:
                    guard_left_s = int(float(trader_data.get("guard_remaining_s", 0) or 0))
                except Exception:
                    guard_left_s = 0
                state_line += f" | GUARD[{guard_left_s}s]"
            panel["state_var"].set(self._format_market_state_line(state_line))
            data_ok = bool(health.get("data_ok", configured)) if isinstance(health, dict) else bool(configured)
            broker_ok = bool(health.get("broker_ok", configured)) if isinstance(health, dict) else bool(configured)
            orders_ok = bool(health.get("orders_ok", True)) if isinstance(health, dict) else True
            cadence_level = str(cadence_meta.get("level", "ok") or "ok").strip().lower() if isinstance(cadence_meta, dict) else "ok"
            self._set_badge_style(panel.get("chip_data"), f"Data: {'OK' if data_ok else 'NO'}", tone=("good" if data_ok else "bad"))
            self._set_badge_style(panel.get("chip_broker"), f"Broker: {'OK' if broker_ok else 'NO'}", tone=("good" if broker_ok else "bad"))
            self._set_badge_style(panel.get("chip_orders"), f"Orders: {'OK' if orders_ok else 'NO'}", tone=("good" if orders_ok else "warn"))
            if cadence_level in {"critical", "error"}:
                cyc_tone = "bad"
            elif cadence_level in {"warning", "warn"}:
                cyc_tone = "warn"
            else:
                cyc_tone = "info"
            cycle_txt = f"Cycle: {cadence_level.upper()}"
            if isinstance(cadence_meta, dict):
                try:
                    obs_s = float(cadence_meta.get("observed_s", 0.0) or 0.0)
                    exp_s = float(cadence_meta.get("expected_s", 0.0) or 0.0)
                    if obs_s > 0.0 and exp_s > 0.0:
                        cycle_txt += f" {obs_s:.0f}/{exp_s:.0f}s"
                except Exception:
                    pass
            self._set_badge_style(panel.get("chip_cycle"), cycle_txt, tone=cyc_tone)
            panel["endpoint_var"].set(f"Broker: {broker} | {mode_txt} | {endpoint or 'endpoint not set'}")
            top_pick = thinker_data.get("top_pick", {}) or {}
            if not isinstance(top_pick, dict):
                top_pick = {}
            top_ident = str(top_pick.get("pair") or top_pick.get("symbol") or "N/A")
            top_side = str(top_pick.get("side", "watch") or "watch").upper()
            try:
                top_score = f"{float(top_pick.get('score', 0.0)):+.4f}"
            except Exception:
                top_score = str(top_pick.get("score", "N/A"))
            try:
                panel["top_pick_var"].set(f"Top: {top_ident} | {top_side} | {top_score}")
            except Exception:
                pass
            try:
                sig_reason_logic, _sig_reason_data = self._market_reason_parts(market_key, top_pick if isinstance(top_pick, dict) else {})
                sig_reason = str(sig_reason_logic or top_pick.get("reason", "") or "").strip()
                sig_txt = f"{sig_reason[:88]}{'...' if len(sig_reason) > 88 else ''}" if sig_reason else ""
                if isinstance(diag_data, dict) and diag_data:
                    leaders_total = int(diag_data.get("leaders_total", 0) or 0)
                    scores_total = int(diag_data.get("scores_total", 0) or 0)
                    rs = diag_data.get("reject_summary", {}) if isinstance(diag_data.get("reject_summary"), dict) else {}
                    rej_rate = float(rs.get("reject_rate_pct", 0.0) or 0.0)
                    c_churn = float(diag_data.get("candidate_churn_pct", 0.0) or 0.0)
                    sig_txt = f"Scan health: leaders={leaders_total} scores={scores_total} reject={rej_rate:.1f}% churn={c_churn:.1f}%"
                if market_key == "forex":
                    fx = awareness.get("forex", {}) if isinstance(awareness.get("forex", {}), dict) else {}
                    try:
                        fx_eta_s = int(fx.get("session_eta_s", 0) or 0)
                    except Exception:
                        fx_eta_s = 0
                    sess = diag_data.get("session_context", {}) if isinstance(diag_data.get("session_context", {}), dict) else {}
                    sess_name = str(sess.get("session", fx.get("session", "N/A")) or "N/A")
                    sess_bias = str(sess.get("bias", fx.get("bias", "FLAT")) or "FLAT")
                    sig_txt = (sig_txt + " | " if sig_txt else "") + (
                        f"Session bias: {sess_name} {sess_bias} ({str(fx.get('volatility', 'MED'))})"
                        + (f" | next in {max(1, fx_eta_s // 60)}m" if fx_eta_s > 0 else "")
                    )
                if panel.get("signal_var") is not None:
                    panel["signal_var"].set(sig_txt)
            except Exception:
                pass
            try:
                focus_combo = panel.get("instrument_combo")
                focus_var = panel.get("instrument_var")
                opts: List[str] = ["AUTO"]
                seen = {"AUTO"}
                for row in list(thinker_data.get("leaders", []) or [])[:20]:
                    if not isinstance(row, dict):
                        continue
                    ident = str(row.get("pair") or row.get("symbol") or "").strip().upper()
                    if ident and ident not in seen:
                        seen.add(ident)
                        opts.append(ident)
                for row in list(status_data.get("raw_positions", []) or []):
                    if not isinstance(row, dict):
                        continue
                    ident = str(row.get("symbol") or row.get("instrument") or "").strip().upper()
                    if ident and ident not in seen:
                        seen.add(ident)
                        opts.append(ident)
                if focus_combo is not None:
                    focus_combo.configure(values=opts)
                if focus_var is not None:
                    cur_focus = str((focus_var.get() if focus_var else "AUTO") or "AUTO").strip().upper()
                    if cur_focus not in opts:
                        focus_var.set("AUTO")
            except Exception:
                pass
            try:
                view_name = str((panel.get("market_view_var").get() if panel.get("market_view_var") else "Overview") or "Overview")
                view_hints = {
                    "Overview": "Overview: context card + focus chart + quick trend diagnostics.",
                    "Scanner": "Scanner: full ranked universe with eligibility gates and reject context. Click a column title to sort.",
                    "Leaders": "Leaders: top-ranked symbols/pairs with logic-first reasons. Hover Reason to view raw metrics.",
                    "Positions": "Positions: broker-linked exposure and live PnL.",
                    "Execution": "Execution: audit trail for order decisions and broker responses.",
                    "Health": "Health: data/broker/order guard signals and drift status.",
                }
                vh = panel.get("market_view_hint_var")
                if isinstance(vh, tk.StringVar):
                    vh.set(view_hints.get(view_name, "Use tabs to inspect scanner, execution, and health details."))
            except Exception:
                pass

            action_hint = ""
            auto_scan_on = bool((panel.get("auto_scan_var").get() if panel.get("auto_scan_var") else True))
            auto_step_on = bool((panel.get("auto_step_var").get() if panel.get("auto_step_var") else True))
            if not configured:
                action_hint = f"Next: add {broker} credentials in Settings, then click Test {broker} Connection."
            elif bool(self._market_test_busy.get(market_key, False)):
                action_hint = f"Next: waiting for {broker} connection test to finish."
            elif health and (not bool(health.get("data_ok", True)) or not bool(health.get("broker_ok", True))):
                action_hint = "Next: broker/data health is degraded; inspect Logs and fix credentials/network."
            elif (market_key == "stocks") and ("HTTP ERROR 403" in str(msg).upper() or "FORBIDDEN" in str(msg).upper()):
                action_hint = "Next: Alpaca SIP feed returned 403. Scanner can still run via IEX fallback; retry scan or set stock_data_feeds=iex."
            elif bool(self._market_thinker_busy.get(market_key, False)):
                action_hint = "Next: market scan in progress; wait for new leaders."
            elif leaders_n <= 0:
                action_hint = "Next: run scan to rank candidates. If none rank, loosen scan filters in Settings."
            elif bool(self._market_trader_busy.get(market_key, False)):
                action_hint = "Next: trader step running; check History/Execution for outcomes."
            elif bool(trader_data.get("guard_active", False)):
                try:
                    rem_s = int(float(trader_data.get("guard_remaining_s", 0) or 0))
                except Exception:
                    rem_s = 0
                action_hint = f"Next: execution temporarily paused for broker stability ({rem_s}s remaining)."
            elif "MAX OPEN POSITIONS" in str(msg).upper():
                max_key = "stock_max_open_positions" if market_key == "stocks" else "forex_max_open_positions"
                try:
                    cfg_max = max(1, int(float(self.settings.get(max_key, 1) or 1)))
                except Exception:
                    cfg_max = 1
                try:
                    cur_open = int(float(status_data.get("open_positions", trader_data.get("open_positions", 0)) or 0))
                except Exception:
                    cur_open = 0
                action_hint = (
                    f"Next: max positions reached ({cur_open}/{cfg_max}). "
                    "Close/reduce positions before opening new ones."
                )
            else:
                action_hint = "Next: monitor top candidate and execution health. Auto loops are running in background."
            if isinstance(diag_data, dict) and diag_data:
                rs = diag_data.get("reject_summary", {}) if isinstance(diag_data.get("reject_summary"), dict) else {}
                dom = str(rs.get("dominant_reason", "") or "").strip()
                rr = float(rs.get("reject_rate_pct", 0.0) or 0.0)
                if rr >= 65.0 and dom:
                    action_hint += f" | Scan bottleneck={dom} ({rr:.1f}% rejected)"
                qsum = str(diag_data.get("quality_summary", "") or "").strip()
                if qsum:
                    action_hint += f" | {qsum}"
            if cadence_meta:
                try:
                    clevel = str(cadence_meta.get("level", "ok") or "ok").strip().lower()
                    clate = float(cadence_meta.get("late_pct", 0.0) or 0.0)
                    cobs = float(cadence_meta.get("observed_s", 0.0) or 0.0)
                    cexp = float(cadence_meta.get("expected_s", 0.0) or 0.0)
                    if clevel != "ok":
                        if cobs > 0.0 and cexp > 0.0:
                            action_hint += f" | Cadence drift {clate:.0f}% ({cobs:.0f}/{cexp:.0f}s, {clevel})"
                        else:
                            action_hint += f" | Cadence drift {clate:.0f}% ({clevel})"
                        if market_key == "forex":
                            action_hint += " | Fix: raise Forex scan interval to ~12-20s or reduce scanner load."
                        else:
                            action_hint += " | Fix: increase Stocks scan interval or reduce universe/filter load."
                except Exception:
                    pass
            action_hint += f" | Auto scan={'ON' if auto_scan_on else 'OFF'} step={'ON' if auto_step_on else 'OFF'}"
            try:
                if panel.get("action_status_var") is not None:
                    panel["action_status_var"].set(action_hint)
            except Exception:
                pass

            pvars = panel.get("portfolio_vars", {})
            if isinstance(pvars, dict):
                bp_val = str(status_data.get("buying_power", "Pending account link") or "Pending account link")
                if bp_val.strip().upper() in {"", "N/A", "PENDING ACCOUNT LINK"}:
                    try:
                        acct = float(trader_data.get("account_value_usd", 0.0) or 0.0)
                        expo = float(trader_data.get("exposure_usd", 0.0) or 0.0)
                        if acct > 0.0:
                            bp_val = _fmt_money(max(0.0, acct - expo))
                    except Exception:
                        pass
                pos_val = str(status_data.get("open_positions", "0") or "0")
                if pos_val.strip().upper() in {"", "N/A"}:
                    try:
                        pos_val = str(int(float(trader_data.get("open_positions", 0) or 0)))
                    except Exception:
                        pos_val = "0"
                pvars["buying_power"].set(bp_val)
                pvars["open_positions"].set(pos_val)
                pvars["realized_pnl"].set(str(status_data.get("realized_pnl", "N/A") or "N/A"))
                pvars["mode"].set(mode_txt or "Paper first")
                if market_key == "stocks":
                    guard_usd = float(self.settings.get("stock_max_daily_loss_usd", 0.0) or 0.0)
                    guard_pct = float(self.settings.get("stock_max_daily_loss_pct", 0.0) or 0.0)
                else:
                    guard_usd = float(self.settings.get("forex_max_daily_loss_usd", 0.0) or 0.0)
                    guard_pct = float(self.settings.get("forex_max_daily_loss_pct", 0.0) or 0.0)
                msg_up = str(trader_data.get("msg", "") or "").upper()
                triggered = ("DAILY LOSS" in msg_up) or ("MAX DAILY LOSS" in msg_up)
                if (guard_usd <= 0.0) and (guard_pct <= 0.0):
                    pvars["daily_guard"].set("Disabled")
                elif triggered:
                    pvars["daily_guard"].set("TRIGGERED")
                else:
                    gtxt = []
                    if guard_usd > 0.0:
                        gtxt.append(f"${guard_usd:,.2f}")
                    if guard_pct > 0.0:
                        gtxt.append(f"{guard_pct:.2f}%")
                    pvars["daily_guard"].set("Armed " + "/".join(gtxt))
            self._set_market_positions(
                market_key,
                list(status_data.get("positions_preview", []) or []),
                raw_positions=list(status_data.get("raw_positions", []) or []),
            )

            history_lines: List[str] = []
            trader_ts = trader_data.get("updated_at")
            if trader_ts:
                try:
                    history_lines.append(f"[{time.strftime('%H:%M:%S', time.localtime(float(trader_ts)))}] {str(trader_data.get('msg', '') or '').strip()}")
                except Exception:
                    history_lines.append(str(trader_data.get("msg", "") or "").strip())
            elif trader_data.get("msg"):
                history_lines.append(str(trader_data.get("msg", "") or "").strip())
            for row in list(trader_data.get("actions", []) or [])[-20:]:
                txt = str(row or "").strip()
                if txt:
                    history_lines.append(txt)
            if not history_lines:
                for row in list(bundle.get("history", []) or [])[-20:]:
                    if not isinstance(row, dict):
                        continue
                    try:
                        when = time.strftime("%H:%M:%S", time.localtime(float(row.get("ts", 0) or 0)))
                    except Exception:
                        when = "--:--:--"
                    ident = str(row.get("symbol", "") or row.get("instrument", "") or "").strip().upper()
                    side = str(row.get("side", "") or "").strip().upper()
                    evt = str(row.get("event", "") or "").strip().upper()
                    msg_txt = str(row.get("msg", "") or "").strip()
                    line = f"[{when}] {ident or '-'} {side or evt or '-'}"
                    if msg_txt:
                        line += f" | {msg_txt[:120]}"
                    history_lines.append(line)
            if thinker_data.get("msg"):
                history_lines.append(f"Thinker: {str(thinker_data.get('msg', '') or '').strip()}")
            history_sig = tuple(history_lines[-40:])
            if panel.get("last_history_sig") != history_sig:
                panel["last_history_sig"] = history_sig
                panel["history_lines"] = list(history_lines[-120:])
                self._set_market_history(market_key, history_lines)

            extra_note = str(thinker_data.get("pdt_note", "") or status_data.get("pdt_note", "") or "").strip()
            diag_note = ""
            if isinstance(diag_data, dict) and diag_data:
                try:
                    rs = diag_data.get("reject_summary", {}) if isinstance(diag_data.get("reject_summary"), dict) else {}
                    c_churn = float(diag_data.get("candidate_churn_pct", 0.0) or 0.0)
                    l_churn = float(diag_data.get("leader_churn_pct", 0.0) or 0.0)
                    qsum = str(diag_data.get("quality_summary", "") or "").strip()
                    diag_note = (
                        f"Scan diagnostics: leaders={int(diag_data.get('leaders_total', 0) or 0)} "
                        f"scores={int(diag_data.get('scores_total', 0) or 0)} "
                        f"reject={float(rs.get('reject_rate_pct', 0.0) or 0.0):.1f}% "
                        f"churn={c_churn:.1f}%/{l_churn:.1f}% "
                        f"dominant={str(rs.get('dominant_reason', '') or 'n/a')}\n"
                    )
                    if qsum:
                        diag_note += f"Quality summary: {qsum}\n"
                except Exception:
                    diag_note = ""
            if cadence_meta:
                try:
                    c_level = str(cadence_meta.get("level", "ok") or "ok").strip().lower()
                    c_obs = float(cadence_meta.get("observed_s", 0.0) or 0.0)
                    c_exp = float(cadence_meta.get("expected_s", 0.0) or 0.0)
                    c_late = float(cadence_meta.get("late_pct", 0.0) or 0.0)
                    diag_note += (
                        f"Cadence: {c_obs:.1f}s observed vs {c_exp:.1f}s target | "
                        f"late={c_late:.1f}% | level={c_level}\n"
                    )
                except Exception:
                    pass

            self._set_market_notes(
                market_key,
                "".join(
                    [
                        f"Status: {'ready to connect' if configured else 'credentials required'}\n",
                        f"Broker: {broker}\n",
                        f"Mode: {mode_txt}\n",
                        f"{snap.get('detail', '')}\n",
                        f"Endpoint: {endpoint or 'not set'}\n",
                        (f"Broker maintenance note: {str(broker_awareness.get('alpaca' if market_key == 'stocks' else 'oanda', 'Normal'))}\n"),
                        (f"{extra_note}\n" if extra_note else ""),
                        (
                            f"Market session: {str((awareness.get('stocks', {}) if market_key == 'stocks' else awareness.get('forex', {})).get('note', 'n/a'))}\n"
                        ),
                        (
                            (
                                f"Session timing: next open {self._market_eta_or_age((awareness.get('stocks', {}) if isinstance(awareness.get('stocks', {}), dict) else {}).get('next_open_ts', 0))}\n"
                                if market_key == "stocks"
                                else (
                                    f"Session timing: next {str((awareness.get('forex', {}) if isinstance(awareness.get('forex', {}), dict) else {}).get('next_session', 'N/A'))} "
                                    f"{self._market_eta_or_age((awareness.get('forex', {}) if isinstance(awareness.get('forex', {}), dict) else {}).get('session_eta_s', 0) + int(time.time()))}\n"
                                )
                            )
                        ),
                        (
                            f"Loop cadence: next snapshot {self._market_eta_or_age(loop_status.get('next_snapshot_ts', 0))} | "
                            f"next market loop {self._market_eta_or_age(loop_status.get('next_stocks_scan_ts' if market_key == 'stocks' else 'next_forex_scan_ts', 0))}\n"
                        ),
                        diag_note,
                        (f"Thinker: {ai_state}\n" if ai_state else ""),
                        "Auto scan + trader step run continuously in the background; use Action Center for manual overrides.",
                    ]
                ),
            )
            self._render_market_canvas(market_key, thinker_data, status_data=status_data, diag_data=diag_data)
            source_ts = (
                trader_data.get("updated_at")
                or thinker_data.get("updated_at")
                or thinker_data.get("ts")
                or status_data.get("ts")
            )
            age_txt = self._market_age_text(source_ts)
            try:
                panel["charts_age_var"].set(age_txt)
            except Exception:
                pass
            try:
                panel["positions_age_var"].set(age_txt)
            except Exception:
                pass
            try:
                panel["history_age_var"].set(age_txt)
            except Exception:
                pass
            try:
                panel["logs_age_var"].set(age_txt)
            except Exception:
                pass

            log_sig = (
                configured,
                mode_txt,
                endpoint,
                str(snap.get("detail", "")),
                str(thinker_data.get("state", "")),
                str(thinker_data.get("msg", "")),
                str(trader_data.get("state", "")),
                str(trader_data.get("msg", "")),
                str(status_data.get("state", "")),
                str(status_data.get("msg", "")),
            )
            try:
                cur_cadence_level = str(cadence_meta.get("level", "ok") or "ok").strip().lower()
                prev_cadence_level = str(panel.get("last_cadence_level", "ok") or "ok").strip().lower()
                if cur_cadence_level != prev_cadence_level:
                    panel["last_cadence_level"] = cur_cadence_level
                    if cur_cadence_level in {"warning", "critical"}:
                        late_pct = float(cadence_meta.get("late_pct", 0.0) or 0.0)
                        self._append_market_log(
                            market_key,
                            f"[CADENCE] drift active level={cur_cadence_level} late={late_pct:.1f}%",
                        )
                    elif prev_cadence_level in {"warning", "critical"}:
                        self._append_market_log(market_key, "[CADENCE] drift cleared")
            except Exception:
                pass
            if panel.get("last_log_sig") != log_sig:
                panel["last_log_sig"] = log_sig
                self._append_market_log(
                    market_key,
                    f"[{broker.upper()}] {state_txt} | mode={mode_txt} | endpoint={endpoint or 'not set'}",
                )

            try:
                busy = bool(self._market_test_busy.get(market_key, False))
                panel["test_btn"].configure(
                    state=("disabled" if busy else "normal"),
                    text=("Testing..." if busy else f"Test {panel.get('broker_name', broker)} Connection"),
                )
            except Exception:
                pass
            try:
                scan_busy = bool(self._market_thinker_busy.get(market_key, False))
                panel["run_btn"].configure(
                    state=("disabled" if (scan_busy or (not configured)) else "normal"),
                    text=("Scanning..." if scan_busy else "Run Scan"),
                )
            except Exception:
                pass
            try:
                step_btn = panel.get("trader_step_btn")
                if step_btn is not None:
                    step_market = str(panel.get("trader_step_market_key", "") or "")
                    busy_step = bool(self._market_trader_busy.get(step_market, False))
                    step_name = "Stocks" if step_market == "stocks" else "Forex"
                    step_btn.configure(
                        state=("disabled" if (busy_step or (not configured)) else "normal"),
                        text=(f"Running {step_name} Step..." if busy_step else f"Run {step_name} Step"),
                    )
            except Exception:
                pass

    def _run_market_connection_test(self, market_key: str) -> None:
        if self._market_test_busy.get(market_key):
            return
        self._market_test_busy[market_key] = True
        self._append_market_log(market_key, "[TEST] Starting broker connectivity check...")
        self._refresh_parallel_market_panels()

        def _worker() -> None:
            if market_key == "stocks":
                ok, msg = self._make_alpaca_client().test_connection()
            else:
                ok, msg = self._make_oanda_client().test_connection()

            def _finish() -> None:
                self._market_test_busy[market_key] = False
                broker = self.market_panels.get(market_key, {}).get("broker_name", market_key.upper())
                prefix = "[OK]" if ok else "[FAIL]"
                self._append_market_log(market_key, f"{prefix} {broker} test: {msg}")
                self._refresh_parallel_market_panels()

            try:
                self.after(0, _finish)
            except Exception:
                self._market_test_busy[market_key] = False

        threading.Thread(target=_worker, daemon=True).start()

    def _write_market_status(self, market_key: str, payload: Dict[str, Any]) -> None:
        path = self.market_status_paths.get(market_key, "")
        if not path:
            return
        try:
            _safe_write_json(path, payload)
        except Exception:
            pass

    def _schedule_market_snapshot_refresh(self, market_key: str, every_s: float = 15.0) -> None:
        if self._market_refresh_busy.get(market_key, False):
            return
        last_ts = float(self._last_market_refresh_ts.get(market_key, 0.0) or 0.0)
        if (time.time() - last_ts) < float(every_s):
            return
        self._market_refresh_busy[market_key] = True

        def _worker() -> None:
            if market_key == "stocks":
                snap = self._make_alpaca_client().fetch_snapshot()
            else:
                snap = self._make_oanda_client().fetch_snapshot()
            snap["ts"] = int(time.time())
            self._write_market_status(market_key, snap)

            def _finish() -> None:
                self._last_market_refresh_ts[market_key] = time.time()
                self._market_refresh_busy[market_key] = False
                self._refresh_parallel_market_panels()

            try:
                self.after(0, _finish)
            except Exception:
                self._last_market_refresh_ts[market_key] = time.time()
                self._market_refresh_busy[market_key] = False

        threading.Thread(target=_worker, daemon=True).start()

    def _run_market_thinker_scan(self, market_key: str, force: bool = False, min_interval_s: Optional[float] = None) -> None:
        if self._market_thinker_busy.get(market_key, False):
            return
        last_ts = float(self._last_market_thinker_ts.get(market_key, 0.0) or 0.0)
        if min_interval_s is None:
            min_interval_s = (45.0 if market_key == "forex" else 60.0)
        if (not force) and ((time.time() - last_ts) < float(min_interval_s)):
            return
        self._market_thinker_busy[market_key] = True
        self._append_market_log(market_key, "[THINKER] Starting scan...")
        self._refresh_parallel_market_panels()

        def _worker() -> None:
            if market_key == "stocks":
                payload = run_stock_scan(self.settings, self.hub_dir)
            else:
                payload = run_forex_scan(self.settings, self.hub_dir)
            payload["ts"] = int(time.time())
            self._write_market_thinker_status(market_key, payload)

            def _finish() -> None:
                self._last_market_thinker_ts[market_key] = time.time()
                self._market_thinker_busy[market_key] = False
                state = str(payload.get("state", "READY") or "READY")
                msg = str(payload.get("msg", "") or "").strip()
                self._append_market_log(market_key, f"[THINKER] {state} | {msg}")
                self._refresh_parallel_market_panels()

            try:
                self.after(0, _finish)
            except Exception:
                self._last_market_thinker_ts[market_key] = time.time()
                self._market_thinker_busy[market_key] = False

        threading.Thread(target=_worker, daemon=True).start()

    def _schedule_market_thinker_scan(self, market_key: str, every_s: float) -> None:
        self._run_market_thinker_scan(market_key, force=False, min_interval_s=every_s)

    def _write_market_trader_status(self, market_key: str, payload: Dict[str, Any]) -> None:
        path = self.market_trader_paths.get(market_key, "")
        if not path:
            return
        try:
            _safe_write_json(path, payload)
        except Exception:
            pass

    def _run_stock_trader_step(self, force: bool = False, min_interval_s: float = 18.0) -> None:
        market_key = "stocks"
        if self._market_trader_busy.get(market_key, False):
            return
        last_ts = float(self._last_market_trader_ts.get(market_key, 0.0) or 0.0)
        if (not force) and ((time.time() - last_ts) < float(min_interval_s)):
            return
        self._market_trader_busy[market_key] = True
        self._append_market_log(market_key, "[TRADER] Running stocks trader step...")
        self._refresh_parallel_market_panels()

        def _worker() -> None:
            payload = run_stock_trader_step(self.settings, self.hub_dir)
            payload["ts"] = int(time.time())
            self._write_market_trader_status(market_key, payload)
            actions = list(payload.get("actions", []) or [])

            def _finish() -> None:
                self._last_market_trader_ts[market_key] = time.time()
                self._market_trader_busy[market_key] = False
                self._append_market_log(
                    market_key,
                    f"[TRADER] {str(payload.get('state', 'READY'))} | {str(payload.get('msg', '') or '').strip()}",
                )
                for line in actions[-3:]:
                    self._append_market_log(market_key, f"[TRADER] {line}")
                self._refresh_parallel_market_panels()

            try:
                self.after(0, _finish)
            except Exception:
                self._last_market_trader_ts[market_key] = time.time()
                self._market_trader_busy[market_key] = False

        threading.Thread(target=_worker, daemon=True).start()

    def _run_forex_trader_step(self, force: bool = False, min_interval_s: float = 12.0) -> None:
        market_key = "forex"
        if self._market_trader_busy.get(market_key, False):
            return
        last_ts = float(self._last_market_trader_ts.get(market_key, 0.0) or 0.0)
        if (not force) and ((time.time() - last_ts) < float(min_interval_s)):
            return
        self._market_trader_busy[market_key] = True
        self._append_market_log(market_key, "[TRADER] Running forex trader step...")
        self._refresh_parallel_market_panels()

        def _worker() -> None:
            payload = run_forex_trader_step(self.settings, self.hub_dir)
            payload["ts"] = int(time.time())
            self._write_market_trader_status(market_key, payload)
            actions = list(payload.get("actions", []) or [])

            def _finish() -> None:
                self._last_market_trader_ts[market_key] = time.time()
                self._market_trader_busy[market_key] = False
                self._append_market_log(
                    market_key,
                    f"[TRADER] {str(payload.get('state', 'READY'))} | {str(payload.get('msg', '') or '').strip()}",
                )
                for line in actions[-3:]:
                    self._append_market_log(market_key, f"[TRADER] {line}")
                self._refresh_parallel_market_panels()

            try:
                self.after(0, _finish)
            except Exception:
                self._last_market_trader_ts[market_key] = time.time()
                self._market_trader_busy[market_key] = False

        threading.Thread(target=_worker, daemon=True).start()

    # ---- panedwindow anti-collapse helpers ----

    def _schedule_paned_clamp(self, pw: ttk.Panedwindow) -> None:
        """
        Debounced clamp so we don't fight the geometry manager mid-resize.

        IMPORTANT: use `after(1, ...)` instead of `after_idle(...)` so it still runs
        while the mouse is held during sash dragging (Tk often doesn't go "idle"
        until after the drag ends, which is exactly when panes can vanish).
        """
        try:
            if not pw or not int(pw.winfo_exists()):
                return
        except Exception:
            return

        key = str(pw)
        if key in self._paned_clamp_after_ids:
            return

        def _run():
            try:
                self._paned_clamp_after_ids.pop(key, None)
            except Exception:
                pass
            self._clamp_panedwindow_sashes(pw)

        try:
            self._paned_clamp_after_ids[key] = self.after(1, _run)
        except Exception:
            pass


    def _clamp_panedwindow_sashes(self, pw: ttk.Panedwindow) -> None:
        """
        Enforces each pane's configured 'minsize' by clamping sash positions.

        NOTE:
        ttk.Panedwindow.paneconfigure(pane) typically returns dict values like:
            {"minsize": ("minsize", "minsize", "Minsize", "140"), ...}
        so we MUST pull the last element when it's a tuple/list.
        """
        try:
            if not pw or not int(pw.winfo_exists()):
                return

            panes = list(pw.panes())
            if len(panes) < 2:
                return

            orient = str(pw.cget("orient"))
            total = pw.winfo_height() if orient == "vertical" else pw.winfo_width()
            if total <= 2:
                return

            def _get_minsize(pane_id) -> int:
                try:
                    cfg = pw.paneconfigure(pane_id)
                    ms = cfg.get("minsize", 0)

                    # ttk returns tuples like ('minsize','minsize','Minsize','140')
                    if isinstance(ms, (tuple, list)) and ms:
                        ms = ms[-1]

                    # sometimes it's already int/float-like, sometimes it's a string
                    return max(0, int(float(ms)))
                except Exception:
                    return 0

            mins: List[int] = [_get_minsize(p) for p in panes]

            # If total space is smaller than sum(mins), we still clamp as best-effort
            # by scaling mins down proportionally but never letting a pane hit 0.
            if sum(mins) >= total:
                # best-effort: keep every pane at least 24px so it can’t disappear
                floor = 24
                mins = [max(floor, m) for m in mins]

                # if even floors don't fit, just stop here (window minsize should prevent this)
                if sum(mins) >= total:
                    return

            # Two-pass clamp so constraints settle even with multiple sashes
            for _ in range(2):
                for i in range(len(panes) - 1):
                    min_pos = sum(mins[: i + 1])
                    max_pos = total - sum(mins[i + 1 :])

                    try:
                        cur = int(pw.sashpos(i))
                    except Exception:
                        continue

                    new = max(min_pos, min(max_pos, cur))
                    if new != cur:
                        try:
                            pw.sashpos(i, new)
                        except Exception:
                            pass


        except Exception:
            pass



    # ---- process control ----


    def _reader_thread(self, proc: subprocess.Popen, q: "queue.Queue[str]", prefix: str) -> None:
        try:
            # line-buffered text mode
            while True:
                line = proc.stdout.readline() if proc.stdout else ""
                if not line:
                    if proc.poll() is not None:
                        break
                    time.sleep(0.05)
                    continue
                q.put(f"{prefix}{line.rstrip()}")
        except Exception:
            pass
        finally:
            q.put(f"{prefix}[process exited]")

    def _start_process(self, p: ProcInfo, log_q: Optional["queue.Queue[str]"] = None, prefix: str = "") -> None:
        if p.proc and p.proc.poll() is None:
            return
        if not os.path.isfile(p.path):
            messagebox.showerror("Missing script", f"Cannot find: {p.path}")
            return

        env = os.environ.copy()
        env["POWERTRADER_HUB_DIR"] = self.hub_dir  # so rhcb writes where GUI reads
        env["POWERTRADER_PROJECT_DIR"] = self.project_dir
        prev_pp = str(env.get("PYTHONPATH", "") or "").strip()
        env["PYTHONPATH"] = self.project_dir if not prev_pp else (self.project_dir + os.pathsep + prev_pp)

        try:
            p.proc = subprocess.Popen(
                [sys.executable, "-u", p.path],  # -u for unbuffered prints
                cwd=self.project_dir,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            if log_q is not None:
                t = threading.Thread(target=self._reader_thread, args=(p.proc, log_q, prefix), daemon=True)
                t.start()
        except Exception as e:
            messagebox.showerror("Failed to start", f"{p.name} failed to start:\n{e}")


    def _stop_process(self, p: ProcInfo) -> None:
        if not p.proc or p.proc.poll() is not None:
            return
        try:
            p.proc.terminate()
        except Exception:
            pass

    @staticmethod
    def _pid_is_alive(pid: Optional[int]) -> bool:
        try:
            if pid is None or int(pid) <= 0:
                return False
            os.kill(int(pid), 0)
            return True
        except OSError:
            return False

    def _read_runner_pid(self) -> Optional[int]:
        try:
            if not os.path.isfile(self.runner_pid_path):
                return None
            with open(self.runner_pid_path, "r", encoding="utf-8") as f:
                raw = (f.read() or "").strip()
            pid = int(raw)
            return pid if pid > 0 else None
        except Exception:
            return None

    def _runner_is_running(self) -> bool:
        return self._pid_is_alive(self._read_runner_pid())

    def _read_runner_status(self) -> Dict[str, Any]:
        data = _safe_read_json(self.trader_status_path)
        if isinstance(data, dict):
            return data
        return {"state": "STOPPED", "ts": None}

    def _launch_runner_detached(self) -> bool:
        if self._runner_is_running():
            return True
        try:
            if os.path.exists(self.stop_flag_path):
                os.remove(self.stop_flag_path)
        except OSError:
            pass

        if not os.path.isfile(self.proc_runner.path):
            messagebox.showerror("Missing script", f"Cannot find: {self.proc_runner.path}")
            return False

        runner_log_path = os.path.join(self.runner_logs_dir, "runner_ui_launch.log")
        log_f = None
        try:
            log_f = open(runner_log_path, "a", encoding="utf-8")
            env = os.environ.copy()
            env["POWERTRADER_HUB_DIR"] = self.hub_dir
            env["POWERTRADER_PROJECT_DIR"] = self.project_dir
            prev_pp = str(env.get("PYTHONPATH", "") or "").strip()
            env["PYTHONPATH"] = self.project_dir if not prev_pp else (self.project_dir + os.pathsep + prev_pp)
            subprocess.Popen(
                [sys.executable, "-u", self.proc_runner.path],
                cwd=self.project_dir,
                env=env,
                stdout=log_f,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                text=True,
            )
            try:
                self.runner_log_q.put("[RUNNER] Started background supervisor\n")
            except Exception:
                pass
            return True
        except Exception as e:
            messagebox.showerror("Failed to start", f"Trade supervisor failed to start:\n{e}")
            return False
        finally:
            if log_f is not None:
                try:
                    log_f.close()
                except Exception:
                    pass

    def _request_runner_stop(self, wait_s: float = 5.0) -> None:
        try:
            with open(self.stop_flag_path, "w", encoding="utf-8") as f:
                f.write(str(int(time.time())))
        except Exception:
            pass

        deadline = time.time() + max(0.0, float(wait_s))
        while time.time() < deadline:
            st = self._read_runner_status()
            state = str(st.get("state", "")).upper().strip()
            if state == "STOPPED" or not self._runner_is_running():
                return
            time.sleep(0.2)

        pid = self._read_runner_pid()
        if not self._pid_is_alive(pid):
            return
        try:
            os.kill(int(pid), signal.SIGTERM)
            self.runner_log_q.put(f"[RUNNER] Sent SIGTERM to supervisor pid={pid}\n")
        except Exception:
            return

        deadline = time.time() + 2.0
        while time.time() < deadline:
            if not self._pid_is_alive(pid):
                return
            time.sleep(0.2)

        try:
            os.kill(int(pid), signal.SIGKILL)
            self.runner_log_q.put(f"[RUNNER] Sent SIGKILL to supervisor pid={pid}\n")
        except Exception:
            pass

    def start_neural(self) -> None:
        self.start_all_scripts()


    def start_trader(self) -> None:
        self.start_all_scripts()


    def stop_neural(self) -> None:
        self.stop_all_scripts()



    def stop_trader(self) -> None:
        self.stop_all_scripts()

    def toggle_all_scripts(self) -> None:
        runner_running = self._runner_is_running()

        # If anything is running (or we're waiting on runner readiness), toggle means "stop"
        if runner_running or bool(getattr(self, "_auto_start_trader_pending", False)):
            self.stop_all_scripts()
            return

        # Otherwise, toggle means "start"
        self.start_all_scripts()

    def _read_runner_ready(self) -> Dict[str, Any]:
        data = _safe_read_json(self.runner_ready_path)
        if isinstance(data, dict):
            return data
        return {"ready": False}

    def _poll_runner_ready_then_start_trader(self) -> None:
        self._auto_start_trader_pending = False

    def start_all_scripts(self) -> None:
        if self._runner_is_running():
            self._auto_start_trader_pending = False
            try:
                self.status.config(text="Trade supervisor already running")
            except Exception:
                pass
            return

        # Enforce flow: training must be current before starting background trading.
        all_trained = all(self._coin_is_trained(c) for c in self.coins) if self.coins else False
        if not all_trained:
            messagebox.showwarning(
                "Training required",
                "All coins must be trained before starting trades.\n\nUse Train All first."
            )
            return

        self._auto_start_trader_pending = False
        self._launch_runner_detached()


    def _coin_is_trained(self, coin: str) -> bool:
        coin = coin.upper().strip()
        folder = self.coin_folders.get(coin, "")
        if not folder or not os.path.isdir(folder):
            return False

        # If trainer reports it's currently training, it's not "trained" yet.
        try:
            st = _safe_read_json(os.path.join(folder, "trainer_status.json"))
            if isinstance(st, dict) and str(st.get("state", "")).upper() == "TRAINING":
                return False
        except Exception:
            pass

        stamp_path = os.path.join(folder, "trainer_last_training_time.txt")
        try:
            if not os.path.isfile(stamp_path):
                return False
            with open(stamp_path, "r", encoding="utf-8") as f:
                raw = (f.read() or "").strip()
            ts = float(raw) if raw else 0.0
            if ts <= 0:
                return False
            return (time.time() - ts) <= (14 * 24 * 60 * 60)
        except Exception:
            return False

    def _running_trainers(self) -> List[str]:
        running: List[str] = []

        # Trainers launched by this GUI instance
        for c, lp in self.trainers.items():
            try:
                if lp.info.proc and lp.info.proc.poll() is None:
                    running.append(c)
            except Exception:
                pass

        # Trainers launched elsewhere: look at per-coin status file
        for c in self.coins:
            try:
                coin = (c or "").strip().upper()
                folder = self.coin_folders.get(coin, "")
                if not folder or not os.path.isdir(folder):
                    continue

                status_path = os.path.join(folder, "trainer_status.json")
                st = _safe_read_json(status_path)

                if isinstance(st, dict) and str(st.get("state", "")).upper() == "TRAINING":
                    stamp_path = os.path.join(folder, "trainer_last_training_time.txt")

                    try:
                        if os.path.isfile(stamp_path) and os.path.isfile(status_path):
                            if os.path.getmtime(stamp_path) >= os.path.getmtime(status_path):
                                continue
                    except Exception:
                        pass

                    running.append(coin)
            except Exception:
                pass

        # de-dupe while preserving order
        out: List[str] = []
        seen = set()
        for c in running:
            cc = (c or "").strip().upper()
            if cc and cc not in seen:
                seen.add(cc)
                out.append(cc)
        return out



    def _training_status_map(self) -> Dict[str, str]:
        """
        Returns {coin: "TRAINED" | "TRAINING" | "NOT TRAINED"}.
        """
        running = set(self._running_trainers())
        out: Dict[str, str] = {}
        for c in self.coins:
            if c in running:
                out[c] = "TRAINING"
            elif self._coin_is_trained(c):
                out[c] = "TRAINED"
            else:
                out[c] = "NOT TRAINED"
        return out

    def train_selected_coin(self) -> None:
        coin = (getattr(self, 'train_coin_var', self.trainer_coin_var).get() or "").strip().upper()

        if not coin:
            return
        # Reuse the trainers pane runner — start trainer for selected coin
        self.start_trainer_for_selected_coin()

    def train_all_coins(self) -> None:
        # Start trainers for every coin (in parallel), then auto-start trading when all are trained.
        if not self.coins:
            return
        self._auto_start_trader_pending = True
        for c in self.coins:
            self.trainer_coin_var.set(c)
            self.start_trainer_for_selected_coin()

    def _maybe_auto_start_after_training(
        self,
        all_trained: bool,
        neural_running: bool,
        trader_running: bool,
        status_map: Dict[str, str],
    ) -> None:
        if neural_running or trader_running:
            return
        if self._runner_is_running():
            return
        if not all_trained:
            return
        if any(str(v).upper() == "TRAINING" for v in (status_map or {}).values()):
            return
        auto_on = bool(self.settings.get("auto_start_trading_when_all_trained", True))
        if not (bool(getattr(self, "_auto_start_trader_pending", False)) or auto_on):
            return
        self.start_all_scripts()

    def start_trainer_for_selected_coin(self) -> None:
        coin = (self.trainer_coin_var.get() or "").strip().upper()
        if not coin:
            return

        # Stop the Neural Runner before any training starts (training modifies artifacts the runner reads)
        self.stop_neural()

        # --- IMPORTANT ---
        # Match the trader's folder convention:
        #   every coin (including BTC) runs from <main_neural_dir>/<COIN>
        coin_cwd = self.coin_folders.get(coin, self.project_dir)

        # Use the trainer script that lives INSIDE that coin's folder so outputs land in the right place.
        trainer_name = os.path.basename(str(self.settings.get("script_neural_trainer", "engines/pt_trainer.py")))

        # Ensure coin folder exists and has a trainer script copy.
        try:
            if not os.path.isdir(coin_cwd):
                os.makedirs(coin_cwd, exist_ok=True)

            src_project_trainer = os.path.join(self.project_dir, trainer_name)
            src_btc_trainer = os.path.join(self.coin_folders.get("BTC", self.project_dir), trainer_name)
            src_cfg_trainer = str(self.settings.get("script_neural_trainer", trainer_name))
            if os.path.isfile(src_project_trainer):
                src_trainer_path = src_project_trainer
            elif os.path.isfile(src_btc_trainer):
                src_trainer_path = src_btc_trainer
            else:
                src_trainer_path = src_cfg_trainer

            dst_trainer_path = os.path.join(coin_cwd, trainer_name)
            if os.path.isfile(src_trainer_path):
                shutil.copy2(src_trainer_path, dst_trainer_path)
        except Exception:
            pass

        trainer_path = os.path.join(coin_cwd, trainer_name)

        if not os.path.isfile(trainer_path):
            messagebox.showerror(
                "Missing trainer",
                f"Cannot find trainer for {coin} at:\n{trainer_path}"
            )
            return

        if coin in self.trainers and self.trainers[coin].info.proc and self.trainers[coin].info.proc.poll() is None:
            return


        try:
            patterns = [
                "trainer_last_training_time.txt",
                "trainer_status.json",
                "trainer_last_start_time.txt",
                "killer.txt",
                "memories_*.txt",
                "memory_weights_*.txt",
                "neural_perfect_threshold_*.txt",
            ]


            deleted = 0
            for pat in patterns:
                for fp in glob.glob(os.path.join(coin_cwd, pat)):
                    try:
                        os.remove(fp)
                        deleted += 1
                    except Exception:
                        pass

            if deleted:
                try:
                    self.status.config(text=f"Deleted {deleted} training file(s) for {coin} before training")
                except Exception:
                    pass
        except Exception:
            pass

        q: "queue.Queue[str]" = queue.Queue()
        info = ProcInfo(name=f"Trainer-{coin}", path=trainer_path)

        env = os.environ.copy()
        env["POWERTRADER_HUB_DIR"] = self.hub_dir
        # Trainer writes status/artifacts into BASE_DIR. Force per-coin BASE_DIR here.
        env["POWERTRADER_PROJECT_DIR"] = coin_cwd
        prev_pp = str(env.get("PYTHONPATH", "") or "").strip()
        env["PYTHONPATH"] = self.project_dir if not prev_pp else (self.project_dir + os.pathsep + prev_pp)

        try:
            # IMPORTANT: pass `coin` so neural_trainer trains the correct market instead of defaulting to BTC
            info.proc = subprocess.Popen(
                [sys.executable, "-u", info.path, coin],
                cwd=coin_cwd,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            t = threading.Thread(target=self._reader_thread, args=(info.proc, q, f"[{coin}] "), daemon=True)
            t.start()

            self.trainers[coin] = LogProc(info=info, log_q=q, thread=t, is_trainer=True, coin=coin)
        except Exception as e:
            messagebox.showerror("Failed to start", f"Trainer for {coin} failed to start:\n{e}")




    def stop_trainer_for_selected_coin(self) -> None:
        coin = (self.trainer_coin_var.get() or "").strip().upper()
        lp = self.trainers.get(coin)
        if not lp or not lp.info.proc or lp.info.proc.poll() is not None:
            return
        try:
            lp.info.proc.terminate()
        except Exception:
            pass


    def stop_all_scripts(self) -> None:
        self._auto_start_trader_pending = False
        self._request_runner_stop(wait_s=5.0)

        # Also reset the runner-ready gate file (best-effort)
        try:
            _safe_write_json(self.runner_ready_path, {"timestamp": time.time(), "ready": False, "stage": "stopped"})
        except Exception:
            pass


    def _on_timeframe_changed(self, event) -> None:
        """
        Immediate redraw when the user changes a timeframe in any CandleChart.
        Avoids waiting for the chart_refresh_seconds throttle in _tick().
        """
        try:
            chart = getattr(event, "widget", None)
            if not isinstance(chart, CandleChart):
                return

            coin = getattr(chart, "coin", None)
            if not coin:
                return

            self.coin_folders = build_coin_folders(self.settings["main_neural_dir"], self.coins)

            pos = self._last_positions.get(coin, {}) if isinstance(self._last_positions, dict) else {}
            buy_px = pos.get("current_buy_price", None)
            sell_px = pos.get("current_sell_price", None)
            trail_line = pos.get("trail_line", None)
            dca_line_price = pos.get("dca_line_price", None)
            avg_cost_basis = pos.get("avg_cost_basis", None)
            qty = pos.get("quantity", None)

            chart.refresh(
                self.coin_folders,
                current_buy_price=buy_px,
                current_sell_price=sell_px,
                trail_line=trail_line,
                dca_line_price=dca_line_price,
                avg_cost_basis=avg_cost_basis,
                quantity=qty,
            )

            # Keep the periodic refresh behavior consistent (prevents an immediate full refresh right after this).
            self._last_chart_refresh = time.time()
        except Exception:
            pass


    # ---- refresh loop ----
    def _drain_queue_to_text(self, q: "queue.Queue[str]", txt: tk.Text, max_lines: int = 2500) -> None:

        try:
            changed = False
            while True:
                line = q.get_nowait()
                txt.insert("end", line + "\n")
                changed = True
        except queue.Empty:
            pass
        except Exception:
            pass

        if changed:
            # trim very old lines
            try:
                current = int(txt.index("end-1c").split(".")[0])
                if current > max_lines:
                    txt.delete("1.0", f"{current - max_lines}.0")
            except Exception:
                pass
            self._style_log_text_widget(txt)
            txt.see("end")

    def _style_log_text_widget(self, txt: tk.Text) -> None:
        try:
            for tag in ("log_ts", "log_warn", "log_err", "log_launch"):
                txt.tag_remove(tag, "1.0", "end")
            end_line = int(txt.index("end-1c").split(".")[0])
            for idx in range(1, end_line + 1):
                line = txt.get(f"{idx}.0", f"{idx}.end")
                if not line:
                    continue
                if len(line) >= 19 and line[4] == "-" and line[7] == "-":
                    txt.tag_add("log_ts", f"{idx}.0", f"{idx}.19")
                lower = line.lower()
                if "[launch]" in lower:
                    txt.tag_add("log_launch", f"{idx}.0", f"{idx}.end")
                elif ("error" in lower) or ("failed" in lower):
                    txt.tag_add("log_err", f"{idx}.0", f"{idx}.end")
                elif ("restart" in lower) or ("exit code" in lower) or ("warning" in lower):
                    txt.tag_add("log_warn", f"{idx}.0", f"{idx}.end")
        except Exception:
            pass

    def _refresh_log_file_to_text(
        self,
        path: str,
        txt: tk.Text,
        cache_key: str,
        max_lines: int = 500,
        prefix_path: Optional[str] = None,
    ) -> None:
        try:
            parts = []
            mtimes = []
            for fp in [p for p in (prefix_path, path) if p]:
                try:
                    mtime = os.path.getmtime(fp)
                    mtimes.append((fp, mtime))
                except Exception:
                    continue

            sig = tuple(mtimes)
            if getattr(self, cache_key, object()) == sig:
                return
            setattr(self, cache_key, sig)

            for fp, _ in mtimes:
                try:
                    with open(fp, "r", encoding="utf-8", errors="ignore") as f:
                        lines = f.read().splitlines()
                    if prefix_path and fp == prefix_path and lines:
                        lines = [f"[launch] {ln}" for ln in lines]
                    parts.extend(lines[-max_lines:])
                except Exception:
                    continue

            txt.configure(state="normal")
            txt.delete("1.0", "end")
            if parts:
                txt.insert("1.0", "\n".join(parts[-max_lines:]) + "\n")
            else:
                name = os.path.basename(path)
                txt.insert("1.0", f"(waiting for {name} output)")
            txt.configure(state="normal")
            self._style_log_text_widget(txt)
            txt.see("end")
        except Exception:
            pass

    def _tick(self) -> None:
        fetcher_changed = False
        try:
            if hasattr(self, "fetcher") and self.fetcher:
                fetcher_changed = bool(self.fetcher.drain_results())
        except Exception:
            fetcher_changed = False
        runtime_early = self._read_runner_status()
        runner_state_early = str(runtime_early.get("state", "") or "").upper().strip()
        runner_pid_early = runtime_early.get("runner_pid", None)
        runner_ts_early = float(runtime_early.get("ts", 0.0) or 0.0)
        runner_live = bool(runner_pid_early) and ((time.time() - runner_ts_early) <= 12.0) and runner_state_early in {"RUNNING", "ERROR", "STOPPING"}
        if not runner_live:
            try:
                stocks_scan_s = max(5.0, float(self.settings.get("market_bg_stocks_interval_s", DEFAULT_SETTINGS.get("market_bg_stocks_interval_s", 15.0)) or 15.0))
            except Exception:
                stocks_scan_s = 15.0
            try:
                forex_scan_s = max(5.0, float(self.settings.get("market_bg_forex_interval_s", DEFAULT_SETTINGS.get("market_bg_forex_interval_s", 10.0)) or 10.0))
            except Exception:
                forex_scan_s = 10.0
            try:
                stocks_step_s = max(3.0, float(self.settings.get("stock_trader_step_interval_s", DEFAULT_SETTINGS.get("stock_trader_step_interval_s", 18.0)) or 18.0))
            except Exception:
                stocks_step_s = 18.0
            try:
                forex_step_s = max(3.0, float(self.settings.get("forex_trader_step_interval_s", DEFAULT_SETTINGS.get("forex_trader_step_interval_s", 12.0)) or 12.0))
            except Exception:
                forex_step_s = 12.0
            stock_panel = self.market_panels.get("stocks", {}) if isinstance(self.market_panels, dict) else {}
            forex_panel = self.market_panels.get("forex", {}) if isinstance(self.market_panels, dict) else {}
            stocks_auto_scan = bool((stock_panel.get("auto_scan_var").get() if stock_panel.get("auto_scan_var") else True))
            forex_auto_scan = bool((forex_panel.get("auto_scan_var").get() if forex_panel.get("auto_scan_var") else True))
            stocks_auto_step = bool((stock_panel.get("auto_step_var").get() if stock_panel.get("auto_step_var") else True))
            forex_auto_step = bool((forex_panel.get("auto_step_var").get() if forex_panel.get("auto_step_var") else True))
            try:
                self._schedule_market_snapshot_refresh("stocks", every_s=20.0)
                self._schedule_market_snapshot_refresh("forex", every_s=10.0)
            except Exception:
                pass
            try:
                if stocks_auto_scan:
                    self._schedule_market_thinker_scan("stocks", every_s=stocks_scan_s)
                if forex_auto_scan:
                    self._schedule_market_thinker_scan("forex", every_s=forex_scan_s)
            except Exception:
                pass
            try:
                if stocks_auto_step:
                    self._run_stock_trader_step(force=False, min_interval_s=stocks_step_s)
            except Exception:
                pass
            try:
                if forex_auto_step:
                    self._run_forex_trader_step(force=False, min_interval_s=forex_step_s)
            except Exception:
                pass
        try:
            self._refresh_parallel_market_panels()
        except Exception:
            pass

        runtime = self._read_runner_status()
        runtime_state = str(runtime.get("state", "STOPPED") or "STOPPED").upper().strip()
        thinker_pid = runtime.get("thinker_pid", None)
        trader_pid = runtime.get("trader_pid", None)
        restarts = runtime.get("restarts", {}) or {}
        neural_running = bool(thinker_pid)
        trader_running = bool(trader_pid)

        neural_txt = f"Neural: {runtime_state if neural_running else 'STOPPED'}"
        trader_txt = f"Trader: {runtime_state if trader_running else 'STOPPED'}"
        if thinker_pid:
            neural_txt += f" (pid {thinker_pid})"
        if trader_pid:
            trader_txt += f" (pid {trader_pid})"
        try:
            neural_txt += f" | restarts {int(restarts.get('thinker', 0) or 0)}"
            trader_txt += f" | restarts {int(restarts.get('trader', 0) or 0)}"
        except Exception:
            pass
        self.lbl_neural.config(text=neural_txt)
        self.lbl_trader.config(text=trader_txt)
        runtime_snapshot: Dict[str, Any] = {}
        try:
            runtime_snapshot = _safe_read_json(os.path.join(self.hub_dir, "runtime_state.json")) or {}
            bh = runtime_snapshot.get("broker_health", {}) if isinstance(runtime_snapshot.get("broker_health", {}), dict) else {}
            aq = runtime_snapshot.get("api_quota", {}) if isinstance(runtime_snapshot.get("api_quota", {}), dict) else {}
            total_15m = int(aq.get("total_15m", 0) or 0)
            def _fmt_state(key: str, default_name: str) -> str:
                row = bh.get(key, {}) if isinstance(bh.get(key, {}), dict) else {}
                state = str(row.get("state", "ok") or "ok").upper()
                q15 = int(row.get("quota_15m", 0) or 0)
                if state not in {"OK", "WARNING", "ERROR"}:
                    state = "OK"
                return f"{default_name} {state}({q15})"
            broker_txt = (
                "Broker API: "
                + _fmt_state("alpaca", "Alpaca")
                + " | "
                + _fmt_state("oanda", "OANDA")
                + " | "
                + _fmt_state("kucoin", "KuCoin")
                + f" | quota15m={total_15m}"
            )
            self.lbl_broker_health.config(text=broker_txt)
            checks = runtime_snapshot.get("checks", {}) if isinstance(runtime_snapshot.get("checks", {}), dict) else {}
            alerts = runtime_snapshot.get("alerts", {}) if isinstance(runtime_snapshot.get("alerts", {}), dict) else {}
            guard = runtime_snapshot.get("execution_guard", {}) if isinstance(runtime_snapshot.get("execution_guard", {}), dict) else {}
            incidents = runtime_snapshot.get("incidents_last_200", {}) if isinstance(runtime_snapshot.get("incidents_last_200", {}), dict) else {}
            drawdown_guard = runtime_snapshot.get("drawdown_guard", {}) if isinstance(runtime_snapshot.get("drawdown_guard", {}), dict) else {}
            stop_flag = runtime_snapshot.get("stop_flag", {}) if isinstance(runtime_snapshot.get("stop_flag", {}), dict) else {}
            market_loop = runtime_snapshot.get("market_loop", {}) if isinstance(runtime_snapshot.get("market_loop", {}), dict) else {}
            guard_markets = guard.get("markets", {}) if isinstance(guard.get("markets", {}), dict) else {}
            guard_active = 0
            ts_now = int(runtime_snapshot.get("ts", 0) or 0)
            for row in guard_markets.values():
                if not isinstance(row, dict):
                    continue
                if int(row.get("disabled_until", 0) or 0) > ts_now:
                    guard_active += 1
            ck_txt = (
                "Checklist: "
                + f"checks={'PASS' if bool(checks.get('ok', False)) else 'FAIL'} | "
                + f"alerts={str(alerts.get('severity', 'n/a') or 'n/a').upper()} | "
                + f"quota={str(aq.get('status', 'n/a') or 'n/a').upper()} | "
                + f"guard={'ON' if guard_active > 0 else 'OFF'} | "
                + f"inc1h={int(incidents.get('count_1h', 0) or 0)}"
            )
            self.lbl_system_checklist.config(text=ck_txt)
            dd_recent = bool(drawdown_guard.get("triggered_recent", False))
            dd_txt = "TRIGGERED" if dd_recent else "OK"
            sf_active = bool(stop_flag.get("active", False))
            sf_txt = "ON" if sf_active else "OFF"
            try:
                loop_age = max(0, int(time.time()) - int(market_loop.get("ts", 0) or 0))
            except Exception:
                loop_age = -1
            if loop_age >= 0 and loop_age <= 600:
                loop_txt = f"{loop_age}s old"
            else:
                loop_txt = "stale"
            self.lbl_runtime_guard.config(
                text=f"Safety: stop-flag {sf_txt} | drawdown guard {dd_txt} | market loops {loop_txt}"
            )
        except Exception:
            pass

        # Start All is now a toggle (Start/Stop)
        try:
            if hasattr(self, "btn_toggle_all") and self.btn_toggle_all:
                if neural_running or trader_running or bool(getattr(self, "_auto_start_trader_pending", False)):
                    self.btn_toggle_all.config(text="Stop Trades")
                else:
                    self.btn_toggle_all.config(text="Start Trades")
        except Exception:
            pass

        # --- flow gating: Train -> Start All ---
        status_map = self._training_status_map()
        all_trained = all(v == "TRAINED" for v in status_map.values()) if status_map else False
        try:
            self._maybe_auto_start_after_training(all_trained, neural_running, trader_running, status_map)
        except Exception:
            pass
        # Refresh state after possible auto-start.
        runtime = self._read_runner_status()
        runtime_state = str(runtime.get("state", "STOPPED") or "STOPPED").upper().strip()
        thinker_pid = runtime.get("thinker_pid", None)
        trader_pid = runtime.get("trader_pid", None)
        neural_running = bool(thinker_pid)
        trader_running = bool(trader_pid)

        # Disable Start All until training is done (but always allow it if something is already running/pending,
        # so the user can still stop everything).
        can_toggle_all = True
        if (not all_trained) and (not neural_running) and (not trader_running) and (not self._auto_start_trader_pending):
            can_toggle_all = False

        try:
            self.btn_toggle_all.configure(state=("normal" if can_toggle_all else "disabled"))
        except Exception:
            pass
        try:
            self._update_global_command_bar(
                runtime_state=runtime_state,
                neural_running=neural_running,
                trader_running=trader_running,
                runtime_snapshot=runtime_snapshot,
                can_toggle=can_toggle_all,
            )
        except Exception:
            pass

        # Make the Start/Stop button intent explicit when gated by training.
        try:
            if not can_toggle_all:
                self.btn_toggle_all.configure(text="Start Trades (Train All First)")
            elif neural_running or trader_running or bool(getattr(self, "_auto_start_trader_pending", False)):
                self.btn_toggle_all.configure(text="Stop Trades")
            else:
                self.btn_toggle_all.configure(text="Start Trades")
        except Exception:
            pass

        # Training overview + per-coin list
        try:
            training_running = [c for c, s in status_map.items() if s == "TRAINING"]
            not_trained = [c for c, s in status_map.items() if s == "NOT TRAINED"]
            done_tokens = ("DONE", "COMPLETE", "COMPLETED", "FINISHED", "READY")

            if training_running:
                self.lbl_training_overview.config(text=f"Training: RUNNING ({', '.join(training_running)})")
            elif not_trained:
                self.lbl_training_overview.config(text=f"Training: REQUIRED ({len(not_trained)} not trained)")
            else:
                self.lbl_training_overview.config(text="Training: Idle (all trained)")

            # show each coin status (ONLY redraw the list if it actually changed)
            sig = tuple((c, status_map.get(c, "N/A")) for c in self.coins)
            display_lines = []
            for c, st in sig:
                line_txt = f"{c}: {st}"
                if str(st).upper() == "TRAINING":
                    try:
                        folder = self.coin_folders.get(c, "")
                        status_path = os.path.join(folder, "trainer_status.json") if folder else ""
                        st_info = _safe_read_json(status_path) if status_path else None
                        pct_val = None
                        if isinstance(st_info, dict) and ("pct" in st_info):
                            try:
                                pct_val = max(0, min(100, int(float(st_info.get("pct", 0)))))
                            except Exception:
                                pct_val = None
                        if pct_val is not None:
                            line_txt = f"{c}: TRAINING {pct_val}%"
                    except Exception:
                        pass
                display_lines.append(line_txt)

            display_sig = tuple(display_lines)
            if getattr(self, "_last_training_sig", None) != display_sig:
                self._last_training_sig = display_sig
                self.training_list.delete(0, "end")
                for line_txt in display_lines:
                    self.training_list.insert("end", line_txt)

            total_training = len(sig)
            completed_training = 0
            for _, st in sig:
                up_st = str(st or "").upper().strip()
                if up_st == "TRAINED":
                    completed_training += 1
                elif any(tok in up_st for tok in done_tokens):
                    completed_training += 1
            if total_training < 0:
                total_training = 0
            if completed_training < 0:
                completed_training = 0
            if completed_training > total_training:
                completed_training = total_training
            progress_pct = int(round((100.0 * completed_training / total_training), 0)) if total_training > 0 else 0
            self.lbl_training_progress.config(
                text=f"Progress: {progress_pct}% ({completed_training} / {total_training})"
            )

            # show gating hint for the detached trade supervisor
            if not all_trained:
                self.lbl_flow_hint.config(text="Flow: Train All required → then Start Trades")
            elif self._auto_start_trader_pending:
                self.lbl_flow_hint.config(text="Flow: Starting supervisor")
            elif neural_running or trader_running:
                self.lbl_flow_hint.config(text="Flow: Training idle | Trades running")
            else:
                self.lbl_flow_hint.config(text="Flow: No training running")

            # System action guidance (single-line "what to do next").
            action_hint = ""
            issue_required = _safe_read_json(self.user_action_required_path)
            if isinstance(issue_required, dict) and issue_required:
                title = str(issue_required.get("title", "User action required") or "User action required").strip()
                action_hint = f"Next: ACTION REQUIRED - {title}"
            elif not all_trained:
                lead = ", ".join(not_trained[:3])
                if len(not_trained) > 3:
                    lead += ", ..."
                action_hint = f"Next: Train remaining coins ({lead}) before starting trades."
            elif self._auto_start_trader_pending:
                action_hint = "Next: waiting for supervisor startup and health checks."
            elif neural_running or trader_running:
                action_hint = "System healthy: trading loop is running. Monitor Portfolio, Trades, and History."
            else:
                action_hint = "Next: click Start Trades to run thinker + trader."
            try:
                runtime_snapshot = _safe_read_json(os.path.join(self.hub_dir, "runtime_state.json")) or {}
                alerts = runtime_snapshot.get("alerts", {}) if isinstance(runtime_snapshot.get("alerts", {}), dict) else {}
                qf = alerts.get("quickfix_suggestions", []) if isinstance(alerts.get("quickfix_suggestions", []), list) else []
                if qf:
                    action_hint += f" | Quick fix: {str(qf[0])[:140]}"
            except Exception:
                pass
            try:
                self.lbl_system_action.config(text=action_hint)
            except Exception:
                pass
        except Exception:
            pass

        # neural overview bars (mtime-cached inside)
        self._refresh_neural_overview()

        # trader status -> current trades table (now mtime-cached inside)
        self._refresh_trader_status()

        # pnl ledger -> realized profit (now mtime-cached inside)
        self._refresh_pnl()

        # trade history (now mtime-cached inside)
        self._refresh_trade_history()

        # One-time relaunch summary popup: compare current P/L snapshot vs last session.
        try:
            self._maybe_show_while_you_were_gone_popup()
        except Exception:
            pass


        # charts (throttle)
        now = time.time()
        if fetcher_changed or (now - self._last_chart_refresh) >= float(self.settings.get("chart_refresh_seconds", 10.0)):
            # account value chart (internally mtime-cached already)
            try:
                if self.account_chart:
                    self.account_chart.refresh()
            except Exception:
                pass

            # Only rebuild coin_folders when inputs change (avoids directory scans every refresh)
            try:
                cf_sig = (self.settings.get("main_neural_dir"), tuple(self.coins))
                if getattr(self, "_coin_folders_sig", None) != cf_sig:
                    self._coin_folders_sig = cf_sig
                    self.coin_folders = build_coin_folders(self.settings["main_neural_dir"], self.coins)
            except Exception:
                try:
                    self.coin_folders = build_coin_folders(self.settings["main_neural_dir"], self.coins)
                except Exception:
                    pass

            # Refresh ONLY the currently visible coin tab (prevents O(N_coins) network/plot stalls)
            selected_tab = None

            # Primary: our custom chart pages (multi-row tab buttons)
            try:
                selected_tab = getattr(self, "_current_chart_page", None)
            except Exception:
                selected_tab = None

            # Fallback: old notebook-based UI (if it exists)
            if not selected_tab:
                try:
                    if hasattr(self, "nb") and self.nb:
                        selected_tab = self.nb.tab(self.nb.select(), "text")
                except Exception:
                    selected_tab = None

            if selected_tab and str(selected_tab).strip().upper() != "ACCOUNT":
                coin = str(selected_tab).strip().upper()
                chart = self.charts.get(coin)
                if chart:
                    pos = self._last_positions.get(coin, {}) if isinstance(self._last_positions, dict) else {}
                    buy_px = pos.get("current_buy_price", None)
                    sell_px = pos.get("current_sell_price", None)
                    trail_line = pos.get("trail_line", None)
                    dca_line_price = pos.get("dca_line_price", None)
                    avg_cost_basis = pos.get("avg_cost_basis", None)
                    qty = pos.get("quantity", None)

                    try:
                        chart.refresh(
                            self.coin_folders,
                            current_buy_price=buy_px,
                            current_sell_price=sell_px,
                            trail_line=trail_line,
                            dca_line_price=dca_line_price,
                            avg_cost_basis=avg_cost_basis,
                            quantity=qty,
                        )
                        try:
                            self._refresh_chart_legend_panel()
                        except Exception:
                            pass
                    except Exception:
                        pass



            self._last_chart_refresh = now

        # drain logs into panes
        self._drain_queue_to_text(self.runner_log_q, self.runner_text)
        self._drain_queue_to_text(self.trader_log_q, self.trader_text)
        self._refresh_log_file_to_text(
            self.runner_log_path,
            self.runner_text,
            "_last_runner_log_sig",
            max_lines=500,
            prefix_path=self.runner_launch_log_path,
        )
        self._refresh_log_file_to_text(
            self.trader_log_path,
            self.trader_text,
            "_last_trader_log_sig",
            max_lines=500,
        )

        # trainer logs: show selected trainer output
        try:
            sel = (self.trainer_coin_var.get() or "").strip().upper()
            running = [c for c, lp in self.trainers.items() if lp.info.proc and lp.info.proc.poll() is None]
            self.trainer_status_lbl.config(text=f"running: {', '.join(running)}" if running else "(no trainers running)")

            lp = self.trainers.get(sel)
            if lp:
                self._drain_queue_to_text(lp.log_q, self.trainer_text)
        except Exception:
            pass

        self._refresh_chart_legend_panel()
        self._refresh_neural_overview_visibility()
        try:
            active_market = str(self.market_nb.tab(self.market_nb.select(), "text") or "Crypto")
        except Exception:
            active_market = "Crypto"
        self.status.config(text=f"{_now_str()} | View={active_market} | hub_dir={self.hub_dir} | Ctrl+, Settings")
        self.after(int(float(self.settings.get("ui_refresh_seconds", 1.0)) * 1000), self._tick)



    def _coerce_float_value(self, value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            if isinstance(value, (int, float)):
                fv = float(value)
                return fv if math.isfinite(fv) else None
        except Exception:
            pass
        txt = str(value or "").strip()
        if not txt:
            return None
        txt = txt.replace(",", "").replace("$", "")
        for tok in txt.split():
            try:
                fv = float(tok)
                if math.isfinite(fv):
                    return fv
            except Exception:
                continue
        return None

    def _collect_while_you_were_gone_snapshot(self) -> Dict[str, Any]:
        crypto_data = _safe_read_json(self.trader_data_path) or {}
        crypto_account = crypto_data.get("account", {}) if isinstance(crypto_data.get("account", {}), dict) else {}
        pnl_data = _safe_read_json(self.pnl_ledger_path) or {}

        stocks_status = _safe_read_json(self.market_status_paths.get("stocks", "")) or {}
        stocks_trader = _safe_read_json(self.market_trader_paths.get("stocks", "")) or {}
        forex_status = _safe_read_json(self.market_status_paths.get("forex", "")) or {}
        forex_trader = _safe_read_json(self.market_trader_paths.get("forex", "")) or {}

        crypto_equity = self._coerce_float_value(crypto_account.get("total_account_value"))
        crypto_realized = self._coerce_float_value(pnl_data.get("total_realized_profit_usd"))

        stocks_equity = self._coerce_float_value(stocks_trader.get("account_value_usd"))
        if stocks_equity is None:
            stocks_equity = self._coerce_float_value(stocks_status.get("equity"))
        stocks_realized = self._coerce_float_value(stocks_status.get("realized_pnl"))
        if stocks_realized is None:
            stocks_realized = self._coerce_float_value(stocks_trader.get("realized_pnl"))

        forex_equity = self._coerce_float_value(forex_trader.get("account_value_usd"))
        if forex_equity is None:
            forex_equity = self._coerce_float_value(forex_status.get("nav"))
        forex_realized = self._coerce_float_value(forex_status.get("realized_pnl"))
        if forex_realized is None:
            forex_realized = self._coerce_float_value(forex_status.get("pl_value"))
        if forex_realized is None:
            forex_realized = self._coerce_float_value(forex_trader.get("realized_pnl"))

        return {
            "ts": int(time.time()),
            "markets": {
                "crypto": {"account_value": crypto_equity, "realized_pnl": crypto_realized},
                "stocks": {"account_value": stocks_equity, "realized_pnl": stocks_realized},
                "forex": {"account_value": forex_equity, "realized_pnl": forex_realized},
            },
        }

    def _snapshot_has_values(self, snapshot: Dict[str, Any]) -> bool:
        if not isinstance(snapshot, dict):
            return False
        markets = snapshot.get("markets", {}) if isinstance(snapshot.get("markets", {}), dict) else {}
        for row in markets.values():
            if not isinstance(row, dict):
                continue
            if (self._coerce_float_value(row.get("account_value")) is not None) or (
                self._coerce_float_value(row.get("realized_pnl")) is not None
            ):
                return True
        return False

    def _format_while_you_were_gone_summary(self, previous: Dict[str, Any], current: Dict[str, Any]) -> str:
        prev_ts = int(previous.get("ts", 0) or 0) if isinstance(previous, dict) else 0
        cur_ts = int(current.get("ts", 0) or 0) if isinstance(current, dict) else 0
        prev_t = time.strftime("%Y-%m-%d %H:%M:%S %Z", time.localtime(prev_ts)) if prev_ts > 0 else "N/A"
        cur_t = time.strftime("%Y-%m-%d %H:%M:%S %Z", time.localtime(cur_ts)) if cur_ts > 0 else "N/A"
        lines = [f"Session window: {prev_t} → {cur_t}", ""]

        prev_markets = previous.get("markets", {}) if isinstance(previous.get("markets", {}), dict) else {}
        cur_markets = current.get("markets", {}) if isinstance(current.get("markets", {}), dict) else {}
        total_delta = 0.0
        total_count = 0
        ordered = [("crypto", "Crypto"), ("stocks", "Stocks"), ("forex", "Forex")]

        for key, label in ordered:
            prev_row = prev_markets.get(key, {}) if isinstance(prev_markets.get(key, {}), dict) else {}
            cur_row = cur_markets.get(key, {}) if isinstance(cur_markets.get(key, {}), dict) else {}
            prev_equity = self._coerce_float_value(prev_row.get("account_value"))
            cur_equity = self._coerce_float_value(cur_row.get("account_value"))
            prev_realized = self._coerce_float_value(prev_row.get("realized_pnl"))
            cur_realized = self._coerce_float_value(cur_row.get("realized_pnl"))

            if (prev_equity is not None) and (cur_equity is not None):
                delta = float(cur_equity - prev_equity)
                total_delta += delta
                total_count += 1
                acct_txt = f"{_fmt_money(prev_equity)} -> {_fmt_money(cur_equity)} (Delta {'+' if delta >= 0 else '-'}${abs(delta):,.2f})"
            else:
                acct_txt = "N/A"

            if (prev_realized is not None) and (cur_realized is not None):
                rdelta = float(cur_realized - prev_realized)
                realized_txt = f"{_fmt_money(prev_realized)} -> {_fmt_money(cur_realized)} (Delta {'+' if rdelta >= 0 else '-'}${abs(rdelta):,.2f})"
            else:
                realized_txt = "N/A"

            lines.append(f"{label}:")
            lines.append(f"  Account: {acct_txt}")
            lines.append(f"  Realized: {realized_txt}")
            lines.append("")

        if total_count > 0:
            lines.append(f"Combined account Delta: {'+' if total_delta >= 0 else '-'}${abs(total_delta):,.2f}")
        else:
            lines.append("Combined account Delta: N/A")
        return "\n".join(lines).strip()

    def _persist_while_you_were_gone_snapshot(self) -> None:
        snap = self._collect_while_you_were_gone_snapshot()
        if self._snapshot_has_values(snap):
            _safe_write_json(self.while_you_were_gone_snapshot_path, snap)

    def _maybe_show_while_you_were_gone_popup(self) -> None:
        if bool(getattr(self, "_while_you_were_gone_shown", False)):
            return
        onboarding = _safe_read_json(self.onboarding_state_path) or {}
        if isinstance(onboarding, dict) and (not bool(onboarding.get("completed", False))):
            return
        current = self._collect_while_you_were_gone_snapshot()
        if not self._snapshot_has_values(current):
            return
        previous = self._while_you_were_gone_previous if isinstance(self._while_you_were_gone_previous, dict) else {}
        self._while_you_were_gone_shown = True
        if self._snapshot_has_values(previous):
            summary = self._format_while_you_were_gone_summary(previous, current)
            if summary:
                messagebox.showinfo("While You Were Gone", summary)
        self._while_you_were_gone_previous = current
        self._persist_while_you_were_gone_snapshot()



    def _refresh_chart_legend_panel(self) -> None:
        widget = getattr(self, "chart_legend_text", None)
        box = getattr(self, "chart_legend_box", None)
        btn = getattr(self, "btn_chart_legend_toggle", None)
        header = getattr(self, "chart_legend_header", None)
        scroll = getattr(self, "chart_legend_scroll", None)
        if widget is None or box is None:
            return

        try:
            collapsed = bool(getattr(self, "chart_legend_collapsed").get())
        except Exception:
            collapsed = False

        page = str(getattr(self, "_current_chart_page", "ACCOUNT") or "ACCOUNT").strip().upper()
        if page == "ACCOUNT":
            text = "Select a coin chart to view legend details."
            try:
                if header is not None and header.winfo_manager():
                    header.pack_forget()
            except Exception:
                pass
            try:
                if box.winfo_manager():
                    box.pack_forget()
            except Exception:
                pass
        else:
            chart = self.charts.get(page) if isinstance(getattr(self, "charts", None), dict) else None
            text = str(getattr(chart, "_legend_panel_text", "") or "").strip()
            if not text:
                text = f"{page}: waiting for chart data..."
            try:
                if header is not None and (not header.winfo_manager()):
                    header.pack(fill="x", padx=6, pady=(0, 0), before=box)
            except Exception:
                pass
            try:
                if collapsed:
                    if box.winfo_manager():
                        box.pack_forget()
                elif not box.winfo_manager():
                    box.pack(fill="x", padx=6, pady=(0, 6))
            except Exception:
                pass
            try:
                mode = str(getattr(chart, "_legend_mode", "clean") or "clean").strip().lower()
                widget.configure(height=(11 if mode == "detailed" else 6))
                needs_scroll = bool(getattr(chart, "_legend_needs_scroll", False))
                if scroll is not None:
                    if needs_scroll and (not collapsed):
                        if not scroll.winfo_manager():
                            scroll.pack(side="right", fill="y")
                    elif scroll.winfo_manager():
                        scroll.pack_forget()
            except Exception:
                pass
        if page == "ACCOUNT":
            try:
                if scroll is not None and scroll.winfo_manager():
                    scroll.pack_forget()
            except Exception:
                pass

        try:
            if btn is not None:
                btn.configure(text=("Show" if (page != "ACCOUNT" and collapsed) else "Hide"))
                btn.configure(state=("disabled" if page == "ACCOUNT" else "normal"))
        except Exception:
            pass

        try:
            widget.configure(state="normal")
            widget.delete("1.0", "end")
            for idx, line in enumerate(str(text).splitlines()):
                tag = "legend_head" if idx == 0 else ("legend_label" if ":" in line else "legend_value")
                widget.insert("end", line + ("\n" if idx < (len(str(text).splitlines()) - 1) else ""), (tag,))
            widget.configure(state="disabled")
        except Exception:
            pass

    def _draw_trades_table(self) -> None:
        canvas = getattr(self, "trades_canvas", None)
        cols = getattr(self, "trades_cols", ())
        rows = list(getattr(self, "_trades_table_rows", []) or [])
        if canvas is None or not cols:
            return

        try:
            view_w = max(200, int(canvas.winfo_width()))
            view_h = max(80, int(canvas.winfo_height()))
        except Exception:
            return

        base = dict(getattr(self, "_trades_base_widths", {}) or {})
        total_base = sum(base.get(c, 100) for c in cols) or 1
        usable_w = max(240, view_w - 4)
        scale = max(0.65, float(usable_w) / float(total_base))
        widths = {c: max(60, int(base.get(c, 100) * scale)) for c in cols}
        total_w = sum(widths.values())
        header_h = int(getattr(self, "_trades_header_height", 28) or 28)
        row_h = int(getattr(self, "_trades_row_height", 28) or 28)
        total_h = header_h + (len(rows) * row_h)

        try:
            canvas.delete("all")
        except Exception:
            return

        canvas.configure(scrollregion=(0, 0, total_w, max(total_h, view_h)))

        x = 0
        group_break_after = {"value", "realized_usd", "sell_pnl", "dca_24h"}
        for col in cols:
            w = widths[col]
            canvas.create_rectangle(x, 0, x + w, header_h, fill=DARK_BG2, outline=DARK_BORDER, width=1)
            anchor = "center"
            tx = x + (w / 2)
            if col in getattr(self, "trades_numeric_cols", set()):
                anchor = "e"
                tx = x + w - 8
            elif col in getattr(self, "trades_center_cols", set()):
                anchor = "center"
            canvas.create_text(
                tx,
                header_h / 2,
                text=str(getattr(self, "trades_header_labels", {}).get(col, col)),
                fill=DARK_ACCENT,
                font=("TkDefaultFont", 10, "bold"),
                anchor=anchor,
            )
            if col in group_break_after:
                canvas.create_line(x + w, 0, x + w, total_h, fill=DARK_ACCENT2, width=1)
            x += w

        canvas.create_line(0, header_h, total_w, header_h, fill=DARK_ACCENT2, width=2)

        for row_index, row in enumerate(rows):
            y0 = header_h + (row_index * row_h)
            y1 = y0 + row_h
            row_bg = DARK_PANEL if (row_index % 2) == 0 else "#0C1827"
            canvas.create_rectangle(0, y0, total_w, y1, fill=row_bg, outline=DARK_BORDER, width=1)

            x = 0
            for col in cols:
                w = widths[col]
                cell_val = str(row.get(col, ""))
                fg = DARK_FG
                if col in {"unrealized_usd", "realized_usd"}:
                    try:
                        num = float(str(cell_val).replace("$", "").replace(",", ""))
                        fg = DARK_ACCENT if num > 0 else ("#FF6B57" if num < 0 else DARK_FG)
                    except Exception:
                        fg = DARK_FG
                elif col == "sell_pnl":
                    try:
                        raw = str(cell_val).replace("%", "").replace(",", "")
                        num = float(raw)
                        fg = DARK_ACCENT if num > 0 else ("#FF6B57" if num < 0 else DARK_FG)
                    except Exception:
                        fg = DARK_FG
                elif col == "coin":
                    fg = DARK_ACCENT2

                anchor = "w"
                tx = x + 8
                if col in getattr(self, "trades_numeric_cols", set()):
                    anchor = "e"
                    tx = x + w - 8
                elif col in getattr(self, "trades_center_cols", set()):
                    anchor = "center"
                    tx = x + (w / 2)

                canvas.create_text(
                    tx,
                    y0 + (row_h / 2),
                    text=cell_val,
                    fill=fg,
                    font=("TkDefaultFont", 10, "bold" if col in {"coin", "value", "unrealized_usd", "realized_usd", "sell_pnl"} else "normal"),
                    anchor=anchor,
                )
                if col in group_break_after:
                    canvas.create_line(x + w, y0, x + w, y1, fill=DARK_BORDER, width=1)
                x += w


    def _refresh_neural_overview_visibility(self) -> None:
        box = getattr(self, "neural_box", None)
        if box is None:
            return

        current_page = str(getattr(self, "_current_chart_page", "ACCOUNT") or "ACCOUNT").strip().upper()
        should_show = (current_page == "ACCOUNT")

        try:
            is_visible = bool(box.winfo_manager())
        except Exception:
            is_visible = True

        try:
            if should_show and (not is_visible):
                box.pack(fill="both", expand=True, padx=6, pady=(0, 6))
            elif (not should_show) and is_visible:
                box.pack_forget()
        except Exception:
            pass

    def _set_system_status_colors(self, state_txt: str, heartbeat_stale: bool = False) -> None:
        state = str(state_txt or "").upper().strip()
        if state == "RUNNING":
            fg = DARK_ACCENT
        elif state == "STOPPING":
            fg = "#FFCC66"
        elif state == "ERROR":
            fg = "#FF6B57"
        else:
            fg = DARK_FG
        stale_fg = "#FFCC66" if heartbeat_stale else fg
        try:
            self.lbl_neural.config(foreground=fg)
            self.lbl_trader.config(foreground=fg)
            self.lbl_last_status.config(foreground=stale_fg)
        except Exception:
            pass


    def _refresh_trader_status(self) -> None:
        # mtime cache: rebuilding the whole tree every tick is expensive with many rows
        try:
            runtime_mtime = os.path.getmtime(self.trader_status_path)
        except Exception:
            runtime_mtime = None
        try:
            detail_mtime = os.path.getmtime(self.trader_data_path)
        except Exception:
            detail_mtime = None

        mtime = (runtime_mtime, detail_mtime)

        if getattr(self, "_last_trader_status_mtime", object()) == mtime:
            return
        self._last_trader_status_mtime = mtime

        runtime = _safe_read_json(self.trader_status_path)
        detail = _safe_read_json(self.trader_data_path)
        if not runtime and not detail:
            self.lbl_last_status.config(text="Last status: N/A (no trader status yet)")

            # account summary (right-side status area)
            try:
                self.lbl_acct_total_value.config(text="N/A")
                self.lbl_acct_holdings_value.config(text="N/A")
                self.lbl_acct_buying_power.config(text="N/A")
                self.lbl_acct_percent_in_trade.config(text="N/A")
                self.lbl_acct_dca_spread.config(text="N/A")
                self.lbl_acct_dca_single.config(text="N/A")
                self.lbl_pnl.config(text="N/A")
                self.lbl_selected_coin_summary.config(text="Selected: ACCOUNT")
            except Exception:
                pass

            # clear tree (once; subsequent ticks are mtime-short-circuited)
            self._trades_table_rows = []
            self._draw_trades_table()
            return

        runtime = runtime if isinstance(runtime, dict) else {}
        detail = detail if isinstance(detail, dict) else {}

        ts = runtime.get("ts", detail.get("timestamp"))
        status_note = str(runtime.get("msg", "") or detail.get("status_note", "") or "").strip()
        autopilot = _safe_read_json(self.autopilot_status_path)
        issue_required = _safe_read_json(self.user_action_required_path)
        runtime_checks = _safe_read_json(self.runtime_startup_checks_path)
        auto_note = ""
        if isinstance(autopilot, dict) and autopilot:
            mode = "AUTO"
            if bool(autopilot.get("api_unstable", False)):
                mode += ":stabilizing"
            elif bool(autopilot.get("markets_healthy", False)):
                mode += ":healthy"
            else:
                mode += ":monitoring"
            auto_note = mode
        issue_note = ""
        if isinstance(issue_required, dict) and issue_required:
            issue_title = str(issue_required.get("title", "User action required") or "User action required").strip()
            issue_note = f"ACTION REQUIRED: {issue_title}"
        checks_note = ""
        if isinstance(runtime_checks, dict) and runtime_checks:
            errs = list(runtime_checks.get("errors", []) or [])
            warns = list(runtime_checks.get("warnings", []) or [])
            if errs:
                checks_note = f"STARTUP CHECKS FAILED ({len(errs)})"
            elif warns:
                checks_note = f"Startup warnings ({len(warns)})"
        if auto_note:
            status_note = (status_note + " | " if status_note else "") + auto_note
        if issue_note:
            status_note = (status_note + " | " if status_note else "") + issue_note
        if checks_note:
            status_note = (status_note + " | " if status_note else "") + checks_note
        state_txt = str(runtime.get("state", "") or "").upper().strip()
        heartbeat_stale = False
        try:
            if isinstance(ts, (int, float)):
                heartbeat_stale = (time.time() - float(ts)) > 6.0
        except Exception:
            heartbeat_stale = False
        try:
            if isinstance(ts, (int, float)):
                base_txt = f"Trade State: {state_txt or 'UNKNOWN'} | Heartbeat: {time.strftime('%H:%M:%S', time.localtime(ts))}"
                if heartbeat_stale:
                    base_txt += " | STALE"
                if status_note:
                    base_txt += f" | {status_note}"
                self.lbl_last_status.config(text=self._format_market_state_line(base_txt, max_lines=8))
            else:
                self.lbl_last_status.config(
                    text=self._format_market_state_line(
                        f"Trade State: {state_txt or 'UNKNOWN'} | Heartbeat: (unknown)" + (f" | {status_note}" if status_note else ""),
                        max_lines=8,
                    )
                )
        except Exception:
            self.lbl_last_status.config(
                text=self._format_market_state_line(
                    f"Trade State: {state_txt or 'UNKNOWN'} | Heartbeat: (parse error)" + (f" | {status_note}" if status_note else ""),
                    max_lines=8,
                )
            )
        self._set_system_status_colors(state_txt, heartbeat_stale)

        if not detail:
            try:
                self.lbl_acct_total_value.config(text="N/A")
                self.lbl_acct_holdings_value.config(text="N/A")
                self.lbl_acct_buying_power.config(text="N/A")
                self.lbl_acct_percent_in_trade.config(text="N/A")
                self.lbl_acct_dca_spread.config(text="N/A")
                self.lbl_acct_dca_single.config(text="N/A")
                self.lbl_selected_coin_summary.config(text="Selected: ACCOUNT")
            except Exception:
                pass
            self._last_positions = {}
            self._trades_table_rows = []
            self._draw_trades_table()
            return

        # --- account summary (same info the trader prints above current trades) ---
        acct = detail.get("account", {}) or {}
        try:
            total_val = float(acct.get("total_account_value", 0.0) or 0.0)

            self._last_total_account_value = total_val

            self.lbl_acct_total_value.config(
                text=_fmt_money(acct.get('total_account_value', None))
            )
            self.lbl_acct_holdings_value.config(
                text=_fmt_money(acct.get('holdings_sell_value', None))
            )
            self.lbl_acct_buying_power.config(
                text=_fmt_money(acct.get('buying_power', None))
            )

            pit = acct.get("percent_in_trade", None)
            try:
                pit_txt = f"{float(pit):.2f}%"
            except Exception:
                pit_txt = "N/A"
            self.lbl_acct_percent_in_trade.config(text=pit_txt)


            # -------------------------
            # DCA affordability
            # - Entry allocation mirrors pt_trader.py:
            #     total_val * ((start_allocation_pct/100) / N) with min $0.50
            # - Each DCA buy mirrors pt_trader.py: dca_amount = value * dca multiplier  (=> total scales ~(1+multiplier)x per DCA)
            # -------------------------
            coins = getattr(self, "coins", None) or []
            n = len(coins)
            spread_levels = 0
            single_levels = 0

            if total_val > 0.0:
                alloc_pct = normalize_start_allocation_pct(
                    self.settings.get("start_allocation_pct", DEFAULT_SETTINGS.get("start_allocation_pct", 0.5)),
                    default_pct=float(DEFAULT_SETTINGS.get("start_allocation_pct", 0.5)),
                )
                if alloc_pct < 0.0:
                    alloc_pct = 0.0
                alloc_frac = alloc_pct / 100.0

                dca_mult = float(self.settings.get("dca_multiplier", 2.0) or 2.0)
                if dca_mult < 0.0:
                    dca_mult = 0.0
                dca_factor = 1.0 + dca_mult

                # Spread across all coins

                alloc_spread = total_val * alloc_frac
                if alloc_spread < 0.5:
                    alloc_spread = 0.5

                required = alloc_spread * n  # initial buys for all coins
                while required > 0.0 and (required * dca_factor) <= (total_val + 1e-9):
                    required *= dca_factor
                    spread_levels += 1


                # All DCA into a single coin
                alloc_single = total_val * alloc_frac
                if alloc_single < 0.5:
                    alloc_single = 0.5

                required = alloc_single  # initial buy for one coin
                while required > 0.0 and (required * dca_factor) <= (total_val + 1e-9):
                    required *= dca_factor
                    single_levels += 1



            # Show labels + number (one line each)
            self.lbl_acct_dca_spread.config(text=str(spread_levels))
            self.lbl_acct_dca_single.config(text=str(single_levels))


        except Exception:
            pass


        positions = detail.get("positions", {}) or {}
        self._last_positions = positions

        # --- precompute per-coin DCA count in rolling 24h (and after last SELL for that coin) ---
        dca_24h_by_coin: Dict[str, int] = {}
        realized_by_coin: Dict[str, float] = {}
        try:
            now = time.time()
            window_floor = now - (24 * 3600)

            trades = _read_trade_history_jsonl(self.trade_history_path) if self.trade_history_path else []

            last_sell_ts: Dict[str, float] = {}
            for tr in trades:
                sym = str(tr.get("symbol", "")).upper().strip()
                base = sym.split("-")[0].strip() if sym else ""
                if not base:
                    continue

                side = str(tr.get("side", "")).lower().strip()
                if side != "sell":
                    continue

                try:
                    tsf = float(tr.get("ts", 0))
                except Exception:
                    continue

                prev = float(last_sell_ts.get(base, 0.0))
                if tsf > prev:
                    last_sell_ts[base] = tsf

            for tr in trades:
                sym = str(tr.get("symbol", "")).upper().strip()
                base = sym.split("-")[0].strip() if sym else ""
                if not base:
                    continue

                side = str(tr.get("side", "")).lower().strip()
                if side != "buy":
                    continue

                tag = str(tr.get("tag") or "").upper().strip()
                if tag != "DCA":
                    continue

                try:
                    tsf = float(tr.get("ts", 0))
                except Exception:
                    continue

                start_ts = max(window_floor, float(last_sell_ts.get(base, 0.0)))
                if tsf >= start_ts:
                    dca_24h_by_coin[base] = int(dca_24h_by_coin.get(base, 0)) + 1

            for tr in trades:
                sym = str(tr.get("symbol", "")).upper().strip()
                base = sym.split("-")[0].strip() if sym else ""
                if not base:
                    continue
                try:
                    realized = float(tr.get("realized_profit_usd", 0.0) or 0.0)
                except Exception:
                    realized = 0.0
                if abs(realized) > 0.0:
                    realized_by_coin[base] = float(realized_by_coin.get(base, 0.0) or 0.0) + realized
        except Exception:
            dca_24h_by_coin = {}
            realized_by_coin = {}

        # rebuild table rows (only when file changes)
        table_rows = []

        selected_coin = str(getattr(self, "_current_chart_page", "ACCOUNT") or "ACCOUNT").strip().upper()
        selected_pos = positions.get(selected_coin, {}) if isinstance(positions, dict) else {}
        if selected_coin == "ACCOUNT":
            try:
                self.lbl_selected_coin_summary.config(text="Selected: ACCOUNT")
            except Exception:
                pass
        else:
            try:
                sel_qty = float(selected_pos.get("quantity", 0.0) or 0.0)
            except Exception:
                sel_qty = 0.0
            try:
                sel_realized = float(realized_by_coin.get(selected_coin, 0.0) or 0.0)
            except Exception:
                sel_realized = 0.0
            try:
                sel_avg = float(selected_pos.get("avg_cost_basis", 0.0) or 0.0)
            except Exception:
                sel_avg = 0.0
            try:
                sel_bid = float(selected_pos.get("current_sell_price", 0.0) or 0.0)
            except Exception:
                sel_bid = 0.0
            sel_unrealized = 0.0
            if sel_qty > 0.0 and sel_avg > 0.0 and sel_bid > 0.0:
                sel_unrealized = (sel_qty * sel_bid) - (sel_qty * sel_avg)
            try:
                sel_stage = int(selected_pos.get("dca_triggered_stages", 0) or 0)
            except Exception:
                sel_stage = 0
            try:
                trail_active = (float(selected_pos.get("trail_line", 0.0) or 0.0) > 0.0)
            except Exception:
                trail_active = False
            summary = (
                f"Selected: {selected_coin} | Qty {sel_qty:.6f}".rstrip("0").rstrip(".")
                + f" | Unrlzd {sel_unrealized:+.2f}"
                + f" | Rlz {sel_realized:+.2f}"
                + f" | Stage {sel_stage}"
                + f" | Trail {'On' if trail_active else 'Off'}"
            )
            try:
                self.lbl_selected_coin_summary.config(text=summary)
            except Exception:
                pass

        visible_row_index = 0
        for sym, pos in positions.items():
            coin = sym
            qty = pos.get("quantity", 0.0)

            # Hide "not in trade" rows (0 qty), but keep them in _last_positions for chart overlays
            try:
                if float(qty) <= 0.0:
                    continue
            except Exception:
                continue

            value = pos.get("value_usd", 0.0)
            avg_cost = pos.get("avg_cost_basis", 0.0)

            buy_price = pos.get("current_buy_price", 0.0)
            buy_pnl = pos.get("gain_loss_pct_buy", 0.0)

            sell_price = pos.get("current_sell_price", 0.0)
            sell_pnl = pos.get("gain_loss_pct_sell", 0.0)

            dca_stages = pos.get("dca_triggered_stages", 0)
            dca_24h = int(dca_24h_by_coin.get(str(coin).upper().strip(), 0))
            realized_usd = float(realized_by_coin.get(str(coin).upper().strip(), 0.0) or 0.0)

            try:
                qtyf = float(qty or 0.0)
            except Exception:
                qtyf = 0.0
            try:
                avgf = float(avg_cost or 0.0)
            except Exception:
                avgf = 0.0
            try:
                sellf = float(sell_price or 0.0)
            except Exception:
                sellf = 0.0
            unrealized_usd = 0.0
            if qtyf > 0.0 and avgf > 0.0 and sellf > 0.0:
                unrealized_usd = (qtyf * sellf) - (qtyf * avgf)

            # Display + heading reflect the current max DCA setting (hot-reload friendly)
            try:
                max_dca_24h = int(float(self.settings.get("max_dca_buys_per_24h", DEFAULT_SETTINGS.get("max_dca_buys_per_24h", 2)) or 2))
            except Exception:
                max_dca_24h = int(DEFAULT_SETTINGS.get("max_dca_buys_per_24h", 2) or 2)
            if max_dca_24h < 0:
                max_dca_24h = 0
            try:
                self.trades_header_labels["dca_24h"] = f"24h DCA ({max_dca_24h})"
            except Exception:
                pass
            dca_24h_display = f"{dca_24h}/{max_dca_24h}"


            # Display + heading reflect trailing PM settings (hot-reload friendly)
            try:
                pm0 = float(self.settings.get("pm_start_pct_no_dca", DEFAULT_SETTINGS.get("pm_start_pct_no_dca", 5.0)) or 5.0)
                pm1 = float(self.settings.get("pm_start_pct_with_dca", DEFAULT_SETTINGS.get("pm_start_pct_with_dca", 2.5)) or 2.5)
                tg = float(self.settings.get("trailing_gap_pct", DEFAULT_SETTINGS.get("trailing_gap_pct", 0.5)) or 0.5)
                self.trades_header_labels["trail_line"] = f"Trail ({pm0:g}/{pm1:g}%)"
            except Exception:
                pass


            next_dca = pos.get("next_dca_display", "")

            trail_line = pos.get("trail_line", 0.0)

            table_rows.append({
                "coin": str(coin),
                "qty": f"{qty:.8f}".rstrip("0").rstrip("."),
                "value": _fmt_money(value),
                "unrealized_usd": f"{unrealized_usd:+.2f}",
                "realized_usd": f"{realized_usd:+.2f}",
                "avg_cost": _fmt_price(avg_cost),
                "buy_price": _fmt_price(buy_price),
                "buy_pnl": _fmt_pct(buy_pnl),
                "sell_price": _fmt_price(sell_price),
                "sell_pnl": _fmt_pct(sell_pnl),
                "dca_stages": str(dca_stages),
                "dca_24h": dca_24h_display,
                "next_dca": str(next_dca),
                "trail_line": _fmt_price(trail_line),
            })
            visible_row_index += 1

        self._trades_table_rows = table_rows
        self._draw_trades_table()









    def _refresh_pnl(self) -> None:
        # mtime cache: avoid reading/parsing every tick
        try:
            mtime = os.path.getmtime(self.pnl_ledger_path)
        except Exception:
            mtime = None

        if getattr(self, "_last_pnl_mtime", object()) == mtime:
            return
        self._last_pnl_mtime = mtime

        data = _safe_read_json(self.pnl_ledger_path)
        if not data:
            self.lbl_pnl.config(text="N/A")
            return
        total = float(data.get("total_realized_profit_usd", 0.0))
        self.lbl_pnl.config(text=_fmt_money(total))


    def _refresh_trade_history(self) -> None:
        # mtime cache: avoid reading/parsing/rebuilding the list every tick
        try:
            mtime = os.path.getmtime(self.trade_history_path)
        except Exception:
            mtime = None

        if getattr(self, "_last_trade_history_mtime", object()) == mtime:
            return
        self._last_trade_history_mtime = mtime

        if not os.path.isfile(self.trade_history_path):
            self.hist_list.delete(0, "end")
            self.hist_list.insert("end", "(no trade_history.jsonl yet)")
            try:
                self.hist_list.itemconfig(0, bg=DARK_PANEL2, fg=DARK_FG)
            except Exception:
                pass
            return

        # show last N lines
        try:
            with open(self.trade_history_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
        except Exception:
            return

        lines = lines[-250:]  # cap for UI
        self.hist_list.delete(0, "end")
        row_index = 0
        for line in reversed(lines):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                ts = obj.get("ts", None)
                tss = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts)) if isinstance(ts, (int, float)) else "?"
                side = str(obj.get("side", "")).upper()
                tag = str(obj.get("tag", "") or "").upper()

                sym = obj.get("symbol", "")
                qty = obj.get("qty", "")
                px = obj.get("price", None)
                pnl = obj.get("realized_profit_usd", None)

                pnl_pct = obj.get("pnl_pct", None)

                px_txt = _fmt_price(px) if px is not None else "N/A"

                action = side
                if tag:
                    action = f"{side}/{tag}"

                txt = f"{tss} | {action:10s} {sym:5s} | qty={qty} | px={px_txt}"

                # Show the exact trade-time PnL%:
                # - DCA buys: show the BUY-side PnL (how far below avg cost it was when it bought)
                # - sells: show the SELL-side PnL (how far above/below avg cost it sold)
                show_trade_pnl_pct = None
                if side == "SELL":
                    show_trade_pnl_pct = pnl_pct
                elif side == "BUY" and tag == "DCA":
                    show_trade_pnl_pct = pnl_pct

                if show_trade_pnl_pct is not None:
                    try:
                        txt += f" | pnl@trade={_fmt_pct(float(show_trade_pnl_pct))}"
                    except Exception:
                        txt += f" | pnl@trade={show_trade_pnl_pct}"

                if pnl is not None:
                    try:
                        txt += f" | realized={float(pnl):+.2f}"
                    except Exception:
                        txt += f" | realized={pnl}"

                self.hist_list.insert("end", txt)
                hist_fg = DARK_FG
                if side == "SELL":
                    hist_fg = DARK_ACCENT
                elif side == "BUY" and tag == "DCA":
                    hist_fg = "#C58BFF"
                elif side == "BUY":
                    hist_fg = DARK_ACCENT2
            except Exception:
                self.hist_list.insert("end", line)
                hist_fg = DARK_FG
            try:
                idx = self.hist_list.size() - 1
                bg = DARK_PANEL if (row_index % 2) == 0 else "#0C1827"
                self.hist_list.itemconfig(idx, bg=bg, fg=hist_fg)
            except Exception:
                pass
            row_index += 1



    def _refresh_coin_dependent_ui(self, prev_coins: List[str]) -> None:
        """
        After settings change: refresh every coin-driven UI element:
          - Training dropdown (Train coin)
          - Trainers tab dropdown (Coin)
          - Chart tabs (Notebook): add/remove tabs to match current coin list
          - Neural overview tiles (new): add/remove tiles to match current coin list
        """
        # Rebuild dependent pieces
        self.coins = [c.upper().strip() for c in (self.settings.get("coins") or []) if c.strip()]
        self.coin_folders = build_coin_folders(self.settings.get("main_neural_dir") or self.project_dir, self.coins)

        # Refresh coin dropdowns (they don't auto-update)
        try:
            # Training pane dropdown
            if hasattr(self, "train_coin_combo") and self.train_coin_combo.winfo_exists():
                self.train_coin_combo["values"] = self.coins
                cur = (self.train_coin_var.get() or "").strip().upper() if hasattr(self, "train_coin_var") else ""
                if self.coins and cur not in self.coins:
                    self.train_coin_var.set(self.coins[0])

            # Trainers tab dropdown
            if hasattr(self, "trainer_coin_combo") and self.trainer_coin_combo.winfo_exists():
                self.trainer_coin_combo["values"] = self.coins
                cur = (self.trainer_coin_var.get() or "").strip().upper() if hasattr(self, "trainer_coin_var") else ""
                if self.coins and cur not in self.coins:
                    self.trainer_coin_var.set(self.coins[0])

            # Keep both selectors aligned if both exist
            if hasattr(self, "train_coin_var") and hasattr(self, "trainer_coin_var"):
                if self.train_coin_var.get():
                    self.trainer_coin_var.set(self.train_coin_var.get())
            if hasattr(self, "chart_search_combo") and self.chart_search_combo.winfo_exists():
                self.chart_search_combo["values"] = ["ACCOUNT"] + list(self.coins)
        except Exception:
            pass

        # Rebuild neural overview tiles (if the widget exists)
        try:
            if hasattr(self, "neural_wrap") and self.neural_wrap.winfo_exists():
                self._rebuild_neural_overview()
                self._refresh_neural_overview()
        except Exception:
            pass

        # Rebuild chart tabs if the coin list changed
        try:
            prev_set = set([str(c).strip().upper() for c in (prev_coins or []) if str(c).strip()])
            if prev_set != set(self.coins):
                self._rebuild_coin_chart_tabs()
        except Exception:
            pass


    def _rebuild_neural_overview(self) -> None:
        """
        Recreate the coin tiles in the left-side Neural Signals box to match self.coins.
        Uses WrapFrame so it automatically breaks into multiple rows.
        Adds hover highlighting and click-to-open chart.
        """
        if not hasattr(self, "neural_wrap") or self.neural_wrap is None:
            return

        # Clear old tiles
        try:
            if hasattr(self.neural_wrap, "clear"):
                self.neural_wrap.clear(destroy_widgets=True)
            else:
                for ch in list(self.neural_wrap.winfo_children()):
                    ch.destroy()
        except Exception:
            pass

        self.neural_tiles = {}

        for coin in (self.coins or []):
            tile = NeuralSignalTile(self.neural_wrap, coin, trade_start_level=int(self.settings.get("trade_start_level", 3) or 3))


            # --- Hover highlighting (real, visible) ---
            def _on_enter(_e=None, t=tile):
                try:
                    t.set_hover(True)
                except Exception:
                    pass

            def _on_leave(_e=None, t=tile):
                # Avoid flicker: when moving between child widgets, ignore "leave" if pointer is still inside tile.
                try:
                    x = t.winfo_pointerx()
                    y = t.winfo_pointery()
                    w = t.winfo_containing(x, y)
                    while w is not None:
                        if w == t:
                            return
                        w = getattr(w, "master", None)
                except Exception:
                    pass

                try:
                    t.set_hover(False)
                except Exception:
                    pass

            tile.bind("<Enter>", _on_enter, add="+")
            tile.bind("<Leave>", _on_leave, add="+")
            try:
                for w in tile.winfo_children():
                    w.bind("<Enter>", _on_enter, add="+")
                    w.bind("<Leave>", _on_leave, add="+")
            except Exception:
                pass

            # --- Click: open chart page ---
            def _open_coin_chart(_e=None, c=coin):
                try:
                    fn = getattr(self, "_show_chart_page", None)
                    if callable(fn):
                        fn(str(c).strip().upper())
                except Exception:
                    pass

            tile.bind("<Button-1>", _open_coin_chart, add="+")
            try:
                for w in tile.winfo_children():
                    w.bind("<Button-1>", _open_coin_chart, add="+")
            except Exception:
                pass

            self.neural_wrap.add(tile, padx=(0, 6), pady=(0, 6))
            self.neural_tiles[coin] = tile

        # Layout and scrollbar refresh
        try:
            self.neural_wrap._schedule_reflow()
        except Exception:
            pass

        try:
            fn = getattr(self, "_update_neural_overview_scrollbars", None)
            if callable(fn):
                self.after_idle(fn)
        except Exception:
            pass






    def _refresh_neural_overview(self) -> None:
        """
        Update each coin tile with long/short neural signals.
        Uses mtime caching so it's cheap to call every UI tick.
        """
        if not hasattr(self, "neural_tiles"):
            return

        # Keep coin_folders aligned with current settings/coins
        try:
            sig = (str(self.settings.get("main_neural_dir") or ""), tuple(self.coins or []))
            if getattr(self, "_coin_folders_sig", None) != sig:
                self._coin_folders_sig = sig
                self.coin_folders = build_coin_folders(self.settings.get("main_neural_dir") or self.project_dir, self.coins)
        except Exception:
            pass

        if not hasattr(self, "_neural_overview_cache"):
            self._neural_overview_cache = {}  # path -> (mtime, value)

        def _cached(path: str, loader, default: Any):
            try:
                mtime = os.path.getmtime(path)
            except Exception:
                return default, None

            hit = self._neural_overview_cache.get(path)
            if hit and hit[0] == mtime:
                return hit[1], mtime

            v = loader(path)
            self._neural_overview_cache[path] = (mtime, v)
            return v, mtime

        def _load_short_from_memory_json(path: str) -> int:
            try:
                obj = _safe_read_json(path) or {}
                return int(float(obj.get("short_dca_signal", 0)))
            except Exception:
                return 0

        latest_ts = None

        for coin, tile in list(self.neural_tiles.items()):
            folder = ""
            try:
                folder = (self.coin_folders or {}).get(coin, "")
            except Exception:
                folder = ""

            if not folder or not os.path.isdir(folder):
                tile.set_values(0, 0)
                continue

            long_sig = 0
            short_sig = 0
            mt_candidates: List[float] = []

            # Long signal
            long_path = os.path.join(folder, "long_dca_signal.txt")
            if os.path.isfile(long_path):
                long_sig, mt = _cached(long_path, read_int_from_file, 0)
                if mt:
                    mt_candidates.append(float(mt))

            # Short signal (prefer txt; fallback to memory.json)
            short_txt = os.path.join(folder, "short_dca_signal.txt")
            if os.path.isfile(short_txt):
                short_sig, mt = _cached(short_txt, read_int_from_file, 0)
                if mt:
                    mt_candidates.append(float(mt))
            else:
                mem = os.path.join(folder, "memory.json")
                if os.path.isfile(mem):
                    short_sig, mt = _cached(mem, _load_short_from_memory_json, 0)
                    if mt:
                        mt_candidates.append(float(mt))

            tile.set_values(long_sig, short_sig)

            if mt_candidates:
                mx = max(mt_candidates)
                latest_ts = mx if (latest_ts is None or mx > latest_ts) else latest_ts

        # Update "Last:" label
        try:
            if hasattr(self, "lbl_neural_overview_last") and self.lbl_neural_overview_last.winfo_exists():
                if latest_ts:
                    self.lbl_neural_overview_last.config(
                        text=f"Last: {time.strftime('%H:%M:%S', time.localtime(float(latest_ts)))}"
                    )
                else:
                    self.lbl_neural_overview_last.config(text="Last: N/A")
        except Exception:
            pass



    def _rebuild_coin_chart_tabs(self) -> None:
        """
        Ensure the Charts multi-row tab bar + pages match self.coins.
        Keeps the ACCOUNT page intact and preserves the currently selected page when possible.
        """
        charts_frame = getattr(self, "_charts_frame", None)
        if charts_frame is None or (hasattr(charts_frame, "winfo_exists") and not charts_frame.winfo_exists()):
            return

        # Remember selected page (coin or ACCOUNT)
        selected = getattr(self, "_current_chart_page", "ACCOUNT")
        if selected not in (["ACCOUNT"] + list(self.coins)):
            selected = "ACCOUNT"

        # Destroy existing tab bar + pages container (clean rebuild)
        try:
            if hasattr(self, "chart_tabs_bar") and self.chart_tabs_bar.winfo_exists():
                self.chart_tabs_bar.destroy()
        except Exception:
            pass

        try:
            if hasattr(self, "chart_pages_container") and self.chart_pages_container.winfo_exists():
                self.chart_pages_container.destroy()
        except Exception:
            pass

        # Recreate (dropdown-only navigation; no visible button tab bar)
        self.chart_tabs_bar = ttk.Frame(charts_frame)

        self.chart_pages_container = ttk.Frame(charts_frame)
        self.chart_pages_container.pack(fill="both", expand=True, padx=6, pady=(0, 6))

        try:
            if hasattr(self, "chart_search_combo") and self.chart_search_combo.winfo_exists():
                self.chart_search_combo["values"] = ["ACCOUNT"] + list(self.coins)
        except Exception:
            pass

        self._chart_tab_buttons = {}
        self.chart_pages = {}
        self._current_chart_page = selected

        def _show_page(name: str) -> None:
            self._current_chart_page = name
            for f in self.chart_pages.values():
                try:
                    f.pack_forget()
                except Exception:
                    pass
            f = self.chart_pages.get(name)
            if f is not None:
                f.pack(fill="both", expand=True)
            try:
                self.chart_search_var.set(name)
            except Exception:
                pass
            try:
                self._refresh_chart_legend_panel()
            except Exception:
                pass
            try:
                self._refresh_neural_overview_visibility()
            except Exception:
                pass

            for txt, b in self._chart_tab_buttons.items():
                try:
                    b.configure(style=("ChartTabSelected.TButton" if txt == name else "ChartTab.TButton"))
                except Exception:
                    pass

        self._show_chart_page = _show_page

        # ACCOUNT page
        acct_page = ttk.Frame(self.chart_pages_container)
        self.chart_pages["ACCOUNT"] = acct_page

        self.account_chart = AccountValueChart(
            acct_page,
            self.account_value_history_path,
            self.trade_history_path,
        )
        self.account_chart.pack(fill="both", expand=True)

        # Coin pages
        self.charts = {}
        for coin in self.coins:
            page = ttk.Frame(self.chart_pages_container)
            self.chart_pages[coin] = page

            chart = CandleChart(page, self.fetcher, coin, self._settings_getter, self.trade_history_path)
            chart.pack(fill="both", expand=True)
            self.charts[coin] = chart

        # Restore selection
        self._show_chart_page(selected)




    # ---- settings dialog ----

    def open_settings_dialog(self, focus_target: str = "") -> None:
        focus_key = str(focus_target or "").strip().lower()
        try:
            if self._settings_win is not None and self._settings_win.winfo_exists():
                self._settings_win.lift()
                self._settings_win.focus_force()
                return
        except Exception:
            pass

        win = tk.Toplevel(self)
        self._settings_win = win
        win.title("Settings")
        # Big enough for the bottom buttons on most screens + still scrolls if someone resizes smaller.
        win.geometry("860x680")
        win.minsize(760, 560)
        win.configure(bg=DARK_BG)

        def _close_settings() -> None:
            try:
                self._settings_win = None
            except Exception:
                pass
            try:
                win.destroy()
            except Exception:
                pass

        win.protocol("WM_DELETE_WINDOW", _close_settings)

        # Scrollable settings content (auto-hides the scrollbar if everything fits),
        # using the same pattern as the Neural Levels scrollbar.
        viewport = ttk.Frame(win)
        viewport.pack(fill="both", expand=True, padx=12, pady=12)
        viewport.grid_rowconfigure(0, weight=1)
        viewport.grid_columnconfigure(0, weight=1)

        settings_canvas = tk.Canvas(
            viewport,
            bg=DARK_BG,
            highlightthickness=1,
            highlightbackground=DARK_BORDER,
            bd=0,
        )
        settings_canvas.grid(row=0, column=0, sticky="nsew")

        settings_scroll = ttk.Scrollbar(
            viewport,
            orient="vertical",
            command=settings_canvas.yview,
        )
        settings_scroll.grid(row=0, column=1, sticky="ns")

        settings_canvas.configure(yscrollcommand=settings_scroll.set)

        frm = ttk.Frame(settings_canvas)
        settings_window = settings_canvas.create_window((0, 0), window=frm, anchor="nw")

        def _update_settings_scrollbars(event=None) -> None:
            """Update scrollregion + hide/show the scrollbar depending on overflow."""
            try:
                c = settings_canvas
                win_id = settings_window

                c.update_idletasks()
                bbox = c.bbox(win_id)
                if not bbox:
                    settings_scroll.grid_remove()
                    return

                c.configure(scrollregion=bbox)
                content_h = int(bbox[3] - bbox[1])
                view_h = int(c.winfo_height())

                if content_h > (view_h + 1):
                    settings_scroll.grid()
                else:
                    settings_scroll.grid_remove()
                    try:
                        c.yview_moveto(0)
                    except Exception:
                        pass
            except Exception:
                pass

        def _on_settings_canvas_configure(e) -> None:
            # Keep the inner frame exactly the canvas width so wrapping is correct.
            try:
                settings_canvas.itemconfigure(settings_window, width=int(e.width))
            except Exception:
                pass
            _update_settings_scrollbars()

        settings_canvas.bind("<Configure>", _on_settings_canvas_configure, add="+")
        frm.bind("<Configure>", _update_settings_scrollbars, add="+")

        # Mousewheel scrolling for the whole settings dialog (including entry widgets).
        def _scroll_settings_units(units: int) -> None:
            try:
                if settings_scroll.winfo_ismapped():
                    settings_canvas.yview_scroll(int(units), "units")
            except Exception:
                pass

        def _wheel(e):
            try:
                delta = int(getattr(e, "delta", 0) or 0)
                if delta == 0:
                    return
                units = int(-delta / 120)
                if units == 0:
                    units = -1 if delta > 0 else 1
                _scroll_settings_units(units)
            except Exception:
                pass

        settings_canvas.bind("<Enter>", lambda _e: settings_canvas.focus_set(), add="+")
        settings_canvas.bind("<MouseWheel>", _wheel, add="+")  # Windows / Mac
        settings_canvas.bind("<Button-4>", lambda _e: _scroll_settings_units(-3), add="+")  # Linux
        settings_canvas.bind("<Button-5>", lambda _e: _scroll_settings_units(3), add="+")   # Linux
        win.bind("<MouseWheel>", _wheel, add="+")  # Capture wheel anywhere in settings dialog
        win.bind("<Button-4>", lambda _e: _scroll_settings_units(-3), add="+")
        win.bind("<Button-5>", lambda _e: _scroll_settings_units(3), add="+")



        # Make the entry column expand
        frm.columnconfigure(0, weight=0)  # labels
        frm.columnconfigure(1, weight=1)  # entries
        frm.columnconfigure(2, weight=0)  # browse buttons

        def _attach_tooltip(widget: tk.Widget, text: str) -> None:
            if not text:
                return
            tip = {"w": None}

            def _show(_e=None):
                try:
                    if tip["w"] is not None:
                        return
                    tw = tk.Toplevel(widget)
                    tw.wm_overrideredirect(True)
                    x = int(widget.winfo_rootx() + 16)
                    y = int(widget.winfo_rooty() + 20)
                    tw.wm_geometry(f"+{x}+{y}")
                    lbl = tk.Label(
                        tw,
                        text=text,
                        justify="left",
                        bg=DARK_PANEL2,
                        fg=DARK_FG,
                        relief="solid",
                        bd=1,
                        padx=6,
                        pady=4,
                        wraplength=320,
                    )
                    lbl.pack()
                    tip["w"] = tw
                except Exception:
                    pass

            def _hide(_e=None):
                try:
                    if tip["w"] is not None:
                        tip["w"].destroy()
                except Exception:
                    pass
                tip["w"] = None

            widget.bind("<Enter>", _show, add="+")
            widget.bind("<Leave>", _hide, add="+")
            widget.bind("<ButtonPress>", _hide, add="+")

        setting_help: Dict[str, str] = {
            "Configuration mode:": "Preset Managed auto-fills and locks configurable fields. Self Managed lets you edit each setting manually.",
            "Preset profile:": "Guarded prioritizes safety, Balanced is default, Performance increases aggressiveness and opportunity capture.",
            "Main neural folder:": "Where per-coin model folders live. Example: moving this to a slow drive can slow training/startup.",
            "Coins (comma):": "Active crypto list. Example: BTC,ETH,SOL. Removing a coin stops active trading but keeps prior training files.",
            "Trade start level (1-7):": "Lower enters earlier with weaker confidence; higher waits for stronger confidence and trades less often.",
            "Start allocation %:": "Initial buy size per coin as % of account value. Example: 0.5 means about $0.50 per $100 account value, before DCA.",
            "DCA levels (% list):": "Drawdown triggers for additional buys. Example: -2.5,-5,-10 adds at progressively deeper pullbacks.",
            "DCA multiplier:": "Scales each DCA leg. Example: 1.0 = equal sizing, 2.0 = each leg is larger than previous.",
            "Max DCA buys / coin (rolling 24h):": "Caps averaging frequency. Lower values reduce over-trading during choppy markets.",
            "Trailing PM start % (no DCA):": "Profit percent required before trailing exits activate on clean winners.",
            "Trailing PM start % (with DCA):": "Profit percent required before trailing exits activate on averaged positions.",
            "Trailing gap % (behind peak):": "How far price can pull back from peak profit before exit. Smaller values lock profit sooner.",
            "Max position USD / coin (0=off):": "Hard cap per crypto symbol. Example: 250 limits each coin to about $250 exposure.",
            "Max total exposure % (0=off):": "Caps crypto capital in active positions. Example: 40 means leave at least 60% uncommitted.",
            "Hub data dir (optional):": "Custom location for logs/state/artifacts. Useful for moving runtime data off the repo root.",
            "Thinker script path:": "Process used to score/scan opportunities. Change only if you intentionally swap engine implementations.",
            "Trainer script path:": "Model training entrypoint for crypto. Wrong path prevents background/Train All jobs from running.",
            "Trader script path:": "Execution loop entrypoint. Wrong path means Start Trades launches nothing.",
            "Market rollout stage:": "Feature gate level for stocks/forex logic. Higher stages enable stricter safety and execution controls.",
            "Alpaca API key ID:": "Stocks credential. Keep in env/secrets manager for production; this field is only for runtime injection.",
            "Alpaca secret key:": "Stocks secret credential. Never share this value.",
            "Alpaca base URL:": "Trading endpoint. Paper uses paper-api; live endpoint sends real orders.",
            "Alpaca data URL:": "Market data endpoint used by stock scanner.",
            "Key rotation warn days:": "Warn when API credentials have aged past this many days so keys get rotated before expiry/incident.",
            "KuCoin unsupported cooldown sec:": "Backoff after unsupported/blocked symbol responses. Higher values reduce repeated API lockouts.",
            "Crypto price error log cooldown sec:": "Log throttling for repeated crypto quote errors to keep logs readable.",
            "UI refresh seconds:": "Dashboard refresh cadence. Lower = fresher data but higher CPU usage.",
            "Chart refresh seconds:": "How often chart panels redraw from cached data.",
            "Candles limit:": "Max candles rendered per chart. Higher values improve context but can slow drawing.",
            "Font scale preset (small/normal/large):": "Global text scaling for readability.",
            "Layout preset (auto/compact/normal/wide):": "UI density/layout mode for screen size and preference.",
            "Stock universe mode:": "core uses curated symbols, watchlist uses your explicit list, all_tradable_filtered scans wider market.",
            "Stock universe symbols (watchlist):": "Comma-separated symbols used when universe mode is watchlist.",
            "Stock scan max symbols:": "Upper bound of symbols evaluated each scan cycle.",
            "Stock min price:": "Rejects very low-priced stocks that can be noisy/slippy.",
            "Stock max price:": "Avoids symbols priced above this ceiling for position-size consistency.",
            "Stock min dollar volume:": "Liquidity gate. Higher values reduce fill risk but shrink opportunity set.",
            "Stock max spread bps:": "Bid/ask spread cap. Lower values reduce slippage risk.",
            "Stock min bars required:": "Minimum history bars required before a symbol is eligible.",
            "Stock min valid bars ratio (0-1):": "Data quality threshold. Example: 0.8 requires 80% of bars to pass validation.",
            "Stock max stale hours:": "Rejects symbols with stale data older than this threshold.",
            "Stock watch-leader count (fallback):": "How many ranked watch-mode leaders to publish when no long setups pass.",
            "Stock leader stability margin %:": "Prevents constant leader flips by keeping current leader unless newcomer is better by this margin.",
            "Stock cached fallback hard-block age sec:": "If scan cache is older than this, new entries are fully blocked.",
            "Stock cached fallback size multiplier (0.1-1.0):": "Position-size reduction while operating on cached scans.",
            "Stock max reject rate % for entries:": "Blocks entries when scanner reject rate exceeds this, signaling degraded data quality.",
            "Stock order notional USD:": "Base dollar size per stock entry.",
            "Stock max open positions:": "Concurrent stock positions cap.",
            "Stock score threshold:": "Minimum model score required for entry. Higher values trade less but usually with higher confidence.",
            "Stock profit target %:": "Profit level where take-profit/trailing logic begins.",
            "Stock trailing gap %:": "Allowed pullback from peak before exit.",
            "Stock max day trades / day:": "Caps intraday round trips to manage compliance/risk.",
            "Stock max position USD/symbol (risk_caps):": "Per-symbol hard cap when risk_caps rollout is active.",
            "Stock max total exposure % (risk_caps):": "Total stock exposure cap when risk_caps rollout is active.",
            "Stock live_guarded score multiplier:": "Raises required score in live_guarded mode for safer live execution.",
            "Stock live_guarded min calibrated prob:": "Minimum calibrated probability in live_guarded mode.",
            "Stock max slippage bps:": "Maximum tolerated entry slippage before order is skipped.",
            "Stock order retry count:": "Number of retry attempts for transient broker/order failures.",
            "Stock max loss streak:": "Consecutive losing trades before defensive throttling activates.",
            "Stock loss-size step per streak (0-0.9):": "How much to reduce next order size after each loss.",
            "Stock loss-size floor scale (0.1-1.0):": "Minimum fractional size allowed while in loss-streak throttling.",
            "Stock loss cooldown seconds:": "Pause duration after streak/loss guard triggers.",
            "Stock max daily loss USD (0=off):": "Absolute daily loss stop; entries pause after breach.",
            "Stock max daily loss % (0=off):": "Percent daily loss stop; entries pause after breach.",
            "Stock min calibration samples (live_guarded):": "Minimum calibration sample count before live_guarded entries are allowed.",
            "Stock max signal age seconds:": "Signals older than this are considered stale and skipped.",
            "Stock reject drift warn %:": "Warn threshold for sudden scanner reject-rate drift.",
            "Stock block mins to close:": "No-new-entry window before market close.",
            "Forex universe pairs:": "Comma-separated tradable instruments, e.g. EUR_USD,USD_JPY,GBP_USD.",
            "Forex scan max pairs:": "Upper bound of FX pairs evaluated each scan cycle.",
            "Forex max spread bps:": "Spread gate for forex entries. Lower values reduce cost/slippage.",
            "Forex min volatility %:": "Rejects low-movement pairs that usually lack tradeable edge.",
            "Forex min bars required:": "Minimum history bars required per pair.",
            "Forex min valid bars ratio (0-1):": "Data-quality threshold for pair eligibility.",
            "Forex max stale hours:": "Rejects pairs with stale data older than this.",
            "Forex leader stability margin %:": "Helps keep top pair stable and reduce churn unless a new leader is materially better.",
            "Forex cached fallback hard-block age sec:": "If FX scan cache is older than this, new entries are blocked.",
            "Forex cached fallback size multiplier (0.1-1.0):": "Position-size reduction while scanner runs on cached data.",
            "Forex max reject rate % for entries:": "Blocks entries when scanner reject rate indicates unstable market-data quality.",
            "Forex trade units:": "Base unit size per FX order.",
            "Forex max open positions:": "Concurrent forex positions cap.",
            "Forex max position USD/pair (risk_caps):": "Per-pair dollar cap when risk caps are active.",
            "Forex score threshold:": "Minimum model score needed for forex entries.",
            "Forex profit target %:": "Profit threshold before trailing logic engages.",
            "Forex trailing gap %:": "Allowed pullback from peak profit before exit.",
            "Forex max exposure % (risk_caps proxy):": "Total forex exposure cap.",
            "Forex session mode (all/london_ny/london/ny/asia):": "Restricts entries to sessions with desired liquidity/behavior.",
            "Forex live_guarded score multiplier:": "Raises score bar during live_guarded stage.",
            "Forex live_guarded min calibrated prob:": "Minimum calibrated probability required in live_guarded mode.",
            "Forex max slippage bps:": "Maximum allowed slippage at execution time.",
            "Forex order retry count:": "Retries for temporary order failures.",
            "Forex max loss streak:": "Consecutive losses before defensive throttling.",
            "Forex loss-size step per streak (0-0.9):": "How aggressively size is reduced after each loss.",
            "Forex loss-size floor scale (0.1-1.0):": "Minimum size fraction during defensive mode.",
            "Forex loss cooldown seconds:": "Pause duration after streak/loss guard trips.",
            "Forex max daily loss USD (0=off):": "Absolute daily stop for forex.",
            "Forex max daily loss % (0=off):": "Percent daily stop for forex.",
            "Forex min calibration samples (live_guarded):": "Minimum calibration history needed for live_guarded forex entries.",
            "Forex max signal age seconds:": "Rejects stale forex signals older than this.",
            "Forex reject drift warn %:": "Warn threshold for forex scanner reject-rate spikes.",
            "Global max exposure % (all markets, 0=off):": "Cross-market exposure cap across stocks and forex.",
            "Chart cache symbols (stocks/forex):": "How many ranked symbols/pairs keep cached chart data ready.",
            "Chart cache bars per symbol:": "How deep each cached chart history is for stocks/forex.",
            "Scan fallback max age sec:": "How long scanner fallback data is allowed before considered too old.",
            "Snapshot fallback max age sec:": "Maximum age for broker snapshot fallback reads.",
            "OANDA account ID:": "Forex account identifier used with OANDA REST calls.",
            "OANDA API token:": "Forex secret credential. Never share this value.",
            "OANDA REST URL:": "Forex trading endpoint; practice uses fxpractice URL.",
            "OANDA stream URL:": "Streaming/pricing endpoint for OANDA.",
        }

        def _resolve_help(label: str, tooltip: str) -> str:
            if tooltip and str(tooltip).strip():
                return str(tooltip).strip()
            return str(setting_help.get(label, "")).strip()

        managed_controls: List[Tuple[tk.Widget, str]] = []

        def _register_managed_control(widget: tk.Widget, restore_state: str = "normal", managed: bool = True) -> None:
            if (not managed) or (widget is None):
                return
            managed_controls.append((widget, str(restore_state or "normal")))

        def add_row(
            r: int,
            label: str,
            var: tk.Variable,
            browse: Optional[str] = None,
            parent: Optional[ttk.Frame] = None,
            tooltip: str = "",
            managed: bool = True,
        ):
            """
            browse: "dir" to attach a directory chooser, else None.
            """
            target = parent or frm
            lbl = ttk.Label(target, text=label)
            lbl.grid(row=r, column=0, sticky="w", padx=(0, 10), pady=6)

            ent = ttk.Entry(target, textvariable=var)
            ent.grid(row=r, column=1, sticky="ew", pady=6)
            _register_managed_control(ent, "normal", managed=managed)
            hint_text = _resolve_help(label, tooltip)
            if hint_text:
                _attach_tooltip(lbl, hint_text)
                _attach_tooltip(ent, hint_text)

            if browse == "dir":
                def do_browse():
                    picked = filedialog.askdirectory()
                    if picked:
                        var.set(picked)
                browse_btn = ttk.Button(target, text="Browse", command=do_browse)
                browse_btn.grid(row=r, column=2, sticky="e", padx=(10, 0), pady=6)
                _register_managed_control(browse_btn, "normal", managed=managed)
            else:
                # keep column alignment consistent
                ttk.Label(target, text="").grid(row=r, column=2, sticky="e", padx=(10, 0), pady=6)

        def add_secret_row(
            r: int,
            label: str,
            var: tk.Variable,
            parent: Optional[ttk.Frame] = None,
            tooltip: str = "",
            managed: bool = True,
        ):
            target = parent or frm
            lbl = ttk.Label(target, text=label)
            lbl.grid(row=r, column=0, sticky="w", padx=(0, 10), pady=6)
            ent = ttk.Entry(target, textvariable=var, show="*")
            ent.grid(row=r, column=1, sticky="ew", pady=6)
            _register_managed_control(ent, "normal", managed=managed)
            hint_text = _resolve_help(label, tooltip)
            if hint_text:
                _attach_tooltip(lbl, hint_text)
                _attach_tooltip(ent, hint_text)
            ttk.Label(target, text="").grid(row=r, column=2, sticky="e", padx=(10, 0), pady=6)

        def add_toggle_row(
            r: int,
            label: str,
            text: str,
            var: tk.Variable,
            parent: Optional[ttk.Frame] = None,
            tooltip: str = "",
            managed: bool = True,
        ) -> None:
            target = parent or frm
            lbl = ttk.Label(target, text=label)
            lbl.grid(row=r, column=0, sticky="w", padx=(0, 10), pady=6)
            chk = ttk.Checkbutton(target, text=text, variable=var)
            chk.grid(row=r, column=1, sticky="w", pady=6)
            _register_managed_control(chk, "normal", managed=managed)
            ttk.Label(target, text="").grid(row=r, column=2, sticky="e", padx=(10, 0), pady=6)
            hint_text = _resolve_help(label, tooltip)
            if hint_text:
                _attach_tooltip(lbl, hint_text)
                _attach_tooltip(chk, hint_text)

        def add_choice_row(
            r: int,
            label: str,
            var: tk.StringVar,
            choices: List[str],
            parent: Optional[ttk.Frame] = None,
            tooltip: str = "",
            managed: bool = True,
        ) -> ttk.Combobox:
            target = parent or frm
            lbl = ttk.Label(target, text=label)
            lbl.grid(row=r, column=0, sticky="w", padx=(0, 10), pady=6)
            combo = ttk.Combobox(
                target,
                textvariable=var,
                values=list(choices or []),
                state="readonly",
            )
            combo.grid(row=r, column=1, sticky="ew", pady=6)
            _register_managed_control(combo, "readonly", managed=managed)
            ttk.Label(target, text="").grid(row=r, column=2, sticky="e", padx=(10, 0), pady=6)
            hint_text = _resolve_help(label, tooltip)
            if hint_text:
                _attach_tooltip(lbl, hint_text)
                _attach_tooltip(combo, hint_text)
            return combo

        def add_status_action_row(
            r: int,
            label: str,
            status_var: tk.StringVar,
            action_text: str,
            action_command,
            parent: Optional[ttk.Frame] = None,
            tooltip: str = "",
        ) -> ttk.Frame:
            target = parent or frm
            lbl = ttk.Label(target, text=label)
            lbl.grid(row=r, column=0, sticky="w", padx=(0, 10), pady=6)
            rowf = ttk.Frame(target)
            rowf.grid(row=r, column=1, columnspan=2, sticky="ew", pady=6)
            rowf.columnconfigure(0, weight=1)
            status_lbl = ttk.Label(rowf, textvariable=status_var)
            status_lbl.grid(row=0, column=0, sticky="w")
            act_btn = ttk.Button(rowf, text=action_text, command=action_command)
            act_btn.grid(row=0, column=1, sticky="e", padx=(10, 0))
            hint_text = _resolve_help(label, tooltip)
            if hint_text:
                _attach_tooltip(lbl, hint_text)
                _attach_tooltip(status_lbl, hint_text)
                _attach_tooltip(act_btn, hint_text)
            return rowf

        main_dir_var = tk.StringVar(value=self.settings["main_neural_dir"])
        coins_var = tk.StringVar(value=",".join(self.settings["coins"]))
        trade_start_level_var = tk.StringVar(value=str(self.settings.get("trade_start_level", 3)))
        start_alloc_pct_var = tk.StringVar(value=str(self.settings.get("start_allocation_pct", DEFAULT_SETTINGS.get("start_allocation_pct", 0.5))))
        dca_mult_var = tk.StringVar(value=str(self.settings.get("dca_multiplier", 2.0)))
        _dca_levels = self.settings.get("dca_levels", DEFAULT_SETTINGS.get("dca_levels", []))
        if not isinstance(_dca_levels, list):
            _dca_levels = DEFAULT_SETTINGS.get("dca_levels", [])
        dca_levels_var = tk.StringVar(value=",".join(str(x) for x in _dca_levels))
        max_dca_var = tk.StringVar(value=str(self.settings.get("max_dca_buys_per_24h", DEFAULT_SETTINGS.get("max_dca_buys_per_24h", 2))))

        # --- Trailing PM settings (editable; hot-reload friendly) ---
        pm_no_dca_var = tk.StringVar(value=str(self.settings.get("pm_start_pct_no_dca", DEFAULT_SETTINGS.get("pm_start_pct_no_dca", 5.0))))
        pm_with_dca_var = tk.StringVar(value=str(self.settings.get("pm_start_pct_with_dca", DEFAULT_SETTINGS.get("pm_start_pct_with_dca", 2.5))))
        trailing_gap_var = tk.StringVar(value=str(self.settings.get("trailing_gap_pct", DEFAULT_SETTINGS.get("trailing_gap_pct", 0.5))))
        max_pos_per_coin_var = tk.StringVar(value=str(self.settings.get("max_position_usd_per_coin", DEFAULT_SETTINGS.get("max_position_usd_per_coin", 0.0))))
        max_total_exposure_var = tk.StringVar(value=str(self.settings.get("max_total_exposure_pct", DEFAULT_SETTINGS.get("max_total_exposure_pct", 0.0))))
        alpaca_base_url_var = tk.StringVar(value=str(self.settings.get("alpaca_base_url", DEFAULT_SETTINGS.get("alpaca_base_url", "")) or ""))
        alpaca_data_url_var = tk.StringVar(value=str(self.settings.get("alpaca_data_url", DEFAULT_SETTINGS.get("alpaca_data_url", "")) or ""))
        alpaca_paper_var = tk.BooleanVar(value=bool(self.settings.get("alpaca_paper_mode", DEFAULT_SETTINGS.get("alpaca_paper_mode", True))))
        rollout_stage_var = tk.StringVar(value=str(self.settings.get("market_rollout_stage", DEFAULT_SETTINGS.get("market_rollout_stage", "legacy")) or "legacy"))
        stock_universe_mode_var = tk.StringVar(value=str(self.settings.get("stock_universe_mode", DEFAULT_SETTINGS.get("stock_universe_mode", "core")) or "core"))
        stock_universe_symbols_var = tk.StringVar(value=str(self.settings.get("stock_universe_symbols", DEFAULT_SETTINGS.get("stock_universe_symbols", "")) or ""))
        stock_scan_max_symbols_var = tk.StringVar(value=str(self.settings.get("stock_scan_max_symbols", DEFAULT_SETTINGS.get("stock_scan_max_symbols", 60))))
        stock_min_price_var = tk.StringVar(value=str(self.settings.get("stock_min_price", DEFAULT_SETTINGS.get("stock_min_price", 5.0))))
        stock_max_price_var = tk.StringVar(value=str(self.settings.get("stock_max_price", DEFAULT_SETTINGS.get("stock_max_price", 500.0))))
        stock_min_dollar_volume_var = tk.StringVar(value=str(self.settings.get("stock_min_dollar_volume", DEFAULT_SETTINGS.get("stock_min_dollar_volume", 5000000.0))))
        stock_max_spread_bps_var = tk.StringVar(value=str(self.settings.get("stock_max_spread_bps", DEFAULT_SETTINGS.get("stock_max_spread_bps", 40.0))))
        stock_gate_hours_var = tk.BooleanVar(value=bool(self.settings.get("stock_gate_market_hours_scan", DEFAULT_SETTINGS.get("stock_gate_market_hours_scan", True))))
        stock_min_bars_var = tk.StringVar(value=str(self.settings.get("stock_min_bars_required", DEFAULT_SETTINGS.get("stock_min_bars_required", 24))))
        stock_min_valid_ratio_var = tk.StringVar(value=str(self.settings.get("stock_min_valid_bars_ratio", DEFAULT_SETTINGS.get("stock_min_valid_bars_ratio", 0.7))))
        stock_max_stale_hours_var = tk.StringVar(value=str(self.settings.get("stock_max_stale_hours", DEFAULT_SETTINGS.get("stock_max_stale_hours", 6.0))))
        stock_show_rejected_var = tk.BooleanVar(value=bool(self.settings.get("stock_show_rejected_rows", DEFAULT_SETTINGS.get("stock_show_rejected_rows", False))))
        stock_publish_watch_leaders_var = tk.BooleanVar(value=bool(self.settings.get("stock_scan_publish_watch_leaders", DEFAULT_SETTINGS.get("stock_scan_publish_watch_leaders", True))))
        stock_watch_leaders_count_var = tk.StringVar(value=str(self.settings.get("stock_scan_watch_leaders_count", DEFAULT_SETTINGS.get("stock_scan_watch_leaders_count", 6))))
        stock_leader_stability_margin_var = tk.StringVar(value=str(self.settings.get("stock_leader_stability_margin_pct", DEFAULT_SETTINGS.get("stock_leader_stability_margin_pct", 10.0))))
        stock_auto_trade_var = tk.BooleanVar(value=bool(self.settings.get("stock_auto_trade_enabled", DEFAULT_SETTINGS.get("stock_auto_trade_enabled", False))))
        stock_block_cached_scan_var = tk.BooleanVar(value=bool(self.settings.get("stock_block_entries_on_cached_scan", DEFAULT_SETTINGS.get("stock_block_entries_on_cached_scan", True))))
        stock_cached_scan_hard_block_age_var = tk.StringVar(value=str(self.settings.get("stock_cached_scan_hard_block_age_s", DEFAULT_SETTINGS.get("stock_cached_scan_hard_block_age_s", 1800))))
        stock_cached_scan_size_mult_var = tk.StringVar(value=str(self.settings.get("stock_cached_scan_entry_size_mult", DEFAULT_SETTINGS.get("stock_cached_scan_entry_size_mult", 0.60))))
        stock_require_data_quality_gate_var = tk.BooleanVar(value=bool(self.settings.get("stock_require_data_quality_ok_for_entries", DEFAULT_SETTINGS.get("stock_require_data_quality_ok_for_entries", True))))
        stock_reject_rate_gate_var = tk.StringVar(value=str(self.settings.get("stock_require_reject_rate_max_pct", DEFAULT_SETTINGS.get("stock_require_reject_rate_max_pct", 92.0))))
        stock_notional_var = tk.StringVar(value=str(self.settings.get("stock_trade_notional_usd", DEFAULT_SETTINGS.get("stock_trade_notional_usd", 100.0))))
        stock_max_pos_var = tk.StringVar(value=str(self.settings.get("stock_max_open_positions", DEFAULT_SETTINGS.get("stock_max_open_positions", 1))))
        stock_score_threshold_var = tk.StringVar(value=str(self.settings.get("stock_score_threshold", DEFAULT_SETTINGS.get("stock_score_threshold", 0.2))))
        stock_profit_target_var = tk.StringVar(value=str(self.settings.get("stock_profit_target_pct", DEFAULT_SETTINGS.get("stock_profit_target_pct", 0.35))))
        stock_trailing_gap_var = tk.StringVar(value=str(self.settings.get("stock_trailing_gap_pct", DEFAULT_SETTINGS.get("stock_trailing_gap_pct", 0.2))))
        stock_day_trades_var = tk.StringVar(value=str(self.settings.get("stock_max_day_trades", DEFAULT_SETTINGS.get("stock_max_day_trades", 3))))
        stock_max_position_usd_var = tk.StringVar(value=str(self.settings.get("stock_max_position_usd_per_symbol", DEFAULT_SETTINGS.get("stock_max_position_usd_per_symbol", 0.0))))
        stock_max_exposure_var = tk.StringVar(value=str(self.settings.get("stock_max_total_exposure_pct", DEFAULT_SETTINGS.get("stock_max_total_exposure_pct", 0.0))))
        stock_guarded_mult_var = tk.StringVar(value=str(self.settings.get("stock_live_guarded_score_mult", DEFAULT_SETTINGS.get("stock_live_guarded_score_mult", 1.2))))
        stock_min_calib_prob_var = tk.StringVar(value=str(self.settings.get("stock_min_calib_prob_live_guarded", DEFAULT_SETTINGS.get("stock_min_calib_prob_live_guarded", 0.58))))
        stock_max_slippage_bps_var = tk.StringVar(value=str(self.settings.get("stock_max_slippage_bps", DEFAULT_SETTINGS.get("stock_max_slippage_bps", 35.0))))
        stock_order_retry_count_var = tk.StringVar(value=str(self.settings.get("stock_order_retry_count", DEFAULT_SETTINGS.get("stock_order_retry_count", 2))))
        stock_max_loss_streak_var = tk.StringVar(value=str(self.settings.get("stock_max_loss_streak", DEFAULT_SETTINGS.get("stock_max_loss_streak", 3))))
        stock_loss_size_step_var = tk.StringVar(value=str(self.settings.get("stock_loss_streak_size_step_pct", DEFAULT_SETTINGS.get("stock_loss_streak_size_step_pct", 0.15))))
        stock_loss_size_floor_var = tk.StringVar(value=str(self.settings.get("stock_loss_streak_size_floor_pct", DEFAULT_SETTINGS.get("stock_loss_streak_size_floor_pct", 0.40))))
        stock_loss_cooldown_var = tk.StringVar(value=str(self.settings.get("stock_loss_cooldown_seconds", DEFAULT_SETTINGS.get("stock_loss_cooldown_seconds", 1800))))
        stock_max_daily_loss_usd_var = tk.StringVar(value=str(self.settings.get("stock_max_daily_loss_usd", DEFAULT_SETTINGS.get("stock_max_daily_loss_usd", 0.0))))
        stock_max_daily_loss_pct_var = tk.StringVar(value=str(self.settings.get("stock_max_daily_loss_pct", DEFAULT_SETTINGS.get("stock_max_daily_loss_pct", 0.0))))
        stock_min_samples_guarded_var = tk.StringVar(value=str(self.settings.get("stock_min_samples_live_guarded", DEFAULT_SETTINGS.get("stock_min_samples_live_guarded", 5))))
        stock_max_signal_age_var = tk.StringVar(value=str(self.settings.get("stock_max_signal_age_seconds", DEFAULT_SETTINGS.get("stock_max_signal_age_seconds", 300))))
        stock_reject_warn_pct_var = tk.StringVar(value=str(self.settings.get("stock_reject_drift_warn_pct", DEFAULT_SETTINGS.get("stock_reject_drift_warn_pct", 65.0))))
        stock_block_near_close_var = tk.BooleanVar(value=bool(self.settings.get("stock_block_new_entries_near_close", DEFAULT_SETTINGS.get("stock_block_new_entries_near_close", True))))
        stock_no_new_close_mins_var = tk.StringVar(value=str(self.settings.get("stock_no_new_entries_mins_to_close", DEFAULT_SETTINGS.get("stock_no_new_entries_mins_to_close", 15))))
        oanda_rest_url_var = tk.StringVar(value=str(self.settings.get("oanda_rest_url", DEFAULT_SETTINGS.get("oanda_rest_url", "")) or ""))
        oanda_stream_url_var = tk.StringVar(value=str(self.settings.get("oanda_stream_url", DEFAULT_SETTINGS.get("oanda_stream_url", "")) or ""))
        oanda_practice_var = tk.BooleanVar(value=bool(self.settings.get("oanda_practice_mode", DEFAULT_SETTINGS.get("oanda_practice_mode", True))))
        paper_only_guard_var = tk.BooleanVar(value=bool(self.settings.get("paper_only_unless_checklist_green", DEFAULT_SETTINGS.get("paper_only_unless_checklist_green", True))))
        forex_pairs_var = tk.StringVar(value=str(self.settings.get("forex_universe_pairs", DEFAULT_SETTINGS.get("forex_universe_pairs", "")) or ""))
        forex_scan_max_pairs_var = tk.StringVar(value=str(self.settings.get("forex_scan_max_pairs", DEFAULT_SETTINGS.get("forex_scan_max_pairs", 16))))
        fx_max_spread_bps_var = tk.StringVar(value=str(self.settings.get("forex_max_spread_bps", DEFAULT_SETTINGS.get("forex_max_spread_bps", 8.0))))
        fx_min_vol_pct_var = tk.StringVar(value=str(self.settings.get("forex_min_volatility_pct", DEFAULT_SETTINGS.get("forex_min_volatility_pct", 0.01))))
        fx_min_bars_var = tk.StringVar(value=str(self.settings.get("forex_min_bars_required", DEFAULT_SETTINGS.get("forex_min_bars_required", 24))))
        fx_min_valid_ratio_var = tk.StringVar(value=str(self.settings.get("forex_min_valid_bars_ratio", DEFAULT_SETTINGS.get("forex_min_valid_bars_ratio", 0.7))))
        fx_max_stale_hours_var = tk.StringVar(value=str(self.settings.get("forex_max_stale_hours", DEFAULT_SETTINGS.get("forex_max_stale_hours", 8.0))))
        fx_show_rejected_var = tk.BooleanVar(value=bool(self.settings.get("forex_show_rejected_rows", DEFAULT_SETTINGS.get("forex_show_rejected_rows", False))))
        fx_leader_stability_margin_var = tk.StringVar(value=str(self.settings.get("forex_leader_stability_margin_pct", DEFAULT_SETTINGS.get("forex_leader_stability_margin_pct", 12.0))))
        fx_auto_trade_var = tk.BooleanVar(value=bool(self.settings.get("forex_auto_trade_enabled", DEFAULT_SETTINGS.get("forex_auto_trade_enabled", False))))
        fx_block_cached_scan_var = tk.BooleanVar(value=bool(self.settings.get("forex_block_entries_on_cached_scan", DEFAULT_SETTINGS.get("forex_block_entries_on_cached_scan", True))))
        fx_cached_scan_hard_block_age_var = tk.StringVar(value=str(self.settings.get("forex_cached_scan_hard_block_age_s", DEFAULT_SETTINGS.get("forex_cached_scan_hard_block_age_s", 1200))))
        fx_cached_scan_size_mult_var = tk.StringVar(value=str(self.settings.get("forex_cached_scan_entry_size_mult", DEFAULT_SETTINGS.get("forex_cached_scan_entry_size_mult", 0.65))))
        fx_require_data_quality_gate_var = tk.BooleanVar(value=bool(self.settings.get("forex_require_data_quality_ok_for_entries", DEFAULT_SETTINGS.get("forex_require_data_quality_ok_for_entries", True))))
        fx_reject_rate_gate_var = tk.StringVar(value=str(self.settings.get("forex_require_reject_rate_max_pct", DEFAULT_SETTINGS.get("forex_require_reject_rate_max_pct", 92.0))))
        fx_trade_units_var = tk.StringVar(value=str(self.settings.get("forex_trade_units", DEFAULT_SETTINGS.get("forex_trade_units", 1000))))
        fx_max_pos_var = tk.StringVar(value=str(self.settings.get("forex_max_open_positions", DEFAULT_SETTINGS.get("forex_max_open_positions", 1))))
        fx_max_pos_usd_pair_var = tk.StringVar(value=str(self.settings.get("forex_max_position_usd_per_pair", DEFAULT_SETTINGS.get("forex_max_position_usd_per_pair", 0.0))))
        fx_score_threshold_var = tk.StringVar(value=str(self.settings.get("forex_score_threshold", DEFAULT_SETTINGS.get("forex_score_threshold", 0.2))))
        fx_profit_target_var = tk.StringVar(value=str(self.settings.get("forex_profit_target_pct", DEFAULT_SETTINGS.get("forex_profit_target_pct", 0.25))))
        fx_trailing_gap_var = tk.StringVar(value=str(self.settings.get("forex_trailing_gap_pct", DEFAULT_SETTINGS.get("forex_trailing_gap_pct", 0.15))))
        fx_max_exposure_var = tk.StringVar(value=str(self.settings.get("forex_max_total_exposure_pct", DEFAULT_SETTINGS.get("forex_max_total_exposure_pct", 0.0))))
        fx_session_mode_var = tk.StringVar(value=str(self.settings.get("forex_session_mode", DEFAULT_SETTINGS.get("forex_session_mode", "all")) or "all"))
        fx_guarded_mult_var = tk.StringVar(value=str(self.settings.get("forex_live_guarded_score_mult", DEFAULT_SETTINGS.get("forex_live_guarded_score_mult", 1.15))))
        fx_min_calib_prob_var = tk.StringVar(value=str(self.settings.get("forex_min_calib_prob_live_guarded", DEFAULT_SETTINGS.get("forex_min_calib_prob_live_guarded", 0.56))))
        fx_max_slippage_bps_var = tk.StringVar(value=str(self.settings.get("forex_max_slippage_bps", DEFAULT_SETTINGS.get("forex_max_slippage_bps", 6.0))))
        fx_order_retry_count_var = tk.StringVar(value=str(self.settings.get("forex_order_retry_count", DEFAULT_SETTINGS.get("forex_order_retry_count", 2))))
        fx_max_loss_streak_var = tk.StringVar(value=str(self.settings.get("forex_max_loss_streak", DEFAULT_SETTINGS.get("forex_max_loss_streak", 3))))
        fx_loss_size_step_var = tk.StringVar(value=str(self.settings.get("forex_loss_streak_size_step_pct", DEFAULT_SETTINGS.get("forex_loss_streak_size_step_pct", 0.15))))
        fx_loss_size_floor_var = tk.StringVar(value=str(self.settings.get("forex_loss_streak_size_floor_pct", DEFAULT_SETTINGS.get("forex_loss_streak_size_floor_pct", 0.40))))
        fx_loss_cooldown_var = tk.StringVar(value=str(self.settings.get("forex_loss_cooldown_seconds", DEFAULT_SETTINGS.get("forex_loss_cooldown_seconds", 1800))))
        fx_max_daily_loss_usd_var = tk.StringVar(value=str(self.settings.get("forex_max_daily_loss_usd", DEFAULT_SETTINGS.get("forex_max_daily_loss_usd", 0.0))))
        fx_max_daily_loss_pct_var = tk.StringVar(value=str(self.settings.get("forex_max_daily_loss_pct", DEFAULT_SETTINGS.get("forex_max_daily_loss_pct", 0.0))))
        fx_min_samples_guarded_var = tk.StringVar(value=str(self.settings.get("forex_min_samples_live_guarded", DEFAULT_SETTINGS.get("forex_min_samples_live_guarded", 5))))
        fx_max_signal_age_var = tk.StringVar(value=str(self.settings.get("forex_max_signal_age_seconds", DEFAULT_SETTINGS.get("forex_max_signal_age_seconds", 300))))
        fx_reject_warn_pct_var = tk.StringVar(value=str(self.settings.get("forex_reject_drift_warn_pct", DEFAULT_SETTINGS.get("forex_reject_drift_warn_pct", 65.0))))
        market_global_exposure_var = tk.StringVar(value=str(self.settings.get("market_max_total_exposure_pct", DEFAULT_SETTINGS.get("market_max_total_exposure_pct", 0.0))))
        chart_cache_symbols_var = tk.StringVar(value=str(self.settings.get("market_chart_cache_symbols", DEFAULT_SETTINGS.get("market_chart_cache_symbols", 8))))
        chart_cache_bars_var = tk.StringVar(value=str(self.settings.get("market_chart_cache_bars", DEFAULT_SETTINGS.get("market_chart_cache_bars", 120))))
        market_fallback_scan_age_var = tk.StringVar(value=str(self.settings.get("market_fallback_scan_max_age_s", DEFAULT_SETTINGS.get("market_fallback_scan_max_age_s", 7200.0))))
        market_fallback_snapshot_age_var = tk.StringVar(value=str(self.settings.get("market_fallback_snapshot_max_age_s", DEFAULT_SETTINGS.get("market_fallback_snapshot_max_age_s", 1800.0))))
        kucoin_unsupported_cooldown_var = tk.StringVar(value=str(self.settings.get("kucoin_unsupported_cooldown_s", DEFAULT_SETTINGS.get("kucoin_unsupported_cooldown_s", 21600.0))))
        crypto_price_error_log_cd_var = tk.StringVar(value=str(self.settings.get("crypto_price_error_log_cooldown_s", DEFAULT_SETTINGS.get("crypto_price_error_log_cooldown_s", 120.0))))
        key_rotation_warn_days_var = tk.StringVar(value=str(self.settings.get("key_rotation_warn_days", DEFAULT_SETTINGS.get("key_rotation_warn_days", 90))))

        hub_dir_var = tk.StringVar(value=self.settings.get("hub_data_dir", ""))



        neural_script_var = tk.StringVar(value=self.settings["script_neural_runner2"])
        trainer_script_var = tk.StringVar(value=self.settings.get("script_neural_trainer", "engines/pt_trainer.py"))
        trader_script_var = tk.StringVar(value=self.settings["script_trader"])

        ui_refresh_var = tk.StringVar(value=str(self.settings["ui_refresh_seconds"]))
        chart_refresh_var = tk.StringVar(value=str(self.settings["chart_refresh_seconds"]))
        candles_limit_var = tk.StringVar(value=str(self.settings["candles_limit"]))
        font_scale_var = tk.StringVar(value=str(self.settings.get("ui_font_scale_preset", DEFAULT_SETTINGS.get("ui_font_scale_preset", "normal")) or "normal"))
        layout_preset_var = tk.StringVar(value=str(self.settings.get("ui_layout_preset", DEFAULT_SETTINGS.get("ui_layout_preset", "auto")) or "auto"))
        auto_start_var = tk.BooleanVar(value=bool(self.settings.get("auto_start_scripts", False)))
        _mode_to_label = {
            "preset_managed": "Preset Managed",
            "self_managed": "Self Managed",
        }
        _label_to_mode = {v: k for k, v in _mode_to_label.items()}
        _profile_to_label = {
            "guarded": "Guarded",
            "balanced": "Balanced",
            "performance": "Performance",
        }
        _label_to_profile = {v: k for k, v in _profile_to_label.items()}
        _settings_mode_raw = str(self.settings.get("settings_control_mode", DEFAULT_SETTINGS.get("settings_control_mode", "self_managed")) or "self_managed").strip().lower()
        if _settings_mode_raw not in _mode_to_label:
            _settings_mode_raw = "self_managed"
        _settings_profile_raw = str(self.settings.get("settings_profile", DEFAULT_SETTINGS.get("settings_profile", "balanced")) or "balanced").strip().lower()
        if _settings_profile_raw not in _profile_to_label:
            _settings_profile_raw = "balanced"
        settings_mode_var = tk.StringVar(value=_mode_to_label.get(_settings_mode_raw, "Self Managed"))
        settings_profile_var = tk.StringVar(value=_profile_to_label.get(_settings_profile_raw, "Balanced"))
        alpaca_status_var = tk.StringVar(value="")
        oanda_status_var = tk.StringVar(value="")
        settings_mode_hint_var = tk.StringVar(value="")

        profile_var_map: Dict[str, tk.Variable] = {
            "trade_start_level": trade_start_level_var,
            "start_allocation_pct": start_alloc_pct_var,
            "dca_levels": dca_levels_var,
            "dca_multiplier": dca_mult_var,
            "max_dca_buys_per_24h": max_dca_var,
            "pm_start_pct_no_dca": pm_no_dca_var,
            "pm_start_pct_with_dca": pm_with_dca_var,
            "trailing_gap_pct": trailing_gap_var,
            "max_position_usd_per_coin": max_pos_per_coin_var,
            "max_total_exposure_pct": max_total_exposure_var,
            "market_rollout_stage": rollout_stage_var,
            "kucoin_unsupported_cooldown_s": kucoin_unsupported_cooldown_var,
            "crypto_price_error_log_cooldown_s": crypto_price_error_log_cd_var,
            "key_rotation_warn_days": key_rotation_warn_days_var,
            "ui_refresh_seconds": ui_refresh_var,
            "chart_refresh_seconds": chart_refresh_var,
            "candles_limit": candles_limit_var,
            "ui_font_scale_preset": font_scale_var,
            "ui_layout_preset": layout_preset_var,
            "auto_start_scripts": auto_start_var,
            "alpaca_paper_mode": alpaca_paper_var,
            "stock_universe_mode": stock_universe_mode_var,
            "stock_scan_max_symbols": stock_scan_max_symbols_var,
            "stock_min_price": stock_min_price_var,
            "stock_max_price": stock_max_price_var,
            "stock_min_dollar_volume": stock_min_dollar_volume_var,
            "stock_max_spread_bps": stock_max_spread_bps_var,
            "stock_gate_market_hours_scan": stock_gate_hours_var,
            "stock_min_bars_required": stock_min_bars_var,
            "stock_min_valid_bars_ratio": stock_min_valid_ratio_var,
            "stock_max_stale_hours": stock_max_stale_hours_var,
            "stock_show_rejected_rows": stock_show_rejected_var,
            "stock_scan_publish_watch_leaders": stock_publish_watch_leaders_var,
            "stock_scan_watch_leaders_count": stock_watch_leaders_count_var,
            "stock_leader_stability_margin_pct": stock_leader_stability_margin_var,
            "stock_auto_trade_enabled": stock_auto_trade_var,
            "stock_block_entries_on_cached_scan": stock_block_cached_scan_var,
            "stock_cached_scan_hard_block_age_s": stock_cached_scan_hard_block_age_var,
            "stock_cached_scan_entry_size_mult": stock_cached_scan_size_mult_var,
            "stock_require_data_quality_ok_for_entries": stock_require_data_quality_gate_var,
            "stock_require_reject_rate_max_pct": stock_reject_rate_gate_var,
            "stock_trade_notional_usd": stock_notional_var,
            "stock_max_open_positions": stock_max_pos_var,
            "stock_score_threshold": stock_score_threshold_var,
            "stock_profit_target_pct": stock_profit_target_var,
            "stock_trailing_gap_pct": stock_trailing_gap_var,
            "stock_max_day_trades": stock_day_trades_var,
            "stock_max_position_usd_per_symbol": stock_max_position_usd_var,
            "stock_max_total_exposure_pct": stock_max_exposure_var,
            "stock_live_guarded_score_mult": stock_guarded_mult_var,
            "stock_min_calib_prob_live_guarded": stock_min_calib_prob_var,
            "stock_max_slippage_bps": stock_max_slippage_bps_var,
            "stock_order_retry_count": stock_order_retry_count_var,
            "stock_max_loss_streak": stock_max_loss_streak_var,
            "stock_loss_streak_size_step_pct": stock_loss_size_step_var,
            "stock_loss_streak_size_floor_pct": stock_loss_size_floor_var,
            "stock_loss_cooldown_seconds": stock_loss_cooldown_var,
            "stock_max_daily_loss_usd": stock_max_daily_loss_usd_var,
            "stock_max_daily_loss_pct": stock_max_daily_loss_pct_var,
            "stock_min_samples_live_guarded": stock_min_samples_guarded_var,
            "stock_max_signal_age_seconds": stock_max_signal_age_var,
            "stock_reject_drift_warn_pct": stock_reject_warn_pct_var,
            "stock_block_new_entries_near_close": stock_block_near_close_var,
            "stock_no_new_entries_mins_to_close": stock_no_new_close_mins_var,
            "oanda_practice_mode": oanda_practice_var,
            "paper_only_unless_checklist_green": paper_only_guard_var,
            "forex_scan_max_pairs": forex_scan_max_pairs_var,
            "forex_max_spread_bps": fx_max_spread_bps_var,
            "forex_min_volatility_pct": fx_min_vol_pct_var,
            "forex_min_bars_required": fx_min_bars_var,
            "forex_min_valid_bars_ratio": fx_min_valid_ratio_var,
            "forex_max_stale_hours": fx_max_stale_hours_var,
            "forex_show_rejected_rows": fx_show_rejected_var,
            "forex_leader_stability_margin_pct": fx_leader_stability_margin_var,
            "forex_auto_trade_enabled": fx_auto_trade_var,
            "forex_block_entries_on_cached_scan": fx_block_cached_scan_var,
            "forex_cached_scan_hard_block_age_s": fx_cached_scan_hard_block_age_var,
            "forex_cached_scan_entry_size_mult": fx_cached_scan_size_mult_var,
            "forex_require_data_quality_ok_for_entries": fx_require_data_quality_gate_var,
            "forex_require_reject_rate_max_pct": fx_reject_rate_gate_var,
            "forex_trade_units": fx_trade_units_var,
            "forex_max_open_positions": fx_max_pos_var,
            "forex_max_position_usd_per_pair": fx_max_pos_usd_pair_var,
            "forex_score_threshold": fx_score_threshold_var,
            "forex_profit_target_pct": fx_profit_target_var,
            "forex_trailing_gap_pct": fx_trailing_gap_var,
            "forex_max_total_exposure_pct": fx_max_exposure_var,
            "forex_session_mode": fx_session_mode_var,
            "forex_live_guarded_score_mult": fx_guarded_mult_var,
            "forex_min_calib_prob_live_guarded": fx_min_calib_prob_var,
            "forex_max_slippage_bps": fx_max_slippage_bps_var,
            "forex_order_retry_count": fx_order_retry_count_var,
            "forex_max_loss_streak": fx_max_loss_streak_var,
            "forex_loss_streak_size_step_pct": fx_loss_size_step_var,
            "forex_loss_streak_size_floor_pct": fx_loss_size_floor_var,
            "forex_loss_cooldown_seconds": fx_loss_cooldown_var,
            "forex_max_daily_loss_usd": fx_max_daily_loss_usd_var,
            "forex_max_daily_loss_pct": fx_max_daily_loss_pct_var,
            "forex_min_samples_live_guarded": fx_min_samples_guarded_var,
            "forex_max_signal_age_seconds": fx_max_signal_age_var,
            "forex_reject_drift_warn_pct": fx_reject_warn_pct_var,
            "market_max_total_exposure_pct": market_global_exposure_var,
            "market_chart_cache_symbols": chart_cache_symbols_var,
            "market_chart_cache_bars": chart_cache_bars_var,
            "market_fallback_scan_max_age_s": market_fallback_scan_age_var,
            "market_fallback_snapshot_max_age_s": market_fallback_snapshot_age_var,
        }

        profile_overrides: Dict[str, Dict[str, Any]] = {
            "guarded": {
                "trade_start_level": 5,
                "start_allocation_pct": 0.25,
                "dca_levels": [-2.5, -5.0, -8.0, -12.0, -18.0],
                "dca_multiplier": 1.4,
                "max_dca_buys_per_24h": 1,
                "pm_start_pct_no_dca": 6.0,
                "pm_start_pct_with_dca": 3.5,
                "trailing_gap_pct": 0.35,
                "max_total_exposure_pct": 20.0,
                "market_rollout_stage": "shadow_only",
                "kucoin_unsupported_cooldown_s": 43200.0,
                "crypto_price_error_log_cooldown_s": 240.0,
                "key_rotation_warn_days": 60,
                "ui_refresh_seconds": 1.2,
                "chart_refresh_seconds": 12.0,
                "candles_limit": 120,
                "auto_start_scripts": False,
                "alpaca_paper_mode": True,
                "stock_universe_mode": "core",
                "stock_scan_max_symbols": 40,
                "stock_min_price": 8.0,
                "stock_max_price": 400.0,
                "stock_min_dollar_volume": 10000000.0,
                "stock_max_spread_bps": 25.0,
                "stock_min_bars_required": 32,
                "stock_min_valid_bars_ratio": 0.85,
                "stock_max_stale_hours": 3.0,
                "stock_show_rejected_rows": True,
                "stock_scan_watch_leaders_count": 5,
                "stock_leader_stability_margin_pct": 18.0,
                "stock_auto_trade_enabled": False,
                "stock_cached_scan_hard_block_age_s": 1200,
                "stock_cached_scan_entry_size_mult": 0.45,
                "stock_require_reject_rate_max_pct": 75.0,
                "stock_trade_notional_usd": 50.0,
                "stock_max_open_positions": 1,
                "stock_score_threshold": 0.35,
                "stock_profit_target_pct": 0.40,
                "stock_trailing_gap_pct": 0.15,
                "stock_max_day_trades": 1,
                "stock_max_total_exposure_pct": 20.0,
                "stock_live_guarded_score_mult": 1.35,
                "stock_min_calib_prob_live_guarded": 0.70,
                "stock_max_slippage_bps": 20.0,
                "stock_max_loss_streak": 2,
                "stock_loss_streak_size_step_pct": 0.20,
                "stock_loss_streak_size_floor_pct": 0.35,
                "stock_loss_cooldown_seconds": 2400,
                "stock_max_daily_loss_usd": 100.0,
                "stock_max_daily_loss_pct": 1.0,
                "stock_min_samples_live_guarded": 12,
                "stock_max_signal_age_seconds": 180,
                "stock_reject_drift_warn_pct": 50.0,
                "stock_block_new_entries_near_close": True,
                "stock_no_new_entries_mins_to_close": 30,
                "oanda_practice_mode": True,
                "paper_only_unless_checklist_green": True,
                "forex_universe_pairs": "",
                "forex_scan_max_pairs": 12,
                "forex_max_spread_bps": 6.0,
                "forex_min_volatility_pct": 0.02,
                "forex_min_bars_required": 32,
                "forex_min_valid_bars_ratio": 0.85,
                "forex_max_stale_hours": 4.0,
                "forex_show_rejected_rows": True,
                "forex_leader_stability_margin_pct": 18.0,
                "forex_auto_trade_enabled": False,
                "forex_cached_scan_hard_block_age_s": 900,
                "forex_cached_scan_entry_size_mult": 0.50,
                "forex_require_reject_rate_max_pct": 78.0,
                "forex_trade_units": 500,
                "forex_max_open_positions": 1,
                "forex_score_threshold": 0.30,
                "forex_profit_target_pct": 0.30,
                "forex_trailing_gap_pct": 0.12,
                "forex_max_total_exposure_pct": 20.0,
                "forex_session_mode": "london_ny",
                "forex_live_guarded_score_mult": 1.25,
                "forex_min_calib_prob_live_guarded": 0.68,
                "forex_max_slippage_bps": 4.0,
                "forex_max_loss_streak": 2,
                "forex_loss_streak_size_step_pct": 0.20,
                "forex_loss_streak_size_floor_pct": 0.35,
                "forex_loss_cooldown_seconds": 2400,
                "forex_max_daily_loss_usd": 100.0,
                "forex_max_daily_loss_pct": 1.0,
                "forex_min_samples_live_guarded": 12,
                "forex_max_signal_age_seconds": 180,
                "forex_reject_drift_warn_pct": 50.0,
                "market_max_total_exposure_pct": 25.0,
                "market_chart_cache_symbols": 6,
                "market_chart_cache_bars": 120,
                "market_fallback_scan_max_age_s": 3600.0,
                "market_fallback_snapshot_max_age_s": 1200.0,
            },
            "balanced": {},
            "performance": {
                "trade_start_level": 2,
                "start_allocation_pct": 0.8,
                "dca_levels": [-2.0, -4.0, -6.0, -9.0, -13.0, -18.0],
                "dca_multiplier": 2.6,
                "max_dca_buys_per_24h": 4,
                "pm_start_pct_no_dca": 4.0,
                "pm_start_pct_with_dca": 1.8,
                "trailing_gap_pct": 0.75,
                "max_total_exposure_pct": 65.0,
                "market_rollout_stage": "live_guarded",
                "kucoin_unsupported_cooldown_s": 14400.0,
                "crypto_price_error_log_cooldown_s": 60.0,
                "key_rotation_warn_days": 45,
                "ui_refresh_seconds": 0.8,
                "chart_refresh_seconds": 6.0,
                "candles_limit": 180,
                "auto_start_scripts": True,
                "alpaca_paper_mode": True,
                "stock_universe_mode": "all_tradable_filtered",
                "stock_scan_max_symbols": 240,
                "stock_min_price": 2.0,
                "stock_max_price": 700.0,
                "stock_min_dollar_volume": 2500000.0,
                "stock_max_spread_bps": 60.0,
                "stock_min_bars_required": 16,
                "stock_min_valid_bars_ratio": 0.6,
                "stock_max_stale_hours": 10.0,
                "stock_show_rejected_rows": False,
                "stock_scan_watch_leaders_count": 8,
                "stock_leader_stability_margin_pct": 6.0,
                "stock_auto_trade_enabled": True,
                "stock_cached_scan_hard_block_age_s": 900,
                "stock_cached_scan_entry_size_mult": 0.80,
                "stock_require_reject_rate_max_pct": 96.0,
                "stock_trade_notional_usd": 200.0,
                "stock_max_open_positions": 3,
                "stock_score_threshold": 0.12,
                "stock_profit_target_pct": 0.25,
                "stock_trailing_gap_pct": 0.28,
                "stock_max_day_trades": 6,
                "stock_max_total_exposure_pct": 55.0,
                "stock_live_guarded_score_mult": 1.05,
                "stock_min_calib_prob_live_guarded": 0.50,
                "stock_max_slippage_bps": 45.0,
                "stock_order_retry_count": 3,
                "stock_max_loss_streak": 5,
                "stock_loss_streak_size_step_pct": 0.12,
                "stock_loss_streak_size_floor_pct": 0.50,
                "stock_loss_cooldown_seconds": 900,
                "stock_max_daily_loss_usd": 0.0,
                "stock_max_daily_loss_pct": 0.0,
                "stock_min_samples_live_guarded": 4,
                "stock_max_signal_age_seconds": 420,
                "stock_reject_drift_warn_pct": 75.0,
                "stock_block_new_entries_near_close": False,
                "stock_no_new_entries_mins_to_close": 5,
                "oanda_practice_mode": True,
                "paper_only_unless_checklist_green": True,
                "forex_universe_pairs": "",
                "forex_scan_max_pairs": 36,
                "forex_max_spread_bps": 12.0,
                "forex_min_volatility_pct": 0.005,
                "forex_min_bars_required": 16,
                "forex_min_valid_bars_ratio": 0.6,
                "forex_max_stale_hours": 12.0,
                "forex_show_rejected_rows": False,
                "forex_leader_stability_margin_pct": 6.0,
                "forex_auto_trade_enabled": True,
                "forex_cached_scan_hard_block_age_s": 900,
                "forex_cached_scan_entry_size_mult": 0.85,
                "forex_require_reject_rate_max_pct": 96.0,
                "forex_trade_units": 2000,
                "forex_max_open_positions": 3,
                "forex_score_threshold": 0.12,
                "forex_profit_target_pct": 0.20,
                "forex_trailing_gap_pct": 0.18,
                "forex_max_total_exposure_pct": 55.0,
                "forex_session_mode": "all",
                "forex_live_guarded_score_mult": 1.05,
                "forex_min_calib_prob_live_guarded": 0.48,
                "forex_max_slippage_bps": 8.0,
                "forex_order_retry_count": 3,
                "forex_max_loss_streak": 5,
                "forex_loss_streak_size_step_pct": 0.12,
                "forex_loss_streak_size_floor_pct": 0.50,
                "forex_loss_cooldown_seconds": 900,
                "forex_max_daily_loss_usd": 0.0,
                "forex_max_daily_loss_pct": 0.0,
                "forex_min_samples_live_guarded": 4,
                "forex_max_signal_age_seconds": 420,
                "forex_reject_drift_warn_pct": 75.0,
                "market_max_total_exposure_pct": 60.0,
                "market_chart_cache_symbols": 12,
                "market_chart_cache_bars": 180,
                "market_fallback_scan_max_age_s": 5400.0,
                "market_fallback_snapshot_max_age_s": 1500.0,
            },
        }

        def _set_var_from_profile(key: str, value: Any) -> None:
            var = profile_var_map.get(key)
            if var is None:
                return
            if isinstance(var, tk.BooleanVar):
                var.set(bool(value))
                return
            if key == "dca_levels":
                if isinstance(value, (list, tuple)):
                    var.set(",".join(str(x) for x in value))
                else:
                    var.set(str(value or ""))
                return
            var.set(str(value))

        def _apply_profile_to_form(profile_key: str) -> None:
            pkey = str(profile_key or "balanced").strip().lower()
            if pkey not in {"guarded", "balanced", "performance"}:
                pkey = "balanced"
            base: Dict[str, Any] = {}
            for key in profile_var_map.keys():
                base[key] = DEFAULT_SETTINGS.get(key, self.settings.get(key))
            base.update(profile_overrides.get(pkey, {}))
            for key, value in base.items():
                _set_var_from_profile(key, value)

        def _sync_settings_mode_ui(*_args: Any) -> None:
            mode_key = _label_to_mode.get(str(settings_mode_var.get() or "").strip(), "self_managed")
            profile_key = _label_to_profile.get(str(settings_profile_var.get() or "").strip(), "balanced")
            is_preset = bool(mode_key == "preset_managed")
            if is_preset:
                _apply_profile_to_form(profile_key)
                settings_mode_hint_var.set(
                    f"Preset Managed is active: {str(settings_profile_var.get() or '').strip()} profile values are locked."
                )
            else:
                settings_mode_hint_var.set("Self Managed is active: you can edit all configurable fields manually.")
            for widget, restore_state in list(managed_controls):
                try:
                    if not widget.winfo_exists():
                        continue
                except Exception:
                    continue
                try:
                    widget.configure(state=("disabled" if is_preset else str(restore_state or "normal")))
                except Exception:
                    try:
                        if is_preset:
                            widget.state(["disabled"])
                        else:
                            widget.state(["!disabled"])
                    except Exception:
                        pass

        def _write_secret_file(path: str, value: str) -> None:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                f.write(str(value or "").strip())
            try:
                os.chmod(path, 0o600)
            except Exception:
                pass

        def _set_env_cred(env_key: str, value: str) -> None:
            txt = str(value or "").strip()
            if txt:
                os.environ[env_key] = txt
            else:
                os.environ.pop(env_key, None)

        def _refresh_alpaca_status() -> None:
            key_id, secret = get_alpaca_creds(self.settings, base_dir=self.project_dir)
            key_ok = bool(str(key_id or "").strip())
            secret_ok = bool(str(secret or "").strip())
            if key_ok and secret_ok:
                alpaca_status_var.set(f"Valid ✅  |  Key {self._mask_secret(key_id)}")
            elif key_ok and (not secret_ok):
                alpaca_status_var.set(f"Incomplete ❌  |  Key {self._mask_secret(key_id)} | Secret missing")
            elif (not key_ok) and secret_ok:
                alpaca_status_var.set("Incomplete ❌  |  API key ID missing")
            else:
                alpaca_status_var.set("Missing/invalid ❌  |  Add key ID + secret")

        def _refresh_oanda_status() -> None:
            account_id, token = get_oanda_creds(self.settings, base_dir=self.project_dir)
            account_ok = bool(str(account_id or "").strip())
            token_ok = bool(str(token or "").strip())
            if account_ok and token_ok:
                oanda_status_var.set(
                    f"Valid ✅  |  Account {str(account_id).strip()} | Token {self._mask_secret(token)}"
                )
            elif account_ok and (not token_ok):
                oanda_status_var.set(f"Incomplete ❌  |  Account {str(account_id).strip()} | Token missing")
            elif (not account_ok) and token_ok:
                oanda_status_var.set("Incomplete ❌  |  Account ID missing")
            else:
                oanda_status_var.set("Missing/invalid ❌  |  Add account ID + token")

        def _open_alpaca_key_editor() -> None:
            dlg = tk.Toplevel(win)
            dlg.title("Update Alpaca Keys")
            dlg.geometry("640x250")
            dlg.minsize(560, 220)
            dlg.transient(win)
            try:
                dlg.grab_set()
            except Exception:
                pass

            body = ttk.Frame(dlg)
            body.pack(fill="both", expand=True, padx=12, pady=12)
            body.columnconfigure(1, weight=1)
            ttk.Label(
                body,
                text="These keys are stored in local files under keys/ and never shown in Settings.",
                foreground=DARK_MUTED,
                justify="left",
                wraplength=580,
            ).grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 10))

            cur_key, cur_secret = get_alpaca_creds(self.settings, base_dir=self.project_dir)
            key_var = tk.StringVar(value=str(cur_key or ""))
            secret_var = tk.StringVar(value=str(cur_secret or ""))
            ttk.Label(body, text="Alpaca API key ID:").grid(row=1, column=0, sticky="w", padx=(0, 10), pady=6)
            ttk.Entry(body, textvariable=key_var).grid(row=1, column=1, sticky="ew", pady=6)
            ttk.Label(body, text="Alpaca secret key:").grid(row=2, column=0, sticky="w", padx=(0, 10), pady=6)
            ttk.Entry(body, textvariable=secret_var, show="*").grid(row=2, column=1, sticky="ew", pady=6)

            btns = ttk.Frame(body)
            btns.grid(row=3, column=0, columnspan=3, sticky="w", pady=(12, 0))

            def _save_alpaca() -> None:
                key_txt = str(key_var.get() or "").strip()
                secret_txt = str(secret_var.get() or "").strip()
                if (not key_txt) or (not secret_txt):
                    messagebox.showerror("Missing values", "Alpaca key ID and secret are both required.")
                    return
                key_path, secret_path = alpaca_credential_paths(self.project_dir)
                try:
                    _write_secret_file(key_path, key_txt)
                    _write_secret_file(secret_path, secret_txt)
                except Exception as exc:
                    messagebox.showerror("Save failed", f"Could not write Alpaca key files.\n\n{type(exc).__name__}: {exc}")
                    return
                _set_env_cred("POWERTRADER_ALPACA_API_KEY_ID", key_txt)
                _set_env_cred("POWERTRADER_ALPACA_SECRET_KEY", secret_txt)
                self.settings["alpaca_api_key_id"] = ""
                self.settings["alpaca_secret_key"] = ""
                _refresh_alpaca_status()
                dlg.destroy()

            ttk.Button(btns, text="Save", command=_save_alpaca).pack(side="left")
            ttk.Button(btns, text="Cancel", command=dlg.destroy).pack(side="left", padx=(8, 0))

        def _open_oanda_key_editor() -> None:
            dlg = tk.Toplevel(win)
            dlg.title("Update OANDA Keys")
            dlg.geometry("640x250")
            dlg.minsize(560, 220)
            dlg.transient(win)
            try:
                dlg.grab_set()
            except Exception:
                pass

            body = ttk.Frame(dlg)
            body.pack(fill="both", expand=True, padx=12, pady=12)
            body.columnconfigure(1, weight=1)
            ttk.Label(
                body,
                text="These credentials are stored in local files under keys/ and hidden from the main settings form.",
                foreground=DARK_MUTED,
                justify="left",
                wraplength=580,
            ).grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 10))

            cur_account, cur_token = get_oanda_creds(self.settings, base_dir=self.project_dir)
            account_var = tk.StringVar(value=str(cur_account or ""))
            token_var = tk.StringVar(value=str(cur_token or ""))
            ttk.Label(body, text="OANDA account ID:").grid(row=1, column=0, sticky="w", padx=(0, 10), pady=6)
            ttk.Entry(body, textvariable=account_var).grid(row=1, column=1, sticky="ew", pady=6)
            ttk.Label(body, text="OANDA API token:").grid(row=2, column=0, sticky="w", padx=(0, 10), pady=6)
            ttk.Entry(body, textvariable=token_var, show="*").grid(row=2, column=1, sticky="ew", pady=6)

            btns = ttk.Frame(body)
            btns.grid(row=3, column=0, columnspan=3, sticky="w", pady=(12, 0))

            def _save_oanda() -> None:
                account_txt = str(account_var.get() or "").strip()
                token_txt = str(token_var.get() or "").strip()
                if (not account_txt) or (not token_txt):
                    messagebox.showerror("Missing values", "OANDA account ID and API token are both required.")
                    return
                account_path, token_path = oanda_credential_paths(self.project_dir)
                try:
                    _write_secret_file(account_path, account_txt)
                    _write_secret_file(token_path, token_txt)
                except Exception as exc:
                    messagebox.showerror("Save failed", f"Could not write OANDA key files.\n\n{type(exc).__name__}: {exc}")
                    return
                _set_env_cred("POWERTRADER_OANDA_ACCOUNT_ID", account_txt)
                _set_env_cred("POWERTRADER_OANDA_API_TOKEN", token_txt)
                self.settings["oanda_account_id"] = ""
                self.settings["oanda_api_token"] = ""
                _refresh_oanda_status()
                dlg.destroy()

            ttk.Button(btns, text="Save", command=_save_oanda).pack(side="left")
            ttk.Button(btns, text="Cancel", command=dlg.destroy).pack(side="left", padx=(8, 0))

        _refresh_alpaca_status()
        _refresh_oanda_status()

        r = 0
        ttk.Label(
            frm,
            text="Settings are split by market tabs. Hover any label or field for impact examples.",
            foreground=DARK_MUTED,
            justify="left",
            wraplength=760,
        ).grid(row=r, column=0, columnspan=3, sticky="w", pady=(0, 8))
        r += 1
        add_choice_row(
            r,
            "Configuration mode:",
            settings_mode_var,
            ["Preset Managed", "Self Managed"],
            managed=False,
        ); r += 1
        add_choice_row(
            r,
            "Preset profile:",
            settings_profile_var,
            ["Guarded", "Balanced", "Performance"],
            managed=False,
        ); r += 1
        ttk.Label(
            frm,
            textvariable=settings_mode_hint_var,
            foreground=DARK_MUTED,
            justify="left",
            wraplength=760,
        ).grid(row=r, column=0, columnspan=3, sticky="w", pady=(0, 8))
        r += 1

        jump_row = ttk.Frame(frm)
        jump_row.grid(row=r, column=0, columnspan=3, sticky="ew", pady=(0, 8))
        jump_row.columnconfigure(2, weight=1)
        ttk.Label(jump_row, text="Quick jump:").grid(row=0, column=0, sticky="w")
        settings_tab_jump_var = tk.StringVar(value="Crypto")
        settings_tab_jump_combo = ttk.Combobox(
            jump_row,
            textvariable=settings_tab_jump_var,
            values=["Crypto", "Stocks", "Forex"],
            state="readonly",
            width=14,
        )
        settings_tab_jump_combo.grid(row=0, column=1, sticky="w", padx=(8, 12))
        ttk.Label(
            jump_row,
            text="Tip: keep Preset Managed for safer defaults; switch to Self Managed only when tuning.",
            foreground=DARK_MUTED,
            justify="left",
        ).grid(row=0, column=2, sticky="w")
        r += 1

        market_settings_nb = ttk.Notebook(frm)
        market_settings_nb.grid(row=r, column=0, columnspan=3, sticky="nsew")
        r += 1
        frm.rowconfigure(r - 1, weight=1)

        crypto_tab = ttk.Frame(market_settings_nb)
        stocks_tab = ttk.Frame(market_settings_nb)
        forex_tab = ttk.Frame(market_settings_nb)
        for tab in (crypto_tab, stocks_tab, forex_tab):
            tab.columnconfigure(0, weight=0)
            tab.columnconfigure(1, weight=1)
            tab.columnconfigure(2, weight=0)

        market_settings_nb.add(crypto_tab, text="Crypto")
        market_settings_nb.add(stocks_tab, text="Stocks")
        market_settings_nb.add(forex_tab, text="Forex")
        def _on_settings_tab_changed(_e=None) -> None:
            try:
                cur = str(market_settings_nb.tab(market_settings_nb.select(), "text") or "Crypto")
                if cur in {"Crypto", "Stocks", "Forex"}:
                    settings_tab_jump_var.set(cur)
            except Exception:
                pass
            win.after(0, _update_settings_scrollbars)

        market_settings_nb.bind("<<NotebookTabChanged>>", _on_settings_tab_changed, add="+")
        settings_tab_jump_combo.bind(
            "<<ComboboxSelected>>",
            lambda _e: market_settings_nb.select(
                crypto_tab
                if settings_tab_jump_var.get() == "Crypto"
                else (stocks_tab if settings_tab_jump_var.get() == "Stocks" else forex_tab)
            ),
            add="+",
        )

        cr = 0
        ttk.Label(
            crypto_tab,
            text="Crypto training/execution controls and runtime behavior.",
            foreground=DARK_MUTED,
            justify="left",
            wraplength=660,
        ).grid(row=cr, column=0, columnspan=3, sticky="w", pady=(0, 8))
        cr += 1
        add_row(cr, "Main neural folder:", main_dir_var, browse="dir", parent=crypto_tab); cr += 1
        add_row(cr, "Coins (comma):", coins_var, parent=crypto_tab); cr += 1
        add_row(cr, "Trade start level (1-7):", trade_start_level_var, parent=crypto_tab); cr += 1

        # Start allocation % (shows approx $/coin using the last known account value; always displays the $0.50 minimum).
        start_alloc_label = ttk.Label(crypto_tab, text="Start allocation %:")
        start_alloc_label.grid(row=cr, column=0, sticky="w", padx=(0, 10), pady=6)
        start_alloc_entry = ttk.Entry(crypto_tab, textvariable=start_alloc_pct_var)
        start_alloc_entry.grid(row=cr, column=1, sticky="ew", pady=6)
        _register_managed_control(start_alloc_entry, "normal", managed=True)
        start_hint = _resolve_help("Start allocation %:", "")
        if start_hint:
            _attach_tooltip(start_alloc_label, start_hint)
            _attach_tooltip(start_alloc_entry, start_hint)

        start_alloc_hint_var = tk.StringVar(value="")
        ttk.Label(crypto_tab, textvariable=start_alloc_hint_var).grid(row=cr, column=2, sticky="w", padx=(10, 0), pady=6)

        def _update_start_alloc_hint(*_):
            # Parse % (allow "0.01" or "0.01%").
            try:
                pct_txt = (start_alloc_pct_var.get() or "").strip().replace("%", "")
                pct = float(pct_txt) if pct_txt else 0.0
            except Exception:
                pct = normalize_start_allocation_pct(
                    self.settings.get("start_allocation_pct", DEFAULT_SETTINGS.get("start_allocation_pct", 0.5)),
                    default_pct=float(DEFAULT_SETTINGS.get("start_allocation_pct", 0.5)),
                )

            if pct < 0.0:
                pct = 0.0

            # Use the last account value we saw in trader_status.json (no extra API calls).
            try:
                total_val = float(getattr(self, "_last_total_account_value", 0.0) or 0.0)
            except Exception:
                total_val = 0.0

            per_coin = 0.0
            if total_val > 0.0:
                per_coin = total_val * (pct / 100.0)
            if per_coin < 0.5:
                per_coin = 0.5

            if total_val > 0.0:
                start_alloc_hint_var.set(f"~ {_fmt_money(per_coin)} per coin (min $0.50)")
            else:
                start_alloc_hint_var.set("~ $0.50 min per coin (needs account value)")

        _update_start_alloc_hint()
        start_alloc_pct_var.trace_add("write", _update_start_alloc_hint)
        coins_var.trace_add("write", _update_start_alloc_hint)
        cr += 1

        add_row(cr, "DCA levels (% list):", dca_levels_var, parent=crypto_tab); cr += 1
        add_row(cr, "DCA multiplier:", dca_mult_var, parent=crypto_tab); cr += 1
        add_row(cr, "Max DCA buys / coin (rolling 24h):", max_dca_var, parent=crypto_tab); cr += 1
        add_row(cr, "Trailing PM start % (no DCA):", pm_no_dca_var, parent=crypto_tab); cr += 1
        add_row(cr, "Trailing PM start % (with DCA):", pm_with_dca_var, parent=crypto_tab); cr += 1
        add_row(cr, "Trailing gap % (behind peak):", trailing_gap_var, parent=crypto_tab); cr += 1
        add_row(cr, "Max position USD / coin (0=off):", max_pos_per_coin_var, parent=crypto_tab); cr += 1
        add_row(cr, "Max total exposure % (0=off):", max_total_exposure_var, parent=crypto_tab); cr += 1

        ttk.Separator(crypto_tab, orient="horizontal").grid(row=cr, column=0, columnspan=3, sticky="ew", pady=10); cr += 1
        ttk.Label(
            crypto_tab,
            text="Runtime and process wiring",
            foreground=DARK_MUTED,
        ).grid(row=cr, column=0, columnspan=3, sticky="w", pady=(0, 4))
        cr += 1
        add_row(cr, "Hub data dir (optional):", hub_dir_var, browse="dir", parent=crypto_tab); cr += 1
        ttk.Label(
            crypto_tab,
            text="Script paths are managed by the app package and hidden from standard settings.",
            foreground=DARK_MUTED,
            justify="left",
            wraplength=660,
        ).grid(row=cr, column=0, columnspan=3, sticky="w", pady=(0, 6))
        cr += 1
        add_choice_row(
            cr,
            "Market rollout stage:",
            rollout_stage_var,
            ["legacy", "scan_expanded", "risk_caps", "execution_v2", "shadow_only", "live_guarded"],
            parent=crypto_tab,
        ); cr += 1
        ttk.Label(
            crypto_tab,
            text="Stages: legacy -> scan_expanded -> risk_caps -> execution_v2 -> shadow_only -> live_guarded",
            foreground=DARK_MUTED,
            justify="left",
            wraplength=660,
        ).grid(row=cr, column=0, columnspan=3, sticky="w", pady=(0, 6))
        cr += 1
        add_row(cr, "KuCoin unsupported cooldown sec:", kucoin_unsupported_cooldown_var, parent=crypto_tab); cr += 1
        add_row(cr, "Crypto price error log cooldown sec:", crypto_price_error_log_cd_var, parent=crypto_tab); cr += 1
        add_row(cr, "Key rotation warn days:", key_rotation_warn_days_var, parent=crypto_tab); cr += 1

        ttk.Separator(crypto_tab, orient="horizontal").grid(row=cr, column=0, columnspan=3, sticky="ew", pady=10); cr += 1
        ttk.Label(
            crypto_tab,
            text="Dashboard behavior",
            foreground=DARK_MUTED,
        ).grid(row=cr, column=0, columnspan=3, sticky="w", pady=(0, 4))
        cr += 1
        add_row(cr, "UI refresh seconds:", ui_refresh_var, parent=crypto_tab); cr += 1
        add_row(cr, "Chart refresh seconds:", chart_refresh_var, parent=crypto_tab); cr += 1
        add_row(cr, "Candles limit:", candles_limit_var, parent=crypto_tab); cr += 1
        add_choice_row(cr, "Font scale preset (small/normal/large):", font_scale_var, ["small", "normal", "large"], parent=crypto_tab); cr += 1
        add_choice_row(cr, "Layout preset (auto/compact/normal/wide):", layout_preset_var, ["auto", "compact", "normal", "wide"], parent=crypto_tab); cr += 1
        add_toggle_row(
            cr,
            "Startup automation:",
            "Auto start scripts on GUI launch",
            auto_start_var,
            parent=crypto_tab,
            tooltip="When enabled, opening the hub immediately launches runtime scripts.",
        ); cr += 1

        sr = 0
        ttk.Label(
            stocks_tab,
            text="Stocks scanner, broker, and execution controls.",
            foreground=DARK_MUTED,
            justify="left",
            wraplength=660,
        ).grid(row=sr, column=0, columnspan=3, sticky="w", pady=(0, 8))
        sr += 1
        add_toggle_row(sr, "Alpaca mode:", "Paper mode", alpaca_paper_var, parent=stocks_tab, tooltip="Paper mode is simulated. Turn off only when you intend live stock trading."); sr += 1
        add_status_action_row(
            sr,
            "Alpaca API keys:",
            alpaca_status_var,
            "Update Keys",
            _open_alpaca_key_editor,
            parent=stocks_tab,
            tooltip="Shows whether Alpaca credentials are valid. Use Update Keys to edit stored credentials.",
        ); sr += 1
        add_row(sr, "Alpaca base URL:", alpaca_base_url_var, parent=stocks_tab); sr += 1
        add_row(sr, "Alpaca data URL:", alpaca_data_url_var, parent=stocks_tab); sr += 1
        add_choice_row(sr, "Stock universe mode:", stock_universe_mode_var, ["core", "watchlist", "all_tradable_filtered"], parent=stocks_tab); sr += 1
        add_row(sr, "Stock universe symbols (watchlist):", stock_universe_symbols_var, parent=stocks_tab); sr += 1
        add_row(sr, "Stock scan max symbols:", stock_scan_max_symbols_var, parent=stocks_tab); sr += 1
        add_row(sr, "Stock min price:", stock_min_price_var, parent=stocks_tab); sr += 1
        add_row(sr, "Stock max price:", stock_max_price_var, parent=stocks_tab); sr += 1
        add_row(sr, "Stock min dollar volume:", stock_min_dollar_volume_var, parent=stocks_tab); sr += 1
        add_row(sr, "Stock max spread bps:", stock_max_spread_bps_var, parent=stocks_tab); sr += 1
        add_toggle_row(sr, "Stock scan market-hours gate:", "Only scan during market hours", stock_gate_hours_var, parent=stocks_tab, tooltip="Avoids off-hours scans with poor liquidity/price quality."); sr += 1
        add_row(sr, "Stock min bars required:", stock_min_bars_var, parent=stocks_tab); sr += 1
        add_row(sr, "Stock min valid bars ratio (0-1):", stock_min_valid_ratio_var, parent=stocks_tab); sr += 1
        add_row(sr, "Stock max stale hours:", stock_max_stale_hours_var, parent=stocks_tab); sr += 1
        add_toggle_row(sr, "Stock scanner show rejected rows:", "Include rejected rows in Scanner table", stock_show_rejected_var, parent=stocks_tab, tooltip="Useful for diagnosing why candidates fail quality gates."); sr += 1
        add_toggle_row(sr, "Stock scanner publish watch leaders:", "Show watch-mode leaders when no long setups", stock_publish_watch_leaders_var, parent=stocks_tab, tooltip="Keeps scanner informative when no symbols pass long-entry thresholds."); sr += 1
        add_row(sr, "Stock watch-leader count (fallback):", stock_watch_leaders_count_var, parent=stocks_tab); sr += 1
        add_row(sr, "Stock leader stability margin %:", stock_leader_stability_margin_var, parent=stocks_tab); sr += 1
        add_toggle_row(sr, "Stocks AI trader:", "Enable auto-trade (paper-safe)", stock_auto_trade_var, parent=stocks_tab, tooltip="When enabled, stock trader can place paper/live entries based on scanner outputs."); sr += 1
        add_toggle_row(sr, "Stock cached-scan safety gate:", "Block new entries when thinker is using cached fallback", stock_block_cached_scan_var, parent=stocks_tab, tooltip="Prevents trading on outdated scanner results."); sr += 1
        add_row(sr, "Stock cached fallback hard-block age sec:", stock_cached_scan_hard_block_age_var, parent=stocks_tab); sr += 1
        add_row(sr, "Stock cached fallback size multiplier (0.1-1.0):", stock_cached_scan_size_mult_var, parent=stocks_tab); sr += 1
        add_toggle_row(sr, "Stock data quality entry gate:", "Require thinker data quality OK before entry", stock_require_data_quality_gate_var, parent=stocks_tab, tooltip="Blocks entries when scanner data quality flags are unhealthy."); sr += 1
        add_row(sr, "Stock max reject rate % for entries:", stock_reject_rate_gate_var, parent=stocks_tab); sr += 1
        add_row(sr, "Stock order notional USD:", stock_notional_var, parent=stocks_tab); sr += 1
        add_row(sr, "Stock max open positions:", stock_max_pos_var, parent=stocks_tab); sr += 1
        add_row(sr, "Stock score threshold:", stock_score_threshold_var, parent=stocks_tab); sr += 1
        add_row(sr, "Stock profit target %:", stock_profit_target_var, parent=stocks_tab); sr += 1
        add_row(sr, "Stock trailing gap %:", stock_trailing_gap_var, parent=stocks_tab); sr += 1
        add_row(sr, "Stock max day trades / day:", stock_day_trades_var, parent=stocks_tab); sr += 1
        add_row(sr, "Stock max position USD/symbol (risk_caps):", stock_max_position_usd_var, parent=stocks_tab); sr += 1
        add_row(sr, "Stock max total exposure % (risk_caps):", stock_max_exposure_var, parent=stocks_tab); sr += 1
        add_row(sr, "Stock live_guarded score multiplier:", stock_guarded_mult_var, parent=stocks_tab); sr += 1
        add_row(sr, "Stock live_guarded min calibrated prob:", stock_min_calib_prob_var, parent=stocks_tab); sr += 1
        add_row(sr, "Stock max slippage bps:", stock_max_slippage_bps_var, parent=stocks_tab); sr += 1
        add_row(sr, "Stock order retry count:", stock_order_retry_count_var, parent=stocks_tab); sr += 1
        add_row(sr, "Stock max loss streak:", stock_max_loss_streak_var, parent=stocks_tab); sr += 1
        add_row(sr, "Stock loss-size step per streak (0-0.9):", stock_loss_size_step_var, parent=stocks_tab); sr += 1
        add_row(sr, "Stock loss-size floor scale (0.1-1.0):", stock_loss_size_floor_var, parent=stocks_tab); sr += 1
        add_row(sr, "Stock loss cooldown seconds:", stock_loss_cooldown_var, parent=stocks_tab); sr += 1
        add_row(sr, "Stock max daily loss USD (0=off):", stock_max_daily_loss_usd_var, parent=stocks_tab); sr += 1
        add_row(sr, "Stock max daily loss % (0=off):", stock_max_daily_loss_pct_var, parent=stocks_tab); sr += 1
        add_row(sr, "Stock min calibration samples (live_guarded):", stock_min_samples_guarded_var, parent=stocks_tab); sr += 1
        add_row(sr, "Stock max signal age seconds:", stock_max_signal_age_var, parent=stocks_tab); sr += 1
        add_row(sr, "Stock reject drift warn %:", stock_reject_warn_pct_var, parent=stocks_tab); sr += 1
        add_toggle_row(sr, "Stock near-close entry block:", "Block new entries near close", stock_block_near_close_var, parent=stocks_tab, tooltip="Prevents fresh entries in the final minutes of regular market session."); sr += 1
        add_row(sr, "Stock block mins to close:", stock_no_new_close_mins_var, parent=stocks_tab); sr += 1

        fr = 0
        ttk.Label(
            forex_tab,
            text="Forex scanner, OANDA connectivity, and execution controls.",
            foreground=DARK_MUTED,
            justify="left",
            wraplength=660,
        ).grid(row=fr, column=0, columnspan=3, sticky="w", pady=(0, 8))
        fr += 1
        add_toggle_row(fr, "OANDA mode:", "Practice mode", oanda_practice_var, parent=forex_tab, tooltip="Practice mode is simulated. Disable only for live forex execution."); fr += 1
        add_toggle_row(fr, "Live-mode guard:", "Paper-only unless checklist is green", paper_only_guard_var, parent=forex_tab, tooltip="Blocks switching to live modes until runtime checklist passes."); fr += 1
        add_status_action_row(
            fr,
            "OANDA API keys:",
            oanda_status_var,
            "Update Keys",
            _open_oanda_key_editor,
            parent=forex_tab,
            tooltip="Shows whether OANDA credentials are valid. Use Update Keys to edit stored credentials.",
        ); fr += 1
        add_row(fr, "OANDA REST URL:", oanda_rest_url_var, parent=forex_tab); fr += 1
        add_row(fr, "OANDA stream URL:", oanda_stream_url_var, parent=forex_tab); fr += 1
        add_row(fr, "Forex universe pairs (blank=auto broker universe):", forex_pairs_var, parent=forex_tab); fr += 1
        add_row(fr, "Forex scan max pairs:", forex_scan_max_pairs_var, parent=forex_tab); fr += 1
        add_row(fr, "Forex max spread bps:", fx_max_spread_bps_var, parent=forex_tab); fr += 1
        add_row(fr, "Forex min volatility %:", fx_min_vol_pct_var, parent=forex_tab); fr += 1
        add_row(fr, "Forex min bars required:", fx_min_bars_var, parent=forex_tab); fr += 1
        add_row(fr, "Forex min valid bars ratio (0-1):", fx_min_valid_ratio_var, parent=forex_tab); fr += 1
        add_row(fr, "Forex max stale hours:", fx_max_stale_hours_var, parent=forex_tab); fr += 1
        add_row(fr, "Forex leader stability margin %:", fx_leader_stability_margin_var, parent=forex_tab); fr += 1
        add_toggle_row(fr, "Forex scanner show rejected rows:", "Include rejected rows in Scanner table", fx_show_rejected_var, parent=forex_tab, tooltip="Shows why pairs fail scanner gates for debugging/tuning."); fr += 1
        add_toggle_row(fr, "Forex AI trader:", "Enable auto-trade (practice only)", fx_auto_trade_var, parent=forex_tab, tooltip="Allows forex trader loop to place entries using ranked scanner outputs."); fr += 1
        add_toggle_row(fr, "Forex cached-scan safety gate:", "Block new entries when thinker is using cached fallback", fx_block_cached_scan_var, parent=forex_tab, tooltip="Avoids trading when scanner is stale."); fr += 1
        add_row(fr, "Forex cached fallback hard-block age sec:", fx_cached_scan_hard_block_age_var, parent=forex_tab); fr += 1
        add_row(fr, "Forex cached fallback size multiplier (0.1-1.0):", fx_cached_scan_size_mult_var, parent=forex_tab); fr += 1
        add_toggle_row(fr, "Forex data quality entry gate:", "Require thinker data quality OK before entry", fx_require_data_quality_gate_var, parent=forex_tab, tooltip="Blocks entries when scanner quality checks fail."); fr += 1
        add_row(fr, "Forex max reject rate % for entries:", fx_reject_rate_gate_var, parent=forex_tab); fr += 1
        add_row(fr, "Forex trade units:", fx_trade_units_var, parent=forex_tab); fr += 1
        add_row(fr, "Forex max open positions:", fx_max_pos_var, parent=forex_tab); fr += 1
        add_row(fr, "Forex max position USD/pair (risk_caps):", fx_max_pos_usd_pair_var, parent=forex_tab); fr += 1
        add_row(fr, "Forex score threshold:", fx_score_threshold_var, parent=forex_tab); fr += 1
        add_row(fr, "Forex profit target %:", fx_profit_target_var, parent=forex_tab); fr += 1
        add_row(fr, "Forex trailing gap %:", fx_trailing_gap_var, parent=forex_tab); fr += 1
        add_row(fr, "Forex max exposure % (risk_caps proxy):", fx_max_exposure_var, parent=forex_tab); fr += 1
        add_choice_row(fr, "Forex session mode (all/london_ny/london/ny/asia):", fx_session_mode_var, ["all", "london_ny", "london", "ny", "asia"], parent=forex_tab); fr += 1
        add_row(fr, "Forex live_guarded score multiplier:", fx_guarded_mult_var, parent=forex_tab); fr += 1
        add_row(fr, "Forex live_guarded min calibrated prob:", fx_min_calib_prob_var, parent=forex_tab); fr += 1
        add_row(fr, "Forex max slippage bps:", fx_max_slippage_bps_var, parent=forex_tab); fr += 1
        add_row(fr, "Forex order retry count:", fx_order_retry_count_var, parent=forex_tab); fr += 1
        add_row(fr, "Forex max loss streak:", fx_max_loss_streak_var, parent=forex_tab); fr += 1
        add_row(fr, "Forex loss-size step per streak (0-0.9):", fx_loss_size_step_var, parent=forex_tab); fr += 1
        add_row(fr, "Forex loss-size floor scale (0.1-1.0):", fx_loss_size_floor_var, parent=forex_tab); fr += 1
        add_row(fr, "Forex loss cooldown seconds:", fx_loss_cooldown_var, parent=forex_tab); fr += 1
        add_row(fr, "Forex max daily loss USD (0=off):", fx_max_daily_loss_usd_var, parent=forex_tab); fr += 1
        add_row(fr, "Forex max daily loss % (0=off):", fx_max_daily_loss_pct_var, parent=forex_tab); fr += 1
        add_row(fr, "Forex min calibration samples (live_guarded):", fx_min_samples_guarded_var, parent=forex_tab); fr += 1
        add_row(fr, "Forex max signal age seconds:", fx_max_signal_age_var, parent=forex_tab); fr += 1
        add_row(fr, "Forex reject drift warn %:", fx_reject_warn_pct_var, parent=forex_tab); fr += 1
        add_row(fr, "Global max exposure % (all markets, 0=off):", market_global_exposure_var, parent=forex_tab); fr += 1
        add_row(fr, "Chart cache symbols (stocks/forex):", chart_cache_symbols_var, parent=forex_tab); fr += 1
        add_row(fr, "Chart cache bars per symbol:", chart_cache_bars_var, parent=forex_tab); fr += 1
        add_row(fr, "Scan fallback max age sec:", market_fallback_scan_age_var, parent=forex_tab); fr += 1
        add_row(fr, "Snapshot fallback max age sec:", market_fallback_snapshot_age_var, parent=forex_tab); fr += 1

        # --- Robinhood API setup (writes keys/r_key.txt + keys/r_secret.txt used by pt_trader.py) ---
        def _api_paths() -> Tuple[str, str]:
            return robinhood_credential_paths(self.project_dir)

        def _read_api_files() -> Tuple[str, str]:
            return get_robinhood_creds_from_files(self.project_dir)

        api_status_var = tk.StringVar(value="")

        def _refresh_api_status() -> None:
            key_path, secret_path = _api_paths()
            env_k, env_s = get_robinhood_creds_from_env()
            k, s = _read_api_files()
            if (not k) and env_k:
                k = env_k
            if (not s) and env_s:
                s = env_s

            missing = []
            if not k:
                missing.append("keys/r_key.txt (API Key)")
            if not s:
                missing.append("keys/r_secret.txt (PRIVATE key)")

            if missing:
                api_status_var.set("Not configured ❌ (missing " + ", ".join(missing) + ")")
            else:
                api_status_var.set("Configured ✅ (credentials found)")

        def _open_api_folder() -> None:
            """Open the folder where Robinhood credential files live."""
            try:
                folder = os.path.join(os.path.abspath(self.project_dir), "keys")
                os.makedirs(folder, exist_ok=True)
                if os.name == "nt":
                    os.startfile(folder)  # type: ignore[attr-defined]
                    return
                if sys.platform == "darwin":
                    subprocess.Popen(["open", folder])
                    return
                subprocess.Popen(["xdg-open", folder])
            except Exception as e:
                messagebox.showerror("Couldn't open folder", f"Tried to open:\n{self.project_dir}\n\nError:\n{e}")

        def _clear_api_files() -> None:
            """Delete Robinhood credential files (with a big confirmation)."""
            key_path, secret_path = _api_paths()
            if not messagebox.askyesno(
                "Delete API credentials?",
                "This will delete:\n"
                f"  {key_path}\n"
                f"  {secret_path}\n\n"
                "After deleting, the trader can NOT authenticate until you run the setup wizard again.\n\n"
                "Are you sure you want to delete these files?"
            ):
                return

            try:
                if os.path.isfile(key_path):
                    os.remove(key_path)
                if os.path.isfile(secret_path):
                    os.remove(secret_path)
            except Exception as e:
                messagebox.showerror("Delete failed", f"Couldn't delete the files:\n\n{e}")
                return

            _refresh_api_status()
            messagebox.showinfo("Deleted", "Deleted keys/r_key.txt and keys/r_secret.txt.")

        def _open_robinhood_api_wizard() -> None:
            """
            Beginner-friendly wizard that creates + stores Robinhood Crypto Trading API credentials.

            What we store:
              - keys/r_key.txt    = your Robinhood *API Key* (safe-ish to store, still treat as sensitive)
              - keys/r_secret.txt = your *PRIVATE key* (treat like a password — never share it)
            """
            import webbrowser
            import base64
            import platform
            from datetime import datetime
            import time

            # Friendly dependency errors (laymen-proof)
            try:
                from cryptography.hazmat.primitives.asymmetric import ed25519
                from cryptography.hazmat.primitives import serialization
            except Exception:
                messagebox.showerror(
                    "Missing dependency",
                    "The 'cryptography' package is required for Robinhood API setup.\n\n"
                    "Fix: open a Command Prompt / Terminal in this folder and run:\n"
                    "  pip install cryptography\n\n"
                    "Then re-open this Setup Wizard."
                )
                return

            try:
                import requests  # for the 'Test credentials' button
            except Exception:
                requests = None

            wiz = tk.Toplevel(win)
            wiz.title("Robinhood API Setup")
            # Big enough to show the bottom buttons, but still scrolls if the window is resized smaller.
            wiz.geometry("980x720")
            wiz.minsize(860, 620)
            wiz.configure(bg=DARK_BG)

            # Scrollable content area (same pattern as the Neural Levels scrollbar).
            viewport = ttk.Frame(wiz)
            viewport.pack(fill="both", expand=True, padx=12, pady=12)
            viewport.grid_rowconfigure(0, weight=1)
            viewport.grid_columnconfigure(0, weight=1)

            wiz_canvas = tk.Canvas(
                viewport,
                bg=DARK_BG,
                highlightthickness=1,
                highlightbackground=DARK_BORDER,
                bd=0,
            )
            wiz_canvas.grid(row=0, column=0, sticky="nsew")

            wiz_scroll = ttk.Scrollbar(viewport, orient="vertical", command=wiz_canvas.yview)
            wiz_scroll.grid(row=0, column=1, sticky="ns")
            wiz_canvas.configure(yscrollcommand=wiz_scroll.set)

            container = ttk.Frame(wiz_canvas)
            wiz_window = wiz_canvas.create_window((0, 0), window=container, anchor="nw")
            container.columnconfigure(0, weight=1)

            def _update_wiz_scrollbars(event=None) -> None:
                """Update scrollregion + hide/show the scrollbar depending on overflow."""
                try:
                    c = wiz_canvas
                    win_id = wiz_window

                    c.update_idletasks()
                    bbox = c.bbox(win_id)
                    if not bbox:
                        wiz_scroll.grid_remove()
                        return

                    c.configure(scrollregion=bbox)
                    content_h = int(bbox[3] - bbox[1])
                    view_h = int(c.winfo_height())

                    if content_h > (view_h + 1):
                        wiz_scroll.grid()
                    else:
                        wiz_scroll.grid_remove()
                        try:
                            c.yview_moveto(0)
                        except Exception:
                            pass
                except Exception:
                    pass

            def _on_wiz_canvas_configure(e) -> None:
                # Keep the inner frame exactly the canvas width so labels wrap nicely.
                try:
                    wiz_canvas.itemconfigure(wiz_window, width=int(e.width))
                except Exception:
                    pass
                _update_wiz_scrollbars()

            wiz_canvas.bind("<Configure>", _on_wiz_canvas_configure, add="+")
            container.bind("<Configure>", _update_wiz_scrollbars, add="+")

            def _wheel(e):
                try:
                    if wiz_scroll.winfo_ismapped():
                        wiz_canvas.yview_scroll(int(-1 * (e.delta / 120)), "units")
                except Exception:
                    pass

            wiz_canvas.bind("<Enter>", lambda _e: wiz_canvas.focus_set(), add="+")
            wiz_canvas.bind("<MouseWheel>", _wheel, add="+")  # Windows / Mac
            wiz_canvas.bind("<Button-4>", lambda _e: wiz_canvas.yview_scroll(-3, "units"), add="+")  # Linux
            wiz_canvas.bind("<Button-5>", lambda _e: wiz_canvas.yview_scroll(3, "units"), add="+")   # Linux


            key_path, secret_path = _api_paths()

            # Load any existing credentials so users can update without re-generating keys.
            existing_api_key, existing_private_b64 = _read_api_files()
            private_b64_state = {"value": (existing_private_b64 or "").strip()}

            # -----------------------------
            # Helpers (open folder, copy, etc.)
            # -----------------------------
            def _open_in_file_manager(path: str) -> None:
                try:
                    p = os.path.abspath(path)
                    if os.name == "nt":
                        os.startfile(p)  # type: ignore[attr-defined]
                        return
                    if sys.platform == "darwin":
                        subprocess.Popen(["open", p])
                        return
                    subprocess.Popen(["xdg-open", p])
                except Exception as e:
                    messagebox.showerror("Couldn't open folder", f"Tried to open:\n{path}\n\nError:\n{e}")

            def _copy_to_clipboard(txt: str, title: str = "Copied") -> None:
                try:
                    wiz.clipboard_clear()
                    wiz.clipboard_append(txt)
                    messagebox.showinfo(title, "Copied to clipboard.")
                except Exception:
                    pass

            def _mask_path(p: str) -> str:
                try:
                    return os.path.abspath(p)
                except Exception:
                    return p

            # -----------------------------
            # Big, beginner-friendly instructions
            # -----------------------------
            intro = (
                "This trader uses Robinhood's Crypto Trading API credentials.\n\n"
                "You only do this once. When finished, pt_trader.py can authenticate automatically.\n\n"
                "✅ What you will do in this window:\n"
                "  1) Generate a Public Key + Private Key (Ed25519).\n"
                "  2) Copy the PUBLIC key and paste it into Robinhood to create an API credential.\n"
                "  3) Robinhood will show you an API Key (usually starts with 'rh...'). Copy it.\n"
                "  4) Paste that API Key back here and click Save.\n\n"
                "🧭 EXACTLY where to paste the Public Key on Robinhood (desktop web is best):\n"
                "  A) Log in to Robinhood on a computer.\n"
                "  B) Click Account (top-right) → Settings.\n"
                "  C) Click Crypto.\n"
                "  D) Scroll down to API Trading and click + Add Key (or Add key).\n"
                "  E) Paste the Public Key into the Public key field.\n"
                "  F) Give it any name (example: PowerTrader).\n"
                "  G) Permissions: this TRADER needs READ + TRADE. (READ-only cannot place orders.)\n"
                "  H) Click Save. Robinhood shows your API Key — copy it right away (it may only show once).\n\n"
                "📱 Mobile note: if you can't find API Trading in the app, use robinhood.com in a browser.\n\n"
                "This wizard will save two files in the keys folder:\n"
                "  - keys/r_key.txt    (your API Key)\n"
                "  - keys/r_secret.txt (your PRIVATE key in base64)  ← keep this secret like a password\n"
            )

            intro_lbl = ttk.Label(container, text=intro, justify="left")
            intro_lbl.grid(row=0, column=0, sticky="ew", pady=(0, 10))

            top_btns = ttk.Frame(container)
            top_btns.grid(row=1, column=0, sticky="ew", pady=(0, 10))
            top_btns.columnconfigure(0, weight=1)

            def open_robinhood_page():
                # Robinhood entry point. User will still need to click into Settings → Crypto → API Trading.
                webbrowser.open("https://robinhood.com/account/crypto")

            ttk.Button(top_btns, text="Open Robinhood API Credentials page (Crypto)", command=open_robinhood_page).pack(side="left")
            ttk.Button(top_btns, text="Open Robinhood Crypto Trading API docs", command=lambda: webbrowser.open("https://docs.robinhood.com/crypto/trading/")).pack(side="left", padx=8)
            keys_dir = os.path.join(self.project_dir, "keys")
            ttk.Button(top_btns, text="Open keys folder", command=lambda: _open_in_file_manager(keys_dir)).pack(side="left", padx=8)

            # -----------------------------
            # Step 1 — Generate keys
            # -----------------------------
            step1 = ttk.LabelFrame(container, text="Step 1 — Generate your keys (click once)")
            step1.grid(row=2, column=0, sticky="nsew", pady=(0, 10))
            step1.columnconfigure(0, weight=1)

            ttk.Label(step1, text="Public Key (this is what you paste into Robinhood):").grid(row=0, column=0, sticky="w", padx=10, pady=(8, 0))

            pub_box = tk.Text(step1, height=4, wrap="none")
            pub_box.grid(row=1, column=0, sticky="nsew", padx=10, pady=(6, 10))
            pub_box.configure(bg=DARK_PANEL, fg=DARK_FG, insertbackground=DARK_FG)

            def _render_public_from_private_b64(priv_b64: str) -> str:
                """Return Robinhood-compatible Public Key: base64(raw_ed25519_public_key_32_bytes)."""
                try:
                    raw = base64.b64decode(priv_b64)

                    # Accept either:
                    #   - 32 bytes: Ed25519 seed
                    #   - 64 bytes: NaCl/tweetnacl secretKey (seed + public)
                    if len(raw) == 64:
                        seed = raw[:32]
                    elif len(raw) == 32:
                        seed = raw
                    else:
                        return ""

                    pk = ed25519.Ed25519PrivateKey.from_private_bytes(seed)
                    pub_raw = pk.public_key().public_bytes(
                        encoding=serialization.Encoding.Raw,
                        format=serialization.PublicFormat.Raw,
                    )
                    return base64.b64encode(pub_raw).decode("utf-8")
                except Exception:
                    return ""

            def _set_pub_text(txt: str) -> None:
                try:
                    pub_box.delete("1.0", "end")
                    pub_box.insert("1.0", txt or "")
                except Exception:
                    pass

            # If already configured before, show the public key again (derived from stored private key)
            if private_b64_state["value"]:
                _set_pub_text(_render_public_from_private_b64(private_b64_state["value"]))

            def generate_keys():
                # Generate an Ed25519 keypair (Robinhood expects base64 raw public key bytes)
                priv = ed25519.Ed25519PrivateKey.generate()
                pub = priv.public_key()

                seed = priv.private_bytes(
                    encoding=serialization.Encoding.Raw,
                    format=serialization.PrivateFormat.Raw,
                    encryption_algorithm=serialization.NoEncryption(),
                )
                pub_raw = pub.public_bytes(
                    encoding=serialization.Encoding.Raw,
                    format=serialization.PublicFormat.Raw,
                )

                # Store PRIVATE key as base64(seed32) because pt_thinker.py uses nacl.signing.SigningKey(seed)
                # and it requires exactly 32 bytes.
                private_b64_state["value"] = base64.b64encode(seed).decode("utf-8")

                # Show what you paste into Robinhood: base64(raw public key)
                _set_pub_text(base64.b64encode(pub_raw).decode("utf-8"))


                messagebox.showinfo(
                    "Step 1 complete",
                    "Public/Private keys generated.\n\n"
                    "Next (Robinhood):\n"
                    "  1) Click 'Copy Public Key' in this window\n"
                    "  2) On Robinhood (desktop web): Account → Settings → Crypto\n"
                    "  3) Scroll to 'API Trading' → click '+ Add Key'\n"
                    "  4) Paste the Public Key (base64) into the 'Public key' field\n"
                    "  5) Enable permissions READ + TRADE (this trader needs both), then Save\n"
                    "  6) Robinhood shows an API Key (usually starts with 'rh...') — copy it right away\n\n"
                    "Then come back here and paste that API Key into the 'API Key' box."
                )



            def copy_public_key():
                txt = (pub_box.get("1.0", "end") or "").strip()
                if not txt:
                    messagebox.showwarning("Nothing to copy", "Click 'Generate Keys' first.")
                    return
                _copy_to_clipboard(txt, title="Public Key copied")

            step1_btns = ttk.Frame(step1)
            step1_btns.grid(row=2, column=0, sticky="w", padx=10, pady=(0, 10))
            ttk.Button(step1_btns, text="Generate Keys", command=generate_keys).pack(side="left")
            ttk.Button(step1_btns, text="Copy Public Key", command=copy_public_key).pack(side="left", padx=8)

            # -----------------------------
            # Step 2 — Paste API key (from Robinhood)
            # -----------------------------
            step2 = ttk.LabelFrame(container, text="Step 2 — Paste your Robinhood API Key here")
            step2.grid(row=3, column=0, sticky="nsew", pady=(0, 10))
            step2.columnconfigure(0, weight=1)

            step2_help = (
                "In Robinhood, after you add the Public Key, Robinhood will show an API Key.\n"
                "Paste that API Key below. (It often starts with 'rh.'.)"
            )
            ttk.Label(step2, text=step2_help, justify="left").grid(row=0, column=0, sticky="w", padx=10, pady=(8, 0))

            api_key_var = tk.StringVar(value=existing_api_key or "")
            api_ent = ttk.Entry(step2, textvariable=api_key_var)
            api_ent.grid(row=1, column=0, sticky="ew", padx=10, pady=(6, 10))

            def _test_credentials() -> None:
                api_key = (api_key_var.get() or "").strip()
                priv_b64 = (private_b64_state.get("value") or "").strip()

                if not requests:
                    messagebox.showerror(
                        "Missing dependency",
                        "The 'requests' package is required for the Test button.\n\n"
                        "Fix: pip install requests\n\n"
                        "(You can still Save without testing.)"
                    )
                    return

                if not priv_b64:
                    messagebox.showerror("Missing private key", "Step 1: click 'Generate Keys' first.")
                    return
                if not api_key:
                    messagebox.showerror("Missing API key", "Paste the API key from Robinhood into Step 2 first.")
                    return

                # Safe test: market-data endpoint (no trading)
                base_url = "https://trading.robinhood.com"
                path = "/api/v1/crypto/marketdata/best_bid_ask/?symbol=BTC-USD"
                method = "GET"
                body = ""
                ts = int(time.time())
                msg = f"{api_key}{ts}{path}{method}{body}".encode("utf-8")

                try:
                    raw = base64.b64decode(priv_b64)

                    # Accept either:
                    #   - 32 bytes: Ed25519 seed
                    #   - 64 bytes: NaCl/tweetnacl secretKey (seed + public)
                    if len(raw) == 64:
                        seed = raw[:32]
                    elif len(raw) == 32:
                        seed = raw
                    else:
                        raise ValueError(f"Unexpected private key length: {len(raw)} bytes (expected 32 or 64)")

                    pk = ed25519.Ed25519PrivateKey.from_private_bytes(seed)
                    sig_b64 = base64.b64encode(pk.sign(msg)).decode("utf-8")
                except Exception as e:
                    messagebox.showerror("Bad private key", f"Couldn't use your private key (keys/r_secret.txt).\n\nError:\n{e}")
                    return


                headers = {
                    "x-api-key": api_key,
                    "x-timestamp": str(ts),
                    "x-signature": sig_b64,
                    "Content-Type": "application/json",
                }

                try:
                    resp = requests.get(f"{base_url}{path}", headers=headers, timeout=10)
                    if resp.status_code >= 400:
                        # Give layman-friendly hints for common failures
                        hint = ""
                        if resp.status_code in (401, 403):
                            hint = (
                                "\n\nCommon fixes:\n"
                                "  • Make sure you pasted the API Key (not the public key).\n"
                                "  • In Robinhood, ensure the key has permissions READ + TRADE.\n"
                                "  • If you just created the key, wait 30–60 seconds and try again.\n"
                            )
                        messagebox.showerror("Test failed", f"Robinhood returned HTTP {resp.status_code}.\n\n{resp.text}{hint}")
                        return

                    data = resp.json()
                    # Try to show something reassuring
                    ask = None
                    try:
                        if data.get("results"):
                            ask = data["results"][0].get("ask_inclusive_of_buy_spread")
                    except Exception:
                        pass

                    messagebox.showinfo(
                        "Test successful",
                        "✅ Your API Key + Private Key worked!\n\n"
                        "Robinhood responded successfully.\n"
                        f"BTC-USD ask (example): {ask if ask is not None else 'received'}\n\n"
                        "Next: click Save."
                    )
                except Exception as e:
                    messagebox.showerror("Test failed", f"Couldn't reach Robinhood.\n\nError:\n{e}")

            step2_btns = ttk.Frame(step2)
            step2_btns.grid(row=2, column=0, sticky="w", padx=10, pady=(0, 10))
            ttk.Button(step2_btns, text="Test Credentials (safe, no trading)", command=_test_credentials).pack(side="left")

            # -----------------------------
            # Step 3 — Save
            # -----------------------------
            step3 = ttk.LabelFrame(container, text="Step 3 — Save to files (required)")
            step3.grid(row=4, column=0, sticky="nsew")
            step3.columnconfigure(0, weight=1)

            ack_var = tk.BooleanVar(value=False)
            ack = ttk.Checkbutton(
                step3,
                text="I understand keys/r_secret.txt is PRIVATE and I will not share it.",
                variable=ack_var,
            )
            ack.grid(row=0, column=0, sticky="w", padx=10, pady=(10, 6))

            save_btns = ttk.Frame(step3)
            save_btns.grid(row=1, column=0, sticky="w", padx=10, pady=(0, 12))

            def do_save():
                api_key = (api_key_var.get() or "").strip()
                priv_b64 = (private_b64_state.get("value") or "").strip()

                if not priv_b64:
                    messagebox.showerror("Missing private key", "Step 1: click 'Generate Keys' first.")
                    return

                # Normalize private key so pt_thinker.py can load it:
                # - Accept 32 bytes (seed) OR 64 bytes (seed+pub) from older hub versions
                # - Save ONLY base64(seed32) to keys/r_secret.txt
                try:
                    raw = base64.b64decode(priv_b64)
                    if len(raw) == 64:
                        raw = raw[:32]
                        priv_b64 = base64.b64encode(raw).decode("utf-8")
                        private_b64_state["value"] = priv_b64  # keep UI state consistent
                    elif len(raw) != 32:
                        messagebox.showerror(
                            "Bad private key",
                            f"Your private key decodes to {len(raw)} bytes, but it must be 32 bytes.\n\n"
                            "Click 'Generate Keys' again to create a fresh keypair."
                        )
                        return
                except Exception as e:
                    messagebox.showerror(
                        "Bad private key",
                        f"Couldn't decode the private key as base64.\n\nError:\n{e}"
                    )
                    return

                if not api_key:
                    messagebox.showerror("Missing API key", "Step 2: paste your API key from Robinhood first.")
                    return
                if not bool(ack_var.get()):
                    messagebox.showwarning(
                        "Please confirm",
                        "For safety, please check the box confirming you understand keys/r_secret.txt is private."
                    )
                    return


                # Small sanity warning (don’t block, just help)
                if len(api_key) < 10:
                    if not messagebox.askyesno(
                        "API key looks short",
                        "That API key looks unusually short. Are you sure you pasted the API Key from Robinhood?"
                    ):
                        return

                # Back up existing files (so user can undo mistakes)
                try:
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    if os.path.isfile(key_path):
                        shutil.copy2(key_path, f"{key_path}.bak_{ts}")
                    if os.path.isfile(secret_path):
                        shutil.copy2(secret_path, f"{secret_path}.bak_{ts}")
                except Exception:
                    pass

                try:
                    with open(key_path, "w", encoding="utf-8") as f:
                        f.write(api_key)
                    with open(secret_path, "w", encoding="utf-8") as f:
                        f.write(priv_b64)
                except Exception as e:
                    messagebox.showerror("Save failed", f"Couldn't write the credential files.\n\nError:\n{e}")
                    return

                _refresh_api_status()
                messagebox.showinfo(
                    "Saved",
                    "✅ Saved!\n\n"
                    "The trader will automatically read these files next time it starts:\n"
                    f"  API Key → {_mask_path(key_path)}\n"
                    f"  Private Key → {_mask_path(secret_path)}\n\n"
                    "Next steps:\n"
                    "  1) Close this window\n"
                    "  2) Start the trader (pt_trader.py)\n"
                    "If something fails, come back here and click 'Test Credentials'."
                )
                wiz.destroy()

            ttk.Button(save_btns, text="Save", command=do_save).pack(side="left")
            ttk.Button(save_btns, text="Close", command=wiz.destroy).pack(side="left", padx=8)

        ttk.Separator(crypto_tab, orient="horizontal").grid(row=cr, column=0, columnspan=3, sticky="ew", pady=10)
        cr += 1

        ttk.Label(crypto_tab, text="Robinhood API:").grid(row=cr, column=0, sticky="w", padx=(0, 10), pady=6)
        api_row = ttk.Frame(crypto_tab)
        api_row.grid(row=cr, column=1, columnspan=2, sticky="ew", pady=6)
        api_row.columnconfigure(0, weight=1)
        ttk.Label(api_row, textvariable=api_status_var).grid(row=0, column=0, sticky="w")
        ttk.Button(api_row, text="Update Keys", command=_open_robinhood_api_wizard).grid(row=0, column=1, sticky="e", padx=(10, 0))
        _attach_tooltip(api_row, "Launches the credential wizard for Robinhood crypto API keys used by the crypto trader.")
        cr += 1

        _refresh_api_status()
        settings_mode_var.trace_add("write", _sync_settings_mode_ui)
        settings_profile_var.trace_add("write", _sync_settings_mode_ui)
        win.after(40, _sync_settings_mode_ui)

        def _apply_focus_target() -> None:
            if not focus_key:
                return
            key = str(focus_key or "").strip().lower()
            if key in {"stocks", "stocks_credentials", "alpaca", "alpaca_credentials"}:
                market_settings_nb.select(stocks_tab)
                if "credential" in key or "alpaca" in key:
                    win.after(120, _open_alpaca_key_editor)
                return
            if key in {"forex", "forex_credentials", "oanda", "oanda_credentials"}:
                market_settings_nb.select(forex_tab)
                if "credential" in key or "oanda" in key:
                    win.after(120, _open_oanda_key_editor)
                return
            if key in {"crypto", "crypto_credentials", "robinhood", "robinhood_credentials"}:
                market_settings_nb.select(crypto_tab)
                if "credential" in key or "robinhood" in key:
                    win.after(120, _open_robinhood_api_wizard)
                return

        win.after(80, _apply_focus_target)

        btns = ttk.Frame(frm)
        btns.grid(row=r, column=0, columnspan=3, sticky="ew", pady=14)
        btns.columnconfigure(0, weight=1)

        def save():
            try:
                # Track coins before changes so we can detect newly added coins
                prev_coins = set([str(c).strip().upper() for c in (self.settings.get("coins") or []) if str(c).strip()])
                mode_key = _label_to_mode.get(str(settings_mode_var.get() or "").strip(), "self_managed")
                profile_key = _label_to_profile.get(str(settings_profile_var.get() or "").strip(), "balanced")
                if mode_key == "preset_managed":
                    _apply_profile_to_form(profile_key)
                self.settings["settings_control_mode"] = str(mode_key)
                self.settings["settings_profile"] = str(profile_key)
                req_alpaca_live = not bool(alpaca_paper_var.get())
                req_oanda_live = not bool(oanda_practice_var.get())
                req_live_markets = []
                if req_alpaca_live:
                    req_live_markets.append("Stocks/Alpaca")
                if req_oanda_live:
                    req_live_markets.append("Forex/OANDA")
                paper_guard_enabled = bool(paper_only_guard_var.get())
                if req_live_markets:
                    runtime_snapshot = _safe_read_json(os.path.join(self.hub_dir, "runtime_state.json")) or {}
                    checklist = evaluate_live_mode_checklist(runtime_snapshot)
                    if paper_guard_enabled and (not bool(checklist.get("ok", False))):
                        reasons = ", ".join([str(x) for x in list(checklist.get("reasons", []) or [])[:5]]) or "checklist_not_green"
                        messagebox.showerror(
                            "Live mode blocked",
                            "Live mode is locked because checklist is not green.\n\n"
                            f"Reasons: {reasons}\n\n"
                            "Fix Runtime/Alerts first, or disable the paper-only guard in Settings.",
                        )
                        return
                    if not messagebox.askyesno(
                        "Confirm live mode",
                        ("You are switching to LIVE execution for:\n"
                        + "".join([f"  - {m}\n" for m in req_live_markets])
                        + "\nThis can place real orders with real funds.\n"
                        + "Continue?"),
                    ):
                        return

                self.settings["main_neural_dir"] = main_dir_var.get().strip()
                self.settings["coins"] = [c.strip().upper() for c in coins_var.get().split(",") if c.strip()]
                self.settings["trade_start_level"] = max(1, min(int(float(trade_start_level_var.get().strip())), 7))

                sap = (start_alloc_pct_var.get() or "").strip()
                self.settings["start_allocation_pct"] = normalize_start_allocation_pct(
                    sap,
                    default_pct=float(DEFAULT_SETTINGS.get("start_allocation_pct", 0.5)),
                )

                dm = (dca_mult_var.get() or "").strip()
                try:
                    dm_f = float(dm)
                except Exception:
                    dm_f = float(self.settings.get("dca_multiplier", DEFAULT_SETTINGS.get("dca_multiplier", 2.0)) or 2.0)
                if dm_f < 0.0:
                    dm_f = 0.0
                self.settings["dca_multiplier"] = dm_f

                raw_dca = (dca_levels_var.get() or "").replace(",", " ").split()
                dca_levels = []
                for tok in raw_dca:
                    try:
                        dca_levels.append(float(tok))
                    except Exception:
                        pass
                if not dca_levels:
                    dca_levels = list(DEFAULT_SETTINGS.get("dca_levels", []))
                self.settings["dca_levels"] = dca_levels

                md = (max_dca_var.get() or "").strip()
                try:
                    md_i = int(float(md))
                except Exception:
                    md_i = int(self.settings.get("max_dca_buys_per_24h", DEFAULT_SETTINGS.get("max_dca_buys_per_24h", 2)) or 2)
                if md_i < 0:
                    md_i = 0
                self.settings["max_dca_buys_per_24h"] = md_i


                # --- Trailing PM settings ---
                try:
                    pm0 = float((pm_no_dca_var.get() or "").strip().replace("%", "") or 0.0)
                except Exception:
                    pm0 = float(self.settings.get("pm_start_pct_no_dca", DEFAULT_SETTINGS.get("pm_start_pct_no_dca", 5.0)) or 5.0)
                if pm0 < 0.0:
                    pm0 = 0.0
                self.settings["pm_start_pct_no_dca"] = pm0

                try:
                    pm1 = float((pm_with_dca_var.get() or "").strip().replace("%", "") or 0.0)
                except Exception:
                    pm1 = float(self.settings.get("pm_start_pct_with_dca", DEFAULT_SETTINGS.get("pm_start_pct_with_dca", 2.5)) or 2.5)
                if pm1 < 0.0:
                    pm1 = 0.0
                self.settings["pm_start_pct_with_dca"] = pm1

                try:
                    tg = float((trailing_gap_var.get() or "").strip().replace("%", "") or 0.0)
                except Exception:
                    tg = float(self.settings.get("trailing_gap_pct", DEFAULT_SETTINGS.get("trailing_gap_pct", 0.5)) or 0.5)
                if tg < 0.0:
                    tg = 0.0
                self.settings["trailing_gap_pct"] = tg

                try:
                    mpc = float((max_pos_per_coin_var.get() or "").strip().replace("$", "") or 0.0)
                except Exception:
                    mpc = float(self.settings.get("max_position_usd_per_coin", DEFAULT_SETTINGS.get("max_position_usd_per_coin", 0.0)) or 0.0)
                if mpc < 0.0:
                    mpc = 0.0
                self.settings["max_position_usd_per_coin"] = mpc

                try:
                    mte = float((max_total_exposure_var.get() or "").strip().replace("%", "") or 0.0)
                except Exception:
                    mte = float(self.settings.get("max_total_exposure_pct", DEFAULT_SETTINGS.get("max_total_exposure_pct", 0.0)) or 0.0)
                if mte < 0.0:
                    mte = 0.0
                self.settings["max_total_exposure_pct"] = mte



                self.settings["hub_data_dir"] = hub_dir_var.get().strip()
                # Keep secrets out of persisted gui_settings.json; use env vars at runtime.
                alpaca_key_in, alpaca_secret_in = get_alpaca_creds(self.settings, base_dir=self.project_dir)
                if alpaca_key_in:
                    os.environ["POWERTRADER_ALPACA_API_KEY_ID"] = alpaca_key_in
                if alpaca_secret_in:
                    os.environ["POWERTRADER_ALPACA_SECRET_KEY"] = alpaca_secret_in
                if alpaca_key_in and alpaca_secret_in:
                    # Persist credentials so restarts do not lose broker access.
                    ak_path, as_path = alpaca_credential_paths(self.project_dir)
                    _write_secret_file(ak_path, alpaca_key_in)
                    _write_secret_file(as_path, alpaca_secret_in)
                self.settings["alpaca_api_key_id"] = ""
                self.settings["alpaca_secret_key"] = ""
                self.settings["alpaca_base_url"] = alpaca_base_url_var.get().strip() or str(DEFAULT_SETTINGS.get("alpaca_base_url", ""))
                self.settings["alpaca_data_url"] = alpaca_data_url_var.get().strip() or str(DEFAULT_SETTINGS.get("alpaca_data_url", ""))
                self.settings["alpaca_paper_mode"] = bool(alpaca_paper_var.get())
                stage = str(rollout_stage_var.get() or "").strip().lower()
                if stage not in {"legacy", "scan_expanded", "risk_caps", "execution_v2", "shadow_only", "live_guarded"}:
                    stage = str(DEFAULT_SETTINGS.get("market_rollout_stage", "legacy"))
                self.settings["market_rollout_stage"] = stage
                mode = str(stock_universe_mode_var.get() or "").strip().lower()
                if mode not in {"core", "watchlist", "all_tradable_filtered"}:
                    mode = str(DEFAULT_SETTINGS.get("stock_universe_mode", "core"))
                self.settings["stock_universe_mode"] = mode
                self.settings["stock_universe_symbols"] = str(stock_universe_symbols_var.get() or "").strip()
                try:
                    self.settings["stock_scan_max_symbols"] = max(8, int(float((stock_scan_max_symbols_var.get() or "").strip() or 60)))
                except Exception:
                    self.settings["stock_scan_max_symbols"] = int(DEFAULT_SETTINGS.get("stock_scan_max_symbols", 60))
                try:
                    self.settings["stock_min_price"] = max(0.0, float((stock_min_price_var.get() or "").strip() or 5.0))
                except Exception:
                    self.settings["stock_min_price"] = float(DEFAULT_SETTINGS.get("stock_min_price", 5.0))
                try:
                    self.settings["stock_max_price"] = max(self.settings["stock_min_price"], float((stock_max_price_var.get() or "").strip() or 500.0))
                except Exception:
                    self.settings["stock_max_price"] = float(DEFAULT_SETTINGS.get("stock_max_price", 500.0))
                try:
                    self.settings["stock_min_dollar_volume"] = max(0.0, float((stock_min_dollar_volume_var.get() or "").strip() or 5000000.0))
                except Exception:
                    self.settings["stock_min_dollar_volume"] = float(DEFAULT_SETTINGS.get("stock_min_dollar_volume", 5000000.0))
                try:
                    self.settings["stock_max_spread_bps"] = max(0.0, float((stock_max_spread_bps_var.get() or "").strip() or 40.0))
                except Exception:
                    self.settings["stock_max_spread_bps"] = float(DEFAULT_SETTINGS.get("stock_max_spread_bps", 40.0))
                self.settings["stock_gate_market_hours_scan"] = bool(stock_gate_hours_var.get())
                try:
                    self.settings["stock_min_bars_required"] = max(8, int(float((stock_min_bars_var.get() or "").strip() or 24)))
                except Exception:
                    self.settings["stock_min_bars_required"] = int(DEFAULT_SETTINGS.get("stock_min_bars_required", 24))
                try:
                    self.settings["stock_min_valid_bars_ratio"] = max(0.0, min(1.0, float((stock_min_valid_ratio_var.get() or "").strip() or 0.7)))
                except Exception:
                    self.settings["stock_min_valid_bars_ratio"] = float(DEFAULT_SETTINGS.get("stock_min_valid_bars_ratio", 0.7))
                try:
                    self.settings["stock_max_stale_hours"] = max(0.5, float((stock_max_stale_hours_var.get() or "").strip() or 6.0))
                except Exception:
                    self.settings["stock_max_stale_hours"] = float(DEFAULT_SETTINGS.get("stock_max_stale_hours", 6.0))
                self.settings["stock_show_rejected_rows"] = bool(stock_show_rejected_var.get())
                self.settings["stock_scan_publish_watch_leaders"] = bool(stock_publish_watch_leaders_var.get())
                try:
                    self.settings["stock_scan_watch_leaders_count"] = max(1, min(20, int(float((stock_watch_leaders_count_var.get() or "").strip() or 6))))
                except Exception:
                    self.settings["stock_scan_watch_leaders_count"] = int(DEFAULT_SETTINGS.get("stock_scan_watch_leaders_count", 6))
                try:
                    self.settings["stock_leader_stability_margin_pct"] = max(0.0, min(100.0, float((stock_leader_stability_margin_var.get() or "").strip().replace("%", "") or 10.0)))
                except Exception:
                    self.settings["stock_leader_stability_margin_pct"] = float(DEFAULT_SETTINGS.get("stock_leader_stability_margin_pct", 10.0))
                self.settings["stock_auto_trade_enabled"] = bool(stock_auto_trade_var.get())
                self.settings["stock_block_entries_on_cached_scan"] = bool(stock_block_cached_scan_var.get())
                self.settings["stock_require_data_quality_ok_for_entries"] = bool(stock_require_data_quality_gate_var.get())
                try:
                    self.settings["stock_cached_scan_hard_block_age_s"] = max(30, int(float((stock_cached_scan_hard_block_age_var.get() or "").strip() or 1800)))
                except Exception:
                    self.settings["stock_cached_scan_hard_block_age_s"] = int(DEFAULT_SETTINGS.get("stock_cached_scan_hard_block_age_s", 1800))
                try:
                    self.settings["stock_cached_scan_entry_size_mult"] = max(0.10, min(1.0, float((stock_cached_scan_size_mult_var.get() or "").strip() or 0.60)))
                except Exception:
                    self.settings["stock_cached_scan_entry_size_mult"] = float(DEFAULT_SETTINGS.get("stock_cached_scan_entry_size_mult", 0.60))
                try:
                    self.settings["stock_require_reject_rate_max_pct"] = max(0.0, min(100.0, float((stock_reject_rate_gate_var.get() or "").strip().replace("%", "") or 92.0)))
                except Exception:
                    self.settings["stock_require_reject_rate_max_pct"] = float(DEFAULT_SETTINGS.get("stock_require_reject_rate_max_pct", 92.0))
                try:
                    self.settings["stock_trade_notional_usd"] = max(1.0, float((stock_notional_var.get() or "").strip().replace("$", "") or 100.0))
                except Exception:
                    self.settings["stock_trade_notional_usd"] = float(DEFAULT_SETTINGS.get("stock_trade_notional_usd", 100.0))
                try:
                    self.settings["stock_max_open_positions"] = max(1, int(float((stock_max_pos_var.get() or "").strip() or 1)))
                except Exception:
                    self.settings["stock_max_open_positions"] = int(DEFAULT_SETTINGS.get("stock_max_open_positions", 1))
                try:
                    self.settings["stock_score_threshold"] = max(0.0, float((stock_score_threshold_var.get() or "").strip() or 0.2))
                except Exception:
                    self.settings["stock_score_threshold"] = float(DEFAULT_SETTINGS.get("stock_score_threshold", 0.2))
                try:
                    self.settings["stock_profit_target_pct"] = max(0.0, float((stock_profit_target_var.get() or "").strip().replace("%", "") or 0.35))
                except Exception:
                    self.settings["stock_profit_target_pct"] = float(DEFAULT_SETTINGS.get("stock_profit_target_pct", 0.35))
                try:
                    self.settings["stock_trailing_gap_pct"] = max(0.0, float((stock_trailing_gap_var.get() or "").strip().replace("%", "") or 0.2))
                except Exception:
                    self.settings["stock_trailing_gap_pct"] = float(DEFAULT_SETTINGS.get("stock_trailing_gap_pct", 0.2))
                try:
                    self.settings["stock_max_day_trades"] = max(0, int(float((stock_day_trades_var.get() or "").strip() or 3)))
                except Exception:
                    self.settings["stock_max_day_trades"] = int(DEFAULT_SETTINGS.get("stock_max_day_trades", 3))
                try:
                    self.settings["stock_max_position_usd_per_symbol"] = max(0.0, float((stock_max_position_usd_var.get() or "").strip().replace("$", "") or 0.0))
                except Exception:
                    self.settings["stock_max_position_usd_per_symbol"] = float(DEFAULT_SETTINGS.get("stock_max_position_usd_per_symbol", 0.0))
                try:
                    self.settings["stock_max_total_exposure_pct"] = max(0.0, float((stock_max_exposure_var.get() or "").strip().replace("%", "") or 0.0))
                except Exception:
                    self.settings["stock_max_total_exposure_pct"] = float(DEFAULT_SETTINGS.get("stock_max_total_exposure_pct", 0.0))
                try:
                    self.settings["stock_live_guarded_score_mult"] = max(1.0, float((stock_guarded_mult_var.get() or "").strip() or 1.2))
                except Exception:
                    self.settings["stock_live_guarded_score_mult"] = float(DEFAULT_SETTINGS.get("stock_live_guarded_score_mult", 1.2))
                try:
                    self.settings["stock_min_calib_prob_live_guarded"] = max(0.0, min(1.0, float((stock_min_calib_prob_var.get() or "").strip() or 0.58)))
                except Exception:
                    self.settings["stock_min_calib_prob_live_guarded"] = float(DEFAULT_SETTINGS.get("stock_min_calib_prob_live_guarded", 0.58))
                try:
                    self.settings["stock_max_slippage_bps"] = max(0.0, float((stock_max_slippage_bps_var.get() or "").strip() or 35.0))
                except Exception:
                    self.settings["stock_max_slippage_bps"] = float(DEFAULT_SETTINGS.get("stock_max_slippage_bps", 35.0))
                try:
                    self.settings["stock_order_retry_count"] = max(1, int(float((stock_order_retry_count_var.get() or "").strip() or 2)))
                except Exception:
                    self.settings["stock_order_retry_count"] = int(DEFAULT_SETTINGS.get("stock_order_retry_count", 2))
                try:
                    self.settings["stock_max_loss_streak"] = max(0, int(float((stock_max_loss_streak_var.get() or "").strip() or 3)))
                except Exception:
                    self.settings["stock_max_loss_streak"] = int(DEFAULT_SETTINGS.get("stock_max_loss_streak", 3))
                try:
                    self.settings["stock_loss_streak_size_step_pct"] = max(0.0, min(0.9, float((stock_loss_size_step_var.get() or "").strip() or 0.15)))
                except Exception:
                    self.settings["stock_loss_streak_size_step_pct"] = float(DEFAULT_SETTINGS.get("stock_loss_streak_size_step_pct", 0.15))
                try:
                    self.settings["stock_loss_streak_size_floor_pct"] = max(0.10, min(1.0, float((stock_loss_size_floor_var.get() or "").strip() or 0.40)))
                except Exception:
                    self.settings["stock_loss_streak_size_floor_pct"] = float(DEFAULT_SETTINGS.get("stock_loss_streak_size_floor_pct", 0.40))
                try:
                    self.settings["stock_loss_cooldown_seconds"] = max(60, int(float((stock_loss_cooldown_var.get() or "").strip() or 1800)))
                except Exception:
                    self.settings["stock_loss_cooldown_seconds"] = int(DEFAULT_SETTINGS.get("stock_loss_cooldown_seconds", 1800))
                try:
                    self.settings["stock_max_daily_loss_usd"] = max(0.0, float((stock_max_daily_loss_usd_var.get() or "").strip().replace("$", "") or 0.0))
                except Exception:
                    self.settings["stock_max_daily_loss_usd"] = float(DEFAULT_SETTINGS.get("stock_max_daily_loss_usd", 0.0))
                try:
                    self.settings["stock_max_daily_loss_pct"] = max(0.0, float((stock_max_daily_loss_pct_var.get() or "").strip().replace("%", "") or 0.0))
                except Exception:
                    self.settings["stock_max_daily_loss_pct"] = float(DEFAULT_SETTINGS.get("stock_max_daily_loss_pct", 0.0))
                try:
                    self.settings["stock_min_samples_live_guarded"] = max(0, int(float((stock_min_samples_guarded_var.get() or "").strip() or 5)))
                except Exception:
                    self.settings["stock_min_samples_live_guarded"] = int(DEFAULT_SETTINGS.get("stock_min_samples_live_guarded", 5))
                try:
                    self.settings["stock_max_signal_age_seconds"] = max(30, int(float((stock_max_signal_age_var.get() or "").strip() or 300)))
                except Exception:
                    self.settings["stock_max_signal_age_seconds"] = int(DEFAULT_SETTINGS.get("stock_max_signal_age_seconds", 300))
                try:
                    self.settings["stock_reject_drift_warn_pct"] = max(10.0, min(100.0, float((stock_reject_warn_pct_var.get() or "").strip().replace("%", "") or 65.0)))
                except Exception:
                    self.settings["stock_reject_drift_warn_pct"] = float(DEFAULT_SETTINGS.get("stock_reject_drift_warn_pct", 65.0))
                self.settings["stock_block_new_entries_near_close"] = bool(stock_block_near_close_var.get())
                try:
                    self.settings["stock_no_new_entries_mins_to_close"] = max(0, int(float((stock_no_new_close_mins_var.get() or "").strip() or 15)))
                except Exception:
                    self.settings["stock_no_new_entries_mins_to_close"] = int(DEFAULT_SETTINGS.get("stock_no_new_entries_mins_to_close", 15))
                oanda_account_in, oanda_token_in = get_oanda_creds(self.settings, base_dir=self.project_dir)
                if oanda_account_in:
                    os.environ["POWERTRADER_OANDA_ACCOUNT_ID"] = oanda_account_in
                if oanda_token_in:
                    os.environ["POWERTRADER_OANDA_API_TOKEN"] = oanda_token_in
                if oanda_account_in and oanda_token_in:
                    # Persist credentials so restarts do not lose broker access.
                    oa_path, ot_path = oanda_credential_paths(self.project_dir)
                    _write_secret_file(oa_path, oanda_account_in)
                    _write_secret_file(ot_path, oanda_token_in)
                # account id is less sensitive but is sourced from env first for consistency.
                self.settings["oanda_account_id"] = ""
                self.settings["oanda_api_token"] = ""
                self.settings["oanda_rest_url"] = oanda_rest_url_var.get().strip() or str(DEFAULT_SETTINGS.get("oanda_rest_url", ""))
                self.settings["oanda_stream_url"] = oanda_stream_url_var.get().strip() or str(DEFAULT_SETTINGS.get("oanda_stream_url", ""))
                self.settings["oanda_practice_mode"] = bool(oanda_practice_var.get())
                self.settings["paper_only_unless_checklist_green"] = bool(paper_only_guard_var.get())
                try:
                    self.settings["key_rotation_warn_days"] = max(7, int(float((key_rotation_warn_days_var.get() or "").strip() or 90)))
                except Exception:
                    self.settings["key_rotation_warn_days"] = int(DEFAULT_SETTINGS.get("key_rotation_warn_days", 90))
                self.settings["forex_universe_pairs"] = str(forex_pairs_var.get() or "").strip()
                try:
                    self.settings["forex_scan_max_pairs"] = max(4, int(float((forex_scan_max_pairs_var.get() or "").strip() or 16)))
                except Exception:
                    self.settings["forex_scan_max_pairs"] = int(DEFAULT_SETTINGS.get("forex_scan_max_pairs", 16))
                try:
                    self.settings["forex_max_spread_bps"] = max(0.0, float((fx_max_spread_bps_var.get() or "").strip() or 8.0))
                except Exception:
                    self.settings["forex_max_spread_bps"] = float(DEFAULT_SETTINGS.get("forex_max_spread_bps", 8.0))
                try:
                    self.settings["forex_min_volatility_pct"] = max(0.0, float((fx_min_vol_pct_var.get() or "").strip() or 0.01))
                except Exception:
                    self.settings["forex_min_volatility_pct"] = float(DEFAULT_SETTINGS.get("forex_min_volatility_pct", 0.01))
                try:
                    self.settings["forex_min_bars_required"] = max(8, int(float((fx_min_bars_var.get() or "").strip() or 24)))
                except Exception:
                    self.settings["forex_min_bars_required"] = int(DEFAULT_SETTINGS.get("forex_min_bars_required", 24))
                try:
                    self.settings["forex_min_valid_bars_ratio"] = max(0.0, min(1.0, float((fx_min_valid_ratio_var.get() or "").strip() or 0.7)))
                except Exception:
                    self.settings["forex_min_valid_bars_ratio"] = float(DEFAULT_SETTINGS.get("forex_min_valid_bars_ratio", 0.7))
                try:
                    self.settings["forex_max_stale_hours"] = max(0.5, float((fx_max_stale_hours_var.get() or "").strip() or 8.0))
                except Exception:
                    self.settings["forex_max_stale_hours"] = float(DEFAULT_SETTINGS.get("forex_max_stale_hours", 8.0))
                try:
                    self.settings["forex_leader_stability_margin_pct"] = max(0.0, min(100.0, float((fx_leader_stability_margin_var.get() or "").strip().replace("%", "") or 12.0)))
                except Exception:
                    self.settings["forex_leader_stability_margin_pct"] = float(DEFAULT_SETTINGS.get("forex_leader_stability_margin_pct", 12.0))
                self.settings["forex_show_rejected_rows"] = bool(fx_show_rejected_var.get())
                self.settings["forex_auto_trade_enabled"] = bool(fx_auto_trade_var.get())
                self.settings["forex_block_entries_on_cached_scan"] = bool(fx_block_cached_scan_var.get())
                self.settings["forex_require_data_quality_ok_for_entries"] = bool(fx_require_data_quality_gate_var.get())
                try:
                    self.settings["forex_cached_scan_hard_block_age_s"] = max(30, int(float((fx_cached_scan_hard_block_age_var.get() or "").strip() or 1200)))
                except Exception:
                    self.settings["forex_cached_scan_hard_block_age_s"] = int(DEFAULT_SETTINGS.get("forex_cached_scan_hard_block_age_s", 1200))
                try:
                    self.settings["forex_cached_scan_entry_size_mult"] = max(0.10, min(1.0, float((fx_cached_scan_size_mult_var.get() or "").strip() or 0.65)))
                except Exception:
                    self.settings["forex_cached_scan_entry_size_mult"] = float(DEFAULT_SETTINGS.get("forex_cached_scan_entry_size_mult", 0.65))
                try:
                    self.settings["forex_require_reject_rate_max_pct"] = max(0.0, min(100.0, float((fx_reject_rate_gate_var.get() or "").strip().replace("%", "") or 92.0)))
                except Exception:
                    self.settings["forex_require_reject_rate_max_pct"] = float(DEFAULT_SETTINGS.get("forex_require_reject_rate_max_pct", 92.0))
                self.settings["forex_trade_units"] = max(1, int(float((fx_trade_units_var.get() or "").strip() or 1000)))
                self.settings["forex_max_open_positions"] = max(1, int(float((fx_max_pos_var.get() or "").strip() or 1)))
                try:
                    self.settings["forex_max_position_usd_per_pair"] = max(0.0, float((fx_max_pos_usd_pair_var.get() or "").strip().replace("$", "") or 0.0))
                except Exception:
                    self.settings["forex_max_position_usd_per_pair"] = float(DEFAULT_SETTINGS.get("forex_max_position_usd_per_pair", 0.0))
                self.settings["forex_score_threshold"] = max(0.0, float((fx_score_threshold_var.get() or "").strip() or 0.2))
                self.settings["forex_profit_target_pct"] = max(0.0, float((fx_profit_target_var.get() or "").strip().replace("%", "") or 0.25))
                self.settings["forex_trailing_gap_pct"] = max(0.0, float((fx_trailing_gap_var.get() or "").strip().replace("%", "") or 0.15))
                try:
                    self.settings["forex_max_total_exposure_pct"] = max(0.0, float((fx_max_exposure_var.get() or "").strip().replace("%", "") or 0.0))
                except Exception:
                    self.settings["forex_max_total_exposure_pct"] = float(DEFAULT_SETTINGS.get("forex_max_total_exposure_pct", 0.0))
                fx_session_mode = str(fx_session_mode_var.get() or "").strip().lower()
                if fx_session_mode not in {"all", "london_ny", "london", "ny", "asia"}:
                    fx_session_mode = str(DEFAULT_SETTINGS.get("forex_session_mode", "all"))
                self.settings["forex_session_mode"] = fx_session_mode
                try:
                    self.settings["forex_live_guarded_score_mult"] = max(1.0, float((fx_guarded_mult_var.get() or "").strip() or 1.15))
                except Exception:
                    self.settings["forex_live_guarded_score_mult"] = float(DEFAULT_SETTINGS.get("forex_live_guarded_score_mult", 1.15))
                try:
                    self.settings["forex_min_calib_prob_live_guarded"] = max(0.0, min(1.0, float((fx_min_calib_prob_var.get() or "").strip() or 0.56)))
                except Exception:
                    self.settings["forex_min_calib_prob_live_guarded"] = float(DEFAULT_SETTINGS.get("forex_min_calib_prob_live_guarded", 0.56))
                try:
                    self.settings["forex_max_slippage_bps"] = max(0.0, float((fx_max_slippage_bps_var.get() or "").strip() or 6.0))
                except Exception:
                    self.settings["forex_max_slippage_bps"] = float(DEFAULT_SETTINGS.get("forex_max_slippage_bps", 6.0))
                try:
                    self.settings["forex_order_retry_count"] = max(1, int(float((fx_order_retry_count_var.get() or "").strip() or 2)))
                except Exception:
                    self.settings["forex_order_retry_count"] = int(DEFAULT_SETTINGS.get("forex_order_retry_count", 2))
                try:
                    self.settings["forex_max_loss_streak"] = max(0, int(float((fx_max_loss_streak_var.get() or "").strip() or 3)))
                except Exception:
                    self.settings["forex_max_loss_streak"] = int(DEFAULT_SETTINGS.get("forex_max_loss_streak", 3))
                try:
                    self.settings["forex_loss_streak_size_step_pct"] = max(0.0, min(0.9, float((fx_loss_size_step_var.get() or "").strip() or 0.15)))
                except Exception:
                    self.settings["forex_loss_streak_size_step_pct"] = float(DEFAULT_SETTINGS.get("forex_loss_streak_size_step_pct", 0.15))
                try:
                    self.settings["forex_loss_streak_size_floor_pct"] = max(0.10, min(1.0, float((fx_loss_size_floor_var.get() or "").strip() or 0.40)))
                except Exception:
                    self.settings["forex_loss_streak_size_floor_pct"] = float(DEFAULT_SETTINGS.get("forex_loss_streak_size_floor_pct", 0.40))
                try:
                    self.settings["forex_loss_cooldown_seconds"] = max(60, int(float((fx_loss_cooldown_var.get() or "").strip() or 1800)))
                except Exception:
                    self.settings["forex_loss_cooldown_seconds"] = int(DEFAULT_SETTINGS.get("forex_loss_cooldown_seconds", 1800))
                try:
                    self.settings["forex_max_daily_loss_usd"] = max(0.0, float((fx_max_daily_loss_usd_var.get() or "").strip().replace("$", "") or 0.0))
                except Exception:
                    self.settings["forex_max_daily_loss_usd"] = float(DEFAULT_SETTINGS.get("forex_max_daily_loss_usd", 0.0))
                try:
                    self.settings["forex_max_daily_loss_pct"] = max(0.0, float((fx_max_daily_loss_pct_var.get() or "").strip().replace("%", "") or 0.0))
                except Exception:
                    self.settings["forex_max_daily_loss_pct"] = float(DEFAULT_SETTINGS.get("forex_max_daily_loss_pct", 0.0))
                try:
                    self.settings["forex_min_samples_live_guarded"] = max(0, int(float((fx_min_samples_guarded_var.get() or "").strip() or 5)))
                except Exception:
                    self.settings["forex_min_samples_live_guarded"] = int(DEFAULT_SETTINGS.get("forex_min_samples_live_guarded", 5))
                try:
                    self.settings["forex_max_signal_age_seconds"] = max(30, int(float((fx_max_signal_age_var.get() or "").strip() or 300)))
                except Exception:
                    self.settings["forex_max_signal_age_seconds"] = int(DEFAULT_SETTINGS.get("forex_max_signal_age_seconds", 300))
                try:
                    self.settings["forex_reject_drift_warn_pct"] = max(10.0, min(100.0, float((fx_reject_warn_pct_var.get() or "").strip().replace("%", "") or 65.0)))
                except Exception:
                    self.settings["forex_reject_drift_warn_pct"] = float(DEFAULT_SETTINGS.get("forex_reject_drift_warn_pct", 65.0))
                try:
                    self.settings["market_max_total_exposure_pct"] = max(0.0, float((market_global_exposure_var.get() or "").strip().replace("%", "") or 0.0))
                except Exception:
                    self.settings["market_max_total_exposure_pct"] = float(DEFAULT_SETTINGS.get("market_max_total_exposure_pct", 0.0))
                try:
                    self.settings["market_chart_cache_symbols"] = max(2, min(32, int(float((chart_cache_symbols_var.get() or "").strip() or 8))))
                except Exception:
                    self.settings["market_chart_cache_symbols"] = int(DEFAULT_SETTINGS.get("market_chart_cache_symbols", 8))
                try:
                    self.settings["market_chart_cache_bars"] = max(40, min(400, int(float((chart_cache_bars_var.get() or "").strip() or 120))))
                except Exception:
                    self.settings["market_chart_cache_bars"] = int(DEFAULT_SETTINGS.get("market_chart_cache_bars", 120))
                try:
                    self.settings["market_fallback_scan_max_age_s"] = max(60.0, min(172800.0, float((market_fallback_scan_age_var.get() or "").strip() or 7200.0)))
                except Exception:
                    self.settings["market_fallback_scan_max_age_s"] = float(DEFAULT_SETTINGS.get("market_fallback_scan_max_age_s", 7200.0))
                try:
                    self.settings["market_fallback_snapshot_max_age_s"] = max(30.0, min(86400.0, float((market_fallback_snapshot_age_var.get() or "").strip() or 1800.0)))
                except Exception:
                    self.settings["market_fallback_snapshot_max_age_s"] = float(DEFAULT_SETTINGS.get("market_fallback_snapshot_max_age_s", 1800.0))
                if float(self.settings["market_fallback_scan_max_age_s"]) < float(self.settings["market_fallback_snapshot_max_age_s"]):
                    self.settings["market_fallback_scan_max_age_s"] = float(self.settings["market_fallback_snapshot_max_age_s"])
                try:
                    self.settings["kucoin_unsupported_cooldown_s"] = max(300.0, min(172800.0, float((kucoin_unsupported_cooldown_var.get() or "").strip() or 21600.0)))
                except Exception:
                    self.settings["kucoin_unsupported_cooldown_s"] = float(DEFAULT_SETTINGS.get("kucoin_unsupported_cooldown_s", 21600.0))
                try:
                    self.settings["crypto_price_error_log_cooldown_s"] = max(5.0, min(3600.0, float((crypto_price_error_log_cd_var.get() or "").strip() or 120.0)))
                except Exception:
                    self.settings["crypto_price_error_log_cooldown_s"] = float(DEFAULT_SETTINGS.get("crypto_price_error_log_cooldown_s", 120.0))

                self.settings["script_neural_runner2"] = neural_script_var.get().strip()
                self.settings["script_neural_trainer"] = trainer_script_var.get().strip()
                self.settings["script_trader"] = trader_script_var.get().strip()

                self.settings["ui_refresh_seconds"] = float(ui_refresh_var.get().strip())
                self.settings["chart_refresh_seconds"] = float(chart_refresh_var.get().strip())
                self.settings["candles_limit"] = int(float(candles_limit_var.get().strip()))
                fs = str(font_scale_var.get() or "").strip().lower()
                if fs not in {"small", "normal", "large"}:
                    fs = str(DEFAULT_SETTINGS.get("ui_font_scale_preset", "normal"))
                self.settings["ui_font_scale_preset"] = fs
                lp = str(layout_preset_var.get() or "").strip().lower()
                if lp not in {"auto", "compact", "normal", "wide"}:
                    lp = str(DEFAULT_SETTINGS.get("ui_layout_preset", "auto"))
                self.settings["ui_layout_preset"] = lp
                self.settings["auto_start_scripts"] = bool(auto_start_var.get())
                self._save_settings()
                self._apply_font_scale_preset(fs, persist=False)
                self._apply_layout_preset(lp, persist=False)

                # If new coin(s) were added and their training folder doesn't exist yet,
                # create the folder and copy neural_trainer.py into it RIGHT AFTER saving settings.
                try:
                    new_coins = [c.strip().upper() for c in (self.settings.get("coins") or []) if c.strip()]
                    added = [c for c in new_coins if c and c not in prev_coins]

                    main_dir = self.settings.get("main_neural_dir") or self.project_dir
                    trainer_name = os.path.basename(str(self.settings.get("script_neural_trainer", "engines/pt_trainer.py")))

                    # Best-effort resolve source trainer path.
                    src_project_trainer = os.path.join(self.project_dir, trainer_name)
                    src_btc_trainer = os.path.join(main_dir, "BTC", trainer_name)
                    src_cfg_trainer = str(self.settings.get("script_neural_trainer", trainer_name))
                    if os.path.isfile(src_project_trainer):
                        src_trainer_path = src_project_trainer
                    elif os.path.isfile(src_btc_trainer):
                        src_trainer_path = src_btc_trainer
                    else:
                        src_trainer_path = src_cfg_trainer

                    for coin in added:
                        coin_dir = os.path.join(main_dir, coin)
                        if not os.path.isdir(coin_dir):
                            os.makedirs(coin_dir, exist_ok=True)

                        dst_trainer_path = os.path.join(coin_dir, trainer_name)
                        if (not os.path.isfile(dst_trainer_path)) and os.path.isfile(src_trainer_path):
                            shutil.copy2(src_trainer_path, dst_trainer_path)
                except Exception:
                    pass

                # Refresh all coin-driven UI (dropdowns + chart tabs)
                self._refresh_coin_dependent_ui(prev_coins)

                messagebox.showinfo("Saved", "Settings saved.")
                _close_settings()


            except Exception as e:
                messagebox.showerror("Error", f"Failed to save settings:\n{e}")


        ttk.Button(btns, text="Save", command=save).pack(side="left")
        ttk.Button(btns, text="Cancel", command=_close_settings).pack(side="left", padx=8)


    # ---- close ----

    def _on_close(self) -> None:
        try:
            self._persist_while_you_were_gone_snapshot()
        except Exception:
            pass
        try:
            self._close_autofix_queue()
        except Exception:
            pass
        self.destroy()


if __name__ == "__main__":
    app = PowerTraderHub()
    app.mainloop()
