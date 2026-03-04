from __future__ import annotations
import os
import sys
import json
import time
import math
import queue
import threading
import subprocess
import shutil
import glob
import bisect
import signal
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
from path_utils import resolve_runtime_paths, resolve_settings_path, read_settings_file, log_once
from broker_alpaca import AlpacaBrokerClient
from broker_oanda import OandaBrokerClient
from stock_thinker import run_scan as run_stock_scan
from forex_thinker import run_scan as run_forex_scan
from stock_trader import run_step as run_stock_trader_step
from forex_trader import run_step as run_forex_trader_step

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
    "start_allocation_pct": 0.005,  # % of total account value for initial entry (min $0.50 per coin)
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
    "hub_data_dir": "",  # if blank, defaults to <this_dir>/hub_data
    "script_neural_runner2": "pt_thinker.py",
    "script_neural_trainer": "pt_trainer.py",
    "script_trader": "pt_trader.py",
    "alpaca_api_key_id": "",
    "alpaca_secret_key": "",
    "alpaca_base_url": "https://paper-api.alpaca.markets",
    "alpaca_data_url": "https://data.alpaca.markets",
    "alpaca_paper_mode": True,
    "market_rollout_stage": "legacy",  # legacy | scan_expanded | risk_caps | execution_v2
    "stock_universe_mode": "core",  # core | watchlist | all_tradable_filtered
    "stock_universe_symbols": "AAPL,MSFT,NVDA,AMZN,META,TSLA,SPY,QQQ",
    "stock_scan_max_symbols": 60,
    "stock_min_price": 5.0,
    "stock_max_price": 500.0,
    "stock_min_dollar_volume": 5000000.0,
    "stock_auto_trade_enabled": False,
    "stock_trade_notional_usd": 100.0,
    "stock_max_open_positions": 1,
    "stock_score_threshold": 0.2,
    "stock_profit_target_pct": 0.35,
    "stock_trailing_gap_pct": 0.2,
    "stock_max_day_trades": 3,
    "stock_max_position_usd_per_symbol": 0.0,
    "stock_max_total_exposure_pct": 0.0,
    "oanda_account_id": "",
    "oanda_api_token": "",
    "oanda_rest_url": "https://api-fxpractice.oanda.com",
    "oanda_stream_url": "https://stream-fxpractice.oanda.com",
    "oanda_practice_mode": True,
    "forex_auto_trade_enabled": False,
    "forex_universe_pairs": "EUR_USD,GBP_USD,USD_JPY,AUD_USD,USD_CAD,EUR_JPY",
    "forex_scan_max_pairs": 16,
    "forex_trade_units": 1000,
    "forex_max_open_positions": 1,
    "forex_score_threshold": 0.2,
    "forex_profit_target_pct": 0.25,
    "forex_trailing_gap_pct": 0.15,
    "forex_max_total_exposure_pct": 0.0,
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
    Mirrors your convention:
      BTC uses main_dir directly
      other coins typically have subfolders inside main_dir (auto-detected)

    Returns { "BTC": "...", "ETH": "...", ... }
    """
    out: Dict[str, str] = {}
    main_dir = main_dir or BASE_DIR

    # BTC folder
    out["BTC"] = main_dir

    # Auto-detect subfolders
    if os.path.isdir(main_dir):
        for name in os.listdir(main_dir):
            p = os.path.join(main_dir, name)
            if not os.path.isdir(p):
                continue
            sym = name.upper().strip()
            if sym in coins and sym != "BTC":
                out[sym] = p

    # Fallbacks for missing ones
    for c in coins:
        c = c.upper().strip()
        if c not in out:
            out[c] = os.path.join(main_dir, c)  # best-effort fallback

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
        self._cache_ttl_seconds: float = 10.0
        self._lock = threading.Lock()
        self._pending: set[Tuple[str, str, int]] = set()
        self._result_q: "queue.Queue[Tuple[Tuple[str, str, int], float, List[dict]]]" = queue.Queue()


    def _fetch_klines_sync(self, pair: str, timeframe: str, limit: int, now: float) -> List[dict]:
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
                return candles
            except Exception:
                return []

        # REST fallback
        try:
            url = "https://api.kucoin.com/api/v1/market/candles"
            params = {"symbol": pair, "type": timeframe, "startAt": start_at, "endAt": end_at}
            resp = self._requests.get(url, params=params, timeout=10)
            j = resp.json()
            data = j.get("data", [])  # newest->oldest
            candles: List[dict] = []
            for row in data:
                ts = int(float(row[0]))
                o = float(row[1]); c = float(row[2]); h = float(row[3]); l = float(row[4])
                candles.append({"ts": ts, "open": o, "high": h, "low": l, "close": c})
            candles.sort(key=lambda x: x["ts"])
            if limit and len(candles) > limit:
                candles = candles[-limit:]
            return candles
        except Exception:
            return []


    def _start_fetch(self, cache_key: Tuple[str, str, int]) -> None:
        with self._lock:
            if cache_key in self._pending:
                return
            self._pending.add(cache_key)

        def _worker() -> None:
            pair, timeframe, limit = cache_key
            now = time.time()
            candles = self._fetch_klines_sync(pair, timeframe, limit, now)
            try:
                self._result_q.put((cache_key, now, candles))
            except Exception:
                pass

        threading.Thread(target=_worker, daemon=True).start()


    def drain_results(self) -> bool:
        changed = False
        while True:
            try:
                cache_key, now, candles = self._result_q.get_nowait()
            except queue.Empty:
                break
            with self._lock:
                self._pending.discard(cache_key)
                if candles:
                    self._cache[cache_key] = (now, candles)
                    changed = True
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
            self.ax.set_title(f"{self.coin} ({tf}) - no candles", color=DARK_FG)
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
        self.market_panels: Dict[str, Dict[str, Any]] = {}
        self._market_test_busy: Dict[str, bool] = {}
        self._market_refresh_busy: Dict[str, bool] = {}
        self._market_thinker_busy: Dict[str, bool] = {}
        self._market_trader_busy: Dict[str, bool] = {}
        self._last_market_refresh_ts: Dict[str, float] = {}
        self._last_market_thinker_ts: Dict[str, float] = {}
        self._last_market_trader_ts: Dict[str, float] = {}

        # file written by pt_thinker.py (runner readiness gate used for Start All)
        self.runner_ready_path = os.path.join(self.hub_dir, "runner_ready.json")


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
            path=os.path.abspath(os.path.join(self.project_dir, "pt_runner.py"))
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

        # Refresh charts immediately when a timeframe is changed (don't wait for the 10s throttle).
        self.bind_all("<<TimeframeChanged>>", self._on_timeframe_changed)

        self._last_chart_refresh = 0.0

        if bool(self.settings.get("auto_start_scripts", False)):
            self.start_all_scripts()

        self.after(250, self._tick)

        self.protocol("WM_DELETE_WINDOW", self._on_close)


    # ---- forced dark mode ----

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

        merged = dict(DEFAULT_SETTINGS)
        merged.update(data)
        # normalize
        merged["coins"] = [c.upper().strip() for c in merged.get("coins", [])]
        return merged

    def _save_settings(self) -> None:
        settings_path = resolve_settings_path(BASE_DIR) or SETTINGS_PATH or os.path.join(BASE_DIR, SETTINGS_FILE)
        _safe_write_json(settings_path, self.settings)


    def _settings_getter(self) -> dict:
        return self.settings

    def _ensure_alt_coin_folders_and_trainer_on_startup(self) -> None:
        """
        Startup behavior (mirrors Settings-save behavior):
        - For every alt coin in the coin list that does NOT have its folder yet:
            - create the folder
            - copy neural_trainer.py from the MAIN (BTC) folder into the new folder
        """
        try:
            coins = [str(c).strip().upper() for c in (self.settings.get("coins") or []) if str(c).strip()]
            main_dir = (self.settings.get("main_neural_dir") or self.project_dir or BASE_DIR).strip()

            trainer_name = os.path.basename(str(self.settings.get("script_neural_trainer", "neural_trainer.py")))

            # Source trainer: MAIN folder (BTC folder)
            src_main_trainer = os.path.join(main_dir, trainer_name)

            # Best-effort fallback if the main folder doesn't have it (keeps behavior robust)
            src_cfg_trainer = str(self.settings.get("script_neural_trainer", trainer_name))
            src_trainer_path = src_main_trainer if os.path.isfile(src_main_trainer) else src_cfg_trainer

            for coin in coins:
                if coin == "BTC":
                    continue  # BTC uses main folder; no per-coin folder needed

                coin_dir = os.path.join(main_dir, coin)

                created = False
                if not os.path.isdir(coin_dir):
                    os.makedirs(coin_dir, exist_ok=True)
                    created = True

                # Only copy into folders created at startup (per your request)
                if created:
                    dst_trainer_path = os.path.join(coin_dir, trainer_name)
                    if (not os.path.isfile(dst_trainer_path)) and os.path.isfile(src_trainer_path):
                        shutil.copy2(src_trainer_path, dst_trainer_path)
        except Exception:
            pass

    # ---- menu / layout ----


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
        menubar.add_cascade(label="Settings", menu=m_settings)

        m_file = tk.Menu(
            menubar,
            tearoff=0,
            bg=DARK_BG2,
            fg=DARK_FG,
            activebackground=DARK_SELECT_BG,
            activeforeground=DARK_SELECT_FG,
        )
        m_file.add_command(label="Exit", command=self._on_close)
        menubar.add_cascade(label="File", menu=m_file)

        self.config(menu=menubar)


    def _build_layout(self) -> None:
        self.market_nb = ttk.Notebook(self)
        self.market_nb.pack(fill="both", expand=True)

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

        self.lbl_last_status = ttk.Label(system_box, text="Last status: N/A")
        self.lbl_last_status.pack(anchor="w", padx=6, pady=(0, 2))
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
        ttk.Label(system_box, textvariable=state_var).pack(anchor="w", padx=6, pady=(0, 2))
        ttk.Label(system_box, textvariable=endpoint_var, foreground=DARK_MUTED).pack(anchor="w", padx=6, pady=(0, 4))
        test_btn = ttk.Button(
            system_box,
            text=f"Test {broker_name} Connection",
            command=lambda mk=market_key: self._run_market_connection_test(mk),
        )
        test_btn.pack(anchor="w", padx=6, pady=(0, 6))
        trader_step_btn = None
        trader_step_market_key = ""
        trader_step_name = ""
        trader_step_cmd = None
        if market_key == "stocks":
            trader_step_market_key = "stocks"
            trader_step_name = "Stocks"
            trader_step_cmd = lambda: self._run_stock_trader_step(force=True)
        if market_key == "forex":
            trader_step_market_key = "forex"
            trader_step_name = "Forex"
            trader_step_cmd = lambda: self._run_forex_trader_step(force=True)
        if trader_step_cmd is not None:
            trader_step_btn = ttk.Button(system_box, text=f"Run {trader_step_name} Trader Step", command=trader_step_cmd)
            trader_step_btn.pack(anchor="w", padx=6, pady=(0, 6))

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
        }
        for idx, (label, key) in enumerate((
            ("Buying Power", "buying_power"),
            ("Open Positions", "open_positions"),
            ("Realized PnL", "realized_pnl"),
            ("Mode", "mode"),
        )):
            ttk.Label(metric_grid, text=label).grid(row=idx, column=0, sticky="w", padx=(0, 10), pady=2)
            ttk.Label(metric_grid, textvariable=portfolio_vars[key]).grid(row=idx, column=1, sticky="e", pady=2)

        notes_box = ttk.LabelFrame(dashboard, text="Market Notes")
        notes_box.pack(fill="both", expand=True, padx=6, pady=(0, 6))
        notes_text = tk.Text(
            notes_box,
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

        left_split.add(dashboard, weight=1)

        charts_frame = ttk.LabelFrame(right_split, text=f"{market_name} Charts")
        charts_top = ttk.Frame(charts_frame)
        charts_top.pack(fill="x", padx=6, pady=(6, 0))
        ttk.Label(charts_top, text=f"{market_name} View:").pack(side="left")
        selector = ttk.Combobox(
            charts_top,
            values=["Overview", "Scanner", "Leaders", "Positions"],
            state="readonly",
            width=18,
        )
        selector.set("Overview")
        selector.pack(side="left", padx=(6, 12))
        ttk.Label(charts_top, text=subtitle, foreground=DARK_MUTED).pack(side="left")
        run_btn = ttk.Button(
            charts_top,
            text="Run Scan",
            command=lambda mk=market_key: self._run_market_thinker_scan(mk, force=True),
        )
        run_btn.pack(side="right")

        center = ttk.Frame(charts_frame)
        center.pack(fill="both", expand=True, padx=6, pady=6)
        center.columnconfigure(0, weight=1)
        center.rowconfigure(0, weight=1)
        placeholder = tk.Canvas(
            center,
            bg=DARK_PANEL2,
            highlightthickness=1,
            highlightbackground=DARK_BORDER,
            bd=0,
        )
        placeholder.grid(row=0, column=0, sticky="nsew")
        placeholder.create_text(
            24,
            24,
            anchor="nw",
            text=(
                f"{market_name} tab scaffold\n\n"
                "This market is not wired into a live engine yet.\n"
                "The layout is ready for:\n"
                "• status + account summary\n"
                "• symbol/pair charts\n"
                "• current positions\n"
                "• trade history\n"
                "• logs / training"
            ),
            fill=DARK_FG,
            font=(self._live_log_font.cget("family"), max(9, int(self._live_log_font.cget("size")) + 1)),
        )
        selector.bind(
            "<<ComboboxSelected>>",
            lambda _e, mk=market_key: self._refresh_parallel_market_panels(),
        )

        lower = ttk.Panedwindow(right_split, orient="vertical")
        positions_box = ttk.LabelFrame(lower, text=f"{market_name} Positions")
        positions_text = tk.Text(
            positions_box,
            height=6,
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
        positions_scroll = ttk.Scrollbar(positions_box, orient="vertical", command=positions_text.yview)
        positions_text.configure(yscrollcommand=positions_scroll.set)
        positions_text.pack(side="left", fill="both", expand=True, padx=(6, 0), pady=6)
        positions_scroll.pack(side="right", fill="y", padx=(0, 6), pady=6)
        positions_text.insert(
            "1.0",
            "No market connection yet. Positions will appear here once the AI is linked.\n",
        )
        positions_text.configure(state="disabled")

        history_box = ttk.LabelFrame(lower, text=f"{market_name} Logs")
        log_text = tk.Text(
            history_box,
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
        log_scroll = ttk.Scrollbar(history_box, orient="vertical", command=log_text.yview)
        log_text.configure(yscrollcommand=log_scroll.set)
        log_text.pack(side="left", fill="both", expand=True, padx=(6, 0), pady=6)
        log_scroll.pack(side="right", fill="y", padx=(0, 6), pady=6)
        log_text.insert(
            "1.0",
            (
                f"[{market_name.upper()}] UI scaffold initialized\n"
                f"[{market_name.upper()}] Waiting for broker credentials and engine wiring\n"
            ),
        )
        log_text.configure(state="disabled")

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
            "log_text": log_text,
            "positions_text": positions_text,
            "test_btn": test_btn,
            "trader_step_btn": trader_step_btn,
            "trader_step_market_key": trader_step_market_key,
            "run_btn": run_btn,
            "selector": selector,
            "chart_canvas": placeholder,
            "last_log_sig": None,
        }

        right_split.add(charts_frame, weight=3)
        right_split.add(lower, weight=2)
        lower.add(positions_box, weight=1)
        lower.add(history_box, weight=1)

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
        widget = panel.get("log_text")
        if not widget:
            return
        try:
            widget.configure(state="normal")
            widget.insert("end", str(line or "").rstrip() + "\n")
            widget.see("end")
            widget.configure(state="disabled")
        except Exception:
            pass

    def _set_market_positions(self, market_key: str, lines: List[str]) -> None:
        panel = self.market_panels.get(market_key, {})
        widget = panel.get("positions_text")
        if not widget:
            return
        payload = list(lines or [])
        if not payload:
            payload = ["No open positions."]
        try:
            widget.configure(state="normal")
            widget.delete("1.0", "end")
            widget.insert("1.0", "\n".join(str(x) for x in payload) + "\n")
            widget.configure(state="disabled")
        except Exception:
            pass

    def _make_alpaca_client(self) -> AlpacaBrokerClient:
        return AlpacaBrokerClient(
            api_key_id=str(self.settings.get("alpaca_api_key_id", "") or ""),
            secret_key=str(self.settings.get("alpaca_secret_key", "") or ""),
            base_url=str(self.settings.get("alpaca_base_url", DEFAULT_SETTINGS.get("alpaca_base_url", "")) or ""),
            data_url=str(self.settings.get("alpaca_data_url", DEFAULT_SETTINGS.get("alpaca_data_url", "")) or ""),
        )

    def _make_oanda_client(self) -> OandaBrokerClient:
        return OandaBrokerClient(
            account_id=str(self.settings.get("oanda_account_id", "") or ""),
            api_token=str(self.settings.get("oanda_api_token", "") or ""),
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

    def _render_market_canvas(self, market_key: str, thinker_data: Dict[str, Any]) -> None:
        panel = self.market_panels.get(market_key, {})
        canvas = panel.get("chart_canvas")
        selector = panel.get("selector")
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
        except Exception:
            return

        view = str(selector.get() if selector else "Overview").strip() or "Overview"
        leaders = list(thinker_data.get("leaders", []) or [])
        top_pick = thinker_data.get("top_pick") or (leaders[0] if leaders else None)
        updated_at = thinker_data.get("updated_at")
        updated_txt = ""
        try:
            if updated_at:
                updated_txt = time.strftime("%H:%M:%S", time.localtime(float(updated_at)))
        except Exception:
            updated_txt = ""

        if view == "Overview":
            title = f"{panel.get('market_name', market_key.title())} Thinker Overview"
            body_lines = []
            if top_pick:
                ident = top_pick.get("pair") or top_pick.get("symbol") or "N/A"
                side = str(top_pick.get("side", "watch") or "watch").upper()
                score = top_pick.get("score", "N/A")
                confidence = str(top_pick.get("confidence", "N/A") or "N/A")
                body_lines.append(f"Top pick: {ident} | {side} | score {score} | {confidence}")
                body_lines.append(str(top_pick.get("reason", "") or "").strip())
            else:
                body_lines.append("No ranked candidates yet.")
            if updated_txt:
                body_lines.append(f"Last scan: {updated_txt}")
        elif view == "Leaders":
            title = "Top Leaders"
            body_lines = []
            if not leaders:
                body_lines.append("No leaders available.")
            for idx, row in enumerate(leaders[:5], start=1):
                ident = row.get("pair") or row.get("symbol") or "N/A"
                side = str(row.get("side", "watch") or "watch").upper()
                body_lines.append(f"{idx}. {ident} | {side} | score {row.get('score', 'N/A')}")
                body_lines.append(f"   {row.get('reason', '')}")
        elif view == "Scanner":
            title = "Scanner"
            all_scores = list(thinker_data.get("all_scores", leaders) or [])
            body_lines = []
            if not all_scores:
                body_lines.append("Scanner waiting for data.")
            for idx, row in enumerate(all_scores[:8], start=1):
                ident = row.get("pair") or row.get("symbol") or "N/A"
                body_lines.append(
                    f"{idx}. {ident} | {str(row.get('side', 'watch')).upper()} | score {row.get('score', 'N/A')} | {row.get('confidence', 'N/A')}"
                )
        else:
            title = "Positions-Aware View"
            body_lines = ["Use the Positions panel below for linked broker positions."]
            if top_pick:
                ident = top_pick.get("pair") or top_pick.get("symbol") or "N/A"
                body_lines.append(f"Current strongest candidate: {ident}")

        try:
            canvas.create_text(
                18,
                16,
                anchor="nw",
                text=title,
                fill=DARK_ACCENT,
                font=(self._live_log_font.cget("family"), max(10, int(self._live_log_font.cget("size")) + 3), "bold"),
            )
            body = "\n".join(str(x) for x in body_lines)
            canvas.create_text(
                18,
                48,
                anchor="nw",
                text=body,
                fill=DARK_FG,
                width=max(260, width - 40),
                font=(self._live_log_font.cget("family"), max(9, int(self._live_log_font.cget("size")) + 1)),
            )
        except Exception:
            pass

    def _market_settings_snapshot(self, market_key: str) -> Dict[str, Any]:
        if market_key == "stocks":
            return {
                "broker": "Alpaca",
                "configured": bool(str(self.settings.get("alpaca_api_key_id", "")).strip() and str(self.settings.get("alpaca_secret_key", "")).strip()),
                "mode": ("Paper" if bool(self.settings.get("alpaca_paper_mode", True)) else "Live"),
                "endpoint": str(self.settings.get("alpaca_base_url", DEFAULT_SETTINGS.get("alpaca_base_url", "")) or "").strip(),
                "detail": f"API Key {self._mask_secret(self.settings.get('alpaca_api_key_id', ''))}",
            }
        return {
            "broker": "OANDA",
            "configured": bool(str(self.settings.get("oanda_account_id", "")).strip() and str(self.settings.get("oanda_api_token", "")).strip()),
            "mode": ("Practice" if bool(self.settings.get("oanda_practice_mode", True)) else "Live"),
            "endpoint": str(self.settings.get("oanda_rest_url", DEFAULT_SETTINGS.get("oanda_rest_url", "")) or "").strip(),
            "detail": f"Account {str(self.settings.get('oanda_account_id', '') or '').strip() or 'not set'} | Token {self._mask_secret(self.settings.get('oanda_api_token', ''))}",
        }

    def _refresh_parallel_market_panels(self) -> None:
        for market_key, panel in self.market_panels.items():
            snap = self._market_settings_snapshot(market_key)
            configured = bool(snap.get("configured"))
            mode_txt = str(snap.get("mode", "") or "")
            endpoint = str(snap.get("endpoint", "") or "").strip()
            broker = str(snap.get("broker", market_key.title()) or market_key.title())
            state_txt = "Configured" if configured else "Credentials missing"

            status_path = str(panel.get("status_path", "") or "")
            status_data = _safe_read_json(status_path) if status_path else None
            if not isinstance(status_data, dict):
                status_data = {}
            trader_status_path = self.market_trader_paths.get(market_key, "")
            trader_data = _safe_read_json(trader_status_path) if trader_status_path else None
            if not isinstance(trader_data, dict):
                trader_data = {}
            thinker_data = self._read_market_thinker_status(market_key)

            ai_state = str(thinker_data.get("ai_state", status_data.get("ai_state", state_txt)) or state_txt)
            trader_state = str(trader_data.get("trader_state", status_data.get("trader_state", "Idle")) or "Idle")
            msg = str(trader_data.get("msg", "") or thinker_data.get("msg", "") or status_data.get("msg", "") or "").strip()
            panel["ai_var"].set(f"{panel['market_name']} AI: {ai_state}")
            panel["trader_var"].set(f"{panel['market_name']} Trader: {trader_state}")
            state_line = f"Trade State: {str(thinker_data.get('state', status_data.get('state', state_txt)) or state_txt)}"
            if msg:
                state_line += f" | {msg}"
            panel["state_var"].set(state_line)
            panel["endpoint_var"].set(f"Broker: {broker} | {mode_txt} | {endpoint or 'endpoint not set'}")

            pvars = panel.get("portfolio_vars", {})
            if isinstance(pvars, dict):
                pvars["buying_power"].set(str(status_data.get("buying_power", "Pending account link") or "Pending account link"))
                pvars["open_positions"].set(str(status_data.get("open_positions", "0") or "0"))
                pvars["realized_pnl"].set(str(status_data.get("realized_pnl", "N/A") or "N/A"))
                pvars["mode"].set(mode_txt or "Paper first")
            self._set_market_positions(market_key, list(status_data.get("positions_preview", []) or []))

            extra_note = str(thinker_data.get("pdt_note", "") or status_data.get("pdt_note", "") or "").strip()

            self._set_market_notes(
                market_key,
                "".join(
                    [
                        f"Status: {'ready to connect' if configured else 'credentials required'}\n",
                        f"Broker: {broker}\n",
                        f"Mode: {mode_txt}\n",
                        f"{snap.get('detail', '')}\n",
                        f"Endpoint: {endpoint or 'not set'}\n",
                        (f"{extra_note}\n" if extra_note else ""),
                        (f"Thinker: {ai_state}\n" if ai_state else ""),
                        "These tabs are broker scaffolds for the upcoming market-specific AI engines.",
                    ]
                ),
            )
            self._render_market_canvas(market_key, thinker_data)

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
            if panel.get("last_log_sig") != log_sig:
                panel["last_log_sig"] = log_sig
                self._append_market_log(
                    market_key,
                    f"[{broker.upper()}] {state_txt} | mode={mode_txt} | endpoint={endpoint or 'not set'}",
                )

            try:
                busy = bool(self._market_test_busy.get(market_key, False))
                panel["test_btn"].configure(state=("disabled" if busy else "normal"))
            except Exception:
                pass
            try:
                scan_busy = bool(self._market_thinker_busy.get(market_key, False))
                panel["run_btn"].configure(state=("disabled" if scan_busy else "normal"))
            except Exception:
                pass
            try:
                step_btn = panel.get("trader_step_btn")
                if step_btn is not None:
                    step_market = str(panel.get("trader_step_market_key", "") or "")
                    step_btn.configure(state=("disabled" if self._market_trader_busy.get(step_market, False) else "normal"))
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
        # Start trainers for every coin (in parallel)
        for c in self.coins:
            self.trainer_coin_var.set(c)
            self.start_trainer_for_selected_coin()

    def start_trainer_for_selected_coin(self) -> None:
        coin = (self.trainer_coin_var.get() or "").strip().upper()
        if not coin:
            return

        # Stop the Neural Runner before any training starts (training modifies artifacts the runner reads)
        self.stop_neural()

        # --- IMPORTANT ---
        # Match the trader's folder convention:
        #   BTC runs from the main neural folder
        #   Alts run from their own coin subfolder
        coin_cwd = self.coin_folders.get(coin, self.project_dir)

        # Use the trainer script that lives INSIDE that coin's folder so outputs land in the right place.
        trainer_name = os.path.basename(str(self.settings.get("script_neural_trainer", "pt_trainer.py")))

        # If an alt coin folder doesn't exist yet, create it and copy the trainer script from the main (BTC) folder.
        # (Also: overwrite to avoid running stale trainer copies in alt folders.)

        if coin != "BTC":
            try:
                if not os.path.isdir(coin_cwd):
                    os.makedirs(coin_cwd, exist_ok=True)

                src_main_folder = self.coin_folders.get("BTC", self.project_dir)
                src_trainer_path = os.path.join(src_main_folder, trainer_name)
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
        try:
            self._schedule_market_snapshot_refresh("stocks", every_s=20.0)
            self._schedule_market_snapshot_refresh("forex", every_s=10.0)
        except Exception:
            pass
        try:
            self._schedule_market_thinker_scan("stocks", every_s=75.0)
            self._schedule_market_thinker_scan("forex", every_s=45.0)
        except Exception:
            pass
        try:
            self._run_stock_trader_step(force=False, min_interval_s=18.0)
        except Exception:
            pass
        try:
            self._run_forex_trader_step(force=False, min_interval_s=12.0)
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

        # Disable Start All until training is done (but always allow it if something is already running/pending,
        # so the user can still stop everything).
        can_toggle_all = True
        if (not all_trained) and (not neural_running) and (not trader_running) and (not self._auto_start_trader_pending):
            can_toggle_all = False

        try:
            self.btn_toggle_all.configure(state=("normal" if can_toggle_all else "disabled"))
        except Exception:
            pass

        # Training overview + per-coin list
        try:
            training_running = [c for c, s in status_map.items() if s == "TRAINING"]
            not_trained = [c for c, s in status_map.items() if s == "NOT TRAINED"]
            done_tokens = ("DONE", "COMPLETE", "COMPLETED", "FINISHED", "READY", "TRAINED")

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

            try:
                training_lines = [str(self.training_list.get(i) or "").strip() for i in range(self.training_list.size())]
            except Exception:
                training_lines = []
            total_training = len(training_lines) if training_lines else len(sig)
            completed_training = 0
            if training_lines:
                for ln in training_lines:
                    up_ln = ln.upper()
                    if any(tok in up_ln for tok in done_tokens):
                        completed_training += 1
            else:
                for _, st in sig:
                    up_st = str(st or "").upper()
                    if any(tok in up_st for tok in done_tokens):
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
        self.status.config(text=f"{_now_str()} | hub_dir={self.hub_dir}")
        self.after(int(float(self.settings.get("ui_refresh_seconds", 1.0)) * 1000), self._tick)



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
                self.lbl_last_status.config(text=base_txt)
            else:
                self.lbl_last_status.config(
                    text=(f"Trade State: {state_txt or 'UNKNOWN'} | Heartbeat: (unknown)" + (f" | {status_note}" if status_note else ""))
                )
        except Exception:
            self.lbl_last_status.config(text=(f"Trade State: {state_txt or 'UNKNOWN'} | Heartbeat: (parse error)" + (f" | {status_note}" if status_note else "")))
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
                alloc_pct = float(self.settings.get("start_allocation_pct", 0.005) or 0.005)
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

    def open_settings_dialog(self) -> None:

        win = tk.Toplevel(self)
        win.title("Settings")
        # Big enough for the bottom buttons on most screens + still scrolls if someone resizes smaller.
        win.geometry("860x680")
        win.minsize(760, 560)
        win.configure(bg=DARK_BG)

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

        def add_row(r: int, label: str, var: tk.Variable, browse: Optional[str] = None, parent: Optional[ttk.Frame] = None):
            """
            browse: "dir" to attach a directory chooser, else None.
            """
            target = parent or frm
            ttk.Label(target, text=label).grid(row=r, column=0, sticky="w", padx=(0, 10), pady=6)

            ent = ttk.Entry(target, textvariable=var)
            ent.grid(row=r, column=1, sticky="ew", pady=6)

            if browse == "dir":
                def do_browse():
                    picked = filedialog.askdirectory()
                    if picked:
                        var.set(picked)
                ttk.Button(target, text="Browse", command=do_browse).grid(row=r, column=2, sticky="e", padx=(10, 0), pady=6)
            else:
                # keep column alignment consistent
                ttk.Label(target, text="").grid(row=r, column=2, sticky="e", padx=(10, 0), pady=6)

        def add_secret_row(r: int, label: str, var: tk.Variable, parent: Optional[ttk.Frame] = None):
            target = parent or frm
            ttk.Label(target, text=label).grid(row=r, column=0, sticky="w", padx=(0, 10), pady=6)
            ent = ttk.Entry(target, textvariable=var, show="*")
            ent.grid(row=r, column=1, sticky="ew", pady=6)
            ttk.Label(target, text="").grid(row=r, column=2, sticky="e", padx=(10, 0), pady=6)

        main_dir_var = tk.StringVar(value=self.settings["main_neural_dir"])
        coins_var = tk.StringVar(value=",".join(self.settings["coins"]))
        trade_start_level_var = tk.StringVar(value=str(self.settings.get("trade_start_level", 3)))
        start_alloc_pct_var = tk.StringVar(value=str(self.settings.get("start_allocation_pct", 0.005)))
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
        alpaca_key_var = tk.StringVar(value=str(self.settings.get("alpaca_api_key_id", DEFAULT_SETTINGS.get("alpaca_api_key_id", "")) or ""))
        alpaca_secret_var = tk.StringVar(value=str(self.settings.get("alpaca_secret_key", DEFAULT_SETTINGS.get("alpaca_secret_key", "")) or ""))
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
        stock_auto_trade_var = tk.BooleanVar(value=bool(self.settings.get("stock_auto_trade_enabled", DEFAULT_SETTINGS.get("stock_auto_trade_enabled", False))))
        stock_notional_var = tk.StringVar(value=str(self.settings.get("stock_trade_notional_usd", DEFAULT_SETTINGS.get("stock_trade_notional_usd", 100.0))))
        stock_max_pos_var = tk.StringVar(value=str(self.settings.get("stock_max_open_positions", DEFAULT_SETTINGS.get("stock_max_open_positions", 1))))
        stock_score_threshold_var = tk.StringVar(value=str(self.settings.get("stock_score_threshold", DEFAULT_SETTINGS.get("stock_score_threshold", 0.2))))
        stock_profit_target_var = tk.StringVar(value=str(self.settings.get("stock_profit_target_pct", DEFAULT_SETTINGS.get("stock_profit_target_pct", 0.35))))
        stock_trailing_gap_var = tk.StringVar(value=str(self.settings.get("stock_trailing_gap_pct", DEFAULT_SETTINGS.get("stock_trailing_gap_pct", 0.2))))
        stock_day_trades_var = tk.StringVar(value=str(self.settings.get("stock_max_day_trades", DEFAULT_SETTINGS.get("stock_max_day_trades", 3))))
        stock_max_position_usd_var = tk.StringVar(value=str(self.settings.get("stock_max_position_usd_per_symbol", DEFAULT_SETTINGS.get("stock_max_position_usd_per_symbol", 0.0))))
        stock_max_exposure_var = tk.StringVar(value=str(self.settings.get("stock_max_total_exposure_pct", DEFAULT_SETTINGS.get("stock_max_total_exposure_pct", 0.0))))
        oanda_account_var = tk.StringVar(value=str(self.settings.get("oanda_account_id", DEFAULT_SETTINGS.get("oanda_account_id", "")) or ""))
        oanda_token_var = tk.StringVar(value=str(self.settings.get("oanda_api_token", DEFAULT_SETTINGS.get("oanda_api_token", "")) or ""))
        oanda_rest_url_var = tk.StringVar(value=str(self.settings.get("oanda_rest_url", DEFAULT_SETTINGS.get("oanda_rest_url", "")) or ""))
        oanda_stream_url_var = tk.StringVar(value=str(self.settings.get("oanda_stream_url", DEFAULT_SETTINGS.get("oanda_stream_url", "")) or ""))
        oanda_practice_var = tk.BooleanVar(value=bool(self.settings.get("oanda_practice_mode", DEFAULT_SETTINGS.get("oanda_practice_mode", True))))
        forex_pairs_var = tk.StringVar(value=str(self.settings.get("forex_universe_pairs", DEFAULT_SETTINGS.get("forex_universe_pairs", "")) or ""))
        forex_scan_max_pairs_var = tk.StringVar(value=str(self.settings.get("forex_scan_max_pairs", DEFAULT_SETTINGS.get("forex_scan_max_pairs", 16))))
        fx_auto_trade_var = tk.BooleanVar(value=bool(self.settings.get("forex_auto_trade_enabled", DEFAULT_SETTINGS.get("forex_auto_trade_enabled", False))))
        fx_trade_units_var = tk.StringVar(value=str(self.settings.get("forex_trade_units", DEFAULT_SETTINGS.get("forex_trade_units", 1000))))
        fx_max_pos_var = tk.StringVar(value=str(self.settings.get("forex_max_open_positions", DEFAULT_SETTINGS.get("forex_max_open_positions", 1))))
        fx_score_threshold_var = tk.StringVar(value=str(self.settings.get("forex_score_threshold", DEFAULT_SETTINGS.get("forex_score_threshold", 0.2))))
        fx_profit_target_var = tk.StringVar(value=str(self.settings.get("forex_profit_target_pct", DEFAULT_SETTINGS.get("forex_profit_target_pct", 0.25))))
        fx_trailing_gap_var = tk.StringVar(value=str(self.settings.get("forex_trailing_gap_pct", DEFAULT_SETTINGS.get("forex_trailing_gap_pct", 0.15))))
        fx_max_exposure_var = tk.StringVar(value=str(self.settings.get("forex_max_total_exposure_pct", DEFAULT_SETTINGS.get("forex_max_total_exposure_pct", 0.0))))

        hub_dir_var = tk.StringVar(value=self.settings.get("hub_data_dir", ""))



        neural_script_var = tk.StringVar(value=self.settings["script_neural_runner2"])
        trainer_script_var = tk.StringVar(value=self.settings.get("script_neural_trainer", "pt_trainer.py"))
        trader_script_var = tk.StringVar(value=self.settings["script_trader"])

        ui_refresh_var = tk.StringVar(value=str(self.settings["ui_refresh_seconds"]))
        chart_refresh_var = tk.StringVar(value=str(self.settings["chart_refresh_seconds"]))
        candles_limit_var = tk.StringVar(value=str(self.settings["candles_limit"]))
        auto_start_var = tk.BooleanVar(value=bool(self.settings.get("auto_start_scripts", False)))

        r = 0
        add_row(r, "Main neural folder:", main_dir_var, browse="dir"); r += 1
        add_row(r, "Coins (comma):", coins_var); r += 1
        add_row(r, "Trade start level (1-7):", trade_start_level_var); r += 1

        # Start allocation % (shows approx $/coin using the last known account value; always displays the $0.50 minimum)
        ttk.Label(frm, text="Start allocation %:").grid(row=r, column=0, sticky="w", padx=(0, 10), pady=6)
        ttk.Entry(frm, textvariable=start_alloc_pct_var).grid(row=r, column=1, sticky="ew", pady=6)

        start_alloc_hint_var = tk.StringVar(value="")
        ttk.Label(frm, textvariable=start_alloc_hint_var).grid(row=r, column=2, sticky="w", padx=(10, 0), pady=6)

        def _update_start_alloc_hint(*_):
            # Parse % (allow "0.01" or "0.01%")
            try:
                pct_txt = (start_alloc_pct_var.get() or "").strip().replace("%", "")
                pct = float(pct_txt) if pct_txt else 0.0
            except Exception:
                pct = float(self.settings.get("start_allocation_pct", 0.005) or 0.005)

            if pct < 0.0:
                pct = 0.0

            # Use the last account value we saw in trader_status.json (no extra API calls).
            try:
                total_val = float(getattr(self, "_last_total_account_value", 0.0) or 0.0)
            except Exception:
                total_val = 0.0

            coins_list = [c.strip().upper() for c in (coins_var.get() or "").split(",") if c.strip()]
            n_coins = len(coins_list) if coins_list else 1

            per_coin = 0.0
            if total_val > 0.0:
                per_coin = total_val * (pct / 100.0)
            if per_coin < 0.5:
                per_coin = 0.5

            if total_val > 0.0:
                start_alloc_hint_var.set(f"≈ {_fmt_money(per_coin)} per coin (min $0.50)")
            else:
                start_alloc_hint_var.set("≈ $0.50 min per coin (needs account value)")

        _update_start_alloc_hint()
        start_alloc_pct_var.trace_add("write", _update_start_alloc_hint)
        coins_var.trace_add("write", _update_start_alloc_hint)

        r += 1

        add_row(r, "DCA levels (% list):", dca_levels_var); r += 1

        add_row(r, "DCA multiplier:", dca_mult_var); r += 1

        add_row(r, "Max DCA buys / coin (rolling 24h):", max_dca_var); r += 1

        add_row(r, "Trailing PM start % (no DCA):", pm_no_dca_var); r += 1
        add_row(r, "Trailing PM start % (with DCA):", pm_with_dca_var); r += 1
        add_row(r, "Trailing gap % (behind peak):", trailing_gap_var); r += 1

        advanced_wrap = ttk.Frame(frm)
        advanced_wrap.grid(row=r, column=0, columnspan=3, sticky="ew", pady=(8, 0)); r += 1
        advanced_wrap.columnconfigure(0, weight=1)

        advanced_visible_var = tk.BooleanVar(value=False)
        advanced_frame = ttk.Frame(advanced_wrap)
        advanced_frame.columnconfigure(0, weight=0)
        advanced_frame.columnconfigure(1, weight=1)
        advanced_frame.columnconfigure(2, weight=0)

        def _toggle_advanced() -> None:
            try:
                if advanced_visible_var.get():
                    advanced_frame.grid(row=1, column=0, sticky="ew", pady=(8, 0))
                    advanced_btn.config(text="Hide Advanced")
                else:
                    advanced_frame.grid_remove()
                    advanced_btn.config(text="Advanced")
                _update_settings_scrollbars()
            except Exception:
                pass

        advanced_btn = ttk.Button(advanced_wrap, text="Advanced", command=lambda: (
            advanced_visible_var.set(not advanced_visible_var.get()),
            _toggle_advanced(),
        ))
        advanced_btn.grid(row=0, column=0, sticky="w")

        ar = 0
        add_row(ar, "Max position USD / coin (0=off):", max_pos_per_coin_var, parent=advanced_frame); ar += 1
        add_row(ar, "Max total exposure % (0=off):", max_total_exposure_var, parent=advanced_frame); ar += 1
        add_row(ar, "Hub data dir (optional):", hub_dir_var, browse="dir", parent=advanced_frame); ar += 1

        ttk.Separator(advanced_frame, orient="horizontal").grid(row=ar, column=0, columnspan=3, sticky="ew", pady=10); ar += 1

        add_row(ar, "pt_thinker.py path:", neural_script_var, parent=advanced_frame); ar += 1
        add_row(ar, "pt_trainer.py path:", trainer_script_var, parent=advanced_frame); ar += 1
        add_row(ar, "pt_trader.py path:", trader_script_var, parent=advanced_frame); ar += 1

        ttk.Separator(advanced_frame, orient="horizontal").grid(row=ar, column=0, columnspan=3, sticky="ew", pady=10); ar += 1
        add_row(ar, "Market rollout stage:", rollout_stage_var, parent=advanced_frame); ar += 1
        ttk.Label(advanced_frame, text="Stages: legacy -> scan_expanded -> risk_caps -> execution_v2", foreground=DARK_MUTED).grid(row=ar, column=0, columnspan=3, sticky="w", pady=(0, 6)); ar += 1

        ttk.Label(advanced_frame, text="Alpaca API:").grid(row=ar, column=0, sticky="w", padx=(0, 10), pady=6)
        alpaca_mode_chk = ttk.Checkbutton(advanced_frame, text="Paper mode", variable=alpaca_paper_var)
        alpaca_mode_chk.grid(row=ar, column=1, sticky="w", pady=6)
        ttk.Label(advanced_frame, text="").grid(row=ar, column=2, sticky="e", padx=(10, 0), pady=6); ar += 1
        add_secret_row(ar, "Alpaca API key ID:", alpaca_key_var, parent=advanced_frame); ar += 1
        add_secret_row(ar, "Alpaca secret key:", alpaca_secret_var, parent=advanced_frame); ar += 1
        add_row(ar, "Alpaca base URL:", alpaca_base_url_var, parent=advanced_frame); ar += 1
        add_row(ar, "Alpaca data URL:", alpaca_data_url_var, parent=advanced_frame); ar += 1
        add_row(ar, "Stock universe mode:", stock_universe_mode_var, parent=advanced_frame); ar += 1
        add_row(ar, "Stock universe symbols (watchlist):", stock_universe_symbols_var, parent=advanced_frame); ar += 1
        add_row(ar, "Stock scan max symbols:", stock_scan_max_symbols_var, parent=advanced_frame); ar += 1
        add_row(ar, "Stock min price:", stock_min_price_var, parent=advanced_frame); ar += 1
        add_row(ar, "Stock max price:", stock_max_price_var, parent=advanced_frame); ar += 1
        add_row(ar, "Stock min dollar volume:", stock_min_dollar_volume_var, parent=advanced_frame); ar += 1
        ttk.Label(advanced_frame, text="Stocks AI trader:").grid(row=ar, column=0, sticky="w", padx=(0, 10), pady=6)
        ttk.Checkbutton(advanced_frame, text="Enable auto-trade (paper-safe)", variable=stock_auto_trade_var).grid(row=ar, column=1, sticky="w", pady=6)
        ttk.Label(advanced_frame, text="").grid(row=ar, column=2, sticky="e", padx=(10, 0), pady=6); ar += 1
        add_row(ar, "Stock order notional USD:", stock_notional_var, parent=advanced_frame); ar += 1
        add_row(ar, "Stock max open positions:", stock_max_pos_var, parent=advanced_frame); ar += 1
        add_row(ar, "Stock score threshold:", stock_score_threshold_var, parent=advanced_frame); ar += 1
        add_row(ar, "Stock profit target %:", stock_profit_target_var, parent=advanced_frame); ar += 1
        add_row(ar, "Stock trailing gap %:", stock_trailing_gap_var, parent=advanced_frame); ar += 1
        add_row(ar, "Stock max day trades / day:", stock_day_trades_var, parent=advanced_frame); ar += 1
        add_row(ar, "Stock max position USD/symbol (risk_caps):", stock_max_position_usd_var, parent=advanced_frame); ar += 1
        add_row(ar, "Stock max total exposure % (risk_caps):", stock_max_exposure_var, parent=advanced_frame); ar += 1

        ttk.Separator(advanced_frame, orient="horizontal").grid(row=ar, column=0, columnspan=3, sticky="ew", pady=10); ar += 1

        ttk.Label(advanced_frame, text="OANDA API:").grid(row=ar, column=0, sticky="w", padx=(0, 10), pady=6)
        oanda_mode_chk = ttk.Checkbutton(advanced_frame, text="Practice mode", variable=oanda_practice_var)
        oanda_mode_chk.grid(row=ar, column=1, sticky="w", pady=6)
        ttk.Label(advanced_frame, text="").grid(row=ar, column=2, sticky="e", padx=(10, 0), pady=6); ar += 1
        add_row(ar, "OANDA account ID:", oanda_account_var, parent=advanced_frame); ar += 1
        add_secret_row(ar, "OANDA API token:", oanda_token_var, parent=advanced_frame); ar += 1
        add_row(ar, "OANDA REST URL:", oanda_rest_url_var, parent=advanced_frame); ar += 1
        add_row(ar, "OANDA stream URL:", oanda_stream_url_var, parent=advanced_frame); ar += 1
        add_row(ar, "Forex universe pairs:", forex_pairs_var, parent=advanced_frame); ar += 1
        add_row(ar, "Forex scan max pairs:", forex_scan_max_pairs_var, parent=advanced_frame); ar += 1
        ttk.Label(advanced_frame, text="Forex AI trader:").grid(row=ar, column=0, sticky="w", padx=(0, 10), pady=6)
        ttk.Checkbutton(advanced_frame, text="Enable auto-trade (practice only)", variable=fx_auto_trade_var).grid(row=ar, column=1, sticky="w", pady=6)
        ttk.Label(advanced_frame, text="").grid(row=ar, column=2, sticky="e", padx=(10, 0), pady=6); ar += 1
        add_row(ar, "Forex trade units:", fx_trade_units_var, parent=advanced_frame); ar += 1
        add_row(ar, "Forex max open positions:", fx_max_pos_var, parent=advanced_frame); ar += 1
        add_row(ar, "Forex score threshold:", fx_score_threshold_var, parent=advanced_frame); ar += 1
        add_row(ar, "Forex profit target %:", fx_profit_target_var, parent=advanced_frame); ar += 1
        add_row(ar, "Forex trailing gap %:", fx_trailing_gap_var, parent=advanced_frame); ar += 1
        add_row(ar, "Forex max exposure % (risk_caps proxy):", fx_max_exposure_var, parent=advanced_frame); ar += 1

        # --- Robinhood API setup (writes r_key.txt + r_secret.txt used by pt_trader.py) ---
        def _api_paths() -> Tuple[str, str]:
            key_path = os.path.join(self.project_dir, "r_key.txt")
            secret_path = os.path.join(self.project_dir, "r_secret.txt")
            return key_path, secret_path

        def _read_api_files() -> Tuple[str, str]:
            key_path, secret_path = _api_paths()
            try:
                with open(key_path, "r", encoding="utf-8") as f:
                    k = (f.read() or "").strip()
            except Exception:
                k = ""
            try:
                with open(secret_path, "r", encoding="utf-8") as f:
                    s = (f.read() or "").strip()
            except Exception:
                s = ""
            return k, s

        api_status_var = tk.StringVar(value="")

        def _refresh_api_status() -> None:
            key_path, secret_path = _api_paths()
            k, s = _read_api_files()

            missing = []
            if not k:
                missing.append("r_key.txt (API Key)")
            if not s:
                missing.append("r_secret.txt (PRIVATE key)")

            if missing:
                api_status_var.set("Not configured ❌ (missing " + ", ".join(missing) + ")")
            else:
                api_status_var.set("Configured ✅ (credentials found)")

        def _open_api_folder() -> None:
            """Open the folder where r_key.txt / r_secret.txt live."""
            try:
                folder = os.path.abspath(self.project_dir)
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
            """Delete r_key.txt / r_secret.txt (with a big confirmation)."""
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
            messagebox.showinfo("Deleted", "Deleted r_key.txt and r_secret.txt.")

        def _open_robinhood_api_wizard() -> None:
            """
            Beginner-friendly wizard that creates + stores Robinhood Crypto Trading API credentials.

            What we store:
              - r_key.txt    = your Robinhood *API Key* (safe-ish to store, still treat as sensitive)
              - r_secret.txt = your *PRIVATE key* (treat like a password — never share it)
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
                "This wizard will save two files in the same folder as pt_hub.py:\n"
                "  - r_key.txt    (your API Key)\n"
                "  - r_secret.txt (your PRIVATE key in base64)  ← keep this secret like a password\n"
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
            ttk.Button(top_btns, text="Open Folder With r_key.txt / r_secret.txt", command=lambda: _open_in_file_manager(self.project_dir)).pack(side="left", padx=8)

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
                    messagebox.showerror("Bad private key", f"Couldn't use your private key (r_secret.txt).\n\nError:\n{e}")
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
                text="I understand r_secret.txt is PRIVATE and I will not share it.",
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
                # - Save ONLY base64(seed32) to r_secret.txt
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
                        "For safety, please check the box confirming you understand r_secret.txt is private."
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

        ttk.Label(advanced_frame, text="Robinhood API:").grid(row=ar, column=0, sticky="w", padx=(0, 10), pady=6)

        api_row = ttk.Frame(advanced_frame)
        api_row.grid(row=ar, column=1, columnspan=2, sticky="ew", pady=6)
        api_row.columnconfigure(0, weight=1)

        ttk.Label(api_row, textvariable=api_status_var).grid(row=0, column=0, sticky="w")
        ttk.Button(api_row, text="Setup Wizard", command=_open_robinhood_api_wizard).grid(row=0, column=1, sticky="e", padx=(10, 0))
        ttk.Button(api_row, text="Open Folder", command=_open_api_folder).grid(row=0, column=2, sticky="e", padx=(8, 0))
        ttk.Button(api_row, text="Clear", command=_clear_api_files).grid(row=0, column=3, sticky="e", padx=(8, 0))

        ar += 1

        _refresh_api_status()

        ttk.Separator(advanced_frame, orient="horizontal").grid(row=ar, column=0, columnspan=3, sticky="ew", pady=10); ar += 1

        add_row(ar, "UI refresh seconds:", ui_refresh_var, parent=advanced_frame); ar += 1
        add_row(ar, "Chart refresh seconds:", chart_refresh_var, parent=advanced_frame); ar += 1
        add_row(ar, "Candles limit:", candles_limit_var, parent=advanced_frame); ar += 1

        chk = ttk.Checkbutton(advanced_frame, text="Auto start scripts on GUI launch", variable=auto_start_var)
        chk.grid(row=ar, column=0, columnspan=3, sticky="w", pady=(10, 0)); ar += 1

        btns = ttk.Frame(frm)
        btns.grid(row=r, column=0, columnspan=3, sticky="ew", pady=14)
        btns.columnconfigure(0, weight=1)

        def save():
            try:
                # Track coins before changes so we can detect newly added coins
                prev_coins = set([str(c).strip().upper() for c in (self.settings.get("coins") or []) if str(c).strip()])

                self.settings["main_neural_dir"] = main_dir_var.get().strip()
                self.settings["coins"] = [c.strip().upper() for c in coins_var.get().split(",") if c.strip()]
                self.settings["trade_start_level"] = max(1, min(int(float(trade_start_level_var.get().strip())), 7))

                sap = (start_alloc_pct_var.get() or "").strip().replace("%", "")
                self.settings["start_allocation_pct"] = max(0.0, float(sap or 0.0))

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
                self.settings["alpaca_api_key_id"] = alpaca_key_var.get().strip()
                self.settings["alpaca_secret_key"] = alpaca_secret_var.get().strip()
                self.settings["alpaca_base_url"] = alpaca_base_url_var.get().strip() or str(DEFAULT_SETTINGS.get("alpaca_base_url", ""))
                self.settings["alpaca_data_url"] = alpaca_data_url_var.get().strip() or str(DEFAULT_SETTINGS.get("alpaca_data_url", ""))
                self.settings["alpaca_paper_mode"] = bool(alpaca_paper_var.get())
                stage = str(rollout_stage_var.get() or "").strip().lower()
                if stage not in {"legacy", "scan_expanded", "risk_caps", "execution_v2"}:
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
                self.settings["stock_auto_trade_enabled"] = bool(stock_auto_trade_var.get())
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
                self.settings["oanda_account_id"] = oanda_account_var.get().strip()
                self.settings["oanda_api_token"] = oanda_token_var.get().strip()
                self.settings["oanda_rest_url"] = oanda_rest_url_var.get().strip() or str(DEFAULT_SETTINGS.get("oanda_rest_url", ""))
                self.settings["oanda_stream_url"] = oanda_stream_url_var.get().strip() or str(DEFAULT_SETTINGS.get("oanda_stream_url", ""))
                self.settings["oanda_practice_mode"] = bool(oanda_practice_var.get())
                self.settings["forex_universe_pairs"] = str(forex_pairs_var.get() or "").strip()
                try:
                    self.settings["forex_scan_max_pairs"] = max(4, int(float((forex_scan_max_pairs_var.get() or "").strip() or 16)))
                except Exception:
                    self.settings["forex_scan_max_pairs"] = int(DEFAULT_SETTINGS.get("forex_scan_max_pairs", 16))
                self.settings["forex_auto_trade_enabled"] = bool(fx_auto_trade_var.get())
                self.settings["forex_trade_units"] = max(1, int(float((fx_trade_units_var.get() or "").strip() or 1000)))
                self.settings["forex_max_open_positions"] = max(1, int(float((fx_max_pos_var.get() or "").strip() or 1)))
                self.settings["forex_score_threshold"] = max(0.0, float((fx_score_threshold_var.get() or "").strip() or 0.2))
                self.settings["forex_profit_target_pct"] = max(0.0, float((fx_profit_target_var.get() or "").strip().replace("%", "") or 0.25))
                self.settings["forex_trailing_gap_pct"] = max(0.0, float((fx_trailing_gap_var.get() or "").strip().replace("%", "") or 0.15))
                try:
                    self.settings["forex_max_total_exposure_pct"] = max(0.0, float((fx_max_exposure_var.get() or "").strip().replace("%", "") or 0.0))
                except Exception:
                    self.settings["forex_max_total_exposure_pct"] = float(DEFAULT_SETTINGS.get("forex_max_total_exposure_pct", 0.0))

                self.settings["script_neural_runner2"] = neural_script_var.get().strip()
                self.settings["script_neural_trainer"] = trainer_script_var.get().strip()
                self.settings["script_trader"] = trader_script_var.get().strip()

                self.settings["ui_refresh_seconds"] = float(ui_refresh_var.get().strip())
                self.settings["chart_refresh_seconds"] = float(chart_refresh_var.get().strip())
                self.settings["candles_limit"] = int(float(candles_limit_var.get().strip()))
                self.settings["auto_start_scripts"] = bool(auto_start_var.get())
                self._save_settings()

                # If new coin(s) were added and their training folder doesn't exist yet,
                # create the folder and copy neural_trainer.py into it RIGHT AFTER saving settings.
                try:
                    new_coins = [c.strip().upper() for c in (self.settings.get("coins") or []) if c.strip()]
                    added = [c for c in new_coins if c and c not in prev_coins]

                    main_dir = self.settings.get("main_neural_dir") or self.project_dir
                    trainer_name = os.path.basename(str(self.settings.get("script_neural_trainer", "neural_trainer.py")))

                    # Best-effort resolve source trainer path:
                    # Prefer trainer living in the main (BTC) folder; fallback to the configured trainer path.
                    src_main_trainer = os.path.join(main_dir, trainer_name)
                    src_cfg_trainer = str(self.settings.get("script_neural_trainer", trainer_name))
                    src_trainer_path = src_main_trainer if os.path.isfile(src_main_trainer) else src_cfg_trainer

                    for coin in added:
                        if coin == "BTC":
                            continue  # BTC uses main folder; no per-coin folder needed

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
                win.destroy()


            except Exception as e:
                messagebox.showerror("Error", f"Failed to save settings:\n{e}")


        ttk.Button(btns, text="Save", command=save).pack(side="left")
        ttk.Button(btns, text="Cancel", command=win.destroy).pack(side="left", padx=8)


    # ---- close ----

    def _on_close(self) -> None:
        self.destroy()


if __name__ == "__main__":
    app = PowerTraderHub()
    app.mainloop()
