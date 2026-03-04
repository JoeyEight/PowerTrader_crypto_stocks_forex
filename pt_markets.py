from __future__ import annotations

import json
import os
import signal
import time
from typing import Any, Dict

from broker_alpaca import AlpacaBrokerClient
from broker_oanda import OandaBrokerClient
from forex_thinker import run_scan as run_forex_scan
from forex_trader import run_step as run_forex_trader_step
from path_utils import resolve_runtime_paths, resolve_settings_path, read_settings_file
from stock_thinker import run_scan as run_stock_scan
from stock_trader import run_step as run_stock_trader_step

BASE_DIR, _SETTINGS_PATH, HUB_DATA_DIR, _BOOT_SETTINGS = resolve_runtime_paths(__file__, "pt_markets")
STOP_FLAG_PATH = os.path.join(HUB_DATA_DIR, "stop_trading.flag")


def _atomic_write_json(path: str, payload: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    os.replace(tmp, path)


def _load_settings() -> Dict[str, Any]:
    settings_path = resolve_settings_path(BASE_DIR) or _SETTINGS_PATH or os.path.join(BASE_DIR, "gui_settings.json")
    data = read_settings_file(settings_path, module_name="pt_markets") or {}
    return data if isinstance(data, dict) else {}


def _write_snapshots(settings: Dict[str, Any]) -> None:
    stocks_dir = os.path.join(HUB_DATA_DIR, "stocks")
    forex_dir = os.path.join(HUB_DATA_DIR, "forex")
    os.makedirs(stocks_dir, exist_ok=True)
    os.makedirs(forex_dir, exist_ok=True)

    alpaca = AlpacaBrokerClient(
        api_key_id=str(settings.get("alpaca_api_key_id", "") or ""),
        secret_key=str(settings.get("alpaca_secret_key", "") or ""),
        base_url=str(settings.get("alpaca_base_url", "https://paper-api.alpaca.markets") or ""),
        data_url=str(settings.get("alpaca_data_url", "https://data.alpaca.markets") or ""),
    )
    oanda = OandaBrokerClient(
        account_id=str(settings.get("oanda_account_id", "") or ""),
        api_token=str(settings.get("oanda_api_token", "") or ""),
        rest_url=str(settings.get("oanda_rest_url", "https://api-fxpractice.oanda.com") or ""),
    )

    try:
        s = alpaca.fetch_snapshot()
        s["ts"] = int(time.time())
        _atomic_write_json(os.path.join(stocks_dir, "alpaca_status.json"), s)
    except Exception as exc:
        print(f"[MARKETS] stocks snapshot failed: {type(exc).__name__}: {exc}")
    try:
        f = oanda.fetch_snapshot()
        f["ts"] = int(time.time())
        _atomic_write_json(os.path.join(forex_dir, "oanda_status.json"), f)
    except Exception as exc:
        print(f"[MARKETS] forex snapshot failed: {type(exc).__name__}: {exc}")


def _run_stocks(settings: Dict[str, Any]) -> None:
    stocks_dir = os.path.join(HUB_DATA_DIR, "stocks")
    os.makedirs(stocks_dir, exist_ok=True)
    try:
        thinker = run_stock_scan(settings, HUB_DATA_DIR)
        thinker["ts"] = int(time.time())
        _atomic_write_json(os.path.join(stocks_dir, "stock_thinker_status.json"), thinker)
    except Exception as exc:
        print(f"[MARKETS] stocks thinker failed: {type(exc).__name__}: {exc}")
    try:
        trader = run_stock_trader_step(settings, HUB_DATA_DIR)
        trader["ts"] = int(time.time())
        _atomic_write_json(os.path.join(stocks_dir, "stock_trader_status.json"), trader)
    except Exception as exc:
        print(f"[MARKETS] stocks trader failed: {type(exc).__name__}: {exc}")


def _run_forex(settings: Dict[str, Any]) -> None:
    forex_dir = os.path.join(HUB_DATA_DIR, "forex")
    os.makedirs(forex_dir, exist_ok=True)
    try:
        thinker = run_forex_scan(settings, HUB_DATA_DIR)
        thinker["ts"] = int(time.time())
        _atomic_write_json(os.path.join(forex_dir, "forex_thinker_status.json"), thinker)
    except Exception as exc:
        print(f"[MARKETS] forex thinker failed: {type(exc).__name__}: {exc}")
    try:
        trader = run_forex_trader_step(settings, HUB_DATA_DIR)
        trader["ts"] = int(time.time())
        _atomic_write_json(os.path.join(forex_dir, "forex_trader_status.json"), trader)
    except Exception as exc:
        print(f"[MARKETS] forex trader failed: {type(exc).__name__}: {exc}")


def main() -> int:
    running = {"ok": True}

    def _stop(_signum, _frame) -> None:
        running["ok"] = False

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    last_snap = 0.0
    last_stock = 0.0
    last_fx = 0.0

    while running["ok"]:
        if os.path.exists(STOP_FLAG_PATH):
            break
        now = time.time()
        settings = _load_settings()
        try:
            snap_every = max(5.0, float(settings.get("market_bg_snapshot_interval_s", 15.0) or 15.0))
            stock_every = max(8.0, float(settings.get("market_bg_stocks_interval_s", 18.0) or 18.0))
            fx_every = max(6.0, float(settings.get("market_bg_forex_interval_s", 12.0) or 12.0))
        except Exception:
            snap_every, stock_every, fx_every = 15.0, 18.0, 12.0

        if (now - last_snap) >= snap_every:
            _write_snapshots(settings)
            last_snap = now
        if (now - last_stock) >= stock_every:
            _run_stocks(settings)
            last_stock = now
        if (now - last_fx) >= fx_every:
            _run_forex(settings)
            last_fx = now

        time.sleep(1.0)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
