from __future__ import annotations

import json
import os
import tempfile
import unittest
from unittest.mock import patch

from engines import forex_trader, stock_trader


class _FakeAlpacaClient:
    def __init__(self, api_key_id: str, secret_key: str, base_url: str, data_url: str) -> None:
        self.order_calls = 0

    def configured(self) -> bool:
        return True

    def list_positions(self) -> list[dict]:
        return []

    def get_mid_prices(self, symbols: list[str]) -> dict[str, float]:
        return {str(s).strip().upper(): 100.0 for s in symbols}

    def get_account_summary(self) -> dict:
        return {"equity": 10_000.0}

    def get_snapshot_details(self, symbols: list[str]) -> dict:
        return {str(s).strip().upper(): {"mid": 100.0, "spread_bps": 1.0} for s in symbols}

    def place_market_order(self, *args, **kwargs):  # pragma: no cover - should not be called in guard test
        self.order_calls += 1
        return False, "unexpected", {}

    def close_position(self, symbol: str):
        return True, "ok", {}


class _FakeOandaClient:
    def __init__(self, account_id: str, api_token: str, rest_url: str) -> None:
        self.order_calls = 0

    def configured(self) -> bool:
        return True

    def fetch_snapshot(self) -> dict:
        return {"raw_positions": [], "nav": 10_000.0}

    def get_mid_prices(self, instruments: list[str]) -> dict[str, float]:
        return {str(p).strip().upper(): 1.2345 for p in instruments}

    def get_pricing_details(self, pairs: list[str]) -> dict:
        return {str(p).strip().upper(): {"mid": 1.2345, "spread_bps": 1.1} for p in pairs}

    def place_market_order(self, *args, **kwargs):  # pragma: no cover - should not be called in guard test
        self.order_calls += 1
        return False, "unexpected", {}

    def close_position(self, instrument: str, side: str = "long"):
        return True, "ok", {}


class TestCachedScanEntryGuard(unittest.TestCase):
    def _write_json(self, path: str, payload: dict) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f)

    def test_stock_trader_blocks_new_entries_on_cached_scan(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            stocks_dir = os.path.join(td, "stocks")
            os.makedirs(stocks_dir, exist_ok=True)
            self._write_json(
                os.path.join(stocks_dir, "stock_thinker_status.json"),
                {
                    "updated_at": 1_700_000_000,
                    "fallback_cached": True,
                    "fallback_age_s": 120,
                    "top_pick": {"symbol": "AAPL", "side": "long", "score": 0.9},
                    "leaders": [{"symbol": "AAPL", "side": "long", "score": 0.9, "eligible_for_entry": True}],
                    "all_scores": [{"symbol": "AAPL", "side": "long", "score": 0.9, "eligible_for_entry": True}],
                },
            )
            settings = {
                "stock_auto_trade_enabled": True,
                "stock_block_entries_on_cached_scan": True,
                "market_rollout_stage": "execution_v2",
                "stock_max_signal_age_seconds": 600,
                "stock_max_open_positions": 1,
                "stock_trade_notional_usd": 100.0,
            }
            with (
                patch.object(stock_trader, "get_alpaca_creds", return_value=("key", "secret")),
                patch.object(stock_trader, "AlpacaBrokerClient", _FakeAlpacaClient),
                patch("engines.stock_trader.time.time", return_value=1_700_000_100),
            ):
                out = stock_trader.run_step(settings, td)
            self.assertEqual(str(out.get("state", "")), "READY")
            self.assertIn("cached fallback", str(out.get("msg", "")).lower())
            self.assertGreaterEqual(int(out.get("entry_eval_total", 0) or 0), 1)
            self.assertIn("cached fallback", str(out.get("entry_eval_top_reason", "")).lower())

    def test_forex_trader_blocks_new_entries_on_cached_scan(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            fx_dir = os.path.join(td, "forex")
            os.makedirs(fx_dir, exist_ok=True)
            self._write_json(
                os.path.join(fx_dir, "forex_thinker_status.json"),
                {
                    "updated_at": 1_700_000_000,
                    "fallback_cached": True,
                    "fallback_age_s": 95,
                    "top_pick": {"pair": "EUR_USD", "side": "long", "score": 0.42},
                    "leaders": [{"pair": "EUR_USD", "side": "long", "score": 0.42, "eligible_for_entry": True}],
                    "all_scores": [{"pair": "EUR_USD", "side": "long", "score": 0.42, "eligible_for_entry": True}],
                },
            )
            settings = {
                "forex_auto_trade_enabled": True,
                "forex_block_entries_on_cached_scan": True,
                "market_rollout_stage": "execution_v2",
                "forex_max_signal_age_seconds": 600,
                "forex_max_open_positions": 1,
                "forex_trade_units": 1000,
            }
            with (
                patch.object(forex_trader, "get_oanda_creds", return_value=("acct", "token")),
                patch.object(forex_trader, "OandaBrokerClient", _FakeOandaClient),
                patch("engines.forex_trader.time.time", return_value=1_700_000_100),
            ):
                out = forex_trader.run_step(settings, td)
            self.assertEqual(str(out.get("state", "")), "READY")
            self.assertIn("cached fallback", str(out.get("msg", "")).lower())
            self.assertGreaterEqual(int(out.get("entry_eval_total", 0) or 0), 1)
            self.assertIn("cached fallback", str(out.get("entry_eval_top_reason", "")).lower())


if __name__ == "__main__":
    unittest.main()
