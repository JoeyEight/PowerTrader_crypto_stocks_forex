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
        self.last_notional = 0.0

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

    def place_market_order(self, symbol: str, side: str, notional: float, client_order_id: str, max_retries: int = 2, max_retry_after_s: float = 300.0):
        self.order_calls += 1
        self.last_notional = float(notional)
        return True, "entry ok", {"id": "alpaca-order-1"}

    def close_position(self, symbol: str):
        return True, "ok", {}


class _FakeOandaClient:
    def __init__(self, account_id: str, api_token: str, rest_url: str) -> None:
        self.order_calls = 0
        self.last_units = 0

    def configured(self) -> bool:
        return True

    def fetch_snapshot(self) -> dict:
        return {"raw_positions": [], "nav": 10_000.0}

    def get_mid_prices(self, instruments: list[str]) -> dict[str, float]:
        return {str(p).strip().upper(): 1.2345 for p in instruments}

    def get_pricing_details(self, pairs: list[str]) -> dict:
        return {str(p).strip().upper(): {"mid": 1.2345, "spread_bps": 1.0} for p in pairs}

    def place_market_order(self, instrument: str, units: int, client_order_id: str, max_retries: int = 2, max_retry_after_s: float = 300.0):
        self.order_calls += 1
        self.last_units = int(units)
        return True, "entry ok", {"orderFillTransaction": {"id": "oanda-order-1"}}

    def close_position(self, instrument: str, side: str = "long"):
        return True, "ok", {}


class TestTraderQualityGates(unittest.TestCase):
    def _write_json(self, path: str, payload: dict) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f)

    def test_stock_blocks_when_thinker_data_quality_is_bad(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            stocks_dir = os.path.join(td, "stocks")
            os.makedirs(stocks_dir, exist_ok=True)
            self._write_json(
                os.path.join(stocks_dir, "stock_thinker_status.json"),
                {
                    "updated_at": 1_700_000_000,
                    "fallback_cached": False,
                    "health": {"data_ok": False},
                    "reject_summary": {"reject_rate_pct": 12.0},
                    "top_pick": {"symbol": "AAPL", "side": "long", "score": 0.9},
                    "leaders": [{"symbol": "AAPL", "side": "long", "score": 0.9, "eligible_for_entry": True, "data_quality_ok": True}],
                    "all_scores": [{"symbol": "AAPL", "side": "long", "score": 0.9, "eligible_for_entry": True, "data_quality_ok": True}],
                },
            )
            settings = {
                "stock_auto_trade_enabled": True,
                "stock_require_data_quality_ok_for_entries": True,
                "stock_require_reject_rate_max_pct": 95.0,
                "stock_block_entries_on_cached_scan": False,
                "market_rollout_stage": "execution_v2",
                "stock_max_signal_age_seconds": 600,
                "stock_max_open_positions": 1,
                "stock_trade_notional_usd": 100.0,
            }
            with (
                patch.object(stock_trader, "get_alpaca_creds", return_value=("key", "secret")),
                patch.object(stock_trader, "AlpacaBrokerClient", _FakeAlpacaClient),
                patch.object(stock_trader, "_market_open_now", return_value=True),
                patch.object(stock_trader, "_near_close_blocked", return_value=False),
                patch("engines.stock_trader.time.time", return_value=1_700_000_100),
            ):
                out = stock_trader.run_step(settings, td)
            self.assertEqual(str(out.get("state", "")), "READY")
            self.assertIn("data-quality gate", str(out.get("msg", "")).lower())
            self.assertIn("data-quality gate", str(out.get("entry_eval_top_reason", "")).lower())

    def test_forex_blocks_on_reject_pressure(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            fx_dir = os.path.join(td, "forex")
            os.makedirs(fx_dir, exist_ok=True)
            self._write_json(
                os.path.join(fx_dir, "forex_thinker_status.json"),
                {
                    "updated_at": 1_700_000_000,
                    "fallback_cached": False,
                    "health": {"data_ok": True},
                    "reject_summary": {"reject_rate_pct": 97.0},
                    "top_pick": {"pair": "EUR_USD", "side": "long", "score": 0.62},
                    "leaders": [{"pair": "EUR_USD", "side": "long", "score": 0.62, "eligible_for_entry": True, "data_quality_ok": True}],
                    "all_scores": [{"pair": "EUR_USD", "side": "long", "score": 0.62, "eligible_for_entry": True, "data_quality_ok": True}],
                },
            )
            settings = {
                "forex_auto_trade_enabled": True,
                "forex_require_data_quality_ok_for_entries": True,
                "forex_require_reject_rate_max_pct": 90.0,
                "forex_block_entries_on_cached_scan": False,
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
            self.assertIn("reject-pressure gate", str(out.get("msg", "")).lower())
            self.assertIn("reject-pressure gate", str(out.get("entry_eval_top_reason", "")).lower())

    def test_stock_reduces_entry_notional_when_cached_fallback_allowed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            stocks_dir = os.path.join(td, "stocks")
            os.makedirs(stocks_dir, exist_ok=True)
            self._write_json(
                os.path.join(stocks_dir, "stock_thinker_status.json"),
                {
                    "updated_at": 1_700_000_000,
                    "fallback_cached": True,
                    "fallback_age_s": 90,
                    "health": {"data_ok": True},
                    "reject_summary": {"reject_rate_pct": 12.0},
                    "top_pick": {"symbol": "AAPL", "side": "long", "score": 0.9},
                    "leaders": [{"symbol": "AAPL", "side": "long", "score": 0.9, "eligible_for_entry": True, "data_quality_ok": True, "calib_prob": 0.8, "samples": 20, "bars_count": 60}],
                    "all_scores": [{"symbol": "AAPL", "side": "long", "score": 0.9, "eligible_for_entry": True, "data_quality_ok": True, "calib_prob": 0.8, "samples": 20, "bars_count": 60}],
                },
            )
            settings = {
                "stock_auto_trade_enabled": True,
                "stock_require_data_quality_ok_for_entries": True,
                "stock_require_reject_rate_max_pct": 95.0,
                "stock_block_entries_on_cached_scan": False,
                "stock_cached_scan_hard_block_age_s": 1200,
                "stock_cached_scan_entry_size_mult": 0.5,
                "market_rollout_stage": "execution_v2",
                "stock_max_signal_age_seconds": 600,
                "stock_max_open_positions": 1,
                "stock_trade_notional_usd": 100.0,
            }
            with (
                patch.object(stock_trader, "get_alpaca_creds", return_value=("key", "secret")),
                patch.object(stock_trader, "AlpacaBrokerClient", _FakeAlpacaClient),
                patch.object(stock_trader, "_market_open_now", return_value=True),
                patch.object(stock_trader, "_near_close_blocked", return_value=False),
                patch("engines.stock_trader.time.time", return_value=1_700_000_100),
            ):
                out = stock_trader.run_step(settings, td)
            self.assertEqual(str(out.get("state", "")), "READY")
            self.assertIn("entry placed", str(out.get("msg", "")).lower())
            self.assertEqual(float(out.get("trade_notional_entry_usd", 0.0) or 0.0), 50.0)
            self.assertEqual(float(out.get("entry_size_scale", 1.0) or 1.0), 0.5)

    def test_forex_reduces_units_when_cached_fallback_allowed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            fx_dir = os.path.join(td, "forex")
            os.makedirs(fx_dir, exist_ok=True)
            self._write_json(
                os.path.join(fx_dir, "forex_thinker_status.json"),
                {
                    "updated_at": 1_700_000_000,
                    "fallback_cached": True,
                    "fallback_age_s": 80,
                    "health": {"data_ok": True},
                    "reject_summary": {"reject_rate_pct": 10.0},
                    "top_pick": {"pair": "EUR_USD", "side": "long", "score": 0.62},
                    "leaders": [{"pair": "EUR_USD", "side": "long", "score": 0.62, "eligible_for_entry": True, "data_quality_ok": True, "calib_prob": 0.8, "samples": 20, "bars_count": 60}],
                    "all_scores": [{"pair": "EUR_USD", "side": "long", "score": 0.62, "eligible_for_entry": True, "data_quality_ok": True, "calib_prob": 0.8, "samples": 20, "bars_count": 60}],
                },
            )
            settings = {
                "forex_auto_trade_enabled": True,
                "forex_require_data_quality_ok_for_entries": True,
                "forex_require_reject_rate_max_pct": 95.0,
                "forex_block_entries_on_cached_scan": False,
                "forex_cached_scan_hard_block_age_s": 1200,
                "forex_cached_scan_entry_size_mult": 0.5,
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
            self.assertIn("entry placed", str(out.get("msg", "")).lower())
            self.assertEqual(int(out.get("trade_units_entry", 0) or 0), 500)
            self.assertEqual(float(out.get("entry_size_scale", 1.0) or 1.0), 0.5)


if __name__ == "__main__":
    unittest.main()
