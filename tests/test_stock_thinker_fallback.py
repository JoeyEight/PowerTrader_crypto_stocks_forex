from __future__ import annotations

import json
import os
import tempfile
import unittest
from unittest.mock import patch

from engines import stock_thinker


def _mk_bar(idx: int, close_px: float) -> dict:
    hh = idx % 24
    day = 1 + (idx % 28)
    ts = f"2026-03-{day:02d}T{hh:02d}:00:00Z"
    c = float(close_px)
    o = c * 1.002
    h = max(o, c) * 1.001
    low_px = min(o, c) * 0.999
    return {"t": ts, "o": o, "h": h, "l": low_px, "c": c, "v": 1000 + idx}


class _FakeAlpacaClient:
    def __init__(self, api_key_id: str, secret_key: str, base_url: str, data_url: str) -> None:
        self.api_key_id = api_key_id
        self.secret_key = secret_key
        self.base_url = base_url
        self.data_url = data_url

    def get_snapshot_details(self, universe: list[str], feed: str = "iex") -> dict[str, dict[str, float]]:
        return {str(sym).strip().upper(): {"mid": 100.0, "spread_bps": 2.0, "dollar_vol": 15_000_000.0} for sym in universe}

    def get_stock_bars(
        self,
        symbol: str,
        timeframe: str = "1Day",
        limit: int = 120,
        feed: str = "iex",
        start_iso: str | None = None,
        end_iso: str | None = None,
    ) -> list[dict]:
        base = 200.0
        out: list[dict] = []
        # Descending closes -> negative score => side watch.
        for i in range(max(24, int(limit or 48))):
            out.append(_mk_bar(i, base - (i * 0.4)))
        return out


class TestStockThinkerFallback(unittest.TestCase):
    def test_uses_cached_scan_when_universe_selection_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            stocks_dir = os.path.join(td, "stocks")
            os.makedirs(stocks_dir, exist_ok=True)
            cached = {
                "state": "READY",
                "ai_state": "Scan ready",
                "msg": "cached baseline",
                "universe": ["AAPL"],
                "leaders": [{"symbol": "AAPL", "side": "watch", "score": -0.32, "reason": "trend"}],
                "all_scores": [{"symbol": "AAPL", "side": "watch", "score": -0.32, "reason": "trend"}],
                "top_pick": {"symbol": "AAPL", "side": "watch", "score": -0.32, "reason": "trend"},
                "top_chart": [{"t": "t1", "o": 100.0, "h": 101.0, "l": 99.0, "c": 100.5}],
                "top_chart_map": {"AAPL": [{"t": "t1", "o": 100.0, "h": 101.0, "l": 99.0, "c": 100.5}]},
                "updated_at": 1000,
                "reject_summary": {"reject_rate_pct": 8.0, "dominant_reason": "spread"},
            }
            with open(os.path.join(stocks_dir, "stock_thinker_status.json"), "w", encoding="utf-8") as f:
                json.dump(cached, f)

            settings = {"alpaca_api_key_id": "abc", "alpaca_secret_key": "xyz"}
            with (
                patch.object(stock_thinker, "get_alpaca_creds", return_value=("abc", "xyz")),
                patch.object(stock_thinker, "_select_universe", side_effect=RuntimeError("boom")),
                patch.object(stock_thinker, "_market_open_now", return_value=True),
                patch("engines.stock_thinker.time.time", return_value=1300),
            ):
                out = stock_thinker.run_scan(settings, td)
            self.assertEqual(str(out.get("state", "")), "READY")
            self.assertTrue(bool(out.get("fallback_cached", False)))
            self.assertIn("cached scan", str(out.get("msg", "")).lower())
            self.assertGreaterEqual(len(list(out.get("leaders", []) or [])), 1)

    def test_publishes_watch_leaders_when_no_longs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            os.makedirs(os.path.join(td, "stocks"), exist_ok=True)
            settings = {
                "alpaca_api_key_id": "abc",
                "alpaca_secret_key": "xyz",
                "stock_scan_publish_watch_leaders": True,
                "stock_scan_watch_leaders_count": 4,
                "stock_scan_max_symbols": 20,
                "stock_scan_use_daily_when_closed": True,
            }
            with (
                patch.object(stock_thinker, "get_alpaca_creds", return_value=("abc", "xyz")),
                patch.object(stock_thinker, "AlpacaBrokerClient", _FakeAlpacaClient),
                patch.object(stock_thinker, "_select_universe", return_value=["AAPL"]),
                patch.object(stock_thinker, "_market_open_now", return_value=False),
            ):
                out = stock_thinker.run_scan(settings, td)
            self.assertEqual(str(out.get("state", "")), "READY")
            self.assertEqual(str(out.get("leader_mode", "")), "watch_fallback")
            self.assertGreaterEqual(len(list(out.get("leaders", []) or [])), 1)
            top = out.get("top_pick", {}) if isinstance(out.get("top_pick", {}), dict) else {}
            self.assertEqual(str(top.get("symbol", "")), "AAPL")
            self.assertEqual(str(top.get("side", "")).lower(), "watch")
            self.assertTrue(bool(str(top.get("reason_logic", "") or "").strip()))
            self.assertTrue(bool(str(top.get("reason_data", "") or "").strip()))
            self.assertNotIn("6h", str(top.get("reason", "") or "").lower())

    def test_applies_leader_hysteresis_to_previous_top(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            stocks_dir = os.path.join(td, "stocks")
            os.makedirs(stocks_dir, exist_ok=True)
            with open(os.path.join(stocks_dir, "stock_thinker_status.json"), "w", encoding="utf-8") as f:
                json.dump({"top_pick": {"symbol": "MSFT", "side": "long", "score": 1.12}}, f)

            settings = {
                "alpaca_api_key_id": "abc",
                "alpaca_secret_key": "xyz",
                "stock_scan_max_symbols": 20,
                "stock_leader_stability_margin_pct": 20.0,
                "stock_scan_use_daily_when_closed": True,
            }

            def _score(symbol: str, bars: list[dict], spread_bps: float = 0.0) -> dict:
                base = 1.10 if str(symbol).upper() == "MSFT" else 1.20
                return {
                    "symbol": str(symbol).upper(),
                    "score": float(base),
                    "side": "long",
                    "last": 100.0,
                    "change_6h_pct": 1.0,
                    "change_24h_pct": 2.0,
                    "volatility_pct": 0.5,
                    "spread_bps": float(spread_bps),
                    "confidence": "MED",
                    "reason": "test",
                }

            with (
                patch.object(stock_thinker, "get_alpaca_creds", return_value=("abc", "xyz")),
                patch.object(stock_thinker, "AlpacaBrokerClient", _FakeAlpacaClient),
                patch.object(stock_thinker, "_select_universe", return_value=["AAPL", "MSFT"]),
                patch.object(stock_thinker, "_market_open_now", return_value=False),
                patch.object(stock_thinker, "_score_bars", side_effect=_score),
            ):
                out = stock_thinker.run_scan(settings, td)
            top = out.get("top_pick", {}) if isinstance(out.get("top_pick", {}), dict) else {}
            self.assertEqual(str(top.get("symbol", "")), "MSFT")
            self.assertTrue(bool(out.get("leader_stability_applied", False)))


if __name__ == "__main__":
    unittest.main()
