from __future__ import annotations

import json
import os
import tempfile
import unittest
import urllib.error
from unittest.mock import patch

from engines import forex_thinker


class _FakeOandaClient:
    def __init__(self, account_id: str, api_token: str, rest_url: str) -> None:
        self._account_id = account_id
        self._api_token = api_token
        self._rest_url = rest_url

    def list_tradeable_instruments(self) -> list[str]:
        return ["EUR_USD", "USD_JPY"]

    def get_pricing_details(self, universe: list[str]) -> dict[str, dict[str, float]]:
        out: dict[str, dict[str, float]] = {}
        for pair in universe:
            out[str(pair)] = {"spread_bps": 1.1}
        return out

    def get_candles(self, pair: str, granularity: str = "H4", count: int = 40) -> list[dict]:
        return []


class TestForexThinkerFallback(unittest.TestCase):
    def test_uses_cached_scan_when_network_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            forex_dir = os.path.join(td, "forex")
            os.makedirs(forex_dir, exist_ok=True)
            cached = {
                "state": "READY",
                "ai_state": "Scan ready",
                "msg": "cached baseline",
                "universe": ["EUR_USD", "USD_JPY"],
                "leaders": [{"pair": "EUR_USD", "side": "long", "score": 0.42, "reason": "trend"}],
                "all_scores": [{"pair": "EUR_USD", "side": "long", "score": 0.42, "reason": "trend"}],
                "top_pick": {"pair": "EUR_USD", "side": "long", "score": 0.42, "reason": "trend"},
                "top_chart": [{"t": "t1", "o": 1.1, "h": 1.2, "l": 1.0, "c": 1.15}],
                "top_chart_map": {"EUR_USD": [{"t": "t1", "o": 1.1, "h": 1.2, "l": 1.0, "c": 1.15}]},
                "updated_at": 1000,
                "reject_summary": {"reject_rate_pct": 5.0, "dominant_reason": "spread"},
            }
            with open(os.path.join(forex_dir, "forex_thinker_status.json"), "w", encoding="utf-8") as f:
                json.dump(cached, f)

            settings = {
                "oanda_account_id": "abc",
                "oanda_api_token": "xyz",
                "oanda_rest_url": "https://api-fxpractice.oanda.com",
                "forex_scan_max_pairs": 4,
            }

            with (
                patch.object(forex_thinker, "OandaBrokerClient", _FakeOandaClient),
                patch.object(forex_thinker, "_request_json", side_effect=urllib.error.URLError("dns down")),
                patch("engines.forex_thinker.time.time", return_value=1300),
            ):
                out = forex_thinker.run_scan(settings, td)

            self.assertEqual(str(out.get("state", "")), "READY")
            self.assertTrue(bool(out.get("fallback_cached", False)))
            self.assertIn("cached scan", str(out.get("msg", "")).lower())
            self.assertGreaterEqual(len(list(out.get("leaders", []) or [])), 1)

    def test_no_cache_keeps_error_state(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            os.makedirs(os.path.join(td, "forex"), exist_ok=True)
            settings = {
                "oanda_account_id": "abc",
                "oanda_api_token": "xyz",
                "oanda_rest_url": "https://api-fxpractice.oanda.com",
                "forex_scan_max_pairs": 4,
            }
            with (
                patch.object(forex_thinker, "OandaBrokerClient", _FakeOandaClient),
                patch.object(forex_thinker, "_request_json", side_effect=urllib.error.URLError("dns down")),
                patch("engines.forex_thinker.time.time", return_value=1300),
            ):
                out = forex_thinker.run_scan(settings, td)
            self.assertEqual(str(out.get("state", "")), "ERROR")
            self.assertEqual(str(out.get("ai_state", "")), "Network error")

    def test_applies_leader_hysteresis_to_previous_top_pair(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            fx_dir = os.path.join(td, "forex")
            os.makedirs(fx_dir, exist_ok=True)
            with open(os.path.join(fx_dir, "forex_thinker_status.json"), "w", encoding="utf-8") as f:
                json.dump({"top_pick": {"pair": "USD_JPY", "side": "long", "score": 0.58}}, f)

            settings = {
                "oanda_account_id": "abc",
                "oanda_api_token": "xyz",
                "oanda_rest_url": "https://api-fxpractice.oanda.com",
                "forex_scan_max_pairs": 4,
                "forex_leader_stability_margin_pct": 10.0,
                "forex_max_stale_hours": 10000.0,
            }

            candles = [
                {"complete": True, "time": f"2026-03-01T{(i % 24):02d}:00:00.000000000Z", "mid": {"o": "1.1000", "h": "1.1100", "l": "1.0900", "c": "1.1050"}, "volume": 1000}
                for i in range(48)
            ]

            def _score(pair: str, rows: list[dict], spread_bps: float = 0.0) -> dict:
                base = 0.58 if str(pair).upper() == "USD_JPY" else 0.62
                return {
                    "pair": str(pair).upper(),
                    "score": float(base),
                    "side": "long",
                    "last": 1.2345,
                    "change_6h_pct": 0.2,
                    "change_24h_pct": 0.4,
                    "volatility_pct": 0.05,
                    "spread_bps": float(spread_bps),
                    "confidence": "MED",
                    "reason": "test",
                }

            with (
                patch.object(forex_thinker, "get_oanda_creds", return_value=("abc", "xyz")),
                patch.object(forex_thinker, "OandaBrokerClient", _FakeOandaClient),
                patch.object(forex_thinker, "_request_json", return_value={"candles": candles}),
                patch.object(forex_thinker, "_score_candles", side_effect=_score),
            ):
                out = forex_thinker.run_scan(settings, td)
            self.assertEqual(str(out.get("state", "")), "READY")
            top = out.get("top_pick", {}) if isinstance(out.get("top_pick", {}), dict) else {}
            self.assertEqual(str(top.get("pair", "")), "USD_JPY")
            self.assertTrue(bool(out.get("leader_stability_applied", False)))


if __name__ == "__main__":
    unittest.main()
