from __future__ import annotations

import unittest
from typing import Any, Dict

from engines.forex_thinker import _apply_pair_cooldown
from engines.stock_thinker import _adaptive_feed_order, _apply_symbol_cooldown, _prune_cooldown_map


class TestScannerStability(unittest.TestCase):
    def test_adaptive_feed_order_prefers_healthier_feed(self) -> None:
        health = {
            "feeds": {
                "sip": {"ok_count": 1, "err_count": 4, "avg_bars": 10, "updated_ts": 1_700_000_000},
                "iex": {"ok_count": 5, "err_count": 1, "avg_bars": 60, "updated_ts": 1_700_000_100},
            }
        }
        out = _adaptive_feed_order(["sip", "iex"], health)
        self.assertEqual(out[0], "iex")

    def test_stock_cooldown_triggers_after_min_hits(self) -> None:
        settings = {
            "stock_symbol_cooldown_minutes": 30,
            "stock_symbol_cooldown_min_hits": 2,
            "stock_symbol_cooldown_reject_reasons": "data_quality,spread",
        }
        now = 1_700_000_000
        m: Dict[str, Dict[str, Any]] = {}
        _apply_symbol_cooldown(m, "AAPL", "data_quality", settings, now)
        self.assertIn("AAPL", m)
        self.assertEqual(int(m["AAPL"]["until"]), 0)
        _apply_symbol_cooldown(m, "AAPL", "data_quality", settings, now + 1)
        self.assertGreater(int(m["AAPL"]["until"]), now)

    def test_forex_pair_cooldown_triggers_after_min_hits(self) -> None:
        settings = {
            "forex_pair_cooldown_minutes": 20,
            "forex_pair_cooldown_min_hits": 2,
            "forex_pair_cooldown_reject_reasons": "spread,low_volatility",
        }
        now = 1_700_000_000
        m: Dict[str, Dict[str, Any]] = {}
        _apply_pair_cooldown(m, "EUR_USD", "spread", settings, now)
        self.assertEqual(int(m["EUR_USD"]["until"]), 0)
        _apply_pair_cooldown(m, "EUR_USD", "spread", settings, now + 1)
        self.assertGreater(int(m["EUR_USD"]["until"]), now)

    def test_prune_cooldown_keeps_active(self) -> None:
        now = 1_700_000_000
        m = {
            "AAPL": {"until": now + 600, "updated_ts": now},
            "MSFT": {"until": now - 1, "updated_ts": now - 10_000},
        }
        out = _prune_cooldown_map(m, now)
        self.assertIn("AAPL", out)
        self.assertNotIn("MSFT", out)


if __name__ == "__main__":
    unittest.main()
