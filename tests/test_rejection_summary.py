from __future__ import annotations

import unittest

from engines.forex_thinker import _summarize_rejections as forex_summarize
from engines.stock_thinker import _summarize_rejections as stock_summarize


class TestRejectionSummary(unittest.TestCase):
    def test_stock_unique_symbol_summary(self) -> None:
        rejected = [
            {"symbol": "AAPL", "reason": "warmup_pending"},
            {"symbol": "AAPL", "reason": "data_quality"},
            {"symbol": "MSFT", "reason": "warmup_pending"},
        ]
        out = stock_summarize(rejected, universe_size=2)
        self.assertEqual(out["total_rejected"], 2)
        self.assertEqual(out["total_rejected_events"], 3)
        self.assertAlmostEqual(float(out["reject_rate_pct"]), 100.0, places=3)
        self.assertIn("counts", out)
        self.assertEqual(int(out["counts"].get("data_quality", 0)), 1)
        self.assertEqual(int(out["counts"].get("warmup_pending", 0)), 1)

    def test_forex_unique_pair_summary(self) -> None:
        rejected = [
            {"pair": "EUR_USD", "reason": "spread"},
            {"pair": "EUR_USD", "reason": "data_quality"},
            {"pair": "USD_JPY", "reason": "low_volatility"},
            {"pair": "USD_JPY", "reason": "spread"},
        ]
        out = forex_summarize(rejected, universe_size=4)
        self.assertEqual(out["total_rejected"], 2)
        self.assertEqual(out["total_rejected_events"], 4)
        self.assertAlmostEqual(float(out["reject_rate_pct"]), 50.0, places=3)
        self.assertIn("counts", out)
        # Higher-priority reason should win per pair.
        self.assertEqual(int(out["counts"].get("data_quality", 0)), 1)
        self.assertEqual(int(out["counts"].get("spread", 0)), 1)


if __name__ == "__main__":
    unittest.main()
