from __future__ import annotations

import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from engines.stock_thinker import _stock_scan_window_policy


class TestStockScannerWindowPolicy(unittest.TestCase):
    def test_opening_window_active(self) -> None:
        now_et = datetime(2026, 3, 4, 9, 35, tzinfo=ZoneInfo("America/New_York"))
        out = _stock_scan_window_policy(
            {
                "stock_scan_open_cooldown_minutes": 15,
                "stock_scan_close_cooldown_minutes": 15,
                "stock_scan_open_score_mult": 0.8,
                "stock_scan_close_score_mult": 0.9,
            },
            now_et=now_et,
        )
        self.assertTrue(bool(out.get("active", False)))
        self.assertEqual(str(out.get("window", "")), "OPENING")
        self.assertAlmostEqual(float(out.get("score_mult", 0.0)), 0.8, places=6)

    def test_closing_window_active(self) -> None:
        now_et = datetime(2026, 3, 4, 15, 50, tzinfo=ZoneInfo("America/New_York"))
        out = _stock_scan_window_policy(
            {
                "stock_scan_open_cooldown_minutes": 15,
                "stock_scan_close_cooldown_minutes": 15,
                "stock_scan_open_score_mult": 0.8,
                "stock_scan_close_score_mult": 0.9,
            },
            now_et=now_et,
        )
        self.assertTrue(bool(out.get("active", False)))
        self.assertEqual(str(out.get("window", "")), "CLOSING")
        self.assertAlmostEqual(float(out.get("score_mult", 0.0)), 0.9, places=6)


if __name__ == "__main__":
    unittest.main()
