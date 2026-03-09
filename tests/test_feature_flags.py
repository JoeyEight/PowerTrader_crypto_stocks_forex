from __future__ import annotations

import unittest

from app.feature_flags import build_feature_flag_snapshot


class TestFeatureFlags(unittest.TestCase):
    def test_snapshot_counts_and_rows(self) -> None:
        settings = {
            "paper_only_unless_checklist_green": True,
            "stock_block_entries_on_cached_scan": False,
            "forex_block_entries_on_cached_scan": True,
            "stock_require_data_quality_ok_for_entries": True,
            "forex_require_data_quality_ok_for_entries": False,
            "autofix_allow_live_apply": False,
            "stock_auto_trade_enabled": True,
            "forex_auto_trade_enabled": False,
        }
        out = build_feature_flag_snapshot(settings)
        self.assertIn("flags", out)
        self.assertGreaterEqual(int(out.get("total_count", 0) or 0), 8)
        rows = list(out.get("flags", []) or [])
        self.assertEqual(int(out.get("enabled_count", 0) or 0), sum(1 for row in rows if bool(row.get("enabled", False))))

        by_id = {str(row.get("id", "") or ""): row for row in rows if isinstance(row, dict)}
        self.assertTrue(bool(by_id.get("paper_only_guard", {}).get("enabled", False)))
        self.assertFalse(bool(by_id.get("stock_cached_scan_block", {}).get("enabled", True)))
        self.assertTrue(bool(by_id.get("stock_auto_trade", {}).get("enabled", False)))


if __name__ == "__main__":
    unittest.main()
