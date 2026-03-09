from __future__ import annotations

import os
import unittest

from app.status_hydration import load_market_status_bundle


def _fixture(*parts: str) -> str:
    return os.path.join(os.path.dirname(__file__), "fixtures", *parts)


class TestUiStatusHydrationIntegration(unittest.TestCase):
    def test_stocks_hydration_bundle_from_files(self) -> None:
        bundle = load_market_status_bundle(
            status_path=_fixture("trader", "stocks_status.json"),
            trader_path=_fixture("trader", "stocks_trader_status.json"),
            thinker_path=_fixture("scanner", "stocks_thinker_status.json"),
            scan_diag_path=_fixture("scanner", "stocks_scan_diagnostics.json"),
            history_path=_fixture("trader", "stocks_execution_audit.jsonl"),
            history_limit=20,
        )
        self.assertEqual(str(bundle["status"].get("state", "")), "READY")
        self.assertEqual(str(bundle["trader"].get("trader_state", "")), "Paper auto-run")
        self.assertEqual(str((bundle["thinker"].get("top_pick", {}) or {}).get("symbol", "")), "AAPL")
        self.assertEqual(int((bundle["scan_diagnostics"].get("leaders_total", 0) or 0)), 3)
        self.assertEqual(int((bundle["scan_diagnostics"].get("schema_version", 0) or 0)), 2)
        self.assertIn("schema_compat_from", bundle["scan_diagnostics"])
        self.assertGreaterEqual(len(list(bundle.get("history", []) or [])), 2)

    def test_forex_hydration_bundle_from_files(self) -> None:
        bundle = load_market_status_bundle(
            status_path=_fixture("trader", "forex_status.json"),
            trader_path=_fixture("trader", "forex_trader_status.json"),
            thinker_path=_fixture("scanner", "forex_thinker_status.json"),
            scan_diag_path=_fixture("scanner", "forex_scan_diagnostics.json"),
            history_path=_fixture("trader", "forex_execution_audit.jsonl"),
            history_limit=20,
        )
        self.assertEqual(str(bundle["status"].get("state", "")), "READY")
        self.assertEqual(str(bundle["trader"].get("trader_state", "")), "Practice auto-run")
        self.assertEqual(str((bundle["thinker"].get("top_pick", {}) or {}).get("pair", "")), "EUR_USD")
        self.assertEqual(int((bundle["scan_diagnostics"].get("leaders_total", 0) or 0)), 4)
        self.assertGreaterEqual(len(list(bundle.get("history", []) or [])), 2)


if __name__ == "__main__":
    unittest.main()
