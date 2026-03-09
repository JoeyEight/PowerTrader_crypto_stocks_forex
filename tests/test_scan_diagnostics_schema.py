from __future__ import annotations

import unittest

from app.scan_diagnostics_schema import (
    SCAN_DIAGNOSTICS_SCHEMA_VERSION,
    normalize_scan_diagnostics,
    with_scan_schema,
)


class TestScanDiagnosticsSchema(unittest.TestCase):
    def test_normalize_backfills_legacy_payload(self) -> None:
        legacy = {
            "state": "ready",
            "leaders_total": "2",
            "reject_summary": {"counts": {"spread": "3"}},
            "candidate_symbols": ["aapl", "AAPL", "msft"],
        }
        out = normalize_scan_diagnostics(legacy, market="stocks")
        self.assertEqual(int(out.get("schema_version", 0) or 0), SCAN_DIAGNOSTICS_SCHEMA_VERSION)
        self.assertEqual(int(out.get("schema_compat_from", 0) or 0), 1)
        self.assertEqual(str(out.get("market", "")), "stocks")
        self.assertEqual(str(out.get("state", "")), "READY")
        self.assertEqual(int(out.get("leaders_total", 0) or 0), 2)
        self.assertEqual(list(out.get("candidate_symbols", []) or []), ["AAPL", "MSFT"])

    def test_with_scan_schema_emits_current_version(self) -> None:
        out = with_scan_schema({"state": "READY", "msg": "ok"}, market="forex")
        self.assertEqual(int(out.get("schema_version", 0) or 0), SCAN_DIAGNOSTICS_SCHEMA_VERSION)
        self.assertNotIn("schema_compat_from", out)
        self.assertEqual(str(out.get("market", "")), "forex")


if __name__ == "__main__":
    unittest.main()

