from __future__ import annotations

import os
import tempfile
import unittest
from unittest.mock import patch

import runtime.pt_markets as pt_markets


class TestMarketE2EModes(unittest.TestCase):
    def test_stocks_paper_mode_cycle(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            settings = {
                "market_bg_stocks_interval_s": 15.0,
                "market_fallback_scan_max_age_s": 300.0,
                "alpaca_paper_mode": True,
            }
            with patch.object(pt_markets, "HUB_DATA_DIR", td), patch.object(pt_markets, "EXEC_GUARD_PATH", os.path.join(td, "broker_execution_guard.json")), patch.object(
                pt_markets, "SCAN_DRIFT_PATH", os.path.join(td, "scan_drift_alerts.json")
            ), patch.object(
                pt_markets, "CADENCE_DRIFT_PATH", os.path.join(td, "scanner_cadence_drift.json")
            ), patch.object(
                pt_markets, "_incident", return_value=None
            ), patch.object(
                pt_markets, "_update_sla_metrics", return_value=None
            ), patch.object(
                pt_markets, "_record_guard_result", return_value={"active": False}
            ), patch.object(
                pt_markets, "market_guard_status", return_value={"active": False}
            ), patch.object(
                pt_markets, "run_stock_scan", return_value={"state": "READY", "reject_summary": {"reject_rate_pct": 12.0}}
            ), patch.object(
                pt_markets, "run_stock_trader_step", return_value={"state": "READY", "msg": "ok"}
            ):
                out = pt_markets._run_stocks(settings)
            self.assertTrue(bool(out.get("scan_ok", False)))
            self.assertTrue(bool(out.get("step_ok", False)))
            self.assertEqual(str(out.get("scan_state", "")), "READY")

    def test_forex_practice_mode_cycle(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            settings = {
                "market_bg_forex_interval_s": 10.0,
                "market_fallback_scan_max_age_s": 300.0,
                "oanda_practice_mode": True,
            }
            with patch.object(pt_markets, "HUB_DATA_DIR", td), patch.object(pt_markets, "EXEC_GUARD_PATH", os.path.join(td, "broker_execution_guard.json")), patch.object(
                pt_markets, "SCAN_DRIFT_PATH", os.path.join(td, "scan_drift_alerts.json")
            ), patch.object(
                pt_markets, "CADENCE_DRIFT_PATH", os.path.join(td, "scanner_cadence_drift.json")
            ), patch.object(
                pt_markets, "_incident", return_value=None
            ), patch.object(
                pt_markets, "_update_sla_metrics", return_value=None
            ), patch.object(
                pt_markets, "_record_guard_result", return_value={"active": False}
            ), patch.object(
                pt_markets, "market_guard_status", return_value={"active": False}
            ), patch.object(
                pt_markets, "run_forex_scan", return_value={"state": "READY", "reject_summary": {"reject_rate_pct": 14.0}}
            ), patch.object(
                pt_markets, "run_forex_trader_step", return_value={"state": "READY", "msg": "ok"}
            ):
                out = pt_markets._run_forex(settings)
            self.assertTrue(bool(out.get("scan_ok", False)))
            self.assertTrue(bool(out.get("step_ok", False)))
            self.assertEqual(str(out.get("scan_state", "")), "READY")


if __name__ == "__main__":
    unittest.main()
