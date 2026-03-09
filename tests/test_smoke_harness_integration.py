from __future__ import annotations

import json
import os
import tempfile
import unittest
from unittest.mock import patch

import runtime.smoke_test_all as smoke_test_all


class TestSmokeHarnessIntegration(unittest.TestCase):
    def test_smoke_harness_writes_expected_report(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            hub = os.path.join(td, "hub_data")
            os.makedirs(hub, exist_ok=True)
            settings_path = os.path.join(td, "gui_settings.json")
            with open(settings_path, "w", encoding="utf-8") as f:
                f.write("{}")

            with patch.object(smoke_test_all, "resolve_runtime_paths", return_value=(td, settings_path, hub, {})), patch.object(
                smoke_test_all, "resolve_settings_path", return_value=settings_path
            ), patch.object(smoke_test_all, "read_settings_file", return_value={}), patch.object(
                smoke_test_all, "sanitize_settings", side_effect=lambda x: (x if isinstance(x, dict) else {})
            ), patch.object(
                smoke_test_all, "run_stock_scan", return_value={"state": "READY", "msg": "scan ok"}
            ), patch.object(
                smoke_test_all, "run_stock_step", return_value={"state": "READY", "msg": "step ok"}
            ), patch.object(
                smoke_test_all, "run_forex_scan", return_value={"state": "READY", "msg": "scan ok"}
            ), patch.object(
                smoke_test_all, "run_forex_step", return_value={"state": "READY", "msg": "step ok"}
            ), patch.object(
                smoke_test_all, "run_autopilot_once", return_value={"stable_cycles": 5, "api_unstable": False, "changes": {}}
            ), patch.object(
                smoke_test_all, "run_autofix_once", return_value={"enabled": True, "mode": "report_only", "tickets_created": 0}
            ):
                rc = smoke_test_all.main()
                self.assertEqual(rc, 0)

            report_path = os.path.join(hub, "smoke_test_report.json")
            self.assertTrue(os.path.isfile(report_path))
            with open(report_path, "r", encoding="utf-8") as f:
                report = json.load(f)

            self.assertTrue(bool(report.get("ok", False)))
            self.assertIn("stock_scan", report.get("steps", {}))
            self.assertIn("forex_scan", report.get("steps", {}))
            self.assertIn("autopilot", report.get("steps", {}))
            self.assertIn("autofix", report.get("steps", {}))
            self.assertIn("runtime_state", report.get("files", {}))
            self.assertIn("autofix_status", report.get("files", {}))
            self.assertIn("market_loop_status", report.get("files", {}))
            self.assertIn("scanner_cadence_drift", report.get("files", {}))
            self.assertIn("stock_universe_quality", report.get("files", {}))
            self.assertIn("forex_universe_quality", report.get("files", {}))

    def test_smoke_harness_failure_path_sets_nonzero(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            hub = os.path.join(td, "hub_data")
            os.makedirs(hub, exist_ok=True)
            settings_path = os.path.join(td, "gui_settings.json")
            with open(settings_path, "w", encoding="utf-8") as f:
                f.write("{}")

            with patch.object(smoke_test_all, "resolve_runtime_paths", return_value=(td, settings_path, hub, {})), patch.object(
                smoke_test_all, "resolve_settings_path", return_value=settings_path
            ), patch.object(smoke_test_all, "read_settings_file", return_value={}), patch.object(
                smoke_test_all, "sanitize_settings", side_effect=lambda x: (x if isinstance(x, dict) else {})
            ), patch.object(
                smoke_test_all, "run_stock_scan", return_value={"state": "READY", "msg": "scan ok"}
            ), patch.object(
                smoke_test_all, "run_stock_step", return_value={"state": "READY", "msg": "step ok"}
            ), patch.object(
                smoke_test_all, "run_forex_scan", side_effect=RuntimeError("boom")
            ), patch.object(
                smoke_test_all, "run_forex_step", return_value={"state": "READY", "msg": "step ok"}
            ), patch.object(
                smoke_test_all, "run_autopilot_once", return_value={"stable_cycles": 1, "api_unstable": False, "changes": {}}
            ), patch.object(
                smoke_test_all, "run_autofix_once", return_value={"enabled": True, "mode": "report_only", "tickets_created": 0}
            ):
                rc = smoke_test_all.main()
                self.assertEqual(rc, 1)

            report_path = os.path.join(hub, "smoke_test_report.json")
            with open(report_path, "r", encoding="utf-8") as f:
                report = json.load(f)
            self.assertFalse(bool(report.get("ok", True)))
            self.assertIn("error", dict((report.get("steps", {}) or {}).get("forex_scan", {})))


if __name__ == "__main__":
    unittest.main()
