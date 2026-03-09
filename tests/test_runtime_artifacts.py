from __future__ import annotations

import json
import os
import tempfile
import unittest

from app.runtime_artifacts import bootstrap_runtime_artifacts


class TestRuntimeArtifactsBootstrap(unittest.TestCase):
    def test_bootstrap_upgrades_legacy_scan_diagnostics(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            stocks_dir = os.path.join(td, "stocks")
            forex_dir = os.path.join(td, "forex")
            os.makedirs(stocks_dir, exist_ok=True)
            os.makedirs(forex_dir, exist_ok=True)
            with open(os.path.join(stocks_dir, "scan_diagnostics.json"), "w", encoding="utf-8") as f:
                json.dump({"state": "READY", "leaders_total": 2}, f)
            out = bootstrap_runtime_artifacts(td)
            self.assertGreaterEqual(int(out.get("updated", 0) or 0), 1)

            with open(os.path.join(stocks_dir, "scan_diagnostics.json"), "r", encoding="utf-8") as f:
                stocks_diag = json.load(f)
            self.assertEqual(int(stocks_diag.get("schema_version", 0) or 0), 2)
            self.assertIn("candidate_churn_pct", stocks_diag)
            self.assertIn("leader_churn_pct", stocks_diag)

            self.assertTrue(os.path.isfile(os.path.join(stocks_dir, "universe_quality.json")))
            self.assertTrue(os.path.isfile(os.path.join(forex_dir, "universe_quality.json")))
            self.assertTrue(os.path.isfile(os.path.join(td, "scanner_cadence_drift.json")))
            self.assertTrue(os.path.isfile(os.path.join(td, "market_trends.json")))

    def test_bootstrap_patches_runtime_state_alert_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            os.makedirs(os.path.join(td, "stocks"), exist_ok=True)
            os.makedirs(os.path.join(td, "forex"), exist_ok=True)
            rt_path = os.path.join(td, "runtime_state.json")
            with open(rt_path, "w", encoding="utf-8") as f:
                json.dump({"market_loop": {"age_s": 44}, "alerts": {"metrics": {}}}, f)
            bootstrap_runtime_artifacts(td)
            with open(rt_path, "r", encoding="utf-8") as f:
                rt = json.load(f)
            metrics = ((rt.get("alerts", {}) if isinstance(rt.get("alerts", {}), dict) else {}).get("metrics", {}))
            self.assertEqual(int((metrics.get("market_loop_age_s", -1) or -1)), 44)


if __name__ == "__main__":
    unittest.main()

