from __future__ import annotations

import json
import os
import tempfile
import unittest
from unittest.mock import patch

import runtime.tools.check_pass3_artifacts as check_pass3


class TestCheckPass3Artifacts(unittest.TestCase):
    def test_main_returns_ok_with_required_fields(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            hub = os.path.join(td, "hub_data")
            os.makedirs(os.path.join(hub, "stocks"), exist_ok=True)
            os.makedirs(os.path.join(hub, "forex"), exist_ok=True)
            with open(os.path.join(hub, "stocks", "scan_diagnostics.json"), "w", encoding="utf-8") as f:
                json.dump({"schema_version": 2, "candidate_churn_pct": 10.0, "leader_churn_pct": 5.0}, f)
            with open(os.path.join(hub, "forex", "scan_diagnostics.json"), "w", encoding="utf-8") as f:
                json.dump({"schema_version": 2, "candidate_churn_pct": 11.0, "leader_churn_pct": 6.0}, f)
            with open(os.path.join(hub, "stocks", "universe_quality.json"), "w", encoding="utf-8") as f:
                json.dump({"summary": "ok"}, f)
            with open(os.path.join(hub, "forex", "universe_quality.json"), "w", encoding="utf-8") as f:
                json.dump({"summary": "ok"}, f)
            with open(os.path.join(hub, "scanner_cadence_drift.json"), "w", encoding="utf-8") as f:
                json.dump({"markets": {}, "active": []}, f)
            with open(os.path.join(hub, "runtime_state.json"), "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "scan_cadence": {},
                        "broker_backoff": {},
                        "alerts": {"metrics": {"market_loop_age_s": 0}},
                    },
                    f,
                )
            with open(os.path.join(hub, "market_trends.json"), "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "stocks": {"quality_aggregates": {}, "cadence_aggregates": {}},
                        "forex": {"quality_aggregates": {}, "cadence_aggregates": {}},
                    },
                    f,
                )
            with patch.object(check_pass3, "resolve_runtime_paths", return_value=(td, "", hub, {})):
                rc = check_pass3.main()
            self.assertEqual(rc, 0)

    def test_main_returns_nonzero_for_legacy_schema(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            hub = os.path.join(td, "hub_data")
            os.makedirs(os.path.join(hub, "stocks"), exist_ok=True)
            os.makedirs(os.path.join(hub, "forex"), exist_ok=True)
            with open(os.path.join(hub, "stocks", "scan_diagnostics.json"), "w", encoding="utf-8") as f:
                json.dump({"schema_version": 1, "candidate_churn_pct": 0.0, "leader_churn_pct": 0.0}, f)
            with open(os.path.join(hub, "forex", "scan_diagnostics.json"), "w", encoding="utf-8") as f:
                json.dump({"schema_version": 2, "candidate_churn_pct": 0.0, "leader_churn_pct": 0.0}, f)
            with open(os.path.join(hub, "stocks", "universe_quality.json"), "w", encoding="utf-8") as f:
                json.dump({"summary": "ok"}, f)
            with open(os.path.join(hub, "forex", "universe_quality.json"), "w", encoding="utf-8") as f:
                json.dump({"summary": "ok"}, f)
            with open(os.path.join(hub, "scanner_cadence_drift.json"), "w", encoding="utf-8") as f:
                json.dump({"markets": {}, "active": []}, f)
            with open(os.path.join(hub, "runtime_state.json"), "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "scan_cadence": {},
                        "broker_backoff": {},
                        "alerts": {"metrics": {"market_loop_age_s": 0}},
                    },
                    f,
                )
            with open(os.path.join(hub, "market_trends.json"), "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "stocks": {"quality_aggregates": {}, "cadence_aggregates": {}},
                        "forex": {"quality_aggregates": {}, "cadence_aggregates": {}},
                    },
                    f,
                )
            with patch.object(check_pass3, "resolve_runtime_paths", return_value=(td, "", hub, {})):
                rc = check_pass3.main()
            self.assertEqual(rc, 1)


if __name__ == "__main__":
    unittest.main()
