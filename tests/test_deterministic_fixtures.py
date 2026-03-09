from __future__ import annotations

import os
import tempfile
import unittest
from datetime import datetime
from unittest.mock import patch

import runtime.pt_markets as pt_markets
from app.scanner_quality import turnover_pct
from app.status_hydration import safe_read_json_dict, safe_read_jsonl_dicts
from engines.forex_thinker import _session_weight_multiplier
from engines.stock_thinker import _stock_scan_window_policy


def _fixture(*parts: str) -> str:
    return os.path.join(os.path.dirname(__file__), "fixtures", *parts)


class TestDeterministicFixtures(unittest.TestCase):
    def test_scanner_fixture_values_are_stable(self) -> None:
        stocks_diag = safe_read_json_dict(_fixture("scanner", "stocks_scan_diagnostics.json"))
        forex_diag = safe_read_json_dict(_fixture("scanner", "forex_scan_diagnostics.json"))
        self.assertEqual(str(stocks_diag.get("state", "")), "READY")
        self.assertEqual(str(forex_diag.get("state", "")), "READY")
        self.assertEqual(int(stocks_diag.get("leaders_total", 0) or 0), 3)
        self.assertEqual(int(forex_diag.get("leaders_total", 0) or 0), 4)

        stocks_thinker = safe_read_json_dict(_fixture("scanner", "stocks_thinker_status.json"))
        forex_thinker = safe_read_json_dict(_fixture("scanner", "forex_thinker_status.json"))
        self.assertEqual(str((stocks_thinker.get("top_pick", {}) or {}).get("symbol", "")), "AAPL")
        self.assertEqual(str((forex_thinker.get("top_pick", {}) or {}).get("pair", "")), "EUR_USD")
        self.assertGreaterEqual(len(list(stocks_thinker.get("top_chart", []) or [])), 3)
        self.assertGreaterEqual(len(list(forex_thinker.get("top_chart", []) or [])), 3)

    def test_trader_fixture_values_are_stable(self) -> None:
        stocks_status = safe_read_json_dict(_fixture("trader", "stocks_status.json"))
        stocks_trader = safe_read_json_dict(_fixture("trader", "stocks_trader_status.json"))
        forex_status = safe_read_json_dict(_fixture("trader", "forex_status.json"))
        forex_trader = safe_read_json_dict(_fixture("trader", "forex_trader_status.json"))

        self.assertEqual(str(stocks_status.get("state", "")), "READY")
        self.assertEqual(str(stocks_trader.get("trader_state", "")), "Paper auto-run")
        self.assertEqual(str(forex_status.get("state", "")), "READY")
        self.assertEqual(str(forex_trader.get("trader_state", "")), "Practice auto-run")

        stocks_hist = safe_read_jsonl_dicts(_fixture("trader", "stocks_execution_audit.jsonl"), limit=10)
        forex_hist = safe_read_jsonl_dicts(_fixture("trader", "forex_execution_audit.jsonl"), limit=10)
        self.assertEqual(len(stocks_hist), 3)
        self.assertEqual(len(forex_hist), 3)
        self.assertEqual(str(stocks_hist[0].get("symbol", "")), "AAPL")
        self.assertEqual(str(forex_hist[0].get("instrument", "")), "EUR_USD")

    def test_churn_fixture_comparisons_are_stable(self) -> None:
        payload = safe_read_json_dict(_fixture("scanner", "churn_cases.json"))
        for row in list(payload.get("cases", []) or []):
            if not isinstance(row, dict):
                continue
            prev_ids = list(row.get("prev", []) or [])
            cur_ids = list(row.get("cur", []) or [])
            expected = float(row.get("expected_pct", 0.0) or 0.0)
            got = turnover_pct(prev_ids, cur_ids)
            self.assertAlmostEqual(got, expected, places=2)

    def test_cadence_transition_fixture_is_stable(self) -> None:
        payload = safe_read_json_dict(_fixture("scanner", "cadence_transitions.json"))
        market = str(payload.get("market", "stocks") or "stocks")
        expected_interval_s = float(payload.get("expected_interval_s", 10.0) or 10.0)
        settings = payload.get("settings", {}) if isinstance(payload.get("settings", {}), dict) else {}
        ts_rows = [int(x) for x in list(payload.get("timestamps", []) or [])]
        exp_levels = list(payload.get("expected_levels", []) or [])
        exp_late = [float(x) for x in list(payload.get("expected_late_pct", []) or [])]

        with tempfile.TemporaryDirectory() as td:
            cadence_path = os.path.join(td, "scanner_cadence_drift.json")
            with patch.object(pt_markets, "CADENCE_DRIFT_PATH", cadence_path), patch.object(pt_markets, "_incident"):
                out_rows = [
                    pt_markets._update_scan_cadence_drift(market, ts_val, expected_interval_s, settings, "READY")
                    for ts_val in ts_rows
                ]
        self.assertEqual(len(out_rows), len(exp_levels))
        for idx, row in enumerate(out_rows):
            self.assertEqual(str(row.get("level", "")), str(exp_levels[idx]))
            self.assertAlmostEqual(float(row.get("late_pct", 0.0) or 0.0), float(exp_late[idx]), places=3)

    def test_stock_window_policy_fixture_is_stable(self) -> None:
        payload = safe_read_json_dict(_fixture("scanner", "stock_window_policy_cases.json"))
        for row in list(payload.get("cases", []) or []):
            if not isinstance(row, dict):
                continue
            settings = row.get("settings", {}) if isinstance(row.get("settings", {}), dict) else {}
            expected = row.get("expected", {}) if isinstance(row.get("expected", {}), dict) else {}
            dt = datetime.fromisoformat(str(row.get("timestamp_iso", "") or "2026-03-02T12:00:00-05:00"))
            out = _stock_scan_window_policy(settings, now_et=dt)
            self.assertEqual(bool(out.get("active", False)), bool(expected.get("active", False)))
            self.assertEqual(str(out.get("window", "")), str(expected.get("window", "")))
            self.assertAlmostEqual(
                float(out.get("score_mult", 0.0) or 0.0),
                float(expected.get("score_mult", 0.0) or 0.0),
                places=3,
            )

    def test_forex_session_weight_fixture_is_stable(self) -> None:
        payload = safe_read_json_dict(_fixture("scanner", "forex_session_weight_cases.json"))
        for row in list(payload.get("cases", []) or []):
            if not isinstance(row, dict):
                continue
            settings = row.get("settings", {}) if isinstance(row.get("settings", {}), dict) else {}
            side = str(row.get("side", "") or "")
            session_ctx = row.get("session_context", {}) if isinstance(row.get("session_context", {}), dict) else {}
            mult, mode = _session_weight_multiplier(settings, side, session_ctx)
            self.assertAlmostEqual(float(mult), float(row.get("expected_mult", 0.0) or 0.0), places=3)
            self.assertEqual(str(mode), str(row.get("expected_mode", "") or ""))


if __name__ == "__main__":
    unittest.main()
