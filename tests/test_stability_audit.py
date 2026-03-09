from __future__ import annotations

import json
import os
import tempfile
import unittest
from unittest.mock import patch

from runtime.tools.stability_audit import build_stability_report


class TestStabilityAudit(unittest.TestCase):
    def _write_json(self, path: str, payload: dict) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f)

    def _write_jsonl(self, path: str, rows: list[dict]) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")

    def _write_lines(self, path: str, rows: list[str]) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(str(row) + "\n")

    def test_report_passes_when_runtime_is_healthy(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            now = 1_700_000_100
            self._write_json(
                os.path.join(td, "runtime_state.json"),
                {
                    "checks_ok": True,
                    "alerts": {"severity": "warning"},
                    "scan_stocks_state": "READY",
                    "scan_forex_state": "READY",
                },
            )
            self._write_json(
                os.path.join(td, "market_trends.json"),
                {"stocks": {"divergence_24h": 3}, "forex": {"divergence_24h": 2}},
            )
            self._write_jsonl(
                os.path.join(td, "incidents.jsonl"),
                [{"ts": now - 60, "severity": "info"}, {"ts": now - 30, "severity": "warning"}],
            )
            self._write_json(
                os.path.join(td, "stocks", "scan_diagnostics.json"),
                {"state": "READY", "leaders_total": 2, "scores_total": 8, "msg": "ok"},
            )
            self._write_json(
                os.path.join(td, "stocks", "stock_thinker_status.json"),
                {"health": {"data_ok": True}, "top_chart_map": {"AAPL": [{}, {}]}},
            )
            self._write_json(
                os.path.join(td, "stocks", "stock_trader_status.json"),
                {"state": "READY", "msg": "ok"},
            )
            self._write_json(
                os.path.join(td, "forex", "scan_diagnostics.json"),
                {"state": "READY", "leaders_total": 1, "scores_total": 5, "msg": "ok"},
            )
            self._write_json(
                os.path.join(td, "forex", "forex_thinker_status.json"),
                {"health": {"data_ok": True}, "top_chart_map": {"EUR_USD": [{}, {}]}},
            )
            self._write_json(
                os.path.join(td, "forex", "forex_trader_status.json"),
                {"state": "READY", "msg": "ok"},
            )
            with patch("runtime.tools.stability_audit.time.time", return_value=now):
                out = build_stability_report(td, max_error_incidents_24h=10)
            self.assertTrue(bool(out.get("pass", False)))
            self.assertEqual(int((out.get("markets", {}).get("stocks", {}) or {}).get("chart_cache_symbols", 0)), 1)
            self.assertEqual(int((out.get("markets", {}).get("forex", {}) or {}).get("chart_cache_symbols", 0)), 1)

    def test_report_fails_when_runtime_critical(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            now = 1_700_000_200
            self._write_json(
                os.path.join(td, "runtime_state.json"),
                {"checks_ok": True, "alerts": {"severity": "critical"}},
            )
            self._write_json(os.path.join(td, "market_trends.json"), {})
            self._write_jsonl(
                os.path.join(td, "incidents.jsonl"),
                [{"ts": now - 20, "severity": "error"} for _ in range(3)],
            )
            self._write_json(os.path.join(td, "stocks", "scan_diagnostics.json"), {"state": "ERROR", "msg": "boom"})
            self._write_json(os.path.join(td, "stocks", "stock_thinker_status.json"), {})
            self._write_json(os.path.join(td, "stocks", "stock_trader_status.json"), {})
            self._write_json(os.path.join(td, "forex", "scan_diagnostics.json"), {"state": "READY"})
            self._write_json(os.path.join(td, "forex", "forex_thinker_status.json"), {})
            self._write_json(os.path.join(td, "forex", "forex_trader_status.json"), {})
            with patch("runtime.tools.stability_audit.time.time", return_value=now):
                out = build_stability_report(td, max_error_incidents_24h=2)
            self.assertFalse(bool(out.get("pass", True)))
            self.assertIn("stocks", list((out.get("markets", {}) or {}).get("critical", []) or []))

    def test_report_fails_on_critical_log_spam(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            now = 1_700_000_300
            self._write_json(os.path.join(td, "runtime_state.json"), {"checks_ok": True, "alerts": {"severity": "info"}})
            self._write_json(os.path.join(td, "market_trends.json"), {})
            self._write_jsonl(os.path.join(td, "incidents.jsonl"), [{"ts": now - 60, "severity": "info"}])
            self._write_json(os.path.join(td, "stocks", "scan_diagnostics.json"), {"state": "READY"})
            self._write_json(os.path.join(td, "stocks", "stock_thinker_status.json"), {"health": {"data_ok": True}})
            self._write_json(os.path.join(td, "stocks", "stock_trader_status.json"), {"state": "READY"})
            self._write_json(os.path.join(td, "forex", "scan_diagnostics.json"), {"state": "READY"})
            self._write_json(os.path.join(td, "forex", "forex_thinker_status.json"), {"health": {"data_ok": True}})
            self._write_json(os.path.join(td, "forex", "forex_trader_status.json"), {"state": "READY"})
            spam = [f"2026-03-05 12:00:{i % 60:02d} Missing Robinhood credentials" for i in range(260)]
            self._write_lines(os.path.join(td, "logs", "thinker.log"), spam)
            with patch("runtime.tools.stability_audit.time.time", return_value=now):
                out = build_stability_report(td, max_error_incidents_24h=10)
            self.assertFalse(bool(out.get("pass", True)))
            self.assertEqual(str((out.get("logs", {}) or {}).get("level", "")), "critical")

    def test_report_marks_stale_cached_fallback_as_critical(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            now = 1_700_000_360
            self._write_json(os.path.join(td, "runtime_state.json"), {"checks_ok": True, "alerts": {"severity": "info"}})
            self._write_json(os.path.join(td, "market_trends.json"), {})
            self._write_jsonl(os.path.join(td, "incidents.jsonl"), [{"ts": now - 60, "severity": "info"}])
            self._write_json(os.path.join(td, "stocks", "scan_diagnostics.json"), {"state": "READY", "leaders_total": 1, "scores_total": 4})
            self._write_json(
                os.path.join(td, "stocks", "stock_thinker_status.json"),
                {
                    "health": {"data_ok": True},
                    "fallback_cached": True,
                    "fallback_age_s": 2400,
                    "top_chart_map": {"AAPL": [{}, {}]},
                },
            )
            self._write_json(os.path.join(td, "stocks", "stock_trader_status.json"), {"state": "READY"})
            self._write_json(os.path.join(td, "forex", "scan_diagnostics.json"), {"state": "READY", "leaders_total": 1, "scores_total": 4})
            self._write_json(os.path.join(td, "forex", "forex_thinker_status.json"), {"health": {"data_ok": True}, "top_chart_map": {"EUR_USD": [{}, {}]}})
            self._write_json(os.path.join(td, "forex", "forex_trader_status.json"), {"state": "READY"})
            with patch("runtime.tools.stability_audit.time.time", return_value=now):
                out = build_stability_report(td, max_error_incidents_24h=10)
            self.assertFalse(bool(out.get("pass", True)))
            self.assertIn("stocks", list((out.get("markets", {}) or {}).get("critical", []) or []))
            self.assertEqual(int(((out.get("markets", {}) or {}).get("stocks", {}) or {}).get("fallback_age_s", 0)), 2400)


if __name__ == "__main__":
    unittest.main()
