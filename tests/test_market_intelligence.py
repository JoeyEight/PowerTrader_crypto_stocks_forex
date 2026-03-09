from __future__ import annotations

import json
import os
import tempfile
import unittest

from app.confidence_calibration import build_confidence_calibration_payload
from app.notification_center import build_notification_center_payload
from app.regime_classifier import build_all_market_regimes, classify_regime_from_series
from app.shadow_scorecard import build_shadow_scorecards
from app.walkforward_report import build_walkforward_report


class TestMarketIntelligence(unittest.TestCase):
    def _write_json(self, path: str, payload: dict) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f)

    def _write_jsonl(self, path: str, rows: list[dict]) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")

    def test_classify_regime_trend_up(self) -> None:
        series = [100.0 + (0.6 * i) for i in range(40)]
        out = classify_regime_from_series(series)
        self.assertEqual(str(out.get("regime", "")), "trend_up")
        self.assertGreater(float(out.get("change_pct", 0.0) or 0.0), 0.0)

    def test_build_all_market_regimes(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            self._write_json(
                os.path.join(td, "stocks", "stock_thinker_status.json"),
                {
                    "top_pick": {"symbol": "SPY"},
                    "top_chart_map": {
                        "SPY": [{"c": 100.0 + i * 0.2} for i in range(60)],
                        "TSLA": [{"c": 200.0 - i * 0.1} for i in range(60)],
                    },
                },
            )
            self._write_json(
                os.path.join(td, "forex", "forex_thinker_status.json"),
                {
                    "top_pick": {"pair": "EUR_USD"},
                    "top_chart_map": {
                        "EUR_USD": [{"c": 1.09 + i * 0.0004} for i in range(70)],
                    },
                },
            )
            out = build_all_market_regimes(td)
            self.assertIn("stocks", out)
            self.assertIn("forex", out)
            self.assertTrue(bool((out.get("stocks", {}) if isinstance(out.get("stocks", {}), dict) else {}).get("focus_symbol", "")))

    def test_walkforward_report_has_windows(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            rows = []
            ts0 = 1_700_000_000
            for day in range(12):
                ts = ts0 + (day * 86400)
                rows.append({"ts": ts + 1, "event": "entry", "ok": True, "pnl_usd": 1.5})
                rows.append({"ts": ts + 2, "event": "entry_fail", "ok": False, "pnl_usd": -0.2})
            self._write_jsonl(os.path.join(td, "stocks", "execution_audit.jsonl"), rows)
            self._write_jsonl(os.path.join(td, "forex", "execution_audit.jsonl"), rows)
            out = build_walkforward_report(td)
            stocks = out.get("stocks", {}) if isinstance(out.get("stocks", {}), dict) else {}
            self.assertEqual(str(stocks.get("state", "")), "READY")
            self.assertGreaterEqual(int(stocks.get("events_considered", 0) or 0), 1)
            self.assertTrue(isinstance(stocks.get("windows", []), list))

    def test_confidence_calibration_curve(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            rows = []
            ts0 = 1_700_000_000
            for i in range(60):
                score = 0.1 + (i * 0.03)
                rows.append({"ts": ts0 + i, "event": "entry", "ok": bool(i % 3), "score": score})
            self._write_jsonl(os.path.join(td, "stocks", "execution_audit.jsonl"), rows)
            self._write_jsonl(os.path.join(td, "forex", "execution_audit.jsonl"), rows)
            out = build_confidence_calibration_payload(
                td,
                {
                    "stock_score_threshold": 0.2,
                    "forex_score_threshold": 0.2,
                    "adaptive_confidence_min_samples": 6,
                    "adaptive_confidence_target_success_pct": 45.0,
                },
            )
            stocks = out.get("stocks", {}) if isinstance(out.get("stocks", {}), dict) else {}
            rec = stocks.get("recommendation", {}) if isinstance(stocks.get("recommendation", {}), dict) else {}
            self.assertTrue(isinstance(stocks.get("curve", []), list))
            self.assertIn("recommended_threshold", rec)

    def test_shadow_scorecards(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            self._write_json(
                os.path.join(td, "market_trends.json"),
                {
                    "stocks": {
                        "quality_aggregates": {"reject_rate_pct": 72.0},
                        "discrepancy_tracker": {"divergence_pressure_pct": 20.0},
                        "data_source_reliability": {"score": 88.0},
                        "cadence_aggregates": {"level": "ok"},
                    },
                    "forex": {
                        "quality_aggregates": {"reject_rate_pct": 74.0},
                        "discrepancy_tracker": {"divergence_pressure_pct": 24.0},
                        "data_source_reliability": {"score": 85.0},
                        "cadence_aggregates": {"level": "ok"},
                    },
                },
            )
            self._write_json(
                os.path.join(td, "walkforward_report.json"),
                {
                    "stocks": {"latest_window": {"test": {"win_rate_pct": 62.0}}},
                    "forex": {"latest_window": {"test": {"win_rate_pct": 59.0}}},
                },
            )
            self._write_json(
                os.path.join(td, "confidence_calibration.json"),
                {
                    "stocks": {"samples": 40},
                    "forex": {"samples": 35},
                },
            )
            self._write_json(
                os.path.join(td, "market_regimes.json"),
                {
                    "stocks": {"dominant_regime": "trend_up"},
                    "forex": {"dominant_regime": "range"},
                },
            )
            out = build_shadow_scorecards(td)
            self.assertIn(str((out.get("stocks", {}) if isinstance(out.get("stocks", {}), dict) else {}).get("promotion_gate", "")), {"PASS", "WARN", "BLOCK"})
            self.assertIn("all_markets_pass", out)

    def test_notification_center_payload(self) -> None:
        runtime_state = {
            "ts": 1_700_000_000,
            "alerts": {"severity": "warning", "reasons": ["scan_reject_pressure"], "hints": ["Tune thresholds."]},
            "market_trends": {
                "stocks": {
                    "quality_aggregates": {"reject_rate_pct": 95.0},
                    "data_source_reliability": {"score": 62.0},
                    "why_not_traded": {"reason": "Top score below threshold"},
                },
                "forex": {
                    "quality_aggregates": {"reject_rate_pct": 30.0},
                    "data_source_reliability": {"score": 90.0},
                    "why_not_traded": {"reason": "Session risk gate"},
                },
            },
        }
        incidents = [
            {"ts": 1_699_999_900, "severity": "error", "event": "stocks_trader_error", "msg": "boom", "details": {"market": "stocks"}},
            {"ts": 1_699_999_901, "severity": "warning", "event": "forex_thinker_failed", "msg": "nope", "details": {"market": "forex"}},
        ]
        out = build_notification_center_payload(runtime_state, incidents_rows=incidents)
        self.assertGreaterEqual(int(out.get("total", 0) or 0), 2)
        self.assertTrue(isinstance(out.get("items", []), list))
        self.assertTrue(isinstance(out.get("by_market", {}), dict))


if __name__ == "__main__":
    unittest.main()
