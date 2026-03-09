from __future__ import annotations

import unittest

from app.scanner_quality import build_universe_quality_report, quality_hints, turnover_pct


class TestScannerQuality(unittest.TestCase):
    def test_turnover_pct(self) -> None:
        self.assertEqual(turnover_pct(["A", "B"], ["A", "B"]), 0.0)
        self.assertEqual(turnover_pct(["A", "B"], ["C", "D"]), 100.0)
        self.assertEqual(turnover_pct([], []), 0.0)

    def test_build_quality_report(self) -> None:
        report = build_universe_quality_report(
            market="stocks",
            ts=123,
            mode="intraday",
            universe_total=10,
            candidates_total=6,
            scores_total=3,
            leaders_total=2,
            reject_summary={
                "reject_rate_pct": 40.0,
                "counts": {"spread": 2, "data_quality": 1},
            },
            rejected_rows=[{"reason": "spread", "source": "batch_1h:sip"}],
            scored_rows=[{"symbol": "AAPL", "data_source": "batch_1h:sip"}],
            candidate_churn_pct=50.0,
            leader_churn_pct=25.0,
        )
        self.assertEqual(report["market"], "stocks")
        self.assertEqual(report["universe_total"], 10)
        self.assertAlmostEqual(float(report["acceptance_rate_pct"]), 60.0, places=3)
        self.assertTrue(isinstance(report.get("rejection_reasons", []), list))
        self.assertTrue(isinstance(report.get("data_source_mix", []), list))

    def test_quality_hints_contains_dominant_reason(self) -> None:
        report = {
            "reject_rate_pct": 82.0,
            "candidate_churn_pct": 72.0,
            "leaders_total": 0,
            "rejection_reasons": [{"reason": "spread", "count": 5, "pct": 80.0}],
        }
        hints = quality_hints(report)
        self.assertGreaterEqual(len(hints), 2)
        self.assertTrue(any("spread" in h.lower() for h in hints))


if __name__ == "__main__":
    unittest.main()
