from __future__ import annotations

import unittest

from app.runtime_insights import (
    build_broker_latency_histogram,
    build_incident_trend,
    detect_equity_anomaly,
    detect_stale_history,
)


class TestRuntimeInsights(unittest.TestCase):
    def test_incident_trend_counts(self) -> None:
        now_ts = 1_700_000_000
        rows = [
            {"ts": now_ts - 30, "severity": "error"},
            {"ts": now_ts - 300, "severity": "warning"},
            {"ts": now_ts - 3700, "severity": "warning"},
            {"ts": now_ts - 100_000, "severity": "error"},
        ]
        out = build_incident_trend(rows, now_ts_value=now_ts)
        counts = out.get("counts", {}) if isinstance(out.get("counts", {}), dict) else {}
        self.assertEqual(int(counts.get("1h", 0) or 0), 2)
        self.assertEqual(int(counts.get("24h", 0) or 0), 3)
        self.assertEqual(int(counts.get("7d", 0) or 0), 4)
        self.assertIn("1h[", str(out.get("sparkline", "")))

    def test_equity_anomaly_detects_spike(self) -> None:
        now_ts = 1_700_000_000
        rows = [{"ts": now_ts - (120 - i), "account_value": 100.0} for i in range(119)]
        rows.append({"ts": now_ts - 1, "account_value": 108.0})
        out = detect_equity_anomaly(rows, now_ts_value=now_ts, lookback_points=120, min_samples=20, spike_pct=3.0)
        self.assertEqual(str(out.get("state", "")), "ok")
        self.assertTrue(bool(out.get("active", False)))
        self.assertEqual(str(out.get("direction", "")), "up")

    def test_stale_history(self) -> None:
        now_ts = 1_700_000_000
        fresh = detect_stale_history([{"ts": now_ts - 20, "account_value": 100.0}], now_ts_value=now_ts, stale_after_s=60)
        stale = detect_stale_history([{"ts": now_ts - 120, "account_value": 100.0}], now_ts_value=now_ts, stale_after_s=60)
        self.assertFalse(bool(fresh.get("active", True)))
        self.assertTrue(bool(stale.get("active", False)))

    def test_broker_latency_histogram_buckets(self) -> None:
        runtime_rows = [
            {"event": "broker_retry_after_wait", "details": {"wait_s": 0.6}},
            {"event": "broker_retry_after_wait", "details": {"wait_s": 2.2}},
            {"event": "broker_retry_after_wait", "details": {"wait_s": 15.0}},
        ]
        audit_rows = {
            "stocks": [{"retry_after_wait_s": 35.0}],
            "forex": [{"retry_after_wait_s": 180.0}],
        }
        out = build_broker_latency_histogram(runtime_rows, market_audit_rows=audit_rows)
        self.assertEqual(int(out.get("samples", 0) or 0), 5)
        buckets = out.get("buckets", {}) if isinstance(out.get("buckets", {}), dict) else {}
        self.assertEqual(int(buckets.get("lt_1s", 0) or 0), 1)
        self.assertEqual(int(buckets.get("1_3s", 0) or 0), 1)
        self.assertEqual(int(buckets.get("10_30s", 0) or 0), 1)
        self.assertEqual(int(buckets.get("30_120s", 0) or 0), 1)
        self.assertEqual(int(buckets.get("gte_120s", 0) or 0), 1)


if __name__ == "__main__":
    unittest.main()
