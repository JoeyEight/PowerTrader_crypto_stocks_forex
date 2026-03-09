from __future__ import annotations

import unittest

from runtime.pt_runner import _summarize_broker_backoff_events


class TestRunnerBackoffSummary(unittest.TestCase):
    def test_summarize_broker_backoff_events(self) -> None:
        now_ts = 2_000_000
        rows = [
            {
                "ts": now_ts - 100,
                "component": "stocks_trader",
                "event": "broker_retry_after_wait",
                "details": {"wait_s": 12.5},
            },
            {
                "ts": now_ts - 80,
                "component": "forex_trader",
                "event": "broker_retry_after_wait",
                "msg": "retry_after=30s",
                "details": {},
            },
            {
                "ts": now_ts - 70,
                "component": "runner",
                "event": "log",
                "details": {},
            },
        ]
        out = _summarize_broker_backoff_events(rows, now_ts_value=float(now_ts))
        self.assertEqual(int(out.get("count_24h", 0) or 0), 2)
        self.assertAlmostEqual(float(out.get("max_wait_s", 0.0) or 0.0), 30.0, places=3)
        by_comp = out.get("by_component", {}) if isinstance(out.get("by_component", {}), dict) else {}
        self.assertEqual(int(by_comp.get("stocks_trader", 0) or 0), 1)
        self.assertEqual(int(by_comp.get("forex_trader", 0) or 0), 1)


if __name__ == "__main__":
    unittest.main()

