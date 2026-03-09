from __future__ import annotations

import unittest

from app.api_quota import summarize_quota_events


class TestApiQuota(unittest.TestCase):
    def test_component_buckets_and_status(self) -> None:
        now = 1_800_000_000.0
        rows = [
            {"ts": now - 10, "event": "stocks_snapshot_failed", "msg": "HTTP 429: Too Many Requests"},
            {"ts": now - 20, "event": "forex_trader_error", "msg": "rate limit from OANDA"},
            {"ts": now - 30, "event": "thinker_error", "msg": "kucoin retry after 3 sec"},
            {"ts": now - 3900, "event": "old", "msg": "HTTP 429"},  # ignored outside 60m
        ]
        out = summarize_quota_events(rows, now_ts=now, warn_15m=2, crit_15m=4)
        self.assertEqual(out["total_15m"], 3)
        self.assertEqual(out["status"], "warning")
        self.assertEqual(out["by_component"]["alpaca"]["count_15m"], 1)
        self.assertEqual(out["by_component"]["oanda"]["count_15m"], 1)
        self.assertEqual(out["by_component"]["kucoin"]["count_15m"], 1)

    def test_critical_threshold(self) -> None:
        now = 1_800_000_000.0
        rows = [{"ts": now - (i * 5), "event": "x", "msg": "HTTP 429"} for i in range(6)]
        out = summarize_quota_events(rows, now_ts=now, warn_15m=2, crit_15m=5)
        self.assertEqual(out["status"], "critical")


if __name__ == "__main__":
    unittest.main()
