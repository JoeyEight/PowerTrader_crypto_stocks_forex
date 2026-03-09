from __future__ import annotations

import unittest
from typing import Any, Dict

from app.execution_guard import market_guard_status, update_market_guard


class TestExecutionGuard(unittest.TestCase):
    def test_disables_after_threshold(self) -> None:
        state: Dict[str, Any] = {}
        now = 1_800_000_000
        state = update_market_guard(state, "stocks", failed=True, now_ts=now, threshold=3, cooldown_s=600, reason="HTTP 429")
        state = update_market_guard(state, "stocks", failed=True, now_ts=now + 1, threshold=3, cooldown_s=600, reason="HTTP 429")
        state = update_market_guard(state, "stocks", failed=True, now_ts=now + 2, threshold=3, cooldown_s=600, reason="HTTP 429")
        st = market_guard_status(state, "stocks", now_ts=now + 3)
        self.assertTrue(st["active"])
        self.assertGreaterEqual(st["remaining_s"], 599)

    def test_success_resets_streak(self) -> None:
        state: Dict[str, Any] = {}
        now = 1_800_000_000
        state = update_market_guard(state, "forex", failed=True, now_ts=now, threshold=3, cooldown_s=600, reason="net")
        state = update_market_guard(state, "forex", failed=False, now_ts=now + 1, threshold=3, cooldown_s=600)
        st = market_guard_status(state, "forex", now_ts=now + 2)
        self.assertFalse(st["active"])
        self.assertEqual(st["failure_streak"], 0)


if __name__ == "__main__":
    unittest.main()
