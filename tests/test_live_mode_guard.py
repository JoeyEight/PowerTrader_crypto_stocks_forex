from __future__ import annotations

import unittest

from app.live_mode_guard import evaluate_live_mode_checklist


class TestLiveModeGuard(unittest.TestCase):
    def test_green_checklist(self) -> None:
        rt = {
            "ts": 1000,
            "checks": {"ok": True},
            "alerts": {"severity": "ok"},
            "api_quota": {"status": "ok"},
            "execution_guard": {"markets": {}},
        }
        out = evaluate_live_mode_checklist(rt)
        self.assertTrue(out["ok"])
        self.assertEqual(out["reasons"], [])

    def test_blocked_by_alerts_and_guard(self) -> None:
        rt = {
            "ts": 1000,
            "checks": {"ok": True},
            "alerts": {"severity": "warn"},
            "api_quota": {"status": "critical"},
            "execution_guard": {"markets": {"stocks": {"disabled_until": 1400}}},
        }
        out = evaluate_live_mode_checklist(rt)
        self.assertFalse(out["ok"])
        self.assertIn("runtime_alerts_not_green", out["reasons"])
        self.assertIn("api_quota_critical", out["reasons"])
        self.assertIn("execution_guard_active:stocks", out["reasons"])


if __name__ == "__main__":
    unittest.main()
