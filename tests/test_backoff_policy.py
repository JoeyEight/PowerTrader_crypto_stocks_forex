from __future__ import annotations

import unittest

from app.backoff_policy import BackoffPolicy


class TestBackoffPolicy(unittest.TestCase):
    def test_respects_retry_after_floor(self) -> None:
        p = BackoffPolicy(base_delay_s=0.2, max_delay_s=5.0, jitter_s=0.0)
        self.assertEqual(p.wait_seconds(1, retry_after_s=1.7), 1.7)

    def test_exponential_capped(self) -> None:
        p = BackoffPolicy(base_delay_s=0.5, max_delay_s=2.0, jitter_s=0.0)
        self.assertEqual(p.wait_seconds(1), 0.5)
        self.assertEqual(p.wait_seconds(2), 1.0)
        self.assertEqual(p.wait_seconds(3), 2.0)
        self.assertEqual(p.wait_seconds(6), 2.0)

    def test_retry_after_can_exceed_exponential_cap(self) -> None:
        p = BackoffPolicy(base_delay_s=0.5, max_delay_s=2.0, jitter_s=0.0, max_retry_after_s=300.0)
        self.assertEqual(p.wait_seconds(2, retry_after_s=15.0), 15.0)

    def test_retry_after_respects_retry_after_cap(self) -> None:
        p = BackoffPolicy(base_delay_s=0.5, max_delay_s=2.0, jitter_s=0.0, max_retry_after_s=30.0)
        self.assertEqual(p.wait_seconds(2, retry_after_s=90.0), 30.0)


if __name__ == "__main__":
    unittest.main()
