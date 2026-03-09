from __future__ import annotations

import unittest

from engines.forex_thinker import _session_weight_multiplier


class TestForexSessionWeighting(unittest.TestCase):
    def test_trend_boost(self) -> None:
        mult, mode = _session_weight_multiplier(
            {
                "forex_session_weight_enabled": True,
                "forex_session_weight_floor": 0.85,
                "forex_session_weight_ceiling": 1.2,
            },
            "long",
            {"bias": "TREND"},
        )
        self.assertAlmostEqual(mult, 1.2, places=6)
        self.assertEqual(mode, "trend_boost")

    def test_range_dampen(self) -> None:
        mult, mode = _session_weight_multiplier(
            {
                "forex_session_weight_enabled": True,
                "forex_session_weight_floor": 0.8,
                "forex_session_weight_ceiling": 1.1,
            },
            "short",
            {"bias": "RANGE"},
        )
        self.assertAlmostEqual(mult, 0.8, places=6)
        self.assertEqual(mode, "range_dampen")

    def test_disabled_weighting(self) -> None:
        mult, mode = _session_weight_multiplier(
            {
                "forex_session_weight_enabled": False,
                "forex_session_weight_floor": 0.8,
                "forex_session_weight_ceiling": 1.1,
            },
            "long",
            {"bias": "TREND"},
        )
        self.assertAlmostEqual(mult, 1.0, places=6)
        self.assertEqual(mode, "disabled")


if __name__ == "__main__":
    unittest.main()
