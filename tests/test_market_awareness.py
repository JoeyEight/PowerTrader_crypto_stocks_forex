from __future__ import annotations

import datetime as dt
import unittest
from zoneinfo import ZoneInfo

from app.market_awareness import broker_maintenance_awareness, forex_session_bias, stock_market_awareness

NY = ZoneInfo("America/New_York")


class TestMarketAwareness(unittest.TestCase):
    def test_stock_open_window_payload(self) -> None:
        now = dt.datetime(2026, 3, 5, 11, 0, 0, tzinfo=NY)  # Thursday
        out = stock_market_awareness(now)
        self.assertTrue(bool(out.get("is_open", False)))
        self.assertEqual(str(out.get("status", "")), "open")
        self.assertGreater(int(out.get("countdown_s", 0) or 0), 0)
        self.assertGreater(int(out.get("next_close_ts", 0) or 0), int(now.timestamp()))

    def test_stock_closed_payload_has_next_open(self) -> None:
        now = dt.datetime(2026, 3, 7, 12, 0, 0, tzinfo=NY)  # Saturday
        out = stock_market_awareness(now)
        self.assertFalse(bool(out.get("is_open", True)))
        self.assertGreater(int(out.get("next_open_ts", 0) or 0), int(now.timestamp()))
        self.assertGreater(int(out.get("countdown_s", 0) or 0), 0)

    def test_forex_session_transition_fields(self) -> None:
        now = dt.datetime(2026, 3, 5, 9, 30, 0, tzinfo=NY)
        out = forex_session_bias(now)
        self.assertEqual(str(out.get("session", "")), "London")
        self.assertEqual(str(out.get("next_session", "")), "London/NY")
        self.assertGreater(int(out.get("session_eta_s", 0) or 0), 0)

    def test_broker_maintenance_levels(self) -> None:
        now = dt.datetime(2026, 3, 6, 22, 30, 0, tzinfo=NY)
        out = broker_maintenance_awareness(now)
        self.assertIn("alpaca_level", out)
        self.assertIn("oanda_level", out)
        self.assertTrue(str(out.get("alpaca", "")).strip())
        self.assertTrue(str(out.get("oanda", "")).strip())


if __name__ == "__main__":
    unittest.main()
