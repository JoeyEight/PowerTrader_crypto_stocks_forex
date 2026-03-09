from __future__ import annotations

import unittest

from app.mock_brokers import MockAlpacaBrokerClient, MockOandaBrokerClient


class TestMockBrokers(unittest.TestCase):
    def test_mock_alpaca_bars_are_deterministic(self) -> None:
        c1 = MockAlpacaBrokerClient(seed=7)
        c2 = MockAlpacaBrokerClient(seed=7)
        b1 = c1.get_stock_bars("AAPL", limit=30)
        b2 = c2.get_stock_bars("AAPL", limit=30)
        self.assertEqual(len(b1), len(b2))
        self.assertEqual(float(b1[-1].get("c", 0.0) or 0.0), float(b2[-1].get("c", 0.0) or 0.0))

    def test_mock_alpaca_order_payload(self) -> None:
        c = MockAlpacaBrokerClient(seed=3)
        out = c.place_order("TSLA", "buy", qty=1.5)
        self.assertTrue(bool(out.get("ok", False)))
        self.assertEqual(str(out.get("symbol", "")), "TSLA")

    def test_mock_oanda_candles_are_deterministic(self) -> None:
        c1 = MockOandaBrokerClient(seed=5)
        c2 = MockOandaBrokerClient(seed=5)
        b1 = c1.get_candles("EUR_USD", count=40)
        b2 = c2.get_candles("EUR_USD", count=40)
        self.assertEqual(len(b1), len(b2))
        self.assertEqual(float(b1[-1].get("c", 0.0) or 0.0), float(b2[-1].get("c", 0.0) or 0.0))

    def test_mock_oanda_order_payload(self) -> None:
        c = MockOandaBrokerClient(seed=9)
        out = c.place_market_order("GBP_USD", "short", units=1200)
        self.assertTrue(bool(out.get("ok", False)))
        self.assertEqual(str(out.get("instrument", "")), "GBP_USD")


if __name__ == "__main__":
    unittest.main()
