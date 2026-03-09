from __future__ import annotations

import json
import os
import tempfile
import unittest

from app.exposure_analytics import build_exposure_payload


class TestExposureAnalytics(unittest.TestCase):
    def test_build_exposure_payload(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            os.makedirs(os.path.join(td, "stocks"), exist_ok=True)
            os.makedirs(os.path.join(td, "forex"), exist_ok=True)

            with open(os.path.join(td, "trader_data.json"), "w", encoding="utf-8") as f:
                json.dump({"positions": {"BTC": {"value_usd": 120.0, "quantity": 0.01}, "ETH": {"value_usd": 0.0}}}, f)
            with open(os.path.join(td, "stocks", "alpaca_status.json"), "w", encoding="utf-8") as f:
                json.dump({"raw_positions": [{"symbol": "AAPL", "market_value": "80.5", "qty": "1"}]}, f)
            with open(os.path.join(td, "forex", "oanda_status.json"), "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "raw_positions": [
                            {"instrument": "EUR_USD", "marginUsed": "20.0", "long": {"units": "1000"}, "short": {"units": "0"}}
                        ]
                    },
                    f,
                )

            out = build_exposure_payload(td)
            self.assertGreater(float(out.get("total_exposure_usd", 0.0) or 0.0), 0.0)
            self.assertIn("by_market_pct", out)
            self.assertIn("top_positions", out)
            self.assertTrue(isinstance(out["top_positions"], list))


if __name__ == "__main__":
    unittest.main()
