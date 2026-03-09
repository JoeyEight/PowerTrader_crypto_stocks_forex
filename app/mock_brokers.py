from __future__ import annotations

import hashlib
import random
import time
from typing import Any, Dict, List


def _seed_for(*parts: str) -> int:
    h = hashlib.sha256("|".join(str(p or "") for p in parts).encode("utf-8")).hexdigest()
    return int(h[:12], 16)


def _bars(seed: int, start: float, count: int) -> List[Dict[str, Any]]:
    rng = random.Random(int(seed))
    out: List[Dict[str, Any]] = []
    px = float(start)
    now = int(time.time())
    for i in range(max(2, int(count))):
        drift = (rng.random() - 0.5) * 0.8
        px = max(0.01, px * (1.0 + drift / 100.0))
        out.append({"t": now - (count - i) * 3600, "c": round(px, 6), "h": round(px * 1.002, 6), "l": round(px * 0.998, 6), "v": 1000 + i})
    return out


class MockAlpacaBrokerClient:
    """Deterministic broker shim for scanner/trader integration tests."""

    def __init__(self, api_key_id: str = "", secret_key: str = "", base_url: str = "", data_url: str = "", seed: int = 1) -> None:
        self.api_key_id = str(api_key_id)
        self.secret_key = str(secret_key)
        self.base_url = str(base_url)
        self.data_url = str(data_url)
        self.seed = int(seed)

    def fetch_snapshot(self) -> Dict[str, Any]:
        return {
            "state": "READY",
            "msg": "mock snapshot",
            "buying_power": 200000.0,
            "equity": 100000.0,
            "raw_positions": [{"symbol": "AAPL", "qty": 1.2, "market_value": 210.0, "unrealized_pl": 4.2}],
        }

    def list_tradable_assets(self) -> List[Dict[str, Any]]:
        return [
            {"symbol": "AAPL", "tradable": True, "status": "active", "class": "us_equity", "exchange": "NASDAQ", "marginable": True, "fractionable": True},
            {"symbol": "MSFT", "tradable": True, "status": "active", "class": "us_equity", "exchange": "NASDAQ", "marginable": True, "fractionable": True},
            {"symbol": "SPY", "tradable": True, "status": "active", "class": "us_equity", "exchange": "ARCA", "marginable": True, "fractionable": True},
            {"symbol": "TSLA", "tradable": True, "status": "active", "class": "us_equity", "exchange": "NASDAQ", "marginable": True, "fractionable": True},
        ]

    def get_stock_bars(self, symbol: str, timeframe: str = "1Hour", limit: int = 120, feed: str = "iex") -> List[Dict[str, Any]]:
        base = 120.0 + (float((sum(ord(c) for c in str(symbol)) % 500)) / 10.0)
        seed = _seed_for("alpaca", str(symbol).upper(), str(feed), str(self.seed))
        return _bars(seed, base, max(10, int(limit)))

    def place_order(self, symbol: str, side: str, qty: float, order_type: str = "market", tif: str = "day") -> Dict[str, Any]:
        return {
            "ok": True,
            "id": f"mock-alpaca-{int(time.time())}",
            "symbol": str(symbol).upper(),
            "side": str(side).lower(),
            "qty": float(qty),
            "type": str(order_type),
            "tif": str(tif),
            "filled_avg_price": self.get_stock_bars(symbol, limit=2)[-1].get("c", 0.0),
        }


class MockOandaBrokerClient:
    """Deterministic OANDA shim for scanner/trader integration tests."""

    def __init__(self, account_id: str = "", api_token: str = "", base_url: str = "", stream_url: str = "", seed: int = 1) -> None:
        self.account_id = str(account_id)
        self.api_token = str(api_token)
        self.base_url = str(base_url)
        self.stream_url = str(stream_url)
        self.seed = int(seed)

    def fetch_snapshot(self) -> Dict[str, Any]:
        return {
            "state": "READY",
            "msg": "mock snapshot",
            "buying_power": 100000.0,
            "currency": "USD",
            "raw_positions": [
                {
                    "instrument": "EUR_USD",
                    "marginUsed": 125.0,
                    "long": {"units": "1000", "unrealizedPL": "1.2"},
                    "short": {"units": "0", "unrealizedPL": "0"},
                }
            ],
        }

    def get_candles(self, pair: str, granularity: str = "H1", count: int = 120) -> List[Dict[str, Any]]:
        base = 1.05 + (float((sum(ord(c) for c in str(pair)) % 200)) / 1000.0)
        seed = _seed_for("oanda", str(pair).upper(), str(granularity), str(self.seed))
        return _bars(seed, base, max(10, int(count)))

    def place_market_order(self, pair: str, side: str, units: int) -> Dict[str, Any]:
        return {
            "ok": True,
            "id": f"mock-oanda-{int(time.time())}",
            "instrument": str(pair).upper(),
            "side": str(side).lower(),
            "units": int(units),
            "price": self.get_candles(pair, count=2)[-1].get("c", 0.0),
        }
