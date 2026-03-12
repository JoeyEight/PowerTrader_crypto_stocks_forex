from __future__ import annotations

import json
import os
import sys
import tempfile
import types
import unittest


def _install_matplotlib_stubs() -> None:
    if "matplotlib.figure" in sys.modules:
        return

    matplotlib = types.ModuleType("matplotlib")
    matplotlib.__path__ = []
    figure_mod = types.ModuleType("matplotlib.figure")
    patches_mod = types.ModuleType("matplotlib.patches")
    ticker_mod = types.ModuleType("matplotlib.ticker")
    transforms_mod = types.ModuleType("matplotlib.transforms")
    backends_mod = types.ModuleType("matplotlib.backends")
    backends_mod.__path__ = []
    backend_tkagg_mod = types.ModuleType("matplotlib.backends.backend_tkagg")

    class Figure:  # pragma: no cover - import shim only
        pass

    class Rectangle:  # pragma: no cover - import shim only
        pass

    class FuncFormatter:  # pragma: no cover - import shim only
        def __init__(self, func=None) -> None:
            self.func = func

    class FigureCanvasTkAgg:  # pragma: no cover - import shim only
        def __init__(self, *args, **kwargs) -> None:
            self.args = args
            self.kwargs = kwargs

    def blended_transform_factory(*args, **kwargs):
        return None

    figure_mod.Figure = Figure
    patches_mod.Rectangle = Rectangle
    ticker_mod.FuncFormatter = FuncFormatter
    transforms_mod.blended_transform_factory = blended_transform_factory
    backend_tkagg_mod.FigureCanvasTkAgg = FigureCanvasTkAgg

    sys.modules["matplotlib"] = matplotlib
    sys.modules["matplotlib.figure"] = figure_mod
    sys.modules["matplotlib.patches"] = patches_mod
    sys.modules["matplotlib.ticker"] = ticker_mod
    sys.modules["matplotlib.transforms"] = transforms_mod
    sys.modules["matplotlib.backends"] = backends_mod
    sys.modules["matplotlib.backends.backend_tkagg"] = backend_tkagg_mod


_install_matplotlib_stubs()

from ui.pt_hub import PowerTraderHub


class _Var:
    def __init__(self) -> None:
        self.value = None

    def set(self, value) -> None:
        self.value = value

    def get(self):
        return self.value


class _Tree:
    def __init__(self) -> None:
        self.rows = {}

    def get_children(self):
        return tuple(self.rows.keys())

    def delete(self, iid) -> None:
        self.rows.pop(iid, None)

    def insert(self, _parent, _where, values=(), tags=()):
        iid = f"row{len(self.rows) + 1}"
        self.rows[iid] = {"values": tuple(values), "tags": tuple(tags)}
        return iid

    def item(self, iid):
        return dict(self.rows.get(iid, {}))


class HubMarketOverviewFallbackTests(unittest.TestCase):
    def _panel(self, market_name: str) -> dict:
        return {
            "market_name": market_name,
            "ai_var": _Var(),
            "trader_var": _Var(),
            "state_var": _Var(),
            "endpoint_var": _Var(),
            "portfolio_vars": {
                "buying_power": _Var(),
                "open_positions": _Var(),
                "realized_pnl": _Var(),
                "mode": _Var(),
            },
        }

    def test_forex_fallback_uses_nav_when_buying_power_is_placeholder(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            status_path = os.path.join(tmp, "oanda_status.json")
            trader_path = os.path.join(tmp, "forex_trader_status.json")
            thinker_path = os.path.join(tmp, "forex_thinker_status.json")
            with open(status_path, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "state": "READY",
                        "buying_power": "Pending account link",
                        "nav": 100.0,
                        "currency": "USD",
                        "open_positions": 0,
                    },
                    f,
                )
            with open(trader_path, "w", encoding="utf-8") as f:
                json.dump({"trader_state": "Practice auto-run", "open_positions": 0}, f)
            with open(thinker_path, "w", encoding="utf-8") as f:
                json.dump({"ai_state": "Scan ready", "state": "READY"}, f)

            hub = PowerTraderHub.__new__(PowerTraderHub)
            hub.settings = {
                "oanda_account_id": "acct-123",
                "oanda_api_token": "token-123",
                "oanda_practice_mode": False,
                "oanda_rest_url": "https://api-fxtrade.oanda.com",
            }
            hub.project_dir = tmp
            hub.market_panels = {"forex": self._panel("Forex")}
            hub.market_status_paths = {"forex": status_path}
            hub.market_trader_paths = {"forex": trader_path}
            hub.market_thinker_paths = {"forex": thinker_path}

            hub._format_market_state_line = lambda text: text
            hub._mask_secret = lambda value: "***"

            hub._refresh_market_overview_fallback()

            panel = hub.market_panels["forex"]
            self.assertEqual(panel["portfolio_vars"]["buying_power"].get(), "$100.00")
            self.assertEqual(panel["portfolio_vars"]["open_positions"].get(), "0")
            self.assertEqual(
                panel["endpoint_var"].get(),
                "Broker: OANDA | Live | https://api-fxtrade.oanda.com",
            )

    def test_stocks_fallback_uses_equity_when_buying_power_is_placeholder(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            status_path = os.path.join(tmp, "alpaca_status.json")
            trader_path = os.path.join(tmp, "stock_trader_status.json")
            thinker_path = os.path.join(tmp, "stock_thinker_status.json")
            with open(status_path, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "state": "READY",
                        "buying_power": "Pending account link",
                        "equity": 99999.49,
                        "open_positions": 1,
                    },
                    f,
                )
            with open(trader_path, "w", encoding="utf-8") as f:
                json.dump({"trader_state": "Paper auto-run", "open_positions": 1}, f)
            with open(thinker_path, "w", encoding="utf-8") as f:
                json.dump({"ai_state": "Scan ready", "state": "READY"}, f)

            hub = PowerTraderHub.__new__(PowerTraderHub)
            hub.settings = {
                "alpaca_api_key": "key-123",
                "alpaca_secret_key": "secret-123",
                "alpaca_paper_mode": True,
                "alpaca_base_url": "https://paper-api.alpaca.markets",
            }
            hub.project_dir = tmp
            hub.market_panels = {"stocks": self._panel("Stocks")}
            hub.market_status_paths = {"stocks": status_path}
            hub.market_trader_paths = {"stocks": trader_path}
            hub.market_thinker_paths = {"stocks": thinker_path}

            hub._format_market_state_line = lambda text: text
            hub._mask_secret = lambda value: "***"

            hub._refresh_market_overview_fallback()

            panel = hub.market_panels["stocks"]
            self.assertEqual(panel["portfolio_vars"]["buying_power"].get(), "$99,999.49")
            self.assertEqual(panel["portfolio_vars"]["open_positions"].get(), "1")
            self.assertEqual(
                panel["endpoint_var"].get(),
                "Broker: Alpaca | Paper | https://paper-api.alpaca.markets",
            )

    def test_stocks_fallback_hydrates_positions_and_scan_canvas(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            status_path = os.path.join(tmp, "alpaca_status.json")
            trader_path = os.path.join(tmp, "stock_trader_status.json")
            thinker_path = os.path.join(tmp, "stock_thinker_status.json")
            diag_path = os.path.join(tmp, "scan_diagnostics.json")
            with open(status_path, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "state": "READY",
                        "buying_power": 199949.56,
                        "equity": 99999.56,
                        "open_positions": 1,
                        "raw_positions": [
                            {
                                "symbol": "AMZN",
                                "qty": "0.230557118",
                                "market_value": "49.60",
                                "unrealized_pl": "-0.39",
                            }
                        ],
                    },
                    f,
                )
            with open(trader_path, "w", encoding="utf-8") as f:
                json.dump({"trader_state": "Paper auto-run", "open_positions": 1, "msg": "Trader ready"}, f)
            with open(thinker_path, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "ai_state": "Scan ready",
                        "state": "READY",
                        "leaders": [{"symbol": "AMZN", "side": "long", "score": 1.23}],
                    },
                    f,
                )
            with open(diag_path, "w", encoding="utf-8") as f:
                json.dump({"state": "READY", "leaders_total": 10, "scores_total": 146}, f)

            tree = _Tree()
            panel = self._panel("Stocks")
            panel["positions_tree"] = tree
            panel["positions_summary_var"] = _Var()

            hub = PowerTraderHub.__new__(PowerTraderHub)
            hub.settings = {
                "alpaca_api_key": "key-123",
                "alpaca_secret_key": "secret-123",
                "alpaca_paper_mode": True,
                "alpaca_base_url": "https://paper-api.alpaca.markets",
            }
            hub.project_dir = tmp
            hub.hub_dir = tmp
            hub.market_panels = {"stocks": panel}
            hub.market_status_paths = {"stocks": status_path}
            hub.market_trader_paths = {"stocks": trader_path}
            hub.market_thinker_paths = {"stocks": thinker_path}
            hub.market_scan_diag_paths = {"stocks": diag_path}
            hub.market_state_dirs = {"stocks": tmp}

            hub._format_market_state_line = lambda text: text
            hub._mask_secret = lambda value: "***"
            hub._market_age_text = lambda ts: "Updated now"
            hub._market_fmt_num = lambda value, digits=6: f"{float(value):.{int(digits)}f}"
            hub._market_fmt_money = lambda value, digits=2: f"${float(value):,.{int(digits)}f}"
            hub._market_fmt_signed_money = lambda value, digits=4: f"{float(value):+,.{int(digits)}f}"

            captured = {}

            def _capture_render(market_key, thinker_data, status_data=None, trader_data=None, diag_data=None):
                captured["market_key"] = market_key
                captured["thinker_data"] = thinker_data
                captured["status_data"] = status_data
                captured["trader_data"] = trader_data
                captured["diag_data"] = diag_data

            hub._render_market_canvas = _capture_render

            hub._refresh_market_overview_fallback()

            self.assertEqual(len(tree.rows), 1)
            row = next(iter(tree.rows.values()))
            self.assertEqual(row["values"][0], "AMZN")
            self.assertEqual(captured["market_key"], "stocks")
            self.assertEqual(captured["diag_data"]["leaders_total"], 10)
            self.assertEqual(captured["thinker_data"]["leaders"][0]["symbol"], "AMZN")

    def test_market_panel_consistency_issues_flag_blank_positions_and_scanner(self) -> None:
        hub = PowerTraderHub.__new__(PowerTraderHub)
        view_var = _Var()
        view_var.set("Scanner")
        hub.market_panels = {
            "stocks": {
                **self._panel("Stocks"),
                "positions_tree": _Tree(),
                "chart_table": _Tree(),
                "market_view_var": view_var,
            }
        }

        issues = hub._market_panel_consistency_issues(
            "stocks",
            {
                "raw_positions": [{"symbol": "AMZN", "qty": "1", "market_value": "100.0", "unrealized_pl": "1.0"}],
            },
            {
                "leaders": [{"symbol": "AMZN", "side": "long", "score": 1.23}],
                "all_scores": [{"symbol": "AMZN", "side": "long", "score": 1.23}],
            },
            {"leaders_total": 10, "scores_total": 148},
        )

        issue_codes = {str(row.get("issue_code", "") or "") for row in issues}
        self.assertIn("positions_blank", issue_codes)
        self.assertIn("scanner_blank", issue_codes)


if __name__ == "__main__":
    unittest.main()
