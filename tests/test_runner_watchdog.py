from __future__ import annotations

import os
import tempfile
import time
import unittest
from contextlib import ExitStack
from unittest.mock import patch

import runtime.pt_runner as pt_runner


class _AliveProc:
    def poll(self) -> None:
        return None


class TestRunnerWatchdog(unittest.TestCase):
    def test_market_loop_stale_note_emitted_and_throttled(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            hub = os.path.join(td, "hub_data")
            logs = os.path.join(hub, "logs")
            os.makedirs(logs, exist_ok=True)
            settings_path = os.path.join(td, "gui_settings.json")
            with open(settings_path, "w", encoding="utf-8") as f:
                f.write("{}")

            scripts = {
                "thinker": os.path.join(td, "noop_thinker.py"),
                "trader": os.path.join(td, "noop_trader.py"),
                "markets": os.path.join(td, "noop_markets.py"),
                "autopilot": os.path.join(td, "noop_autopilot.py"),
                "autofix": os.path.join(td, "noop_autofix.py"),
            }
            for path in scripts.values():
                with open(path, "w", encoding="utf-8") as f:
                    f.write("print('noop')\n")

            with ExitStack() as stack:
                stack.enter_context(patch.object(pt_runner, "BASE_DIR", td))
                stack.enter_context(patch.object(pt_runner, "HUB_DATA_DIR", hub))
                stack.enter_context(patch.object(pt_runner, "LOG_DIR", logs))
                stack.enter_context(patch.object(pt_runner, "RUNNER_LOG_PATH", os.path.join(logs, "runner.log")))
                stack.enter_context(patch.object(pt_runner, "THINKER_LOG_PATH", os.path.join(logs, "thinker.log")))
                stack.enter_context(patch.object(pt_runner, "TRADER_LOG_PATH", os.path.join(logs, "trader.log")))
                stack.enter_context(patch.object(pt_runner, "MARKETS_LOG_PATH", os.path.join(logs, "markets.log")))
                stack.enter_context(patch.object(pt_runner, "AUTOPILOT_LOG_PATH", os.path.join(logs, "autopilot.log")))
                stack.enter_context(patch.object(pt_runner, "AUTOFIX_LOG_PATH", os.path.join(logs, "autofix.log")))
                stack.enter_context(patch.object(pt_runner, "MARKET_LOOP_STATUS_PATH", os.path.join(hub, "market_loop_status.json")))
                stack.enter_context(patch.object(pt_runner, "RUNTIME_EVENTS_PATH", os.path.join(hub, "runtime_events.jsonl")))
                stack.enter_context(patch.object(pt_runner, "INCIDENTS_PATH", os.path.join(hub, "incidents.jsonl")))
                stack.enter_context(patch.object(pt_runner, "TRADER_STATUS_PATH", os.path.join(hub, "trader_status.json")))
                stack.enter_context(patch.object(pt_runner, "resolve_settings_path", return_value=settings_path))
                stack.enter_context(
                    patch.object(
                        pt_runner,
                        "read_settings_file",
                        return_value={"market_bg_forex_interval_s": 12.0},
                    )
                )
                stack.enter_context(
                    patch.object(
                        pt_runner,
                        "sanitize_settings",
                        side_effect=lambda x: (x if isinstance(x, dict) else {}),
                    )
                )
                stack.enter_context(patch.object(pt_runner, "_settings_scripts", return_value=scripts))
                runner = pt_runner.Runner()
                runner.children["markets"].proc = _AliveProc()  # type: ignore[assignment]

                def _stale(path: str, _max_age_s: float) -> bool:
                    if str(path) == str(pt_runner.MARKET_LOOP_STATUS_PATH):
                        return True
                    return False

                with patch.object(runner, "_status_file_stale", side_effect=_stale), patch.object(
                    pt_runner, "_append_incident"
                ) as mock_incident, patch.object(pt_runner, "_runner_log"):
                    base_now = time.time() + 100.0
                    runner._watchdog_tick(base_now)
                    calls_after_first = int(mock_incident.call_count)
                    runner._watchdog_tick(base_now + 10.0)
                    calls_after_second = int(mock_incident.call_count)
                    runner._watchdog_tick(base_now + 80.0)
                    calls_after_third = int(mock_incident.call_count)

                self.assertGreaterEqual(calls_after_first, 1)
                self.assertEqual(calls_after_second, calls_after_first)
                self.assertGreaterEqual(calls_after_third, calls_after_first + 1)
                events = [str(call.args[1]) for call in mock_incident.call_args_list if len(call.args) >= 2]
                self.assertIn("runner_market_loop_status_stale", events)
                self.assertIn("runner_market_loop_restart", events)

    def test_market_watchdog_startup_grace_skips_early_restarts(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            hub = os.path.join(td, "hub_data")
            logs = os.path.join(hub, "logs")
            os.makedirs(logs, exist_ok=True)
            settings_path = os.path.join(td, "gui_settings.json")
            with open(settings_path, "w", encoding="utf-8") as f:
                f.write("{}")

            scripts = {
                "thinker": os.path.join(td, "noop_thinker.py"),
                "trader": os.path.join(td, "noop_trader.py"),
                "markets": os.path.join(td, "noop_markets.py"),
                "autopilot": os.path.join(td, "noop_autopilot.py"),
                "autofix": os.path.join(td, "noop_autofix.py"),
            }
            for path in scripts.values():
                with open(path, "w", encoding="utf-8") as f:
                    f.write("print('noop')\n")

            with ExitStack() as stack:
                stack.enter_context(patch.object(pt_runner, "BASE_DIR", td))
                stack.enter_context(patch.object(pt_runner, "HUB_DATA_DIR", hub))
                stack.enter_context(patch.object(pt_runner, "LOG_DIR", logs))
                stack.enter_context(patch.object(pt_runner, "RUNNER_LOG_PATH", os.path.join(logs, "runner.log")))
                stack.enter_context(patch.object(pt_runner, "THINKER_LOG_PATH", os.path.join(logs, "thinker.log")))
                stack.enter_context(patch.object(pt_runner, "TRADER_LOG_PATH", os.path.join(logs, "trader.log")))
                stack.enter_context(patch.object(pt_runner, "MARKETS_LOG_PATH", os.path.join(logs, "markets.log")))
                stack.enter_context(patch.object(pt_runner, "AUTOPILOT_LOG_PATH", os.path.join(logs, "autopilot.log")))
                stack.enter_context(patch.object(pt_runner, "AUTOFIX_LOG_PATH", os.path.join(logs, "autofix.log")))
                stack.enter_context(patch.object(pt_runner, "MARKET_LOOP_STATUS_PATH", os.path.join(hub, "market_loop_status.json")))
                stack.enter_context(patch.object(pt_runner, "RUNTIME_EVENTS_PATH", os.path.join(hub, "runtime_events.jsonl")))
                stack.enter_context(patch.object(pt_runner, "INCIDENTS_PATH", os.path.join(hub, "incidents.jsonl")))
                stack.enter_context(patch.object(pt_runner, "TRADER_STATUS_PATH", os.path.join(hub, "trader_status.json")))
                stack.enter_context(patch.object(pt_runner, "resolve_settings_path", return_value=settings_path))
                stack.enter_context(
                    patch.object(
                        pt_runner,
                        "read_settings_file",
                        return_value={
                            "market_bg_forex_interval_s": 12.0,
                            "runner_market_watchdog_startup_grace_s": 120.0,
                            "runner_market_loop_startup_grace_s": 150.0,
                        },
                    )
                )
                stack.enter_context(
                    patch.object(
                        pt_runner,
                        "sanitize_settings",
                        side_effect=lambda x: (x if isinstance(x, dict) else {}),
                    )
                )
                stack.enter_context(patch.object(pt_runner, "_settings_scripts", return_value=scripts))
                runner = pt_runner.Runner()
                runner.children["markets"].proc = _AliveProc()  # type: ignore[assignment]
                runner.children["markets"].started_at = time.time()

                with patch.object(runner, "_status_file_stale", return_value=True), patch.object(
                    pt_runner, "_append_incident"
                ) as mock_incident, patch.object(pt_runner, "_runner_log"), patch.object(
                    pt_runner, "_terminate_process"
                ) as mock_term:
                    runner._watchdog_tick(time.time() + 10.0)

                self.assertEqual(int(mock_incident.call_count), 0)
                self.assertEqual(int(mock_term.call_count), 0)


if __name__ == "__main__":
    unittest.main()
