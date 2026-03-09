from __future__ import annotations

import os
import tempfile
import time
import unittest
from contextlib import ExitStack
from unittest.mock import patch

import runtime.pt_runner as pt_runner


def _write_script(path: str, body: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(body)


class TestRunnerLifecycleIntegration(unittest.TestCase):
    def test_start_and_graceful_shutdown_child(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            hub = os.path.join(td, "hub_data")
            logs = os.path.join(hub, "logs")
            os.makedirs(logs, exist_ok=True)
            settings_path = os.path.join(td, "gui_settings.json")
            with open(settings_path, "w", encoding="utf-8") as f:
                f.write("{}")

            sleeper = os.path.join(td, "sleeper.py")
            _write_script(
                sleeper,
                "import time\n"
                "while True:\n"
                "    time.sleep(0.25)\n",
            )

            scripts = {"thinker": sleeper, "trader": sleeper, "markets": sleeper, "autopilot": sleeper, "autofix": sleeper}
            with ExitStack() as stack:
                stack.enter_context(patch.object(pt_runner, "BASE_DIR", td))
                stack.enter_context(patch.object(pt_runner, "HUB_DATA_DIR", hub))
                stack.enter_context(patch.object(pt_runner, "LOG_DIR", logs))
                stack.enter_context(patch.object(pt_runner, "THINKER_LOG_PATH", os.path.join(logs, "thinker.log")))
                stack.enter_context(patch.object(pt_runner, "TRADER_LOG_PATH", os.path.join(logs, "trader.log")))
                stack.enter_context(patch.object(pt_runner, "MARKETS_LOG_PATH", os.path.join(logs, "markets.log")))
                stack.enter_context(patch.object(pt_runner, "AUTOPILOT_LOG_PATH", os.path.join(logs, "autopilot.log")))
                stack.enter_context(patch.object(pt_runner, "AUTOFIX_LOG_PATH", os.path.join(logs, "autofix.log")))
                stack.enter_context(patch.object(pt_runner, "RUNNER_LOG_PATH", os.path.join(logs, "runner.log")))
                stack.enter_context(patch.object(pt_runner, "TRADER_STATUS_PATH", os.path.join(hub, "trader_status.json")))
                stack.enter_context(patch.object(pt_runner, "RUNTIME_EVENTS_PATH", os.path.join(hub, "runtime_events.jsonl")))
                stack.enter_context(patch.object(pt_runner, "INCIDENTS_PATH", os.path.join(hub, "incidents.jsonl")))
                stack.enter_context(patch.object(pt_runner, "RUNTIME_STATE_PATH", os.path.join(hub, "runtime_state.json")))
                stack.enter_context(patch.object(pt_runner, "RUNTIME_CHECKS_PATH", os.path.join(hub, "runtime_startup_checks.json")))
                stack.enter_context(patch.object(pt_runner, "KEY_ROTATION_STATUS_PATH", os.path.join(hub, "key_rotation_status.json")))
                stack.enter_context(patch.object(pt_runner, "DRAWDOWN_GUARD_PATH", os.path.join(hub, "global_drawdown_guard.json")))
                stack.enter_context(patch.object(pt_runner, "STOP_FLAG_PATH", os.path.join(hub, "stop_trading.flag")))
                stack.enter_context(patch.object(pt_runner, "_settings_scripts", return_value=scripts))
                stack.enter_context(patch.object(pt_runner, "resolve_settings_path", return_value=settings_path))
                stack.enter_context(patch.object(pt_runner, "read_settings_file", return_value={}))
                stack.enter_context(patch.object(pt_runner, "sanitize_settings", side_effect=lambda x: x if isinstance(x, dict) else {}))

                runner = pt_runner.Runner()
                runner.start_child("thinker")
                self.assertIsNotNone(runner.children["thinker"].pid())
                runner.graceful_shutdown()
                runner.wait_for_children(timeout_s=2.0)
                proc = runner.children["thinker"].proc
                if proc is not None:
                    self.assertIsNotNone(proc.poll())
                for child in runner.children.values():
                    if child.proc and child.proc.poll() is None:
                        try:
                            child.proc.kill()
                        except Exception:
                            pass
                    if child.log_handle is not None:
                        try:
                            child.log_handle.close()
                        except Exception:
                            pass
                        child.log_handle = None

    def test_crash_loop_sets_lockout(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            hub = os.path.join(td, "hub_data")
            logs = os.path.join(hub, "logs")
            os.makedirs(logs, exist_ok=True)
            settings_path = os.path.join(td, "gui_settings.json")
            with open(settings_path, "w", encoding="utf-8") as f:
                f.write("{}")

            crasher = os.path.join(td, "crasher.py")
            _write_script(crasher, "raise SystemExit(7)\n")
            scripts = {"thinker": crasher, "trader": crasher, "markets": crasher, "autopilot": crasher, "autofix": crasher}

            with ExitStack() as stack:
                stack.enter_context(patch.object(pt_runner, "BASE_DIR", td))
                stack.enter_context(patch.object(pt_runner, "HUB_DATA_DIR", hub))
                stack.enter_context(patch.object(pt_runner, "LOG_DIR", logs))
                stack.enter_context(patch.object(pt_runner, "THINKER_LOG_PATH", os.path.join(logs, "thinker.log")))
                stack.enter_context(patch.object(pt_runner, "TRADER_LOG_PATH", os.path.join(logs, "trader.log")))
                stack.enter_context(patch.object(pt_runner, "MARKETS_LOG_PATH", os.path.join(logs, "markets.log")))
                stack.enter_context(patch.object(pt_runner, "AUTOPILOT_LOG_PATH", os.path.join(logs, "autopilot.log")))
                stack.enter_context(patch.object(pt_runner, "AUTOFIX_LOG_PATH", os.path.join(logs, "autofix.log")))
                stack.enter_context(patch.object(pt_runner, "RUNNER_LOG_PATH", os.path.join(logs, "runner.log")))
                stack.enter_context(patch.object(pt_runner, "TRADER_STATUS_PATH", os.path.join(hub, "trader_status.json")))
                stack.enter_context(patch.object(pt_runner, "RUNTIME_EVENTS_PATH", os.path.join(hub, "runtime_events.jsonl")))
                stack.enter_context(patch.object(pt_runner, "INCIDENTS_PATH", os.path.join(hub, "incidents.jsonl")))
                stack.enter_context(patch.object(pt_runner, "RUNTIME_STATE_PATH", os.path.join(hub, "runtime_state.json")))
                stack.enter_context(patch.object(pt_runner, "RUNTIME_CHECKS_PATH", os.path.join(hub, "runtime_startup_checks.json")))
                stack.enter_context(patch.object(pt_runner, "KEY_ROTATION_STATUS_PATH", os.path.join(hub, "key_rotation_status.json")))
                stack.enter_context(patch.object(pt_runner, "DRAWDOWN_GUARD_PATH", os.path.join(hub, "global_drawdown_guard.json")))
                stack.enter_context(patch.object(pt_runner, "STOP_FLAG_PATH", os.path.join(hub, "stop_trading.flag")))
                stack.enter_context(patch.object(pt_runner, "_settings_scripts", return_value=scripts))
                stack.enter_context(patch.object(pt_runner, "resolve_settings_path", return_value=settings_path))
                stack.enter_context(patch.object(pt_runner, "read_settings_file", return_value={}))
                stack.enter_context(patch.object(pt_runner, "sanitize_settings", side_effect=lambda x: x if isinstance(x, dict) else {}))

                runner = pt_runner.Runner()
                runner.start_child("thinker")
                child = runner.children["thinker"]
                deadline = time.time() + 5.0
                while child.proc and child.proc.poll() is None and time.time() < deadline:
                    time.sleep(0.05)
                self.assertTrue(child.proc is not None)
                child.crash_times = [time.time() - 1.0 for _ in range(max(0, pt_runner.CRASH_THRESHOLD - 1))]
                runner.handle_exit("thinker")
                self.assertEqual(runner.state, "ERROR")
                self.assertGreater(child.lockout_until, time.time())
                self.assertIn("crash loop", runner.msg.lower())
                for row in runner.children.values():
                    if row.proc and row.proc.poll() is None:
                        try:
                            row.proc.kill()
                        except Exception:
                            pass
                    if row.log_handle is not None:
                        try:
                            row.log_handle.close()
                        except Exception:
                            pass
                        row.log_handle = None


if __name__ == "__main__":
    unittest.main()
