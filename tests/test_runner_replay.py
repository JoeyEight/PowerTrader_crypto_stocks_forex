from __future__ import annotations

import unittest

from app.runner_replay import replay_runner_heartbeats


class TestRunnerReplay(unittest.TestCase):
    def test_replay_detects_restart_and_stale_transition(self) -> None:
        rows = [
            {"ts": 100, "runner_pid": 1001, "state": "RUNNING"},
            {"ts": 104, "runner_pid": 1001, "state": "RUNNING"},
            {"ts": 120, "runner_pid": 1001, "state": "RUNNING"},
            {"ts": 122, "runner_pid": 1002, "state": "RUNNING"},
            {"ts": 124, "runner_pid": 1002, "state": "RUNNING"},
        ]
        out = replay_runner_heartbeats(rows, stale_after_s=10)
        self.assertEqual(int(out.get("samples", 0) or 0), 5)
        self.assertEqual(int(out.get("restarts", 0) or 0), 1)
        self.assertGreaterEqual(int(out.get("stale_transitions", 0) or 0), 1)


if __name__ == "__main__":
    unittest.main()
