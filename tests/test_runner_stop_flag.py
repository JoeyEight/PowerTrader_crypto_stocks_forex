from __future__ import annotations

import os
import tempfile
import time
import unittest

import runtime.pt_runner as pt_runner


class TestRunnerStopFlagPayload(unittest.TestCase):
    def test_inactive_stop_flag_payload(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "stop.flag")
            out = pt_runner._stop_flag_payload(path)
            self.assertFalse(bool(out.get("active", True)))
            self.assertEqual(int(out.get("ts", -1)), 0)

    def test_active_stop_flag_payload(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "stop.flag")
            now = int(time.time())
            with open(path, "w", encoding="utf-8") as f:
                f.write(str(now))
            out = pt_runner._stop_flag_payload(path)
            self.assertTrue(bool(out.get("active", False)))
            self.assertEqual(int(out.get("ts", 0)), now)
            self.assertGreaterEqual(int(out.get("age_s", -1)), 0)


if __name__ == "__main__":
    unittest.main()
