from __future__ import annotations

import os
import tempfile
import time
import json
import threading
import unittest

from app.runtime_logging import atomic_write_json, cleanup_logs, redact_text, runtime_event, trim_jsonl_max_lines


class TestRuntimeLogging(unittest.TestCase):
    def test_redact_text(self) -> None:
        self.assertEqual(redact_text("api_key=abc"), "[redacted-sensitive]")
        self.assertEqual(redact_text("normal status line"), "normal status line")

    def test_cleanup_logs_age_and_size(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            old_log = os.path.join(td, "old.log")
            new_log = os.path.join(td, "new.log")
            keep_log = os.path.join(td, "runner.log")
            with open(old_log, "w", encoding="utf-8") as f:
                f.write("x" * 50)
            with open(new_log, "w", encoding="utf-8") as f:
                f.write("y" * 120)
            with open(keep_log, "w", encoding="utf-8") as f:
                f.write("z" * 200)

            very_old = time.time() - (20 * 86400)
            os.utime(old_log, (very_old, very_old))

            stats = cleanup_logs(td, keep_patterns=("runner.log",), max_age_days=14.0, max_total_bytes=100)
            self.assertGreaterEqual(int(stats.get("removed", 0)), 1)
            self.assertFalse(os.path.isfile(old_log))
            self.assertTrue(os.path.isfile(keep_log))

    def test_runtime_event_jsonl_write(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "events.jsonl")
            runtime_event(path, component="runner", event="test", msg="token=abc")
            self.assertTrue(os.path.isfile(path))
            with open(path, "r", encoding="utf-8") as f:
                txt = f.read()
            self.assertIn("\"component\":\"runner\"", txt)
            self.assertIn("[redacted-sensitive]", txt)

    def test_trim_jsonl_max_lines(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "incidents.jsonl")
            with open(path, "w", encoding="utf-8") as f:
                for i in range(15):
                    f.write(f"{{\"i\":{i}}}\n")
            out = trim_jsonl_max_lines(path, max_lines=10)
            self.assertTrue(bool(out.get("trimmed", False)))
            self.assertEqual(int(out.get("kept", 0) or 0), 10)
            self.assertEqual(int(out.get("dropped", 0) or 0), 5)
            with open(path, "r", encoding="utf-8") as f:
                lines = [ln.strip() for ln in f if ln.strip()]
            self.assertEqual(len(lines), 10)
            self.assertIn("\"i\":14", lines[-1])

    def test_atomic_write_json_is_safe_under_concurrent_writes(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "forex", "oanda_status.json")
            errs = []

            def _worker(worker_id: int) -> None:
                try:
                    for i in range(30):
                        atomic_write_json(path, {"worker": int(worker_id), "i": int(i)})
                except Exception as exc:  # pragma: no cover - assertion below catches this
                    errs.append(exc)

            threads = [threading.Thread(target=_worker, args=(idx,)) for idx in range(4)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            self.assertEqual(errs, [])
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            self.assertIn("worker", payload)
            self.assertIn("i", payload)


if __name__ == "__main__":
    unittest.main()
