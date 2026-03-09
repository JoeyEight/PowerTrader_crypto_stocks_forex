from __future__ import annotations

import os
import tempfile
import unittest

from app.status_hydration import load_market_status_bundle, safe_read_json_dict, safe_read_jsonl_dicts


class TestMissingFileNoJsonRobustness(unittest.TestCase):
    def test_safe_read_json_dict_handles_missing_and_invalid(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            missing = os.path.join(td, "missing.json")
            self.assertEqual(safe_read_json_dict(missing), {})

            bad = os.path.join(td, "bad.json")
            with open(bad, "w", encoding="utf-8") as f:
                f.write("{not valid json")
            self.assertEqual(safe_read_json_dict(bad), {})

            arr = os.path.join(td, "arr.json")
            with open(arr, "w", encoding="utf-8") as f:
                f.write("[1,2,3]")
            self.assertEqual(safe_read_json_dict(arr), {})

    def test_safe_read_jsonl_dicts_handles_mixed_lines(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "mixed.jsonl")
            with open(path, "w", encoding="utf-8") as f:
                f.write("not-json\n")
                f.write("{\"ok\": true, \"x\": 1}\n")
                f.write("[1,2,3]\n")
                f.write("{\"ok\": true, \"x\": 2}\n")
            out = safe_read_jsonl_dicts(path, limit=10)
            self.assertEqual(len(out), 2)
            self.assertEqual(int(out[-1].get("x", 0) or 0), 2)

    def test_bundle_defaults_on_missing_paths(self) -> None:
        out = load_market_status_bundle(
            status_path="",
            trader_path="",
            thinker_path="",
            scan_diag_path="",
            history_path="",
            history_limit=12,
        )
        self.assertEqual(out["status"], {})
        self.assertEqual(out["trader"], {})
        self.assertEqual(out["thinker"], {})
        self.assertEqual(int((out["scan_diagnostics"].get("schema_version", 0) or 0)), 2)
        self.assertEqual(int(out["scan_diagnostics"].get("leaders_total", -1)), 0)
        self.assertEqual(out["history"], [])


if __name__ == "__main__":
    unittest.main()
