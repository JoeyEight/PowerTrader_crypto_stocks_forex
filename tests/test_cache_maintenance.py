from __future__ import annotations

import os
import tempfile
import time
import unittest

from app.cache_maintenance import prune_data_cache, prune_scanner_quality_artifacts


class TestCacheMaintenance(unittest.TestCase):
    def test_prune_data_cache_removes_old_files(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cache_dir = os.path.join(td, "cache")
            os.makedirs(cache_dir, exist_ok=True)
            old_path = os.path.join(cache_dir, "old.json")
            new_path = os.path.join(cache_dir, "new.json")
            with open(old_path, "w", encoding="utf-8") as f:
                f.write("old")
            with open(new_path, "w", encoding="utf-8") as f:
                f.write("new")
            very_old = time.time() - (40 * 86400)
            os.utime(old_path, (very_old, very_old))

            out = prune_data_cache(td, max_age_days=14.0, max_total_bytes=1_000_000)
            self.assertGreaterEqual(int(out.get("removed", 0) or 0), 1)
            self.assertFalse(os.path.isfile(old_path))
            self.assertTrue(os.path.isfile(new_path))

    def test_prune_scanner_quality_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            charts = os.path.join(td, "exports", "charts")
            os.makedirs(charts, exist_ok=True)
            old_export = os.path.join(charts, "scanner_quality_20260101_000000.json")
            keep_export = os.path.join(charts, "scanner_quality_20260305_000000.json")
            with open(old_export, "w", encoding="utf-8") as f:
                f.write("{}")
            with open(keep_export, "w", encoding="utf-8") as f:
                f.write("{}")
            very_old = time.time() - (45 * 86400)
            os.utime(old_export, (very_old, very_old))

            out = prune_scanner_quality_artifacts(td, max_age_days=14.0)
            self.assertGreaterEqual(int(out.get("removed", 0) or 0), 1)
            self.assertFalse(os.path.isfile(old_export))
            self.assertTrue(os.path.isfile(keep_export))


if __name__ == "__main__":
    unittest.main()

