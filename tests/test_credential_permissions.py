from __future__ import annotations

import os
import tempfile
import unittest

from app.credential_utils import key_file_permission_issues, key_rotation_reminder_issues


class TestCredentialPermissions(unittest.TestCase):
    def test_detects_weak_permissions(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            keys = os.path.join(td, "keys")
            os.makedirs(keys, exist_ok=True)
            p = os.path.join(keys, "r_secret.txt")
            with open(p, "w", encoding="utf-8") as f:
                f.write("secret")
            os.chmod(p, 0o644)
            issues = key_file_permission_issues(td)
            self.assertTrue(any("weak_key_permissions" in x for x in issues))

    def test_accepts_strict_permissions(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            keys = os.path.join(td, "keys")
            os.makedirs(keys, exist_ok=True)
            p = os.path.join(keys, "r_secret.txt")
            with open(p, "w", encoding="utf-8") as f:
                f.write("secret")
            os.chmod(p, 0o600)
            issues = key_file_permission_issues(td)
            self.assertEqual(issues, [])

    def test_key_rotation_reminder_due(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            keys = os.path.join(td, "keys")
            os.makedirs(keys, exist_ok=True)
            p = os.path.join(keys, "r_secret.txt")
            with open(p, "w", encoding="utf-8") as f:
                f.write("secret")
            very_old = 1000000000
            os.utime(p, (very_old, very_old))
            issues = key_rotation_reminder_issues(td, max_age_days=30)
            self.assertTrue(any(x.startswith("key_rotation_due:") for x in issues))


if __name__ == "__main__":
    unittest.main()
