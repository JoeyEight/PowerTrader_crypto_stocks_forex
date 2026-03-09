from __future__ import annotations

import os
import tempfile
import unittest
from unittest.mock import patch

from app.credential_utils import get_alpaca_creds, get_alpaca_creds_from_files


class TestAlpacaCredentialResolution(unittest.TestCase):
    def test_reads_apca_env_variable_names(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            env = {
                "APCA_API_KEY_ID": "env_key_id",
                "APCA_API_SECRET_KEY": "env_secret_key",
            }
            with patch.dict(os.environ, env, clear=False):
                key, secret = get_alpaca_creds({}, base_dir=td)
            self.assertEqual(key, "env_key_id")
            self.assertEqual(secret, "env_secret_key")

    def test_reads_legacy_root_pair(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            with open(os.path.join(td, "alpaca_key_id.txt"), "w", encoding="utf-8") as f:
                f.write("legacy_key")
            with open(os.path.join(td, "alpaca_secret_key.txt"), "w", encoding="utf-8") as f:
                f.write("legacy_secret")
            key, secret = get_alpaca_creds_from_files(td)
            self.assertEqual(key, "legacy_key")
            self.assertEqual(secret, "legacy_secret")

    def test_combines_partial_primary_with_legacy_secret(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            keys_dir = os.path.join(td, "keys")
            os.makedirs(keys_dir, exist_ok=True)
            with open(os.path.join(keys_dir, "alpaca_key_id.txt"), "w", encoding="utf-8") as f:
                f.write("primary_key")
            with open(os.path.join(td, "alpaca_secret_key.txt"), "w", encoding="utf-8") as f:
                f.write("legacy_secret")
            key, secret = get_alpaca_creds_from_files(td)
            self.assertEqual(key, "primary_key")
            self.assertEqual(secret, "legacy_secret")


if __name__ == "__main__":
    unittest.main()
