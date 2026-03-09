from __future__ import annotations

import json
import os
import tempfile
import unittest
from unittest.mock import patch

from app import path_utils


class TestPathUtils(unittest.TestCase):
    def test_resolve_base_dir_runtime_module(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = os.path.join(td, "proj")
            runtime_dir = os.path.join(root, "runtime")
            os.makedirs(runtime_dir, exist_ok=True)
            fake_module = os.path.join(runtime_dir, "pt_runner.py")
            out = path_utils.resolve_base_dir(fake_module)
            self.assertEqual(out, os.path.abspath(root))

    def test_resolve_base_dir_env_override(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            prior = os.environ.get("POWERTRADER_PROJECT_DIR")
            try:
                os.environ["POWERTRADER_PROJECT_DIR"] = td
                out = path_utils.resolve_base_dir("/tmp/whatever/runtime/x.py")
                self.assertEqual(out, os.path.abspath(td))
            finally:
                if prior is None:
                    os.environ.pop("POWERTRADER_PROJECT_DIR", None)
                else:
                    os.environ["POWERTRADER_PROJECT_DIR"] = prior

    def test_resolve_settings_path_precedence(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root_settings = os.path.join(td, "gui_settings.json")
            hub_dir = os.path.join(td, "hub_data")
            os.makedirs(hub_dir, exist_ok=True)
            hub_settings = os.path.join(hub_dir, "gui_settings.json")
            env_settings = os.path.join(td, "env_settings.json")
            for p in (root_settings, hub_settings, env_settings):
                with open(p, "w", encoding="utf-8") as f:
                    json.dump({"ok": True}, f)

            prior = os.environ.get("POWERTRADER_GUI_SETTINGS")
            try:
                os.environ["POWERTRADER_GUI_SETTINGS"] = "env_settings.json"
                out = path_utils.resolve_settings_path(td)
                self.assertEqual(out, os.path.abspath(env_settings))
            finally:
                if prior is None:
                    os.environ.pop("POWERTRADER_GUI_SETTINGS", None)
                else:
                    os.environ["POWERTRADER_GUI_SETTINGS"] = prior

    def test_resolve_runtime_paths_creates_default_hub_dir(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = os.path.join(td, "proj")
            app_dir = os.path.join(root, "app")
            os.makedirs(app_dir, exist_ok=True)
            mod = os.path.join(app_dir, "mod.py")
            prior = os.environ.get("POWERTRADER_PROJECT_DIR")
            try:
                os.environ["POWERTRADER_PROJECT_DIR"] = root
                base, settings_path, hub_dir, settings_data = path_utils.resolve_runtime_paths(mod, module_name="")
                self.assertEqual(base, os.path.abspath(root))
                self.assertTrue(os.path.isdir(hub_dir))
                self.assertEqual(os.path.basename(hub_dir), "hub_data")
                self.assertIsNone(settings_path)
                self.assertEqual(settings_data, {})
            finally:
                if prior is None:
                    os.environ.pop("POWERTRADER_PROJECT_DIR", None)
                else:
                    os.environ["POWERTRADER_PROJECT_DIR"] = prior

    def test_log_throttled_respects_cooldown(self) -> None:
        key = "unit:test:throttle"
        path_utils._THROTTLED_LOG_TS.pop(key, None)  # type: ignore[attr-defined]
        with patch("app.path_utils.time.monotonic", side_effect=[1.0, 1.5, 3.2]), patch("builtins.print") as mock_print:
            path_utils.log_throttled(key, "hello", cooldown_s=1.0)
            path_utils.log_throttled(key, "hello", cooldown_s=1.0)
            path_utils.log_throttled(key, "hello", cooldown_s=1.0)
        self.assertEqual(mock_print.call_count, 2)

    def test_read_settings_file_recovers_partial_json(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "gui_settings.json")
            with open(path, "w", encoding="utf-8") as f:
                f.write('{\"coins\":[\"BTC\"],\"stock_auto_trade_enabled\":true}\\ngarbage-tail')
            out = path_utils.read_settings_file(path, module_name="unit_test")
            self.assertTrue(isinstance(out, dict))
            self.assertEqual(list(out.get("coins", []) or []), ["BTC"])
            self.assertTrue(bool(out.get("stock_auto_trade_enabled", False)))


if __name__ == "__main__":
    unittest.main()
