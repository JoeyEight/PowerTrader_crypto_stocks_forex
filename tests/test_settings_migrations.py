from __future__ import annotations

import unittest

from app.settings_migrations import CURRENT_SETTINGS_VERSION, migrate_settings
from app.settings_utils import sanitize_settings


class TestSettingsMigrations(unittest.TestCase):
    def test_migrate_legacy_script_paths(self) -> None:
        raw = {
            "settings_schema_version": 1,
            "script_neural_runner2": "pt_thinker.py",
            "script_trader": "pt_trader.py",
            "script_markets_runner": "pt_markets.py",
        }
        out, notes, from_v, to_v = migrate_settings(raw)
        self.assertEqual(from_v, 1)
        self.assertEqual(to_v, int(CURRENT_SETTINGS_VERSION))
        self.assertEqual(str(out.get("script_neural_runner2", "")), "engines/pt_thinker.py")
        self.assertEqual(str(out.get("script_trader", "")), "engines/pt_trader.py")
        self.assertEqual(str(out.get("script_markets_runner", "")), "runtime/pt_markets.py")
        self.assertTrue(isinstance(notes, list))

    def test_sanitize_applies_schema_and_upgrade_notes(self) -> None:
        raw = {
            "settings_schema_version": 1,
            "script_autopilot": "pt_autopilot.py",
            "script_autofix": "pt_autofix.py",
        }
        out = sanitize_settings(raw)
        self.assertEqual(int(out.get("settings_schema_version", 0) or 0), int(CURRENT_SETTINGS_VERSION))
        notes = list(out.get("settings_upgrade_notes", []) or [])
        self.assertTrue(len(notes) >= 1)


if __name__ == "__main__":
    unittest.main()
