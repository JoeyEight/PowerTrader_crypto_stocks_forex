from __future__ import annotations

import os
import tempfile
import unittest

from app.operator_notes import (
    append_operator_note_entry,
    ensure_operator_notes_files,
    operator_notes_paths,
    read_operator_notes_markdown,
    read_recent_operator_note_entries,
    write_operator_notes_markdown,
)


class TestOperatorNotes(unittest.TestCase):
    def test_bootstrap_creates_files(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            md_path, log_path = ensure_operator_notes_files(td)
            self.assertTrue(os.path.isfile(md_path))
            self.assertTrue(os.path.isfile(log_path))
            text = read_operator_notes_markdown(md_path)
            self.assertIn("Operator Notes", text)

    def test_append_entry_writes_markdown_and_jsonl(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            md_path, log_path = ensure_operator_notes_files(td)
            row = append_operator_note_entry(td, "Scan Tuning", "Raised threshold for noise control.", actor="hub_ui")
            self.assertIn("title", row)
            text = read_operator_notes_markdown(md_path)
            self.assertIn("Scan Tuning", text)
            entries = read_recent_operator_note_entries(log_path, max_entries=20)
            self.assertGreaterEqual(len(entries), 1)
            self.assertEqual(str(entries[0].get("title", "")), "Scan Tuning")

    def test_write_markdown_replaces_content(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            md_path, _ = operator_notes_paths(td)
            ok = write_operator_notes_markdown(md_path, "# Operator Notes\n\nCustom block\n")
            self.assertTrue(ok)
            text = read_operator_notes_markdown(md_path)
            self.assertIn("Custom block", text)


if __name__ == "__main__":
    unittest.main()
