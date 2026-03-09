from __future__ import annotations

import json
import os
import tempfile
import unittest
from unittest.mock import patch

from runtime.tools.preflight_readiness import build_preflight_report


class TestPreflightReadiness(unittest.TestCase):
    def _write_json(self, path: str, payload: dict) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f)

    def _touch(self, path: str, content: str = "print('ok')\n") -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)

    def _seed_script_paths(self, base_dir: str) -> None:
        self._touch(os.path.join(base_dir, "engines", "pt_thinker.py"))
        self._touch(os.path.join(base_dir, "engines", "pt_trainer.py"))
        self._touch(os.path.join(base_dir, "engines", "pt_trader.py"))
        self._touch(os.path.join(base_dir, "runtime", "pt_markets.py"))
        self._touch(os.path.join(base_dir, "runtime", "pt_autopilot.py"))
        self._touch(os.path.join(base_dir, "runtime", "pt_autofix.py"))

    def test_report_passes_without_critical_issues(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            self._seed_script_paths(td)
            self._write_json(
                os.path.join(td, "gui_settings.json"),
                {
                    "stock_auto_trade_enabled": False,
                    "forex_auto_trade_enabled": False,
                    "market_rollout_stage": "shadow_only",
                    "alpaca_paper_mode": True,
                    "oanda_practice_mode": True,
                },
            )
            with patch.dict(os.environ, {}, clear=True):
                out = build_preflight_report(td, now_ts=1_700_000_000)
            self.assertTrue(bool(out.get("pass", False)))
            self.assertEqual(int((out.get("counts", {}) or {}).get("critical", 0)), 0)
            self.assertIn("scripts", out)
            self.assertIn("credentials", out)

    def test_report_flags_missing_broker_credentials_when_auto_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            self._seed_script_paths(td)
            self._write_json(
                os.path.join(td, "gui_settings.json"),
                {
                    "stock_auto_trade_enabled": True,
                    "forex_auto_trade_enabled": True,
                    "market_rollout_stage": "execution_v2",
                    "alpaca_paper_mode": True,
                    "oanda_practice_mode": True,
                },
            )
            with patch.dict(os.environ, {}, clear=True):
                out = build_preflight_report(td, now_ts=1_700_000_000)
            self.assertFalse(bool(out.get("pass", True)))
            critical = int((out.get("counts", {}) or {}).get("critical", 0))
            self.assertGreaterEqual(critical, 2)
            issues = list(out.get("issues", []) or [])
            codes = {str(i.get("code", "")) for i in issues}
            self.assertIn("alpaca_creds_missing", codes)
            self.assertIn("oanda_creds_missing", codes)

    def test_report_flags_runtime_credential_mismatch_when_runner_alive(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            self._seed_script_paths(td)
            self._write_json(
                os.path.join(td, "gui_settings.json"),
                {
                    "stock_auto_trade_enabled": False,
                    "forex_auto_trade_enabled": False,
                    "market_rollout_stage": "shadow_only",
                    "alpaca_paper_mode": True,
                    "oanda_practice_mode": True,
                },
            )
            keys_dir = os.path.join(td, "keys")
            os.makedirs(keys_dir, exist_ok=True)
            with open(os.path.join(keys_dir, "alpaca_key_id.txt"), "w", encoding="utf-8") as f:
                f.write("AKIA_TEST_KEY")
            with open(os.path.join(keys_dir, "alpaca_secret_key.txt"), "w", encoding="utf-8") as f:
                f.write("alpaca_secret_test")
            with open(os.path.join(keys_dir, "oanda_account_id.txt"), "w", encoding="utf-8") as f:
                f.write("001-001-1234567-001")
            with open(os.path.join(keys_dir, "oanda_api_token.txt"), "w", encoding="utf-8") as f:
                f.write("oanda_token_test")
            with open(os.path.join(keys_dir, "r_key.txt"), "w", encoding="utf-8") as f:
                f.write("rh_key_test")
            with open(os.path.join(keys_dir, "r_secret.txt"), "w", encoding="utf-8") as f:
                f.write("rh_secret_test")

            hub_dir = os.path.join(td, "hub_data")
            os.makedirs(os.path.join(hub_dir, "stocks"), exist_ok=True)
            os.makedirs(os.path.join(hub_dir, "forex"), exist_ok=True)
            with open(os.path.join(hub_dir, "runner.pid"), "w", encoding="utf-8") as f:
                f.write("4242")
            self._write_json(
                os.path.join(hub_dir, "stocks", "stock_thinker_status.json"),
                {"state": "NOT CONFIGURED", "msg": "Add Alpaca keys in Settings"},
            )
            self._write_json(
                os.path.join(hub_dir, "forex", "forex_thinker_status.json"),
                {"state": "NOT CONFIGURED", "msg": "Add OANDA account/token in Settings"},
            )

            with patch.dict(os.environ, {}, clear=True):
                with patch("runtime.tools.preflight_readiness._pid_is_alive", return_value=True):
                    out = build_preflight_report(td, now_ts=1_700_000_000)
            issues = list(out.get("issues", []) or [])
            codes = {str(i.get("code", "")) for i in issues}
            self.assertIn("market_loop_status_missing", codes)
            self.assertIn("stocks_cred_runtime_mismatch", codes)
            self.assertIn("forex_cred_runtime_mismatch", codes)

    def test_report_warns_when_ai_assist_key_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            self._seed_script_paths(td)
            self._write_json(
                os.path.join(td, "gui_settings.json"),
                {
                    "autofix_enabled": True,
                    "stock_auto_trade_enabled": False,
                    "forex_auto_trade_enabled": False,
                    "market_rollout_stage": "shadow_only",
                },
            )
            with patch.dict(os.environ, {}, clear=True):
                out = build_preflight_report(td, now_ts=1_700_000_000)
            issues = list(out.get("issues", []) or [])
            codes = {str(i.get("code", "")) for i in issues}
            self.assertIn("autofix_openai_key_missing", codes)


if __name__ == "__main__":
    unittest.main()
