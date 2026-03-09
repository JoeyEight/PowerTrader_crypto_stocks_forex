from __future__ import annotations

import os
import json
import tempfile
import unittest
from typing import Any, Dict
from unittest.mock import patch

import runtime.pt_autofix as pt_autofix


class TestAutofixRuntime(unittest.TestCase):
    def test_llm_patch_proposal_body_omits_temperature(self) -> None:
        captured: Dict[str, Any] = {}

        class _Resp:
            def __enter__(self) -> "_Resp":
                return self

            def __exit__(self, exc_type, exc, tb) -> bool:
                return False

            def read(self) -> bytes:
                payload = {
                    "output_text": json.dumps(
                        {
                            "summary": "s",
                            "target_files": ["ui/pt_hub.py"],
                            "diff": "diff --git a/ui/pt_hub.py b/ui/pt_hub.py\n",
                            "tests": [],
                        }
                    )
                }
                return json.dumps(payload).encode("utf-8")

        def _fake_urlopen(req, timeout=0):  # type: ignore[no-untyped-def]
            captured["body"] = json.loads((req.data or b"{}").decode("utf-8"))
            return _Resp()

        with patch.object(pt_autofix, "_resolve_openai_api_key", return_value="sk-test"), patch.object(
            pt_autofix, "_repo_context_for_ticket", return_value={}
        ), patch("runtime.pt_autofix.urllib.request.urlopen", side_effect=_fake_urlopen):
            out = pt_autofix._llm_patch_proposal({"id": "af_1", "incident": {}, "classifier": {}, "evidence": {}}, {})
        self.assertTrue(bool(out.get("ok", False)))
        body = captured.get("body", {}) if isinstance(captured.get("body", {}), dict) else {}
        self.assertNotIn("temperature", body)

    def test_resolve_openai_api_key_from_file(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            keys_dir = os.path.join(td, "keys")
            os.makedirs(keys_dir, exist_ok=True)
            key_path = os.path.join(keys_dir, "openai_api_key.txt")
            with open(key_path, "w", encoding="utf-8") as f:
                f.write("sk-test-local-key\n")
            with patch.dict(os.environ, {"OPENAI_API_KEY": ""}, clear=False), patch.object(pt_autofix, "BASE_DIR", td):
                key = pt_autofix._resolve_openai_api_key({})
            self.assertEqual(key, "sk-test-local-key")

    def test_llm_patch_proposal_missing_key_has_actionable_detail(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            with patch.dict(os.environ, {"OPENAI_API_KEY": ""}, clear=False), patch.object(pt_autofix, "BASE_DIR", td):
                out = pt_autofix._llm_patch_proposal({"id": "af_test"}, {})
            self.assertFalse(bool(out.get("ok", False)))
            self.assertEqual(str(out.get("error", "")), "missing_openai_api_key")
            self.assertIn("openai_api_key.txt", str(out.get("detail", "")))

    def test_classify_error_module_import(self) -> None:
        out = pt_autofix._classify_error("Traceback ... ModuleNotFoundError: No module named 'app'")
        self.assertEqual(str(out.get("kind", "")), "module_import")
        self.assertGreaterEqual(float(out.get("confidence", 0.0)), 0.9)

    def test_extract_trace_files_normalizes_paths(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            text = (
                f'Traceback (most recent call last):\n  File "{os.path.join(td, "runtime", "pt_runner.py")}", line 11, in <module>\n'
                '  File "engines/pt_trader.py", line 44, in run\n'
            )
            with patch.object(pt_autofix, "BASE_DIR", td):
                out = pt_autofix._extract_trace_files(text)
            self.assertGreaterEqual(len(out), 2)
            self.assertEqual(str(out[0].get("path", "")), "runtime/pt_runner.py")
            self.assertEqual(int(out[0].get("line", 0)), 11)
            self.assertEqual(str(out[1].get("path", "")), "engines/pt_trader.py")

    def test_is_code_incident(self) -> None:
        row = {"severity": "error", "event": "runner_child_exit", "msg": "TypeError: bad operand type"}
        self.assertTrue(pt_autofix._is_code_incident(row))
        self.assertFalse(pt_autofix._is_code_incident({"severity": "info", "msg": "all good"}))

    def test_can_apply_gates(self) -> None:
        ok, reason = pt_autofix._can_apply({"autofix_mode": "report_only"}, applied_count_day=0)
        self.assertFalse(ok)
        self.assertEqual(reason, "mode_not_shadow_apply")

        ok2, reason2 = pt_autofix._can_apply(
            {
                "autofix_mode": "shadow_apply",
                "market_rollout_stage": "live_guarded",
                "autofix_allow_live_apply": False,
                "autofix_max_fixes_per_day": 2,
            },
            applied_count_day=0,
        )
        self.assertFalse(ok2)
        self.assertEqual(reason2, "live_guarded_blocked")

        ok3, reason3 = pt_autofix._can_apply(
            {
                "autofix_mode": "shadow_apply",
                "market_rollout_stage": "shadow_only",
                "autofix_allow_live_apply": False,
                "autofix_max_fixes_per_day": 1,
            },
            applied_count_day=1,
        )
        self.assertFalse(ok3)
        self.assertEqual(reason3, "daily_limit_reached")

        ok4, reason4 = pt_autofix._can_apply(
            {
                "autofix_mode": "shadow_apply",
                "market_rollout_stage": "shadow_only",
                "autofix_allow_live_apply": False,
                "autofix_max_fixes_per_day": 2,
            },
            applied_count_day=1,
        )
        self.assertTrue(ok4)
        self.assertEqual(reason4, "ok")

        ok5, reason5 = pt_autofix._can_apply(
            {
                "autofix_mode": "report_only",
                "market_rollout_stage": "shadow_only",
                "autofix_allow_live_apply": False,
                "autofix_max_fixes_per_day": 2,
            },
            applied_count_day=0,
            manual_override=True,
        )
        self.assertTrue(ok5)
        self.assertEqual(reason5, "ok")

    def test_retry_delay_for_llm_error(self) -> None:
        self.assertGreaterEqual(
            pt_autofix._retry_delay_for_llm_error({"error": "http_429", "detail": "insufficient_quota"}, 90),
            1800,
        )
        self.assertGreaterEqual(
            pt_autofix._retry_delay_for_llm_error({"error": "URLError: dns"}, 90),
            180,
        )
        self.assertEqual(
            pt_autofix._retry_delay_for_llm_error({"error": "http_400", "detail": "invalid request"}, 90),
            90,
        )

    def test_llm_apply_block_reason(self) -> None:
        self.assertEqual(
            pt_autofix._llm_apply_block_reason({"error": "missing_openai_api_key"}),
            "missing_openai_api_key",
        )
        self.assertEqual(
            pt_autofix._llm_apply_block_reason({"error": "http_429", "detail": "insufficient_quota"}),
            "llm_quota_blocked",
        )
        self.assertEqual(
            pt_autofix._llm_apply_block_reason({"error": "URLError: dns"}),
            "llm_network_unreachable",
        )
        self.assertEqual(
            pt_autofix._llm_apply_block_reason({"error": "http_400"}),
            "llm_request_rejected",
        )

    def test_apply_ticket_once_marks_ticket_applied(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tickets_dir = os.path.join(td, "autofix", "tickets")
            patches_dir = os.path.join(td, "autofix", "patches")
            os.makedirs(tickets_dir, exist_ok=True)
            os.makedirs(patches_dir, exist_ok=True)
            ticket_id = "af_test_1"
            patch_path = os.path.join(patches_dir, f"{ticket_id}.diff")
            with open(patch_path, "w", encoding="utf-8") as f:
                f.write("diff --git a/a b/a\n")
            ticket_path = os.path.join(tickets_dir, f"{ticket_id}.json")
            with open(ticket_path, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "id": ticket_id,
                        "status": "open",
                        "proposal": {"patch_diff_path": patch_path},
                        "apply": {"attempted": False, "ok": False, "reason": "pending"},
                    },
                    f,
                )

            state_path = os.path.join(td, "autofix_state.json")
            status_path = os.path.join(td, "autofix_status.json")
            events_path = os.path.join(td, "runtime_events.jsonl")
            with patch.object(pt_autofix, "AUTOFIX_TICKETS_DIR", tickets_dir), patch.object(
                pt_autofix, "AUTOFIX_PATCHES_DIR", patches_dir
            ), patch.object(pt_autofix, "AUTOFIX_STATE_PATH", state_path), patch.object(
                pt_autofix, "AUTOFIX_STATUS_PATH", status_path
            ), patch.object(pt_autofix, "RUNTIME_EVENTS_PATH", events_path), patch.object(
                pt_autofix,
                "_load_settings",
                return_value=(
                    {
                        "autofix_enabled": True,
                        "autofix_mode": "manual",
                        "market_rollout_stage": "shadow_only",
                        "autofix_allow_live_apply": False,
                        "autofix_max_fixes_per_day": 2,
                    },
                    os.path.join(td, "gui_settings.json"),
                ),
            ), patch.object(
                pt_autofix,
                "_apply_patch",
                return_value={"attempted": True, "ok": True, "reason": "applied_and_tests_passed", "ts": 1},
            ):
                out = pt_autofix.apply_ticket_once(ticket_id)
            self.assertTrue(bool(out.get("ok", False)))
            row = pt_autofix._safe_read_json(ticket_path)
            self.assertEqual(str(row.get("status", "")), "applied")
            self.assertTrue(bool((row.get("apply", {}) if isinstance(row.get("apply", {}), dict) else {}).get("approved_manual", False)))

    def test_apply_ticket_once_respects_live_guard(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tickets_dir = os.path.join(td, "autofix", "tickets")
            patches_dir = os.path.join(td, "autofix", "patches")
            os.makedirs(tickets_dir, exist_ok=True)
            os.makedirs(patches_dir, exist_ok=True)
            ticket_id = "af_test_2"
            patch_path = os.path.join(patches_dir, f"{ticket_id}.diff")
            with open(patch_path, "w", encoding="utf-8") as f:
                f.write("diff --git a/a b/a\n")
            ticket_path = os.path.join(tickets_dir, f"{ticket_id}.json")
            with open(ticket_path, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "id": ticket_id,
                        "status": "open",
                        "proposal": {"patch_diff_path": patch_path},
                    },
                    f,
                )

            with patch.object(pt_autofix, "AUTOFIX_TICKETS_DIR", tickets_dir), patch.object(
                pt_autofix, "AUTOFIX_PATCHES_DIR", patches_dir
            ), patch.object(
                pt_autofix,
                "_load_settings",
                return_value=(
                    {
                        "autofix_enabled": True,
                        "autofix_mode": "manual",
                        "market_rollout_stage": "live_guarded",
                        "autofix_allow_live_apply": False,
                        "autofix_max_fixes_per_day": 2,
                    },
                    os.path.join(td, "gui_settings.json"),
                ),
            ):
                out = pt_autofix.apply_ticket_once(ticket_id)
            self.assertFalse(bool(out.get("ok", False)))
            self.assertEqual(str(out.get("reason", "")), "live_guarded_blocked")

    def test_create_request_ticket_requires_text(self) -> None:
        out = pt_autofix.create_request_ticket("")
        self.assertFalse(bool(out.get("ok", False)))
        self.assertEqual(str(out.get("reason", "")), "missing_request_text")

    def test_create_request_ticket_writes_open_ticket(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tickets_dir = os.path.join(td, "autofix", "tickets")
            patches_dir = os.path.join(td, "autofix", "patches")
            os.makedirs(tickets_dir, exist_ok=True)
            os.makedirs(patches_dir, exist_ok=True)
            state_path = os.path.join(td, "autofix_state.json")
            status_path = os.path.join(td, "autofix_status.json")
            events_path = os.path.join(td, "runtime_events.jsonl")
            with patch.object(pt_autofix, "AUTOFIX_TICKETS_DIR", tickets_dir), patch.object(
                pt_autofix, "AUTOFIX_PATCHES_DIR", patches_dir
            ), patch.object(pt_autofix, "AUTOFIX_STATE_PATH", state_path), patch.object(
                pt_autofix, "AUTOFIX_STATUS_PATH", status_path
            ), patch.object(pt_autofix, "RUNTIME_EVENTS_PATH", events_path), patch.object(
                pt_autofix,
                "_load_settings",
                return_value=(
                    {
                        "autofix_enabled": True,
                        "autofix_mode": "manual",
                        "market_rollout_stage": "shadow_only",
                        "autofix_allow_live_apply": False,
                        "autofix_max_fixes_per_day": 2,
                    },
                    os.path.join(td, "gui_settings.json"),
                ),
            ), patch.object(
                pt_autofix,
                "_llm_patch_proposal",
                return_value={
                    "used": True,
                    "ok": True,
                    "summary": "Patch summary",
                    "diff": "diff --git a/a b/a\n",
                    "tests": ["python -m unittest tests.test_health_rules"],
                    "target_files": ["ui/pt_hub.py"],
                },
            ):
                out = pt_autofix.create_request_ticket("Please improve Stocks leaders reason text.", auto_apply=False)
            self.assertTrue(bool(out.get("ok", False)))
            tid = str(out.get("ticket_id", "") or "")
            self.assertTrue(bool(tid))
            tpath = os.path.join(tickets_dir, f"{tid}.json")
            self.assertTrue(os.path.isfile(tpath))
            row = pt_autofix._safe_read_json(tpath)
            self.assertEqual(str((row.get("classifier", {}) if isinstance(row.get("classifier", {}), dict) else {}).get("kind", "")), "user_request")
            self.assertEqual(str((row.get("status", "") or "")), "open")
            proposal = row.get("proposal", {}) if isinstance(row.get("proposal", {}), dict) else {}
            self.assertTrue(bool(proposal.get("patch_diff_path", "")))
            risk = proposal.get("risk", {}) if isinstance(proposal.get("risk", {}), dict) else {}
            self.assertTrue(bool(str(risk.get("level", "") or "").strip()))
            diff_stats = proposal.get("diff_stats", {}) if isinstance(proposal.get("diff_stats", {}), dict) else {}
            self.assertGreaterEqual(int(diff_stats.get("changed", 0) or 0), 0)

    def test_create_request_ticket_auto_apply_sets_missing_patch_reason(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tickets_dir = os.path.join(td, "autofix", "tickets")
            patches_dir = os.path.join(td, "autofix", "patches")
            os.makedirs(tickets_dir, exist_ok=True)
            os.makedirs(patches_dir, exist_ok=True)
            state_path = os.path.join(td, "autofix_state.json")
            status_path = os.path.join(td, "autofix_status.json")
            events_path = os.path.join(td, "runtime_events.jsonl")
            with patch.object(pt_autofix, "AUTOFIX_TICKETS_DIR", tickets_dir), patch.object(
                pt_autofix, "AUTOFIX_PATCHES_DIR", patches_dir
            ), patch.object(pt_autofix, "AUTOFIX_STATE_PATH", state_path), patch.object(
                pt_autofix, "AUTOFIX_STATUS_PATH", status_path
            ), patch.object(pt_autofix, "RUNTIME_EVENTS_PATH", events_path), patch.object(
                pt_autofix,
                "_load_settings",
                return_value=(
                    {
                        "autofix_enabled": True,
                        "autofix_mode": "report_only",
                        "market_rollout_stage": "live_guarded",
                        "autofix_allow_live_apply": False,
                        "autofix_max_fixes_per_day": 2,
                    },
                    os.path.join(td, "gui_settings.json"),
                ),
            ), patch.object(
                pt_autofix,
                "_llm_patch_proposal",
                return_value={"used": True, "ok": False, "error": "model_output_not_json"},
            ):
                out = pt_autofix.create_request_ticket("Fix chart autosizing.", auto_apply=True)
            self.assertTrue(bool(out.get("ok", False)))
            self.assertEqual(str(out.get("apply_reason", "")), "missing_patch_file")
            tid = str(out.get("ticket_id", "") or "")
            row = pt_autofix._safe_read_json(os.path.join(tickets_dir, f"{tid}.json"))
            apply = row.get("apply", {}) if isinstance(row.get("apply", {}), dict) else {}
            self.assertEqual(str(apply.get("reason", "")), "missing_patch_file")
            self.assertTrue(bool(apply.get("requested_auto_apply", False)))

    def test_create_request_ticket_records_force_apply_requested(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tickets_dir = os.path.join(td, "autofix", "tickets")
            patches_dir = os.path.join(td, "autofix", "patches")
            os.makedirs(tickets_dir, exist_ok=True)
            os.makedirs(patches_dir, exist_ok=True)
            state_path = os.path.join(td, "autofix_state.json")
            status_path = os.path.join(td, "autofix_status.json")
            events_path = os.path.join(td, "runtime_events.jsonl")
            with patch.object(pt_autofix, "AUTOFIX_TICKETS_DIR", tickets_dir), patch.object(
                pt_autofix, "AUTOFIX_PATCHES_DIR", patches_dir
            ), patch.object(pt_autofix, "AUTOFIX_STATE_PATH", state_path), patch.object(
                pt_autofix, "AUTOFIX_STATUS_PATH", status_path
            ), patch.object(pt_autofix, "RUNTIME_EVENTS_PATH", events_path), patch.object(
                pt_autofix,
                "_load_settings",
                return_value=(
                    {
                        "autofix_enabled": True,
                        "autofix_mode": "report_only",
                        "market_rollout_stage": "live_guarded",
                        "autofix_allow_live_apply": False,
                        "autofix_max_fixes_per_day": 2,
                    },
                    os.path.join(td, "gui_settings.json"),
                ),
            ), patch.object(
                pt_autofix,
                "_llm_patch_proposal",
                return_value={"used": True, "ok": False, "error": "model_output_not_json"},
            ):
                out = pt_autofix.create_request_ticket(
                    "Make leaders reason explain logic.",
                    auto_apply=True,
                    force_apply=True,
                )
            self.assertTrue(bool(out.get("ok", False)))
            self.assertTrue(bool(out.get("auto_apply_requested", False)))
            self.assertTrue(bool(out.get("force_apply_requested", False)))
            tid = str(out.get("ticket_id", "") or "")
            row = pt_autofix._safe_read_json(os.path.join(tickets_dir, f"{tid}.json"))
            req = row.get("request", {}) if isinstance(row.get("request", {}), dict) else {}
            self.assertTrue(bool(req.get("auto_apply_requested", False)))
            self.assertTrue(bool(req.get("force_apply_requested", False)))

    def test_create_request_ticket_auto_apply_quota_blocked_reason(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tickets_dir = os.path.join(td, "autofix", "tickets")
            patches_dir = os.path.join(td, "autofix", "patches")
            os.makedirs(tickets_dir, exist_ok=True)
            os.makedirs(patches_dir, exist_ok=True)
            state_path = os.path.join(td, "autofix_state.json")
            status_path = os.path.join(td, "autofix_status.json")
            events_path = os.path.join(td, "runtime_events.jsonl")
            with patch.object(pt_autofix, "AUTOFIX_TICKETS_DIR", tickets_dir), patch.object(
                pt_autofix, "AUTOFIX_PATCHES_DIR", patches_dir
            ), patch.object(pt_autofix, "AUTOFIX_STATE_PATH", state_path), patch.object(
                pt_autofix, "AUTOFIX_STATUS_PATH", status_path
            ), patch.object(pt_autofix, "RUNTIME_EVENTS_PATH", events_path), patch.object(
                pt_autofix,
                "_load_settings",
                return_value=(
                    {
                        "autofix_enabled": True,
                        "autofix_mode": "report_only",
                        "market_rollout_stage": "live_guarded",
                        "autofix_allow_live_apply": False,
                        "autofix_max_fixes_per_day": 2,
                    },
                    os.path.join(td, "gui_settings.json"),
                ),
            ), patch.object(
                pt_autofix,
                "_llm_patch_proposal",
                return_value={
                    "used": True,
                    "ok": False,
                    "error": "http_429",
                    "detail": "insufficient_quota",
                },
            ):
                out = pt_autofix.create_request_ticket(
                    "Please resize charts.",
                    auto_apply=True,
                    force_apply=True,
                )
            self.assertTrue(bool(out.get("ok", False)))
            self.assertEqual(str(out.get("apply_reason", "")), "llm_quota_blocked")
            tid = str(out.get("ticket_id", "") or "")
            row = pt_autofix._safe_read_json(os.path.join(tickets_dir, f"{tid}.json"))
            self.assertEqual(str(row.get("status", "") or ""), "blocked")
            apply = row.get("apply", {}) if isinstance(row.get("apply", {}), dict) else {}
            self.assertEqual(str(apply.get("reason", "")), "llm_quota_blocked")
            blocked = row.get("blocked", {}) if isinstance(row.get("blocked", {}), dict) else {}
            self.assertEqual(str(blocked.get("reason", "")), "llm_quota_blocked")

    def test_create_request_ticket_ticket_only_quota_blocks_immediately(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tickets_dir = os.path.join(td, "autofix", "tickets")
            patches_dir = os.path.join(td, "autofix", "patches")
            os.makedirs(tickets_dir, exist_ok=True)
            os.makedirs(patches_dir, exist_ok=True)
            state_path = os.path.join(td, "autofix_state.json")
            status_path = os.path.join(td, "autofix_status.json")
            events_path = os.path.join(td, "runtime_events.jsonl")
            with patch.object(pt_autofix, "AUTOFIX_TICKETS_DIR", tickets_dir), patch.object(
                pt_autofix, "AUTOFIX_PATCHES_DIR", patches_dir
            ), patch.object(pt_autofix, "AUTOFIX_STATE_PATH", state_path), patch.object(
                pt_autofix, "AUTOFIX_STATUS_PATH", status_path
            ), patch.object(pt_autofix, "RUNTIME_EVENTS_PATH", events_path), patch.object(
                pt_autofix,
                "_load_settings",
                return_value=(
                    {
                        "autofix_enabled": True,
                        "autofix_mode": "report_only",
                        "market_rollout_stage": "live_guarded",
                        "autofix_allow_live_apply": False,
                        "autofix_max_fixes_per_day": 2,
                    },
                    os.path.join(td, "gui_settings.json"),
                ),
            ), patch.object(
                pt_autofix,
                "_llm_patch_proposal",
                return_value={
                    "used": True,
                    "ok": False,
                    "error": "http_429",
                    "detail": "insufficient_quota",
                },
            ):
                out = pt_autofix.create_request_ticket("Please resize charts.", auto_apply=False, force_apply=False)
            self.assertTrue(bool(out.get("ok", False)))
            self.assertEqual(str(out.get("status", "") or ""), "blocked")
            self.assertEqual(str(out.get("blocked_reason", "") or ""), "llm_quota_blocked")
            self.assertEqual(str(out.get("apply_reason", "") or ""), "llm_quota_blocked")
            tid = str(out.get("ticket_id", "") or "")
            row = pt_autofix._safe_read_json(os.path.join(tickets_dir, f"{tid}.json"))
            self.assertEqual(str(row.get("status", "") or ""), "blocked")
            apply = row.get("apply", {}) if isinstance(row.get("apply", {}), dict) else {}
            self.assertEqual(str(apply.get("reason", "") or ""), "llm_quota_blocked")

    def test_run_once_retries_open_user_request_ticket_and_applies(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tickets_dir = os.path.join(td, "autofix", "tickets")
            patches_dir = os.path.join(td, "autofix", "patches")
            os.makedirs(tickets_dir, exist_ok=True)
            os.makedirs(patches_dir, exist_ok=True)
            state_path = os.path.join(td, "autofix_state.json")
            status_path = os.path.join(td, "autofix_status.json")
            events_path = os.path.join(td, "runtime_events.jsonl")
            incidents_path = os.path.join(td, "incidents.jsonl")
            log_path = os.path.join(td, "logs", "autofix.log")
            os.makedirs(os.path.dirname(log_path), exist_ok=True)

            ticket_id = "af_req_retry_1"
            ticket_path = os.path.join(tickets_dir, f"{ticket_id}.json")
            with open(ticket_path, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "id": ticket_id,
                        "status": "open",
                        "classifier": {"kind": "user_request", "confidence": 1.0, "match": "assistant_chat"},
                        "request": {
                            "text": "Make stocks chart resize like crypto",
                            "submitted_ts": 1,
                            "auto_apply_requested": True,
                            "force_apply_requested": True,
                        },
                        "proposal": {
                            "summary": "retry candidate",
                            "patch_diff_path": "",
                        },
                        "apply": {
                            "attempted": False,
                            "ok": False,
                            "reason": "missing_patch_file",
                            "ts": 1,
                            "approved_manual": False,
                            "requested_auto_apply": True,
                        },
                        "incident": {"msg": "user request"},
                        "evidence": {"trace_files": [], "log_tail": []},
                    },
                    f,
                )
            patch_path = os.path.join(patches_dir, f"{ticket_id}.diff")
            with open(patch_path, "w", encoding="utf-8") as f:
                f.write("diff --git a/ui/pt_hub.py b/ui/pt_hub.py\n")

            with patch.object(pt_autofix, "AUTOFIX_TICKETS_DIR", tickets_dir), patch.object(
                pt_autofix, "AUTOFIX_PATCHES_DIR", patches_dir
            ), patch.object(pt_autofix, "AUTOFIX_STATE_PATH", state_path), patch.object(
                pt_autofix, "AUTOFIX_STATUS_PATH", status_path
            ), patch.object(pt_autofix, "RUNTIME_EVENTS_PATH", events_path), patch.object(
                pt_autofix, "INCIDENTS_PATH", incidents_path
            ), patch.object(
                pt_autofix, "AUTOFIX_LOG_PATH", log_path
            ), patch.object(
                pt_autofix,
                "_read_jsonl_incremental",
                return_value=([], 0),
            ), patch.object(
                pt_autofix,
                "_load_settings",
                return_value=(
                    {
                        "autofix_enabled": True,
                        "autofix_mode": "report_only",
                        "market_rollout_stage": "live_guarded",
                        "autofix_allow_live_apply": False,
                        "autofix_max_fixes_per_day": 2,
                        "autofix_request_retries_per_tick": 2,
                    },
                    os.path.join(td, "gui_settings.json"),
                ),
            ), patch.object(
                pt_autofix,
                "_llm_patch_proposal",
                return_value={
                    "used": True,
                    "ok": True,
                    "summary": "resize stocks chart with container events",
                    "diff": "diff --git a/ui/pt_hub.py b/ui/pt_hub.py\n",
                    "tests": ["python -m unittest tests.test_ui_chart_layout"],
                    "target_files": ["ui/pt_hub.py"],
                },
            ), patch.object(
                pt_autofix,
                "_write_patch",
                return_value=patch_path,
            ), patch.object(
                pt_autofix,
                "_apply_patch",
                return_value={"attempted": True, "ok": True, "reason": "applied_and_tests_passed", "ts": 1},
            ):
                out = pt_autofix.run_once(dry_run=False)

            self.assertEqual(int(out.get("request_retry_attempted", 0) or 0), 1)
            self.assertEqual(int(out.get("request_retry_applied", 0) or 0), 1)
            self.assertEqual(str(out.get("last_ticket_id", "")), ticket_id)
            row = pt_autofix._safe_read_json(ticket_path)
            self.assertEqual(str(row.get("status", "")), "applied")
            apply = row.get("apply", {}) if isinstance(row.get("apply", {}), dict) else {}
            self.assertTrue(bool(apply.get("attempted", False)))
            self.assertTrue(bool(apply.get("ok", False)))
            self.assertEqual(str(apply.get("reason", "")), "applied_and_tests_passed")
            retry = row.get("request_retry", {}) if isinstance(row.get("request_retry", {}), dict) else {}
            self.assertGreater(int(retry.get("next_retry_ts", 0) or 0), 0)
            proposal = row.get("proposal", {}) if isinstance(row.get("proposal", {}), dict) else {}
            self.assertEqual(str(proposal.get("patch_diff_path", "")), patch_path)

    def test_run_once_blocks_quota_ticket_after_retry_exhausted(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tickets_dir = os.path.join(td, "autofix", "tickets")
            patches_dir = os.path.join(td, "autofix", "patches")
            os.makedirs(tickets_dir, exist_ok=True)
            os.makedirs(patches_dir, exist_ok=True)
            state_path = os.path.join(td, "autofix_state.json")
            status_path = os.path.join(td, "autofix_status.json")
            events_path = os.path.join(td, "runtime_events.jsonl")
            incidents_path = os.path.join(td, "incidents.jsonl")
            log_path = os.path.join(td, "logs", "autofix.log")
            os.makedirs(os.path.dirname(log_path), exist_ok=True)

            ticket_id = "af_req_block_1"
            ticket_path = os.path.join(tickets_dir, f"{ticket_id}.json")
            with open(ticket_path, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "id": ticket_id,
                        "status": "open",
                        "classifier": {"kind": "user_request", "confidence": 1.0, "match": "assistant_chat"},
                        "request": {"text": "Resize stocks chart", "auto_apply_requested": True, "force_apply_requested": True},
                        "proposal": {
                            "summary": "quota-blocked",
                            "patch_diff_path": "",
                            "llm": {"used": True, "ok": False, "error": "http_429", "detail": "insufficient_quota"},
                        },
                        "request_retry": {"attempts": 3, "last_ts": 1, "last_error": "http_429"},
                        "apply": {"attempted": False, "ok": False, "reason": "llm_quota_blocked", "ts": 1},
                        "incident": {"msg": "user request"},
                        "evidence": {"trace_files": [], "log_tail": []},
                    },
                    f,
                )

            with patch.object(pt_autofix, "AUTOFIX_TICKETS_DIR", tickets_dir), patch.object(
                pt_autofix, "AUTOFIX_PATCHES_DIR", patches_dir
            ), patch.object(pt_autofix, "AUTOFIX_STATE_PATH", state_path), patch.object(
                pt_autofix, "AUTOFIX_STATUS_PATH", status_path
            ), patch.object(pt_autofix, "RUNTIME_EVENTS_PATH", events_path), patch.object(
                pt_autofix, "INCIDENTS_PATH", incidents_path
            ), patch.object(
                pt_autofix, "AUTOFIX_LOG_PATH", log_path
            ), patch.object(
                pt_autofix, "_read_jsonl_incremental", return_value=([], 0)
            ), patch.object(
                pt_autofix,
                "_load_settings",
                return_value=(
                    {
                        "autofix_enabled": True,
                        "autofix_mode": "report_only",
                        "market_rollout_stage": "live_guarded",
                        "autofix_allow_live_apply": False,
                        "autofix_max_fixes_per_day": 2,
                        "autofix_request_retry_max_attempts": 3,
                    },
                    os.path.join(td, "gui_settings.json"),
                ),
            ), patch.object(pt_autofix, "_llm_patch_proposal") as llm_mock:
                out = pt_autofix.run_once(dry_run=False)

            llm_mock.assert_not_called()
            self.assertEqual(int(out.get("request_retry_attempted", 0) or 0), 0)
            self.assertEqual(int(out.get("request_retry_blocked", 0) or 0), 1)
            row = pt_autofix._safe_read_json(ticket_path)
            self.assertEqual(str(row.get("status", "")), "blocked")
            blocked = row.get("blocked", {}) if isinstance(row.get("blocked", {}), dict) else {}
            self.assertEqual(str(blocked.get("reason", "")), "llm_quota_blocked_retries_exhausted")
            apply = row.get("apply", {}) if isinstance(row.get("apply", {}), dict) else {}
            self.assertEqual(str(apply.get("reason", "")), "llm_quota_blocked_retries_exhausted")

    def test_run_once_ticket_includes_proposal_risk(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tickets_dir = os.path.join(td, "autofix", "tickets")
            patches_dir = os.path.join(td, "autofix", "patches")
            os.makedirs(tickets_dir, exist_ok=True)
            os.makedirs(patches_dir, exist_ok=True)
            state_path = os.path.join(td, "autofix_state.json")
            status_path = os.path.join(td, "autofix_status.json")
            events_path = os.path.join(td, "runtime_events.jsonl")
            incidents_path = os.path.join(td, "incidents.jsonl")

            incident_row = {
                "ts": 1,
                "severity": "error",
                "event": "runner_child_exit",
                "component": "runner",
                "msg": "ModuleNotFoundError: No module named 'app'",
                "details": {"child": "markets"},
            }
            with patch.object(pt_autofix, "AUTOFIX_TICKETS_DIR", tickets_dir), patch.object(
                pt_autofix, "AUTOFIX_PATCHES_DIR", patches_dir
            ), patch.object(pt_autofix, "AUTOFIX_STATE_PATH", state_path), patch.object(
                pt_autofix, "AUTOFIX_STATUS_PATH", status_path
            ), patch.object(pt_autofix, "RUNTIME_EVENTS_PATH", events_path), patch.object(
                pt_autofix, "INCIDENTS_PATH", incidents_path
            ), patch.object(
                pt_autofix,
                "_read_jsonl_incremental",
                return_value=([incident_row], 1),
            ), patch.object(
                pt_autofix,
                "_load_settings",
                return_value=(
                    {
                        "autofix_enabled": True,
                        "autofix_mode": "report_only",
                        "market_rollout_stage": "shadow_only",
                        "autofix_allow_live_apply": False,
                        "autofix_max_fixes_per_day": 2,
                    },
                    os.path.join(td, "gui_settings.json"),
                ),
            ), patch.object(
                pt_autofix,
                "_llm_patch_proposal",
                return_value={
                    "used": True,
                    "ok": True,
                    "summary": "Fix import path",
                    "diff": "diff --git a/runtime/pt_runner.py b/runtime/pt_runner.py\n+from app.path_utils import resolve_runtime_paths\n",
                    "tests": ["python -m unittest tests.test_runner_watchdog"],
                    "target_files": ["runtime/pt_runner.py"],
                },
            ):
                out = pt_autofix.run_once(dry_run=False)
            self.assertGreaterEqual(int(out.get("tickets_created", 0) or 0), 1)
            paths = [os.path.join(tickets_dir, n) for n in os.listdir(tickets_dir) if n.endswith(".json")]
            self.assertTrue(bool(paths))
            row = pt_autofix._safe_read_json(paths[0])
            proposal = row.get("proposal", {}) if isinstance(row.get("proposal", {}), dict) else {}
            risk = proposal.get("risk", {}) if isinstance(proposal.get("risk", {}), dict) else {}
            self.assertIn(str(risk.get("level", "") or ""), {"low", "medium", "high"})
            diff_stats = proposal.get("diff_stats", {}) if isinstance(proposal.get("diff_stats", {}), dict) else {}
            self.assertGreaterEqual(int(diff_stats.get("changed", 0) or 0), 1)


if __name__ == "__main__":
    unittest.main()
