from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Tuple

if __package__ in (None, ""):
    _ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if _ROOT not in sys.path:
        sys.path.insert(0, _ROOT)

from app.credential_utils import get_openai_api_key, openai_credential_path
from app.path_utils import read_settings_file, resolve_runtime_paths, resolve_settings_path
from app.runtime_logging import atomic_write_json, runtime_event
from app.settings_utils import sanitize_settings

BASE_DIR, _SETTINGS_PATH, HUB_DATA_DIR, _BOOT_SETTINGS = resolve_runtime_paths(__file__, "pt_autofix")
STOP_FLAG_PATH = os.path.join(HUB_DATA_DIR, "stop_trading.flag")
INCIDENTS_PATH = os.path.join(HUB_DATA_DIR, "incidents.jsonl")
RUNTIME_EVENTS_PATH = os.path.join(HUB_DATA_DIR, "runtime_events.jsonl")

LOG_DIR = os.path.join(HUB_DATA_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
AUTOFIX_LOG_PATH = os.path.join(LOG_DIR, "autofix.log")

AUTOFIX_STATUS_PATH = os.path.join(HUB_DATA_DIR, "autofix_status.json")
AUTOFIX_STATE_PATH = os.path.join(HUB_DATA_DIR, "autofix_state.json")
AUTOFIX_DIR = os.path.join(HUB_DATA_DIR, "autofix")
AUTOFIX_TICKETS_DIR = os.path.join(AUTOFIX_DIR, "tickets")
AUTOFIX_PATCHES_DIR = os.path.join(AUTOFIX_DIR, "patches")

MAX_RECENT_FINGERPRINTS = 400
MAX_TICKETS_PER_TICK = 3
MAX_REQUEST_RETRIES_PER_TICK = 3
MAX_REQUEST_RETRY_MAX_ATTEMPTS = 3
MAX_REQUEST_CHARS = 8000
DEFAULT_TEST_COMMAND = "python -m unittest tests.test_settings_sanitize tests.test_runner_watchdog"

_EXCEPTION_PATTERNS: List[Tuple[str, str, float]] = [
    ("modulenotfounderror", "module_import", 0.95),
    ("importerror", "module_import", 0.9),
    ("filenotfounderror", "file_not_found", 0.9),
    ("no such file or directory", "file_not_found", 0.85),
    ("syntaxerror", "syntax", 0.95),
    ("indentationerror", "syntax", 0.95),
    ("nameerror", "name", 0.9),
    ("attributeerror", "attribute", 0.9),
    ("typeerror", "type", 0.85),
    ("keyerror", "key", 0.75),
    ("valueerror", "value", 0.7),
]
_REPO_CONTEXT_DIRS: Tuple[str, ...] = ("ui", "runtime", "engines", "app", "brokers")
_REPO_CONTEXT_EXTS: Tuple[str, ...] = (".py", ".json", ".md")
_REPO_CONTEXT_KEYWORD_STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "that",
    "this",
    "from",
    "into",
    "when",
    "where",
    "what",
    "have",
    "will",
    "make",
    "should",
    "need",
    "able",
    "code",
    "app",
    "ai",
    "assist",
    "ticket",
    "request",
    "change",
    "changes",
    "update",
    "updates",
    "error",
}


def _log(msg: str, level: str = "info") -> None:
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}\n"
    try:
        with open(AUTOFIX_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass
    runtime_event(
        RUNTIME_EVENTS_PATH,
        component="autofix",
        event="log",
        level=str(level or "info").lower(),
        msg=str(msg or ""),
    )


def _safe_read_json(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _atomic_write_json(path: str, payload: Dict[str, Any]) -> None:
    atomic_write_json(path, payload)


def _load_settings() -> Tuple[Dict[str, Any], str]:
    settings_path = resolve_settings_path(BASE_DIR) or _SETTINGS_PATH or os.path.join(BASE_DIR, "gui_settings.json")
    raw = read_settings_file(settings_path, module_name="pt_autofix") or {}
    return sanitize_settings(raw if isinstance(raw, dict) else {}), str(settings_path)


def _read_jsonl_incremental(path: str, offset: int, fallback_bytes: int = 500_000) -> Tuple[List[Dict[str, Any]], int]:
    if not os.path.isfile(path):
        return [], 0
    rows: List[Dict[str, Any]] = []
    try:
        size = int(os.path.getsize(path))
    except Exception:
        return [], 0

    off = int(offset or 0)
    if off < 0 or off > size:
        off = max(0, size - max(0, int(fallback_bytes)))

    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            f.seek(off)
            chunk = str(f.read() or "")
            new_off = int(f.tell())
    except Exception:
        return [], off

    for line in chunk.splitlines():
        ln = str(line or "").strip()
        if not ln:
            continue
        try:
            row = json.loads(ln)
        except Exception:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows, new_off


def _read_log_tail(path: str, max_lines: int = 80) -> List[str]:
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            lines = [str(ln or "").rstrip("\n") for ln in f]
    except Exception:
        return []
    return lines[-max(1, int(max_lines)) :]


def _extract_trace_files(text: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen = set()
    for hit in re.finditer(r'File "([^"]+)", line\s+(\d+)', str(text or "")):
        raw_path = str(hit.group(1) or "").strip()
        if not raw_path:
            continue
        try:
            line_no = int(hit.group(2))
        except Exception:
            line_no = 0
        abs_path = raw_path if os.path.isabs(raw_path) else os.path.abspath(os.path.join(BASE_DIR, raw_path))
        rel_path = abs_path
        if abs_path.startswith(BASE_DIR):
            rel_path = os.path.relpath(abs_path, BASE_DIR).replace("\\", "/")
        key = (rel_path, int(line_no))
        if key in seen:
            continue
        seen.add(key)
        out.append({"path": rel_path, "line": int(max(0, line_no))})
        if len(out) >= 12:
            break
    return out


def _classify_error(text: str) -> Dict[str, Any]:
    t = str(text or "")
    lo = t.lower()
    for needle, kind, conf in _EXCEPTION_PATTERNS:
        if needle in lo:
            return {"kind": kind, "confidence": float(conf), "match": needle}
    if "traceback" in lo:
        return {"kind": "unknown_traceback", "confidence": 0.5, "match": "traceback"}
    return {"kind": "unknown", "confidence": 0.0, "match": ""}


def _is_code_incident(row: Dict[str, Any]) -> bool:
    if not isinstance(row, dict):
        return False
    sev = str(row.get("severity", "") or "").strip().lower()
    if sev not in {"error", "critical", "warning"}:
        return False
    msg = str(row.get("msg", "") or "")
    evt = str(row.get("event", "") or "")
    details = row.get("details", {}) if isinstance(row.get("details", {}), dict) else {}
    joined = " ".join(
        [
            msg,
            evt,
            str(details.get("error", "") or ""),
            str(details.get("exception", "") or ""),
        ]
    ).lower()
    markers = (
        "traceback",
        "exception",
        "modulenotfounderror",
        "importerror",
        "attributeerror",
        "typeerror",
        "nameerror",
        "syntaxerror",
        "filenotfounderror",
        "keyerror",
        "valueerror",
    )
    if any(m in joined for m in markers):
        return True
    if "runner_child_exit" in joined and sev in {"error", "critical"}:
        return True
    return False


def _incident_fingerprint(row: Dict[str, Any]) -> str:
    component = str(row.get("component", "") or "").strip().lower()
    event = str(row.get("event", "") or "").strip().lower()
    msg = str(row.get("msg", "") or "").strip().lower()[:240]
    details = row.get("details", {}) if isinstance(row.get("details", {}), dict) else {}
    child = str(details.get("child", "") or "").strip().lower()
    return f"{component}|{event}|{child}|{msg}"


def _suggestion_for_kind(kind: str) -> str:
    mapping = {
        "module_import": "Check package import paths and PYTHONPATH; verify relocated modules use package paths.",
        "file_not_found": "Verify runtime file paths and ensure required artifacts/scripts exist under the package structure.",
        "syntax": "Patch syntax/indentation in the failing file and run targeted unit tests.",
        "name": "Initialize missing variables or update renamed symbols across the refactor.",
        "attribute": "Update object attribute access to match current data model and broker payload schemas.",
        "type": "Normalize input types and guard casts before calculations/JSON serialization.",
        "key": "Use defensive `.get()` reads or schema migrations for missing keys.",
        "value": "Validate bounds and sanitize settings before applying runtime values.",
        "unknown_traceback": "Inspect traceback file/line and generate a minimal patch with a reproducer test.",
        "unknown": "Manual triage needed; extract traceback and failing component logs first.",
    }
    return str(mapping.get(str(kind or "unknown"), mapping["unknown"]))


def _child_log_path(child: str) -> str:
    name = str(child or "").strip().lower()
    if name in {"thinker", "trader", "markets", "autopilot", "autofix"}:
        return os.path.join(LOG_DIR, f"{name}.log")
    return ""


def _build_ticket(row: Dict[str, Any], settings: Dict[str, Any], seq: int) -> Dict[str, Any]:
    now = int(time.time())
    details = row.get("details", {}) if isinstance(row.get("details", {}), dict) else {}
    child = str(details.get("child", "") or "").strip().lower()
    log_tail = _read_log_tail(_child_log_path(child), max_lines=120) if child else []

    msg = str(row.get("msg", "") or "")
    event = str(row.get("event", "") or "")
    comp = str(row.get("component", "") or details.get("component", "") or "")
    text = "\n".join([msg, event, comp, "\n".join(log_tail[-60:])])
    cls = _classify_error(text)
    trace_files = _extract_trace_files(text)

    ticket_id = f"af_{now}_{int(max(1, seq))}"
    ticket = {
        "id": ticket_id,
        "ts": int(now),
        "date": time.strftime("%Y-%m-%d", time.localtime(now)),
        "status": "open",
        "mode": str(settings.get("autofix_mode", "report_only") or "report_only"),
        "market_rollout_stage": str(settings.get("market_rollout_stage", "legacy") or "legacy"),
        "incident": {
            "ts": int(float(row.get("ts", 0) or 0)),
            "severity": str(row.get("severity", "") or "").strip().lower(),
            "event": event,
            "msg": msg,
            "component": comp,
            "details": details,
        },
        "classifier": {
            "kind": str(cls.get("kind", "unknown") or "unknown"),
            "confidence": float(cls.get("confidence", 0.0) or 0.0),
            "match": str(cls.get("match", "") or ""),
        },
        "evidence": {
            "trace_files": trace_files,
            "log_tail": log_tail[-80:],
        },
        "proposal": {
            "summary": _suggestion_for_kind(str(cls.get("kind", "unknown") or "unknown")),
            "llm": {"used": False, "ok": False, "model": str(settings.get("autofix_model", "gpt-5-mini") or "gpt-5-mini")},
            "target_files": [str(x.get("path", "") or "") for x in trace_files][:6],
            "recommended_tests": [str(settings.get("autofix_test_command", DEFAULT_TEST_COMMAND) or DEFAULT_TEST_COMMAND)],
            "patch_diff_path": "",
        },
        "apply": {
            "attempted": False,
            "ok": False,
            "reason": "mode_not_shadow_apply",
            "ts": int(now),
        },
    }
    _attach_proposal_meta(ticket, ticket.get("proposal", {}) if isinstance(ticket.get("proposal", {}), dict) else {})
    return ticket


def _extract_json_obj(text: str) -> Dict[str, Any]:
    raw = str(text or "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        pass
    start = raw.find("{")
    end = raw.rfind("}")
    if start < 0 or end <= start:
        return {}
    snippet = raw[start : end + 1]
    try:
        parsed = json.loads(snippet)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _extract_llm_text(payload: Dict[str, Any]) -> str:
    if not isinstance(payload, dict):
        return ""
    direct = str(payload.get("output_text", "") or "").strip()
    if direct:
        return direct

    parts: List[str] = []
    for item in list(payload.get("output", []) or []):
        if not isinstance(item, dict):
            continue
        for content in list(item.get("content", []) or []):
            if not isinstance(content, dict):
                continue
            txt = str(content.get("text", "") or "").strip()
            if txt:
                parts.append(txt)
    return "\n".join(parts).strip()


def _normalize_target_files(value: Any) -> List[str]:
    out: List[str] = []
    seen = set()
    rows = value if isinstance(value, list) else []
    for raw in rows:
        path = str(raw or "").strip().replace("\\", "/")
        if not path:
            continue
        if path in seen:
            continue
        seen.add(path)
        out.append(path)
    return out


def _diff_stats(diff_text: str) -> Dict[str, int]:
    text = str(diff_text or "")
    files = 0
    added = 0
    removed = 0
    hunks = 0
    for ln in text.splitlines():
        if ln.startswith("diff --git "):
            files += 1
            continue
        if ln.startswith("@@"):
            hunks += 1
            continue
        if ln.startswith("+++ ") or ln.startswith("--- "):
            continue
        if ln.startswith("+"):
            added += 1
            continue
        if ln.startswith("-"):
            removed += 1
    changed = int(added + removed)
    return {
        "files": int(max(0, files)),
        "hunks": int(max(0, hunks)),
        "added": int(max(0, added)),
        "removed": int(max(0, removed)),
        "changed": int(max(0, changed)),
    }


def _proposal_risk(proposal: Dict[str, Any], classifier: Dict[str, Any]) -> Dict[str, Any]:
    target_files = _normalize_target_files(proposal.get("target_files", []))
    diff = str(proposal.get("diff", "") or "")
    stats = _diff_stats(diff)
    score = 0
    reasons: List[str] = []

    files_n = max(int(stats.get("files", 0) or 0), len(target_files))
    changed_n = int(stats.get("changed", 0) or 0)

    if files_n >= 6:
        score += 4
        reasons.append("many_files")
    elif files_n >= 3:
        score += 2
        reasons.append("multi_file")
    elif files_n >= 1:
        score += 1

    if changed_n >= 180:
        score += 5
        reasons.append("large_diff")
    elif changed_n >= 80:
        score += 3
        reasons.append("medium_diff")
    elif changed_n >= 24:
        score += 1

    if any(p.startswith("runtime/") or p.startswith("engines/") for p in target_files):
        score += 2
        reasons.append("runtime_touch")
    if any(p.startswith("app/") for p in target_files):
        score += 1
    if any(p.startswith("ui/") for p in target_files):
        score += 1
    if any(p.startswith("tests/") for p in target_files):
        score = max(0, score - 1)
        reasons.append("has_tests")

    kind = str(classifier.get("kind", "") or "").strip().lower()
    if kind in {"unknown", "unknown_traceback"}:
        score += 1
        reasons.append("unknown_root_cause")

    level = "low"
    if score >= 7:
        level = "high"
    elif score >= 4:
        level = "medium"

    return {
        "score": int(score),
        "level": level,
        "reasons": reasons[:6],
        "stats": stats,
        "target_files": target_files[:20],
    }


def _attach_proposal_meta(ticket: Dict[str, Any], proposal_raw: Dict[str, Any]) -> None:
    if not isinstance(ticket, dict):
        return
    proposal = ticket.get("proposal", {}) if isinstance(ticket.get("proposal", {}), dict) else {}
    cls = ticket.get("classifier", {}) if isinstance(ticket.get("classifier", {}), dict) else {}
    if isinstance(proposal_raw, dict):
        if isinstance(proposal_raw.get("target_files", []), list):
            proposal["target_files"] = _normalize_target_files(proposal_raw.get("target_files", []))[:20]
        if str(proposal_raw.get("diff", "") or "").strip():
            proposal["diff_stats"] = _diff_stats(str(proposal_raw.get("diff", "") or ""))
    proposal["risk"] = _proposal_risk(proposal_raw if isinstance(proposal_raw, dict) else proposal, cls)
    ticket["proposal"] = proposal


def _keyword_terms(text: str, max_terms: int = 14) -> List[str]:
    out: List[str] = []
    seen = set()
    for token in re.findall(r"[A-Za-z_][A-Za-z0-9_]{2,}", str(text or "")):
        low = str(token or "").strip().lower()
        if (not low) or low in seen or low in _REPO_CONTEXT_KEYWORD_STOPWORDS:
            continue
        seen.add(low)
        out.append(low)
        if len(out) >= max(3, int(max_terms)):
            break
    return out


def _candidate_context_paths(ticket: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    seen = set()
    ev = ticket.get("evidence", {}) if isinstance(ticket.get("evidence", {}), dict) else {}
    trace_rows = list(ev.get("trace_files", []) or []) if isinstance(ev.get("trace_files", []), list) else []
    for row in trace_rows:
        if not isinstance(row, dict):
            continue
        p = str(row.get("path", "") or "").strip().replace("\\", "/")
        if (not p) or p in seen:
            continue
        seen.add(p)
        out.append(p)
    prop = ticket.get("proposal", {}) if isinstance(ticket.get("proposal", {}), dict) else {}
    for raw in list(prop.get("target_files", []) or []) if isinstance(prop.get("target_files", []), list) else []:
        p = str(raw or "").strip().replace("\\", "/")
        if (not p) or p in seen:
            continue
        seen.add(p)
        out.append(p)
    return out[:24]


def _iter_repo_files(max_files: int = 500) -> List[str]:
    out: List[str] = []
    seen = set()
    for rel_dir in _REPO_CONTEXT_DIRS:
        abs_dir = os.path.join(BASE_DIR, rel_dir)
        if not os.path.isdir(abs_dir):
            continue
        for root, _dirs, files in os.walk(abs_dir):
            for name in files:
                low = str(name or "").lower()
                if not low.endswith(_REPO_CONTEXT_EXTS):
                    continue
                ap = os.path.join(root, name)
                rp = os.path.relpath(ap, BASE_DIR).replace("\\", "/")
                if rp in seen:
                    continue
                seen.add(rp)
                out.append(rp)
                if len(out) >= max(50, int(max_files)):
                    return out
    return out


def _file_snippet_hits(rel_path: str, keywords: List[str], max_snips: int = 3) -> Dict[str, Any]:
    out = {"path": rel_path, "keyword_hits": [], "snippets": []}
    if not rel_path:
        return out
    abs_path = os.path.join(BASE_DIR, rel_path)
    if not os.path.isfile(abs_path):
        return out
    try:
        with open(abs_path, "r", encoding="utf-8", errors="ignore") as f:
            raw = f.read()
    except Exception:
        return out
    if not raw:
        return out
    lines = str(raw).splitlines()
    hit_terms: List[str] = []
    snippets: List[Dict[str, Any]] = []
    for idx, line in enumerate(lines):
        ll = str(line or "").lower()
        if not ll.strip():
            continue
        matched = ""
        for kw in keywords:
            if kw in ll:
                matched = kw
                break
        if not matched:
            continue
        if matched not in hit_terms:
            hit_terms.append(matched)
        if len(snippets) < max(1, int(max_snips)):
            snippets.append({"line": int(idx + 1), "text": str(line).strip()[:260]})
        if len(snippets) >= max(1, int(max_snips)) and len(hit_terms) >= max(1, min(6, len(keywords))):
            break
    out["keyword_hits"] = hit_terms[:8]
    out["snippets"] = snippets
    return out


def _repo_context_for_ticket(ticket: Dict[str, Any], max_files: int = 450) -> Dict[str, Any]:
    incident = ticket.get("incident", {}) if isinstance(ticket.get("incident", {}), dict) else {}
    req = ticket.get("request", {}) if isinstance(ticket.get("request", {}), dict) else {}
    base_text = " | ".join(
        [
            str(incident.get("event", "") or ""),
            str(incident.get("msg", "") or ""),
            str(req.get("text", "") or ""),
        ]
    )
    keywords = _keyword_terms(base_text)
    seed_paths = _candidate_context_paths(ticket)
    candidates = seed_paths + [p for p in _iter_repo_files(max_files=max_files) if p not in set(seed_paths)]

    rows: List[Dict[str, Any]] = []
    for rel_path in candidates:
        hits = _file_snippet_hits(rel_path, keywords, max_snips=3)
        if not list(hits.get("snippets", []) or []):
            continue
        rows.append(hits)
        if len(rows) >= 12:
            break

    if not rows and seed_paths:
        # Always include trace/seed paths even if keyword scans did not hit, so the model has concrete files.
        for rel_path in seed_paths[:8]:
            rows.append({"path": rel_path, "keyword_hits": [], "snippets": []})

    return {
        "keywords": keywords[:14],
        "files": rows[:12],
        "seed_files": seed_paths[:20],
        "searched_dirs": list(_REPO_CONTEXT_DIRS),
    }


def _resolve_openai_api_key(settings: Dict[str, Any]) -> str:
    return str(get_openai_api_key(settings=settings, base_dir=BASE_DIR) or "").strip()


def _openai_key_hint_path() -> str:
    try:
        return os.path.abspath(openai_credential_path(BASE_DIR))
    except Exception:
        return os.path.join(BASE_DIR, "keys", "openai_api_key.txt")


def _llm_patch_proposal(ticket: Dict[str, Any], settings: Dict[str, Any]) -> Dict[str, Any]:
    api_key = _resolve_openai_api_key(settings)
    if not api_key:
        return {
            "used": False,
            "ok": False,
            "error": "missing_openai_api_key",
            "detail": f"Set OPENAI_API_KEY or save key in {_openai_key_hint_path()}",
        }

    try:
        timeout_s = max(5.0, float(settings.get("autofix_request_timeout_s", 25.0) or 25.0))
    except Exception:
        timeout_s = 25.0

    model = str(settings.get("autofix_model", "gpt-5-mini") or "gpt-5-mini").strip() or "gpt-5-mini"
    api_base = str(
        settings.get("autofix_api_base", os.environ.get("OPENAI_API_BASE", "https://api.openai.com/v1"))
        or "https://api.openai.com/v1"
    ).strip()
    api_base = api_base.rstrip("/")

    prompt = {
        "ticket": {
            "id": ticket.get("id"),
            "incident": ticket.get("incident", {}),
            "classifier": ticket.get("classifier", {}),
            "trace_files": (ticket.get("evidence", {}) if isinstance(ticket.get("evidence", {}), dict) else {}).get("trace_files", []),
            "log_tail": (ticket.get("evidence", {}) if isinstance(ticket.get("evidence", {}), dict) else {}).get("log_tail", [])[-30:],
        },
        "constraints": {
            "base_dir": BASE_DIR,
            "format": "Return JSON only with keys: summary, target_files, diff, tests.",
            "diff": "Use unified diff. Keep patch minimal.",
        },
        "repo_context": _repo_context_for_ticket(ticket),
    }

    body = {
        "model": model,
        "input": [
            {
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": (
                            "You are a senior Python reliability engineer. Generate a minimal safe patch proposal for the given runtime error ticket."
                        ),
                    }
                ],
            },
            {
                "role": "user",
                "content": [{"type": "input_text", "text": json.dumps(prompt, ensure_ascii=True)}],
            },
        ],
        "max_output_tokens": 1200,
    }

    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        f"{api_base}/responses",
        data=data,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=float(timeout_s)) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
    except urllib.error.HTTPError as exc:
        txt = exc.read().decode("utf-8", errors="ignore") if hasattr(exc, "read") else ""
        return {"used": True, "ok": False, "error": f"http_{exc.code}", "detail": txt[:500]}
    except Exception as exc:
        return {"used": True, "ok": False, "error": f"{type(exc).__name__}: {exc}"}

    try:
        payload = json.loads(raw)
    except Exception:
        return {"used": True, "ok": False, "error": "invalid_json_response", "detail": raw[:500]}

    text = _extract_llm_text(payload)
    obj = _extract_json_obj(text)
    if not obj:
        return {"used": True, "ok": False, "error": "model_output_not_json", "detail": text[:500]}

    diff = str(obj.get("diff", "") or "")
    tests = obj.get("tests", []) if isinstance(obj.get("tests", []), list) else []
    target_files = obj.get("target_files", []) if isinstance(obj.get("target_files", []), list) else []
    return {
        "used": True,
        "ok": bool(diff.strip()),
        "model": model,
        "summary": str(obj.get("summary", "") or "").strip(),
        "diff": diff,
        "tests": [str(x).strip() for x in tests if str(x).strip()][:8],
        "target_files": [str(x).strip() for x in target_files if str(x).strip()][:12],
    }


def _retry_delay_for_llm_error(llm: Dict[str, Any], default_s: int) -> int:
    base = max(15, int(default_s))
    err = str((llm if isinstance(llm, dict) else {}).get("error", "") or "").strip().lower()
    detail = str((llm if isinstance(llm, dict) else {}).get("detail", "") or "").strip().lower()
    if ("http_429" in err) or ("insufficient_quota" in detail):
        return max(base, 1800)
    if ("http_5" in err) or ("service unavailable" in detail):
        return max(base, 300)
    if ("timeout" in err) or ("timed out" in detail):
        return max(base, 180)
    if "urlerror" in err:
        return max(base, 180)
    return base


def _request_retry_max_attempts_from_settings(settings: Dict[str, Any]) -> int:
    try:
        return max(
            1,
            int(float(settings.get("autofix_request_retry_max_attempts", MAX_REQUEST_RETRY_MAX_ATTEMPTS) or MAX_REQUEST_RETRY_MAX_ATTEMPTS)),
        )
    except Exception:
        return int(MAX_REQUEST_RETRY_MAX_ATTEMPTS)


def _llm_apply_block_reason(llm: Dict[str, Any]) -> str:
    err = str((llm if isinstance(llm, dict) else {}).get("error", "") or "").strip().lower()
    detail = str((llm if isinstance(llm, dict) else {}).get("detail", "") or "").strip().lower()
    if "missing_openai_api_key" in err:
        return "missing_openai_api_key"
    if ("http_429" in err) or ("insufficient_quota" in detail):
        return "llm_quota_blocked"
    if "urlerror" in err:
        return "llm_network_unreachable"
    if "http_400" in err:
        return "llm_request_rejected"
    return ""


def _terminal_request_block_reason(llm: Dict[str, Any], settings: Dict[str, Any]) -> str:
    reason = _llm_apply_block_reason(llm)
    if not reason:
        return ""
    block_quota = bool(settings.get("autofix_request_block_on_quota", True))
    block_missing_key = bool(settings.get("autofix_request_block_on_missing_key", True))
    block_bad_request = bool(settings.get("autofix_request_block_on_bad_request", True))
    if reason == "llm_quota_blocked" and block_quota:
        return reason
    if reason == "missing_openai_api_key" and block_missing_key:
        return reason
    if reason == "llm_request_rejected" and block_bad_request:
        return reason
    return ""


def _request_retry_terminal_block_reason(row: Dict[str, Any], settings: Dict[str, Any]) -> str:
    retry = row.get("request_retry", {}) if isinstance(row.get("request_retry", {}), dict) else {}
    proposal = row.get("proposal", {}) if isinstance(row.get("proposal", {}), dict) else {}
    llm = proposal.get("llm", {}) if isinstance(proposal.get("llm", {}), dict) else {}
    apply_row = row.get("apply", {}) if isinstance(row.get("apply", {}), dict) else {}
    base_reason = str(apply_row.get("reason", "") or "").strip().lower()

    llm_reason = _terminal_request_block_reason(
        {
            "error": str(retry.get("last_error", "") or str(llm.get("error", "") or "")),
            "detail": str(llm.get("detail", "") or ""),
        },
        settings,
    )
    if not llm_reason:
        fallback_reason = ""
        if "llm_quota_blocked" in base_reason:
            fallback_reason = "llm_quota_blocked"
        elif "missing_openai_api_key" in base_reason:
            fallback_reason = "missing_openai_api_key"
        elif "llm_request_rejected" in base_reason:
            fallback_reason = "llm_request_rejected"
        if fallback_reason == "llm_quota_blocked":
            llm_reason = _terminal_request_block_reason({"error": "http_429", "detail": "insufficient_quota"}, settings)
        elif fallback_reason == "missing_openai_api_key":
            llm_reason = _terminal_request_block_reason({"error": "missing_openai_api_key", "detail": ""}, settings)
        elif fallback_reason == "llm_request_rejected":
            llm_reason = _terminal_request_block_reason({"error": "http_400", "detail": ""}, settings)
    if llm_reason:
        return f"{llm_reason}_retries_exhausted"
    return ""


def _mark_request_ticket_blocked(row: Dict[str, Any], reason: str, now_ts: int, retry_max_attempts: int) -> Dict[str, Any]:
    row["status"] = "blocked"
    blocked_row = row.get("blocked", {}) if isinstance(row.get("blocked", {}), dict) else {}
    blocked_row["reason"] = str(reason)
    blocked_row["ts"] = int(now_ts)
    blocked_row["retry_max_attempts"] = int(retry_max_attempts)
    row["blocked"] = blocked_row
    apply_row = row.get("apply", {}) if isinstance(row.get("apply", {}), dict) else {}
    apply_row["attempted"] = bool(apply_row.get("attempted", False))
    apply_row["ok"] = False
    apply_row["reason"] = str(reason)
    apply_row["ts"] = int(now_ts)
    row["apply"] = apply_row
    return row


def _write_patch(ticket_id: str, diff_text: str) -> str:
    os.makedirs(AUTOFIX_PATCHES_DIR, exist_ok=True)
    path = os.path.join(AUTOFIX_PATCHES_DIR, f"{ticket_id}.diff")
    with open(path, "w", encoding="utf-8") as f:
        f.write(str(diff_text or ""))
    return path


def _ticket_path(ticket_id: str) -> str:
    return os.path.join(AUTOFIX_TICKETS_DIR, f"{str(ticket_id or '').strip()}.json")


def _load_ticket(ticket_id: str) -> Tuple[Dict[str, Any], str]:
    path = _ticket_path(ticket_id)
    data = _safe_read_json(path)
    return (data if isinstance(data, dict) else {}), path


def _resolve_patch_path(ticket: Dict[str, Any], ticket_id: str) -> str:
    proposal = ticket.get("proposal", {}) if isinstance(ticket.get("proposal", {}), dict) else {}
    patch_path = str(proposal.get("patch_diff_path", "") or "").strip()
    if patch_path:
        if not os.path.isabs(patch_path):
            patch_path = os.path.abspath(os.path.join(BASE_DIR, patch_path))
        if os.path.isfile(patch_path):
            return patch_path
    fallback = os.path.join(AUTOFIX_PATCHES_DIR, f"{str(ticket_id or '').strip()}.diff")
    if os.path.isfile(fallback):
        return fallback
    return ""


def _shell_run(cmd: str, timeout_s: float = 120.0) -> Dict[str, Any]:
    try:
        proc = subprocess.run(
            cmd,
            cwd=BASE_DIR,
            shell=True,
            capture_output=True,
            text=True,
            timeout=max(1.0, float(timeout_s)),
        )
        return {
            "ok": bool(proc.returncode == 0),
            "code": int(proc.returncode),
            "stdout": str(proc.stdout or "")[-2000:],
            "stderr": str(proc.stderr or "")[-2000:],
        }
    except Exception as exc:
        return {"ok": False, "code": -1, "stdout": "", "stderr": f"{type(exc).__name__}: {exc}"}


def _can_apply(settings: Dict[str, Any], applied_count_day: int, manual_override: bool = False) -> Tuple[bool, str]:
    mode = str(settings.get("autofix_mode", "report_only") or "report_only").strip().lower()
    if (mode != "shadow_apply") and (not bool(manual_override)):
        return False, "mode_not_shadow_apply"

    stage = str(settings.get("market_rollout_stage", "legacy") or "legacy").strip().lower()
    allow_live = bool(settings.get("autofix_allow_live_apply", False))
    if stage == "live_guarded" and (not allow_live):
        # Chat-driven requests may still force apply explicitly from UI.
        return False, "live_guarded_blocked"

    try:
        limit = max(0, int(float(settings.get("autofix_max_fixes_per_day", 2) or 2)))
    except Exception:
        limit = 2
    if limit <= 0:
        return False, "daily_limit_zero"
    if int(applied_count_day) >= int(limit):
        return False, "daily_limit_reached"
    return True, "ok"


def apply_ticket_once(ticket_id: str, force: bool = False, dry_run: bool = False) -> Dict[str, Any]:
    tid = str(ticket_id or "").strip()
    now = int(time.time())
    settings, settings_path = _load_settings()
    out: Dict[str, Any] = {
        "ticket_id": tid,
        "ok": False,
        "attempted": False,
        "reason": "",
        "ts": now,
        "settings_path": settings_path,
    }
    if not tid:
        out["reason"] = "missing_ticket_id"
        return out

    ticket, ticket_path = _load_ticket(tid)
    if not ticket:
        out["reason"] = "ticket_not_found"
        out["ticket_path"] = ticket_path
        return out
    out["ticket_path"] = ticket_path

    current_status = str(ticket.get("status", "open") or "open").strip().lower()
    out["current_status"] = current_status
    if current_status == "applied" and (not bool(force)):
        out["ok"] = True
        out["reason"] = "already_applied"
        return out

    patch_path = _resolve_patch_path(ticket, tid)
    if not patch_path:
        reason = "missing_patch_file"
        apply_payload = {"attempted": False, "ok": False, "reason": reason, "ts": now, "approved_manual": True}
        ticket["apply"] = apply_payload
        ticket["status"] = "open"
        _atomic_write_json(ticket_path, ticket)
        out["reason"] = reason
        return out
    out["patch_path"] = patch_path

    state = _safe_read_json(AUTOFIX_STATE_PATH)
    day_key = time.strftime("%Y-%m-%d", time.localtime(now))
    state_day = str(state.get("applied_day", "") or "")
    applied_count_day = int(state.get("applied_count_day", 0) or 0) if state_day == day_key else 0

    can_apply, gate_reason = _can_apply(settings, applied_count_day=applied_count_day, manual_override=True)
    if (not can_apply) and (not bool(force)):
        apply_payload = {"attempted": False, "ok": False, "reason": gate_reason, "ts": now, "approved_manual": True}
        ticket["apply"] = apply_payload
        ticket["status"] = "open"
        _atomic_write_json(ticket_path, ticket)
        out["reason"] = gate_reason
        return out

    if bool(dry_run):
        out["ok"] = True
        out["reason"] = "dry_run"
        out["attempted"] = False
        return out

    apply_out = _apply_patch(tid, patch_path, settings)
    apply_payload = dict(apply_out if isinstance(apply_out, dict) else {})
    apply_payload["approved_manual"] = True
    apply_payload["ticket_id"] = tid
    apply_payload["ts"] = int(time.time())
    out["attempted"] = bool(apply_payload.get("attempted", False))
    out["apply"] = apply_payload

    if bool(apply_payload.get("ok", False)):
        ticket["status"] = "applied"
        out["ok"] = True
        out["reason"] = "applied"
        applied_count_day = int(applied_count_day) + 1
        runtime_event(
            RUNTIME_EVENTS_PATH,
            component="autofix",
            event="autofix_ticket_applied",
            level="warning",
            msg=f"ticket {tid} applied by manual approval",
            details={"ticket_id": tid, "patch_path": patch_path},
        )
    else:
        ticket["status"] = "open"
        out["reason"] = str(apply_payload.get("reason", "") or "apply_failed")
        runtime_event(
            RUNTIME_EVENTS_PATH,
            component="autofix",
            event="autofix_ticket_apply_failed",
            level="error",
            msg=f"ticket {tid} manual apply failed",
            details={"ticket_id": tid, "reason": out["reason"]},
        )
    ticket["apply"] = apply_payload
    _atomic_write_json(ticket_path, ticket)

    state_out = {
        "ts": int(time.time()),
        "settings_path": settings_path,
        "incidents_offset": int(state.get("incidents_offset", 0) or 0),
        "recent_fingerprints": list(state.get("recent_fingerprints", []) or [])[-MAX_RECENT_FINGERPRINTS:],
        "applied_day": day_key,
        "applied_count_day": int(applied_count_day),
        "last_ticket_id": tid,
        "enabled": bool(settings.get("autofix_enabled", True)),
        "mode": str(settings.get("autofix_mode", "report_only") or "report_only"),
        "created_count_total": int(state.get("created_count_total", 0) or 0),
    }
    _atomic_write_json(AUTOFIX_STATE_PATH, state_out)

    status_prev = _safe_read_json(AUTOFIX_STATUS_PATH)
    if not isinstance(status_prev, dict):
        status_prev = {}
    status_prev["ts"] = int(time.time())
    status_prev["settings_path"] = settings_path
    status_prev["enabled"] = bool(settings.get("autofix_enabled", True))
    status_prev["mode"] = str(settings.get("autofix_mode", "report_only") or "report_only")
    status_prev["last_ticket_id"] = tid
    status_prev["applied_count_day"] = int(applied_count_day)
    counts = _ticket_counts()
    status_prev["ticket_counts"] = counts
    if bool(out.get("ok", False)):
        status_prev["applied_ok"] = int(status_prev.get("applied_ok", 0) or 0) + 1
    _atomic_write_json(AUTOFIX_STATUS_PATH, status_prev)
    return out


def _apply_patch(ticket_id: str, patch_path: str, settings: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {"attempted": True, "ok": False, "reason": "unknown", "ts": int(time.time())}
    if not os.path.isfile(patch_path):
        out["reason"] = "missing_patch_file"
        return out

    cmd_check = f"git apply --check {shlex.quote(patch_path)}"
    check = _shell_run(cmd_check, timeout_s=20.0)
    if not bool(check.get("ok", False)):
        out["reason"] = "git_apply_check_failed"
        out["check"] = check
        return out

    cmd_apply = f"git apply {shlex.quote(patch_path)}"
    apply_res = _shell_run(cmd_apply, timeout_s=20.0)
    if not bool(apply_res.get("ok", False)):
        out["reason"] = "git_apply_failed"
        out["apply"] = apply_res
        return out

    test_cmd = str(settings.get("autofix_test_command", DEFAULT_TEST_COMMAND) or "").strip()
    if not test_cmd:
        out.update({"ok": True, "reason": "applied_no_tests"})
        return out

    test_res = _shell_run(test_cmd, timeout_s=300.0)
    out["tests"] = test_res
    if bool(test_res.get("ok", False)):
        out.update({"ok": True, "reason": "applied_and_tests_passed"})
        return out

    revert_res = _shell_run(f"git apply -R {shlex.quote(patch_path)}", timeout_s=20.0)
    out["revert"] = revert_res
    out["reason"] = "tests_failed_reverted"
    return out


def _ticket_counts() -> Dict[str, int]:
    open_count = 0
    applied_count = 0
    blocked_count = 0
    total = 0
    if not os.path.isdir(AUTOFIX_TICKETS_DIR):
        return {"total": 0, "open": 0, "applied": 0, "blocked": 0}
    for name in os.listdir(AUTOFIX_TICKETS_DIR):
        if not name.endswith(".json"):
            continue
        total += 1
        path = os.path.join(AUTOFIX_TICKETS_DIR, name)
        row = _safe_read_json(path)
        status = str(row.get("status", "open") or "open").strip().lower()
        if status == "applied":
            applied_count += 1
        elif status == "open":
            open_count += 1
        elif status == "blocked":
            blocked_count += 1
    return {"total": int(total), "open": int(open_count), "applied": int(applied_count), "blocked": int(blocked_count)}


def _retry_open_user_request_tickets(
    settings: Dict[str, Any],
    dry_run: bool,
    applied_count_day: int,
) -> Dict[str, Any]:
    now = int(time.time())
    try:
        retries_per_tick = max(
            1,
            int(float(settings.get("autofix_request_retries_per_tick", MAX_REQUEST_RETRIES_PER_TICK) or MAX_REQUEST_RETRIES_PER_TICK)),
        )
    except Exception:
        retries_per_tick = int(MAX_REQUEST_RETRIES_PER_TICK)
    try:
        retry_cooldown_s = max(15, int(float(settings.get("autofix_request_retry_cooldown_s", 90) or 90)))
    except Exception:
        retry_cooldown_s = 90
    retry_max_attempts = _request_retry_max_attempts_from_settings(settings)

    attempted = 0
    updated = 0
    applied = 0
    blocked = 0
    latest_ticket_id = ""

    if not os.path.isdir(AUTOFIX_TICKETS_DIR):
        return {
            "attempted": 0,
            "updated": 0,
            "applied": 0,
            "blocked": 0,
            "applied_count_day": int(applied_count_day),
            "last_ticket_id": "",
        }

    paths = sorted(
        [os.path.join(AUTOFIX_TICKETS_DIR, n) for n in os.listdir(AUTOFIX_TICKETS_DIR) if str(n).endswith(".json")],
        key=lambda p: os.path.getmtime(p) if os.path.isfile(p) else 0.0,
    )
    for path in paths:
        if attempted >= retries_per_tick:
            break
        row = _safe_read_json(path)
        if not isinstance(row, dict) or (not row):
            continue
        if str(row.get("status", "open") or "open").strip().lower() != "open":
            continue
        cls = row.get("classifier", {}) if isinstance(row.get("classifier", {}), dict) else {}
        if str(cls.get("kind", "") or "").strip().lower() != "user_request":
            continue
        tid = str(row.get("id", "") or "").strip()
        if not tid:
            continue
        proposal = row.get("proposal", {}) if isinstance(row.get("proposal", {}), dict) else {}
        if str(proposal.get("patch_diff_path", "") or "").strip():
            continue
        retry = row.get("request_retry", {}) if isinstance(row.get("request_retry", {}), dict) else {}
        try:
            attempts = int(float(retry.get("attempts", 0) or 0))
        except Exception:
            attempts = 0
        try:
            last_ts = int(float(retry.get("last_ts", 0) or 0))
        except Exception:
            last_ts = 0
        try:
            next_retry_ts = int(float(retry.get("next_retry_ts", 0) or 0))
        except Exception:
            next_retry_ts = 0
        if attempts >= retry_max_attempts:
            block_reason = _request_retry_terminal_block_reason(row, settings)
            if block_reason:
                latest_ticket_id = tid
                row = _mark_request_ticket_blocked(row, block_reason, now_ts=now, retry_max_attempts=retry_max_attempts)
                blocked += 1
                updated += 1
                if not dry_run:
                    _atomic_write_json(path, row)
                    runtime_event(
                        RUNTIME_EVENTS_PATH,
                        component="autofix",
                        event="autofix_user_request_blocked",
                        level="warning",
                        msg=f"user request ticket {tid} blocked after retry exhaustion",
                        details={
                            "ticket_id": tid,
                            "reason": str(block_reason),
                            "attempts": int(attempts),
                            "retry_max_attempts": int(retry_max_attempts),
                        },
                    )
                    _log(
                        "request-blocked "
                        f"ticket={tid} "
                        f"reason={block_reason} "
                        f"attempts={int(attempts)} "
                        f"retry_max_attempts={int(retry_max_attempts)}"
                    )
            continue
        if next_retry_ts > int(now):
            continue
        if last_ts > 0 and (int(now) - int(last_ts)) < retry_cooldown_s:
            continue

        attempted += 1
        latest_ticket_id = tid
        llm = _llm_patch_proposal(row, settings)
        proposal["llm"] = llm
        if bool(llm.get("ok", False)):
            patch_path = _write_patch(tid, str(llm.get("diff", "") or ""))
            proposal["patch_diff_path"] = patch_path
            if isinstance(llm.get("target_files", []), list) and llm.get("target_files", []):
                proposal["target_files"] = list(llm.get("target_files", []))
            if isinstance(llm.get("tests", []), list) and llm.get("tests", []):
                proposal["recommended_tests"] = list(llm.get("tests", []))
            if str(llm.get("summary", "") or "").strip():
                proposal["summary"] = str(llm.get("summary", "") or "").strip()
        row["proposal"] = proposal
        _attach_proposal_meta(
            row,
            {
                "target_files": proposal.get("target_files", []) if isinstance(proposal.get("target_files", []), list) else [],
                "diff": str(llm.get("diff", "") or ""),
            },
        )
        retry_delay_s = _retry_delay_for_llm_error(llm, retry_cooldown_s)
        row["request_retry"] = {
            "attempts": int(attempts + 1),
            "last_ts": int(now),
            "last_error": str(llm.get("error", "") or ""),
            "next_retry_ts": int(now + retry_delay_s),
        }

        req = row.get("request", {}) if isinstance(row.get("request", {}), dict) else {}
        auto_apply_requested = bool(req.get("auto_apply_requested", False))
        force_apply_requested = bool(req.get("force_apply_requested", False)) or bool(auto_apply_requested)
        patch_path = _resolve_patch_path(row, tid)
        apply_reason = "awaiting_manual_approval"
        apply_attempted = False
        applied_ok = False
        if auto_apply_requested:
            can_apply, gate_reason = _can_apply(settings, applied_count_day=applied_count_day, manual_override=True)
            llm_block_reason = _llm_apply_block_reason(llm)
            if not patch_path:
                apply_reason = (llm_block_reason or "missing_patch_file")
            elif (not can_apply) and (not force_apply_requested):
                apply_reason = gate_reason
            else:
                apply_out = _apply_patch(tid, patch_path, settings)
                apply_attempted = bool(apply_out.get("attempted", False))
                row["apply"] = dict(apply_out if isinstance(apply_out, dict) else {})
                row["apply"]["approved_manual"] = False
                row["apply"]["requested_auto_apply"] = True
                row["apply"]["ts"] = int(now)
                if bool(row["apply"].get("ok", False)):
                    row["status"] = "applied"
                    applied_ok = True
                    applied_count_day = int(applied_count_day) + 1
                    applied += 1
                    apply_reason = str(row["apply"].get("reason", "") or "applied")
                else:
                    row["status"] = "open"
                    apply_reason = str(row["apply"].get("reason", "") or "apply_failed")
        apply_row = row.get("apply", {}) if isinstance(row.get("apply", {}), dict) else {}
        apply_row["attempted"] = bool(apply_attempted)
        apply_row["ok"] = bool(applied_ok)
        apply_row["reason"] = str(apply_reason)
        apply_row["ts"] = int(now)
        apply_row["approved_manual"] = False
        apply_row["requested_auto_apply"] = bool(auto_apply_requested)
        row["apply"] = apply_row

        block_reason = ""
        try:
            retry_attempts_now = int(float((row.get("request_retry", {}) if isinstance(row.get("request_retry", {}), dict) else {}).get("attempts", 0) or 0))
        except Exception:
            retry_attempts_now = 0
        if (not applied_ok) and (str(row.get("status", "open") or "open").strip().lower() == "open") and (retry_attempts_now >= retry_max_attempts):
            block_reason = _request_retry_terminal_block_reason(row, settings)
            if block_reason:
                row = _mark_request_ticket_blocked(row, block_reason, now_ts=now, retry_max_attempts=retry_max_attempts)
                blocked += 1
                apply_reason = str(block_reason)

        updated += 1
        if not dry_run:
            _atomic_write_json(path, row)
            runtime_event(
                RUNTIME_EVENTS_PATH,
                component="autofix",
                event="autofix_user_request_retry",
                level=("warning" if applied_ok else "info"),
                msg=f"user request ticket {tid} retry attempt",
                details={
                    "ticket_id": tid,
                    "attempt": int(attempts + 1),
                    "proposal_ok": bool(llm.get("ok", False)),
                    "applied": bool(applied_ok),
                    "apply_reason": str(apply_reason),
                    "llm_error": str(llm.get("error", "") or ""),
                },
            )
            _log(
                "request-retry "
                f"ticket={tid} "
                f"attempt={int(attempts + 1)} "
                f"proposal_ok={bool(llm.get('ok', False))} "
                f"applied={bool(applied_ok)} "
                f"apply_reason={apply_reason} "
                f"llm_error={str(llm.get('error', '') or '')}"
            )
            if block_reason:
                runtime_event(
                    RUNTIME_EVENTS_PATH,
                    component="autofix",
                    event="autofix_user_request_blocked",
                    level="warning",
                    msg=f"user request ticket {tid} blocked after retry exhaustion",
                    details={
                        "ticket_id": tid,
                        "reason": str(block_reason),
                        "attempts": int(retry_attempts_now),
                        "retry_max_attempts": int(retry_max_attempts),
                    },
                )
                _log(
                    "request-blocked "
                    f"ticket={tid} "
                    f"reason={block_reason} "
                    f"attempts={int(retry_attempts_now)} "
                    f"retry_max_attempts={int(retry_max_attempts)}"
                )

    return {
        "attempted": int(attempted),
        "updated": int(updated),
        "applied": int(applied),
        "blocked": int(blocked),
        "applied_count_day": int(applied_count_day),
        "last_ticket_id": str(latest_ticket_id),
    }


def create_request_ticket(
    request_text: str,
    auto_apply: bool = False,
    force_apply: bool = False,
    dry_run: bool = False,
) -> Dict[str, Any]:
    now = int(time.time())
    settings, settings_path = _load_settings()
    os.makedirs(AUTOFIX_TICKETS_DIR, exist_ok=True)
    os.makedirs(AUTOFIX_PATCHES_DIR, exist_ok=True)

    req_txt = str(request_text or "").strip()
    if not req_txt:
        return {
            "ok": False,
            "reason": "missing_request_text",
            "ts": now,
            "settings_path": settings_path,
        }
    if len(req_txt) > MAX_REQUEST_CHARS:
        req_txt = req_txt[:MAX_REQUEST_CHARS]

    state = _safe_read_json(AUTOFIX_STATE_PATH)
    created_total_prev = int(state.get("created_count_total", 0) or 0)
    seq = max(1, created_total_prev + 1)

    row = {
        "ts": int(now),
        "severity": "info",
        "event": "user_improvement_request",
        "msg": req_txt,
        "component": "autofix",
        "details": {
            "component": "ui",
            "child": "autofix",
            "source": "assistant_chat",
            "request_type": "improvement",
        },
    }
    ticket = _build_ticket(row, settings, seq=seq)
    ticket["classifier"] = {"kind": "user_request", "confidence": 1.0, "match": "assistant_chat"}
    ticket["request"] = {
        "text": req_txt,
        "submitted_ts": int(now),
        "auto_apply_requested": bool(auto_apply),
        "force_apply_requested": bool(force_apply),
    }
    ticket["proposal"]["summary"] = "User-requested improvement from AI Assist chat."
    ticket["proposal"]["recommended_tests"] = [str(settings.get("autofix_test_command", DEFAULT_TEST_COMMAND) or DEFAULT_TEST_COMMAND)]
    ticket["incident"]["msg"] = req_txt
    ticket["incident"]["event"] = "user_improvement_request"
    ticket["incident"]["component"] = "ui"
    ticket["incident"]["severity"] = "info"

    llm = _llm_patch_proposal(ticket, settings)
    ticket["proposal"]["llm"] = llm
    if bool(llm.get("ok", False)):
        patch_path = _write_patch(ticket["id"], str(llm.get("diff", "") or ""))
        ticket["proposal"]["patch_diff_path"] = patch_path
        if isinstance(llm.get("target_files", []), list) and llm.get("target_files", []):
            ticket["proposal"]["target_files"] = list(llm.get("target_files", []))
        if isinstance(llm.get("tests", []), list) and llm.get("tests", []):
            ticket["proposal"]["recommended_tests"] = list(llm.get("tests", []))
        if str(llm.get("summary", "") or "").strip():
            ticket["proposal"]["summary"] = str(llm.get("summary", "") or "").strip()
    _attach_proposal_meta(
        ticket,
        {
            "target_files": ticket.get("proposal", {}).get("target_files", []) if isinstance(ticket.get("proposal", {}), dict) else [],
            "diff": str(llm.get("diff", "") or ""),
        },
    )

    day_key = time.strftime("%Y-%m-%d", time.localtime(now))
    state_day = str(state.get("applied_day", "") or "")
    applied_count_day = int(state.get("applied_count_day", 0) or 0) if state_day == day_key else 0
    can_apply, gate_reason = _can_apply(settings, applied_count_day=applied_count_day, manual_override=True)
    patch_path = _resolve_patch_path(ticket, str(ticket.get("id", "") or ""))
    llm_block_reason = _llm_apply_block_reason(llm)

    applied_ok = False
    apply_reason = "awaiting_manual_approval"
    apply_attempted = False
    if bool(auto_apply):
        if bool(dry_run):
            apply_reason = "dry_run"
        elif (not patch_path):
            apply_reason = (llm_block_reason or "missing_patch_file")
        elif (not can_apply) and (not bool(force_apply)):
            apply_reason = gate_reason
        else:
            apply_out = _apply_patch(str(ticket.get("id", "") or ""), patch_path, settings)
            apply_attempted = bool(apply_out.get("attempted", False))
            ticket["apply"] = dict(apply_out if isinstance(apply_out, dict) else {})
            ticket["apply"]["approved_manual"] = False
            ticket["apply"]["requested_auto_apply"] = True
            ticket["apply"]["ts"] = int(time.time())
            if bool(ticket["apply"].get("ok", False)):
                ticket["status"] = "applied"
                applied_ok = True
                applied_count_day = int(applied_count_day) + 1
                apply_reason = str(ticket["apply"].get("reason", "") or "applied")
            else:
                ticket["status"] = "open"
                apply_reason = str(ticket["apply"].get("reason", "") or "apply_failed")
    elif (not patch_path) and llm_block_reason:
        apply_reason = str(llm_block_reason)
    current_apply = ticket.get("apply", {}) if isinstance(ticket.get("apply", {}), dict) else {}
    if (not bool(current_apply.get("attempted", False))) and str(current_apply.get("reason", "") or "").strip():
        # Preserve existing explicit reasons only when this request did not run auto-apply.
        if (not bool(auto_apply)) and (not llm_block_reason):
            apply_reason = str(current_apply.get("reason", "") or apply_reason)
    ticket["apply"] = {
        "attempted": bool(apply_attempted),
        "ok": bool(applied_ok),
        "reason": str(apply_reason),
        "ts": int(now),
        "approved_manual": False,
        "requested_auto_apply": bool(auto_apply),
    }

    terminal_block_reason = ""
    if not applied_ok:
        terminal_block_reason = _terminal_request_block_reason(llm, settings)
    if terminal_block_reason:
        retry_max_attempts = _request_retry_max_attempts_from_settings(settings)
        ticket["request_retry"] = {
            "attempts": 1,
            "last_ts": int(now),
            "last_error": str(llm.get("error", "") or ""),
            "next_retry_ts": 0,
        }
        ticket = _mark_request_ticket_blocked(
            ticket,
            terminal_block_reason,
            now_ts=now,
            retry_max_attempts=retry_max_attempts,
        )
        apply_reason = str(terminal_block_reason)

    ticket_path = os.path.join(AUTOFIX_TICKETS_DIR, f"{str(ticket.get('id', '') or '').strip()}.json")
    if not dry_run:
        _atomic_write_json(ticket_path, ticket)
        runtime_event(
            RUNTIME_EVENTS_PATH,
            component="autofix",
            event="autofix_user_request_ticket_created",
            level=("warning" if applied_ok else "info"),
            msg=f"user request ticket {ticket.get('id')} created",
            details={
                "ticket_id": ticket.get("id"),
                "proposal_ok": bool(llm.get("ok", False)),
                "auto_apply_requested": bool(auto_apply),
                "applied": bool(applied_ok),
            },
        )

    state_out = {
        "ts": int(time.time()),
        "settings_path": settings_path,
        "incidents_offset": int(state.get("incidents_offset", 0) or 0),
        "recent_fingerprints": list(state.get("recent_fingerprints", []) or [])[-MAX_RECENT_FINGERPRINTS:],
        "applied_day": day_key,
        "applied_count_day": int(applied_count_day),
        "last_ticket_id": str(ticket.get("id", "") or ""),
        "enabled": bool(settings.get("autofix_enabled", True)),
        "mode": str(settings.get("autofix_mode", "report_only") or "report_only"),
        "created_count_total": int(created_total_prev + 1),
    }
    if not dry_run:
        _atomic_write_json(AUTOFIX_STATE_PATH, state_out)

    counts = _ticket_counts()
    status_prev = _safe_read_json(AUTOFIX_STATUS_PATH)
    if not isinstance(status_prev, dict):
        status_prev = {}
    status_prev["ts"] = int(time.time())
    status_prev["settings_path"] = settings_path
    status_prev["enabled"] = bool(settings.get("autofix_enabled", True))
    status_prev["mode"] = str(settings.get("autofix_mode", "report_only") or "report_only")
    status_prev["last_ticket_id"] = str(ticket.get("id", "") or "")
    status_prev["applied_count_day"] = int(applied_count_day)
    status_prev["ticket_counts"] = counts
    status_prev["last_user_request_ts"] = int(now)
    status_prev["last_user_request_preview"] = req_txt[:180]
    status_prev["api_key_configured"] = bool(_resolve_openai_api_key(settings))
    if bool(applied_ok):
        status_prev["applied_ok"] = int(status_prev.get("applied_ok", 0) or 0) + 1
    if not dry_run:
        _atomic_write_json(AUTOFIX_STATUS_PATH, status_prev)

    return {
        "ok": True,
        "ticket_id": str(ticket.get("id", "") or ""),
        "status": str(ticket.get("status", "open") or "open"),
        "blocked_reason": str(
            ((ticket.get("blocked", {}) if isinstance(ticket.get("blocked", {}), dict) else {}).get("reason", "") or "")
        ),
        "proposal_ok": bool(llm.get("ok", False)),
        "llm_error": str(llm.get("error", "") or ""),
        "llm_detail": str(llm.get("detail", "") or "")[:600],
        "apply_attempted": bool(apply_attempted),
        "applied": bool(applied_ok),
        "apply_reason": str(apply_reason),
        "reason": "ticket_created",
        "ticket_path": ticket_path,
        "patch_path": patch_path,
        "dry_run": bool(dry_run),
        "settings_path": settings_path,
        "api_key_configured": bool(_resolve_openai_api_key(settings)),
        "auto_apply_requested": bool(auto_apply),
        "force_apply_requested": bool(force_apply),
    }


def run_once(dry_run: bool = False) -> Dict[str, Any]:
    os.makedirs(AUTOFIX_TICKETS_DIR, exist_ok=True)
    os.makedirs(AUTOFIX_PATCHES_DIR, exist_ok=True)

    settings, settings_path = _load_settings()
    now = int(time.time())

    enabled = bool(settings.get("autofix_enabled", True))
    mode = str(settings.get("autofix_mode", "report_only") or "report_only").strip().lower()
    try:
        poll_interval_s = max(5.0, float(settings.get("autofix_poll_interval_s", 45.0) or 45.0))
    except Exception:
        poll_interval_s = 45.0

    state = _safe_read_json(AUTOFIX_STATE_PATH)
    recent_fingerprints = state.get("recent_fingerprints", []) if isinstance(state.get("recent_fingerprints", []), list) else []
    recent_fingerprints = [str(x) for x in recent_fingerprints if str(x).strip()][-MAX_RECENT_FINGERPRINTS:]

    day_key = time.strftime("%Y-%m-%d", time.localtime(now))
    state_day = str(state.get("applied_day", "") or "")
    applied_count_day = int(state.get("applied_count_day", 0) or 0) if state_day == day_key else 0

    rows, new_offset = _read_jsonl_incremental(INCIDENTS_PATH, int(state.get("incidents_offset", 0) or 0))

    created = 0
    inspected = 0
    llm_ok = 0
    applied_ok = 0
    last_ticket_id = str(state.get("last_ticket_id", "") or "")
    retry_out: Dict[str, Any] = {"attempted": 0, "updated": 0, "applied": 0, "applied_count_day": int(applied_count_day), "last_ticket_id": ""}

    if enabled:
        for row in rows:
            inspected += 1
            if created >= MAX_TICKETS_PER_TICK:
                break
            if not _is_code_incident(row):
                continue
            fp = _incident_fingerprint(row)
            if fp in recent_fingerprints:
                continue

            ticket = _build_ticket(row, settings, seq=created + 1)
            llm = _llm_patch_proposal(ticket, settings)
            ticket["proposal"]["llm"] = llm
            if bool(llm.get("ok", False)):
                llm_ok += 1
                patch_path = _write_patch(ticket["id"], str(llm.get("diff", "") or ""))
                ticket["proposal"]["patch_diff_path"] = patch_path
                if isinstance(llm.get("target_files", []), list) and llm.get("target_files", []):
                    ticket["proposal"]["target_files"] = list(llm.get("target_files", []))
                if isinstance(llm.get("tests", []), list) and llm.get("tests", []):
                    ticket["proposal"]["recommended_tests"] = list(llm.get("tests", []))
                if str(llm.get("summary", "") or "").strip():
                    ticket["proposal"]["summary"] = str(llm.get("summary", "") or "").strip()
            _attach_proposal_meta(
                ticket,
                {
                    "target_files": ticket.get("proposal", {}).get("target_files", [])
                    if isinstance(ticket.get("proposal", {}), dict)
                    else [],
                    "diff": str(llm.get("diff", "") or ""),
                },
            )

            can_apply, reason = _can_apply(settings, applied_count_day=applied_count_day)
            if (not dry_run) and can_apply and str(ticket["proposal"].get("patch_diff_path", "") or "").strip():
                apply_out = _apply_patch(ticket["id"], str(ticket["proposal"].get("patch_diff_path", "") or ""), settings)
                ticket["apply"] = apply_out
                if bool(apply_out.get("ok", False)):
                    ticket["status"] = "applied"
                    applied_count_day += 1
                    applied_ok += 1
                else:
                    ticket["status"] = "open"
            else:
                ticket["apply"] = {
                    "attempted": False,
                    "ok": False,
                    "reason": reason,
                    "ts": int(now),
                }

            if not dry_run:
                ticket_path = os.path.join(AUTOFIX_TICKETS_DIR, f"{ticket['id']}.json")
                _atomic_write_json(ticket_path, ticket)
                runtime_event(
                    RUNTIME_EVENTS_PATH,
                    component="autofix",
                    event="autofix_ticket_created",
                    level="warning",
                    msg=f"ticket {ticket['id']} created",
                    details={
                        "ticket_id": ticket["id"],
                        "classifier": ticket.get("classifier", {}),
                        "status": ticket.get("status", "open"),
                    },
                )

            recent_fingerprints.append(fp)
            if len(recent_fingerprints) > MAX_RECENT_FINGERPRINTS:
                recent_fingerprints = recent_fingerprints[-MAX_RECENT_FINGERPRINTS:]
            created += 1
            last_ticket_id = str(ticket["id"])

        retry_out = _retry_open_user_request_tickets(settings, dry_run=dry_run, applied_count_day=applied_count_day)
        applied_count_day = int(retry_out.get("applied_count_day", applied_count_day) or applied_count_day)
        applied_ok += int(retry_out.get("applied", 0) or 0)
        retry_last = str(retry_out.get("last_ticket_id", "") or "").strip()
        if retry_last:
            last_ticket_id = retry_last

    created_count_total = int(state.get("created_count_total", 0) or 0) + int(created)

    state_out = {
        "ts": int(now),
        "settings_path": settings_path,
        "incidents_offset": int(new_offset),
        "recent_fingerprints": recent_fingerprints,
        "applied_day": day_key,
        "applied_count_day": int(applied_count_day),
        "last_ticket_id": last_ticket_id,
        "enabled": enabled,
        "mode": mode,
        "created_count_total": int(created_count_total),
        "request_retry_attempted": int(retry_out.get("attempted", 0) or 0),
        "request_retry_updated": int(retry_out.get("updated", 0) or 0),
        "request_retry_applied": int(retry_out.get("applied", 0) or 0),
        "request_retry_blocked": int(retry_out.get("blocked", 0) or 0),
    }
    if not dry_run:
        _atomic_write_json(AUTOFIX_STATE_PATH, state_out)

    metrics = _ticket_counts()
    status = {
        "ts": int(now),
        "enabled": enabled,
        "mode": mode,
        "poll_interval_s": float(poll_interval_s),
        "settings_path": settings_path,
        "inspected_incidents": int(inspected),
        "new_incidents_rows": int(len(rows)),
        "tickets_created": int(created),
        "llm_patch_ok": int(llm_ok),
        "applied_ok": int(applied_ok),
        "applied_count_day": int(applied_count_day),
        "last_ticket_id": last_ticket_id,
        "ticket_counts": metrics,
        "request_retry_attempted": int(retry_out.get("attempted", 0) or 0),
        "request_retry_updated": int(retry_out.get("updated", 0) or 0),
        "request_retry_applied": int(retry_out.get("applied", 0) or 0),
        "request_retry_blocked": int(retry_out.get("blocked", 0) or 0),
        "api_key_configured": bool(_resolve_openai_api_key(settings)),
        "dry_run": bool(dry_run),
    }
    if not dry_run:
        _atomic_write_json(AUTOFIX_STATUS_PATH, status)
    return status


def main() -> int:
    ap = argparse.ArgumentParser(description="PowerTrader runtime autofix overseer.")
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--apply-ticket", default="")
    ap.add_argument("--request-text", default="")
    ap.add_argument("--request-file", default="")
    ap.add_argument("--request-auto-apply", action="store_true")
    ap.add_argument("--force-apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    running = {"ok": True}

    def _stop(_signum: int, _frame: Any) -> None:
        running["ok"] = False

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    req_text = str(args.request_text or "").strip()
    req_file = str(args.request_file or "").strip()
    if (not req_text) and req_file:
        try:
            with open(req_file, "r", encoding="utf-8", errors="ignore") as f:
                req_text = str(f.read() or "").strip()
        except Exception as exc:
            out = {
                "ok": False,
                "reason": f"request_file_read_failed:{type(exc).__name__}",
                "request_file": req_file,
            }
            print(json.dumps(out, indent=2))
            return 1

    if req_text:
        out = create_request_ticket(
            req_text,
            auto_apply=bool(args.request_auto_apply),
            force_apply=bool(args.force_apply),
            dry_run=bool(args.dry_run),
        )
        print(json.dumps(out, indent=2))
        _log(
            "request "
            f"ticket={out.get('ticket_id')} "
            f"proposal_ok={out.get('proposal_ok')} "
            f"applied={out.get('applied')} "
            f"apply_reason={out.get('apply_reason')} "
            f"llm_error={out.get('llm_error')}"
        )
        return 0 if bool(out.get("ok", False)) else 1

    if str(args.apply_ticket or "").strip():
        out = apply_ticket_once(str(args.apply_ticket or "").strip(), force=bool(args.force_apply), dry_run=bool(args.dry_run))
        print(json.dumps(out, indent=2))
        _log(
            "apply-ticket "
            f"ticket={out.get('ticket_id')} "
            f"ok={out.get('ok')} "
            f"reason={out.get('reason')}"
        )
        if bool(out.get("ok", False)):
            return 0
        if str(out.get("reason", "") or "").strip().lower() in {"already_applied", "dry_run"}:
            return 0
        return 1

    if args.once:
        out = run_once(dry_run=bool(args.dry_run))
        _log(
            "once complete "
            f"enabled={out.get('enabled')} "
            f"mode={out.get('mode')} "
            f"tickets={out.get('tickets_created')} "
            f"request_retries={out.get('request_retry_attempted')} "
            f"request_blocked={out.get('request_retry_blocked')}"
        )
        return 0

    sleep_s = 45.0
    while running["ok"]:
        if os.path.exists(STOP_FLAG_PATH):
            break
        try:
            out = run_once(dry_run=bool(args.dry_run))
            sleep_s = max(5.0, float(out.get("poll_interval_s", 45.0) or 45.0))
            _log(
                "tick "
                f"enabled={out.get('enabled')} "
                f"mode={out.get('mode')} "
                f"rows={out.get('new_incidents_rows')} "
                f"tickets={out.get('tickets_created')} "
                f"request_retries={out.get('request_retry_attempted')} "
                f"request_blocked={out.get('request_retry_blocked')}"
            )
        except Exception as exc:
            _log(f"tick error {type(exc).__name__}: {exc}", level="error")
        time.sleep(float(sleep_s))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
