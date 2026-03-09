from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List, Tuple


def _ensure_dir(path: str) -> None:
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass


def operator_notes_paths(hub_dir: str) -> Tuple[str, str]:
    base = os.path.abspath(str(hub_dir or "hub_data"))
    return (
        os.path.join(base, "operator_notes.md"),
        os.path.join(base, "operator_notes_log.jsonl"),
    )


def ensure_operator_notes_files(hub_dir: str) -> Tuple[str, str]:
    md_path, log_path = operator_notes_paths(hub_dir)
    _ensure_dir(os.path.dirname(md_path))
    if not os.path.isfile(md_path):
        header = (
            "# Operator Notes\n\n"
            "Use this file for handoff context, risk decisions, and incident summaries.\n\n"
            "## Format Guidance\n"
            "- Use short headers for each shift or event.\n"
            "- Keep action items explicit.\n"
            "- Include timestamps for critical decisions.\n\n"
        )
        try:
            with open(md_path, "w", encoding="utf-8") as f:
                f.write(header)
        except Exception:
            pass
    if not os.path.isfile(log_path):
        try:
            with open(log_path, "a", encoding="utf-8"):
                pass
        except Exception:
            pass
    return md_path, log_path


def read_operator_notes_markdown(path: str, max_chars: int = 500_000) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = f.read()
    except Exception:
        return ""
    txt = str(data or "")
    return txt[-max(1, int(max_chars)) :]


def write_operator_notes_markdown(path: str, text: str, max_chars: int = 750_000) -> bool:
    payload = str(text or "")
    if len(payload) > int(max_chars):
        payload = payload[-int(max_chars) :]
    try:
        _ensure_dir(os.path.dirname(path))
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(payload)
        os.replace(tmp, path)
        return True
    except Exception:
        return False


def append_operator_note_entry(
    hub_dir: str,
    title: str,
    body: str,
    actor: str = "operator",
) -> Dict[str, Any]:
    md_path, log_path = ensure_operator_notes_files(hub_dir)
    ts = int(time.time())
    stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
    title_txt = str(title or "").strip() or "Untitled note"
    body_txt = str(body or "").strip()
    actor_txt = str(actor or "operator").strip() or "operator"
    row = {
        "ts": int(ts),
        "actor": actor_txt,
        "title": title_txt,
        "body": body_txt,
    }

    try:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=True) + "\n")
    except Exception:
        pass

    md_block = (
        f"\n## {stamp} | {actor_txt} | {title_txt}\n\n"
        f"{body_txt}\n"
    )
    try:
        with open(md_path, "a", encoding="utf-8") as f:
            f.write(md_block)
    except Exception:
        pass
    return row


def read_recent_operator_note_entries(path: str, max_entries: int = 120) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = [ln.strip() for ln in f if ln.strip()]
    except Exception:
        return out
    for ln in lines[-max(1, int(max_entries)) :]:
        try:
            row = json.loads(ln)
        except Exception:
            continue
        if isinstance(row, dict):
            out.append(row)
    out.sort(key=lambda r: int(float(r.get("ts", 0) or 0)), reverse=True)
    return out
