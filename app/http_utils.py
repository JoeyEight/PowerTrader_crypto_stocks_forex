from __future__ import annotations

import email.utils
import re
import time
from typing import Any


def parse_retry_after_value(raw: Any, now_ts: float | None = None, max_wait_s: float = 3600.0) -> float:
    text = str(raw or "").strip()
    if not text:
        return 0.0
    now = float(time.time() if now_ts is None else now_ts)
    cap = max(0.0, float(max_wait_s))

    # Delta-seconds form.
    try:
        v = float(text)
        if v < 0.0:
            return 0.0
        return min(cap, v)
    except Exception:
        pass

    # HTTP-date form.
    try:
        dt = email.utils.parsedate_to_datetime(text)
        if dt is not None:
            target = float(dt.timestamp())
            delta = max(0.0, target - now)
            return min(cap, delta)
    except Exception:
        pass

    # Text fallback: "... retry after 12.5 sec ..."
    try:
        m = re.search(r"retry[-_ ]after[^0-9]*([0-9]+(?:\.[0-9]+)?)", text, flags=re.IGNORECASE)
        if m:
            return min(cap, max(0.0, float(m.group(1))))
        m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*(s|sec|secs|second|seconds)\b", text, flags=re.IGNORECASE)
        if m and ("retry" in text.lower() or "wait" in text.lower()):
            return min(cap, max(0.0, float(m.group(1))))
    except Exception:
        pass
    return 0.0


def retry_after_from_urllib_http_error(exc: Exception, max_wait_s: float = 3600.0) -> float:
    try:
        headers = getattr(exc, "headers", {}) or {}
        raw = headers.get("Retry-After", "")
    except Exception:
        raw = ""
    wait_s = parse_retry_after_value(raw, max_wait_s=max_wait_s)
    if wait_s > 0.0:
        return wait_s
    parts = [str(exc or "")]
    try:
        reason = str(getattr(exc, "reason", "") or "").strip()
        if reason:
            parts.append(reason)
    except Exception:
        pass
    try:
        reader = getattr(exc, "read", None)
        body = ""
        if callable(reader):
            raw_body = reader()
            if isinstance(raw_body, bytes):
                body = raw_body.decode("utf-8", errors="ignore")
            elif isinstance(raw_body, str):
                body = raw_body
        if body:
            parts.append(body[:4000])
    except Exception:
        pass
    return parse_retry_after_value(" | ".join(parts), max_wait_s=max_wait_s)


def retry_after_from_requests_exception(exc: Exception, max_wait_s: float = 3600.0) -> float:
    raw = ""
    try:
        resp = getattr(exc, "response", None)
        if resp is not None:
            raw = str((getattr(resp, "headers", {}) or {}).get("Retry-After", "") or "")
    except Exception:
        raw = ""
    if str(raw or "").strip():
        return parse_retry_after_value(raw, max_wait_s=max_wait_s)
    return parse_retry_after_value(str(exc or ""), max_wait_s=max_wait_s)
