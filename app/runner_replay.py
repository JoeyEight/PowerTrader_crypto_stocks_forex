from __future__ import annotations

from typing import Any, Dict, Iterable, List


def replay_runner_heartbeats(rows: Iterable[Dict[str, Any]], stale_after_s: int = 12) -> Dict[str, Any]:
    seq = [r for r in rows if isinstance(r, dict)]
    seq = sorted(seq, key=lambda r: int(float(r.get("ts", 0) or 0)))
    if not seq:
        return {"samples": 0, "restarts": 0, "stale_transitions": 0, "states": []}

    restarts = 0
    stale_transitions = 0
    prev_pid = int(float(seq[0].get("runner_pid", 0) or 0))
    prev_ts = int(float(seq[0].get("ts", 0) or 0))
    states: List[Dict[str, Any]] = []
    was_stale = False

    for row in seq:
        ts = int(float(row.get("ts", 0) or 0))
        pid = int(float(row.get("runner_pid", 0) or 0))
        state = str(row.get("state", "") or "").strip().upper()
        if prev_pid > 0 and pid > 0 and pid != prev_pid:
            restarts += 1
        age = max(0, ts - prev_ts)
        stale = bool(age >= max(1, int(stale_after_s)))
        if stale and (not was_stale):
            stale_transitions += 1
        was_stale = stale
        states.append({"ts": ts, "pid": pid, "state": state, "age_s": age, "stale": stale})
        prev_pid = pid if pid > 0 else prev_pid
        prev_ts = ts

    return {
        "samples": int(len(seq)),
        "restarts": int(restarts),
        "stale_transitions": int(stale_transitions),
        "states": states[-120:],
    }
