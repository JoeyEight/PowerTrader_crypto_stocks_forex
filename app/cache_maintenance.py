from __future__ import annotations

import os
import time
from typing import Any, Dict, List, TypedDict


class _FileMeta(TypedDict):
    path: str
    size: int
    mtime: float


def _candidate_files(hub_dir: str) -> List[str]:
    roots = [
        os.path.join(hub_dir, "current_prices"),
        os.path.join(hub_dir, "cache"),
    ]
    out: List[str] = []
    for root in roots:
        if not os.path.isdir(root):
            continue
        for base, _, files in os.walk(root):
            for name in files:
                out.append(os.path.join(base, name))
    return out


def _quality_artifact_files(hub_dir: str) -> List[str]:
    out: List[str] = []
    exports_charts = os.path.join(hub_dir, "exports", "charts")
    if os.path.isdir(exports_charts):
        for name in os.listdir(exports_charts):
            if (not name.startswith("scanner_quality_")) or (not name.endswith(".json")):
                continue
            path = os.path.join(exports_charts, name)
            if os.path.isfile(path):
                out.append(path)
    for market in ("stocks", "forex"):
        market_dir = os.path.join(hub_dir, market)
        if not os.path.isdir(market_dir):
            continue
        for name in os.listdir(market_dir):
            if (not name.startswith("universe_quality_")) or (not name.endswith(".json")):
                continue
            path = os.path.join(market_dir, name)
            if os.path.isfile(path):
                out.append(path)
        history_dir = os.path.join(market_dir, "quality_reports")
        if os.path.isdir(history_dir):
            for base, _, files in os.walk(history_dir):
                for name in files:
                    path = os.path.join(base, name)
                    if os.path.isfile(path):
                        out.append(path)
    return out


def prune_data_cache(hub_dir: str, max_age_days: float = 14.0, max_total_bytes: int = 300 * 1024 * 1024) -> Dict[str, Any]:
    now = time.time()
    max_age_s = max(1.0, float(max_age_days)) * 86400.0
    files: List[_FileMeta] = []
    for path in _candidate_files(hub_dir):
        try:
            st = os.stat(path)
            files.append({"path": path, "size": int(st.st_size), "mtime": float(st.st_mtime)})
        except Exception:
            continue

    removed = 0
    removed_bytes = 0
    total_bytes = sum(x["size"] for x in files)

    for row in sorted(files, key=lambda x: x["mtime"]):
        if (now - row["mtime"]) <= max_age_s:
            continue
        try:
            os.remove(row["path"])
            removed += 1
            removed_bytes += row["size"]
            total_bytes -= row["size"]
        except Exception:
            pass

    if total_bytes > int(max_total_bytes):
        survivors: List[_FileMeta] = [row for row in files if os.path.isfile(row["path"])]
        for row in sorted(survivors, key=lambda x: x["mtime"]):
            if total_bytes <= int(max_total_bytes):
                break
            try:
                os.remove(row["path"])
                removed += 1
                removed_bytes += row["size"]
                total_bytes -= row["size"]
            except Exception:
                pass

    return {
        "removed": int(removed),
        "removed_bytes": int(removed_bytes),
        "total_bytes": int(max(0, total_bytes)),
        "candidates": int(len(files)),
    }


def prune_scanner_quality_artifacts(hub_dir: str, max_age_days: float = 14.0) -> Dict[str, Any]:
    now = time.time()
    max_age_s = max(1.0, float(max_age_days)) * 86400.0
    files: List[_FileMeta] = []
    for path in _quality_artifact_files(hub_dir):
        try:
            st = os.stat(path)
            files.append({"path": path, "size": int(st.st_size), "mtime": float(st.st_mtime)})
        except Exception:
            continue

    removed = 0
    removed_bytes = 0
    kept = 0
    for row in sorted(files, key=lambda x: x["mtime"]):
        if (now - row["mtime"]) <= max_age_s:
            kept += 1
            continue
        try:
            os.remove(row["path"])
            removed += 1
            removed_bytes += row["size"]
        except Exception:
            kept += 1
    return {
        "removed": int(removed),
        "removed_bytes": int(removed_bytes),
        "kept": int(kept),
        "candidates": int(len(files)),
    }
