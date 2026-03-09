from __future__ import annotations

import argparse
import json
import os
import time
import zipfile
from typing import Any, Dict, List


def _collect_files(hub_dir: str) -> List[str]:
    picks = [
        "runtime_state.json",
        "runtime_startup_checks.json",
        "market_sla_metrics.json",
        "market_trends.json",
        "autopilot_status.json",
        "autofix_status.json",
        "autofix_state.json",
        "incidents.jsonl",
        "runtime_events.jsonl",
    ]
    out: List[str] = []
    for name in picks:
        p = os.path.join(hub_dir, name)
        if os.path.isfile(p):
            out.append(p)
    logs_dir = os.path.join(hub_dir, "logs")
    if os.path.isdir(logs_dir):
        for name in os.listdir(logs_dir):
            p = os.path.join(logs_dir, name)
            if os.path.isfile(p):
                out.append(p)
    autofix_dir = os.path.join(hub_dir, "autofix")
    if os.path.isdir(autofix_dir):
        for root, _dirs, files in os.walk(autofix_dir):
            for name in files:
                p = os.path.join(root, name)
                if os.path.isfile(p):
                    out.append(p)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Archive diagnostics into a zip bundle.")
    ap.add_argument("--hub-dir", default="hub_data")
    ap.add_argument("--out-dir", default="hub_data/archives")
    args = ap.parse_args()

    hub_dir = os.path.abspath(str(args.hub_dir or "hub_data"))
    out_dir = os.path.abspath(str(args.out_dir or "hub_data/archives"))
    os.makedirs(out_dir, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S", time.localtime())
    out_path = os.path.join(out_dir, f"diagnostics_{ts}.zip")

    files = _collect_files(hub_dir)
    manifest_files: List[Dict[str, Any]] = []
    manifest: Dict[str, Any] = {"ts": int(time.time()), "hub_dir": hub_dir, "files": manifest_files}
    with zipfile.ZipFile(out_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for p in files:
            rel = os.path.relpath(p, hub_dir)
            try:
                st = os.stat(p)
                manifest_files.append({"path": rel, "size": int(st.st_size), "mtime": int(st.st_mtime)})
            except Exception:
                manifest_files.append({"path": rel, "size": 0, "mtime": 0})
            zf.write(p, arcname=rel)
        zf.writestr("manifest.json", json.dumps(manifest, indent=2))

    print(out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
