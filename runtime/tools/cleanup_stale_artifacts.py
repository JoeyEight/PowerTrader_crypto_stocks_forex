from __future__ import annotations

import argparse
import os
import shutil
import time
from typing import List


def _coin_dirs(project_dir: str) -> List[str]:
    out = []
    for name in os.listdir(project_dir):
        p = os.path.join(project_dir, name)
        if not os.path.isdir(p):
            continue
        if not name.isalnum() or len(name) > 12:
            continue
        out.append(p)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Cleanup stale coin artifacts.")
    ap.add_argument("--project-dir", default=".")
    ap.add_argument("--days", type=float, default=30.0)
    args = ap.parse_args()

    project_dir = os.path.abspath(str(args.project_dir or "."))
    cutoff = time.time() - (max(1.0, float(args.days)) * 86400.0)
    patterns = ("trainer_status.json", "trainer_last_start_time.txt", "trainer_last_training_time.txt")
    removed = 0
    for cdir in _coin_dirs(project_dir):
        for name in os.listdir(cdir):
            path = os.path.join(cdir, name)
            if not os.path.isfile(path):
                continue
            if name in patterns or name.startswith("memories_") or name.startswith("memory_weights_"):
                try:
                    if os.path.getmtime(path) < cutoff:
                        os.remove(path)
                        removed += 1
                except Exception:
                    pass
        stale_tmp = os.path.join(cdir, "__pycache__")
        if os.path.isdir(stale_tmp):
            try:
                shutil.rmtree(stale_tmp, ignore_errors=True)
            except Exception:
                pass
    print(f"cleanup_stale_artifacts removed={removed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
