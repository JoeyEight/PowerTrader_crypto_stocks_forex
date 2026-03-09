from __future__ import annotations

import argparse
import json
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if __package__ in (None, ""):
    if _ROOT not in sys.path:
        sys.path.insert(0, _ROOT)


def main() -> int:
    from app.path_utils import resolve_runtime_paths
    from app.runtime_artifacts import bootstrap_runtime_artifacts

    ap = argparse.ArgumentParser(description="Create/upgrade baseline runtime artifacts in hub_data.")
    ap.add_argument("--hub-dir", default="", help="Optional explicit hub_data path.")
    ap.add_argument("--force", action="store_true", help="Rewrite baseline artifacts even if present.")
    args = ap.parse_args()

    probe_file = os.path.join(_ROOT, "runtime", "pt_runner.py")
    _base, _settings, auto_hub_dir, _ = resolve_runtime_paths(probe_file, "bootstrap_runtime_artifacts")
    hub_dir = os.path.abspath(str(args.hub_dir or auto_hub_dir))
    out = bootstrap_runtime_artifacts(hub_dir, force=bool(args.force))
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
