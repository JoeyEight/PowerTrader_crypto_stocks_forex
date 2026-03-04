from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from typing import Any, Dict, Optional

from path_utils import resolve_runtime_paths, resolve_settings_path, read_settings_file

BASE_DIR, _SETTINGS_PATH, HUB_DATA_DIR, _BOOT_SETTINGS = resolve_runtime_paths(__file__, "pt_runner")
RUNNER_PID_PATH = os.path.join(HUB_DATA_DIR, "runner.pid")
STOP_FLAG_PATH = os.path.join(HUB_DATA_DIR, "stop_trading.flag")
TRADER_STATUS_PATH = os.path.join(HUB_DATA_DIR, "trader_status.json")
LOG_DIR = os.path.join(HUB_DATA_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

RUNNER_LOG_PATH = os.path.join(LOG_DIR, "runner.log")
THINKER_LOG_PATH = os.path.join(LOG_DIR, "thinker.log")
TRADER_LOG_PATH = os.path.join(LOG_DIR, "trader.log")
MARKETS_LOG_PATH = os.path.join(LOG_DIR, "markets.log")

HEARTBEAT_INTERVAL_S = 2.0
MAX_BACKOFF_S = 30.0
CRASH_WINDOW_S = 600.0
CRASH_THRESHOLD = 10


def _runner_log(msg: str) -> None:
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}\n"
    try:
        with open(RUNNER_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass
    try:
        print(msg)
    except Exception:
        pass


def _atomic_write_json(path: str, data: Dict[str, Any]) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)


def _pid_is_alive(pid: Optional[int]) -> bool:
    try:
        if not pid or int(pid) <= 0:
            return False
        os.kill(int(pid), 0)
        return True
    except OSError:
        return False


def _read_pid_file(path: str) -> Optional[int]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = (f.read() or "").strip()
        pid = int(raw)
        return pid if pid > 0 else None
    except Exception:
        return None


def _write_pid_file(path: str, pid: int) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(str(int(pid)))
    os.replace(tmp, path)


def _remove_file(path: str) -> None:
    try:
        if os.path.exists(path):
            os.remove(path)
    except OSError:
        pass


def _settings_scripts() -> Dict[str, str]:
    settings_path = resolve_settings_path(BASE_DIR) or _SETTINGS_PATH or os.path.join(BASE_DIR, "gui_settings.json")
    data = read_settings_file(settings_path, module_name="pt_runner") or {}
    thinker_name = str(data.get("script_neural_runner2", "pt_thinker.py") or "pt_thinker.py").strip()
    trader_name = str(data.get("script_trader", "pt_trader.py") or "pt_trader.py").strip()
    markets_name = str(data.get("script_markets_runner", "pt_markets.py") or "pt_markets.py").strip()
    return {
        "thinker": os.path.abspath(os.path.join(BASE_DIR, thinker_name)),
        "trader": os.path.abspath(os.path.join(BASE_DIR, trader_name)),
        "markets": os.path.abspath(os.path.join(BASE_DIR, markets_name)),
    }


def _terminate_process(proc: Optional[subprocess.Popen], name: str, force: bool = False) -> None:
    if not proc or proc.poll() is not None:
        return
    try:
        if force:
            proc.kill()
        else:
            proc.terminate()
    except OSError as exc:
        _runner_log(f"{name}: terminate error {type(exc).__name__}: {exc}")


class ChildSpec:
    def __init__(self, name: str, script_path: str, log_path: str) -> None:
        self.name = name
        self.script_path = script_path
        self.log_path = log_path
        self.log_handle = None
        self.proc: Optional[subprocess.Popen] = None
        self.restarts = 0
        self.backoff_s = 1.0
        self.next_restart_at = 0.0
        self.crash_times = []
        self.last_exit: Dict[str, Any] = {}

    def pid(self) -> Optional[int]:
        if self.proc and self.proc.poll() is None:
            return int(self.proc.pid)
        return None


class Runner:
    def __init__(self) -> None:
        scripts = _settings_scripts()
        self.children = {
            "thinker": ChildSpec("thinker", scripts["thinker"], THINKER_LOG_PATH),
            "trader": ChildSpec("trader", scripts["trader"], TRADER_LOG_PATH),
            "markets": ChildSpec("markets", scripts["markets"], MARKETS_LOG_PATH),
        }
        self.state = "RUNNING"
        self.msg = "Supervisor starting"
        self.running = True
        self.current_backoff_s = 0.0

    def write_heartbeat(self) -> None:
        payload = {
            "state": self.state,
            "ts": int(time.time()),
            "runner_pid": int(os.getpid()),
            "thinker_pid": self.children["thinker"].pid(),
            "trader_pid": self.children["trader"].pid(),
            "markets_pid": self.children["markets"].pid(),
            "restarts": {
                "thinker": int(self.children["thinker"].restarts),
                "trader": int(self.children["trader"].restarts),
                "markets": int(self.children["markets"].restarts),
            },
            "last_exit": {
                "thinker": self.children["thinker"].last_exit or None,
                "trader": self.children["trader"].last_exit or None,
                "markets": self.children["markets"].last_exit or None,
            },
            "backoff_s": float(self.current_backoff_s),
            "msg": self.msg,
        }
        try:
            _atomic_write_json(TRADER_STATUS_PATH, payload)
        except Exception as exc:
            _runner_log(f"heartbeat write failed {type(exc).__name__}: {exc}")

    def start_child(self, key: str) -> None:
        child = self.children[key]
        if child.proc and child.proc.poll() is None:
            return
        if not os.path.isfile(child.script_path):
            self.state = "ERROR"
            self.msg = f"Missing script: {child.script_path}"
            _runner_log(self.msg)
            return
        try:
            if child.log_handle is not None:
                try:
                    child.log_handle.close()
                except Exception:
                    pass
            log_f = open(child.log_path, "a", encoding="utf-8")
            child.log_handle = log_f
            env = os.environ.copy()
            env["POWERTRADER_HUB_DIR"] = HUB_DATA_DIR
            proc = subprocess.Popen(
                [sys.executable, "-u", child.script_path],
                cwd=BASE_DIR,
                env=env,
                stdout=log_f,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                text=True,
            )
            child.proc = proc
            child.next_restart_at = 0.0
            self.msg = f"Started {key} pid={proc.pid}"
            _runner_log(self.msg)
        except Exception as exc:
            child.proc = None
            child.next_restart_at = time.time() + min(MAX_BACKOFF_S, child.backoff_s)
            self.state = "ERROR"
            self.msg = f"Failed to start {key}: {type(exc).__name__}: {exc}"
            _runner_log(self.msg)

    def handle_exit(self, key: str) -> None:
        child = self.children[key]
        if not child.proc:
            return
        code = child.proc.poll()
        if code is None:
            return
        now = time.time()
        child.last_exit = {"code": int(code), "ts": int(now)}
        child.crash_times = [ts for ts in child.crash_times if (now - ts) <= CRASH_WINDOW_S]
        child.crash_times.append(now)
        child.restarts += 1
        child.backoff_s = min(MAX_BACKOFF_S, child.backoff_s * 2.0 if child.backoff_s > 0 else 1.0)
        child.next_restart_at = now + child.backoff_s
        self.current_backoff_s = child.backoff_s
        child.proc = None
        if child.log_handle is not None:
            try:
                child.log_handle.close()
            except Exception:
                pass
            child.log_handle = None
        if len(child.crash_times) >= CRASH_THRESHOLD:
            self.state = "ERROR"
            self.msg = f"{key} crash loop; retrying in {child.backoff_s:.0f}s"
        else:
            self.state = "RUNNING"
            self.msg = f"{key} exited code={code}; restarting in {child.backoff_s:.0f}s"
        _runner_log(self.msg)

    def stop_all(self, force: bool = False) -> None:
        self.state = "STOPPING"
        self.msg = "Stopping child processes"
        self.write_heartbeat()
        for child in self.children.values():
            _terminate_process(child.proc, child.name, force=force)

    def wait_for_children(self, timeout_s: float = 5.0) -> None:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            alive = False
            for child in self.children.values():
                if child.proc and child.proc.poll() is None:
                    alive = True
                    break
            if not alive:
                return
            time.sleep(0.2)

    def run(self) -> int:
        last_heartbeat = 0.0
        while self.running:
            if os.path.exists(STOP_FLAG_PATH):
                self.state = "STOPPING"
                self.msg = "Stop flag detected"
                self.stop_all(force=False)
                self.wait_for_children(timeout_s=5.0)
                self.stop_all(force=True)
                self.state = "STOPPED"
                self.msg = "Stopped by flag"
                self.write_heartbeat()
                return 0

            now = time.time()
            for key, child in self.children.items():
                if child.proc and child.proc.poll() is not None:
                    self.handle_exit(key)
                if (not child.proc) and now >= float(child.next_restart_at):
                    self.start_child(key)

            if (now - last_heartbeat) >= HEARTBEAT_INTERVAL_S:
                if self.state not in ("ERROR", "STOPPING"):
                    self.state = "RUNNING"
                if not any(child.pid() for child in self.children.values()):
                    self.msg = "Waiting for child processes"
                self.write_heartbeat()
                last_heartbeat = now
            time.sleep(0.5)
        return 0


def _install_signal_handlers(runner: Runner) -> None:
    def _handle(_signum, _frame) -> None:
        runner.running = False
        runner.state = "STOPPING"
        runner.msg = "Signal received"
        runner.stop_all(force=False)
        runner.wait_for_children(timeout_s=3.0)
        runner.stop_all(force=True)
        runner.state = "STOPPED"
        runner.msg = "Stopped by signal"
        runner.write_heartbeat()

    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)


def main() -> int:
    existing_pid = _read_pid_file(RUNNER_PID_PATH)
    if _pid_is_alive(existing_pid):
        _runner_log(f"runner already active pid={existing_pid}; exiting")
        return 0

    _write_pid_file(RUNNER_PID_PATH, os.getpid())
    runner = Runner()
    _install_signal_handlers(runner)

    try:
        runner.write_heartbeat()
        return runner.run()
    finally:
        runner.stop_all(force=True)
        for child in runner.children.values():
            if child.log_handle is not None:
                try:
                    child.log_handle.close()
                except Exception:
                    pass
                child.log_handle = None
        runner.state = "STOPPED"
        runner.msg = "Runner exiting"
        runner.write_heartbeat()
        _remove_file(RUNNER_PID_PATH)


if __name__ == "__main__":
    sys.exit(main())
