from __future__ import annotations

import os
import stat
from typing import Any, Dict, Iterable, Optional, Tuple


def _clean(v: Any) -> str:
    return str(v or "").strip()


def env_or_setting(settings: Dict[str, Any], setting_key: str, env_keys: Iterable[str], default: str = "") -> str:
    for k in env_keys:
        v = _clean(os.environ.get(str(k)))
        if v:
            return v
    v = _clean((settings or {}).get(setting_key, ""))
    return v if v else _clean(default)


def _resolve_base_dir(base_dir: Optional[str] = None) -> str:
    if str(base_dir or "").strip():
        return os.path.abspath(str(base_dir).strip())
    env_base = _clean(os.environ.get("POWERTRADER_PROJECT_DIR"))
    if env_base:
        return os.path.abspath(env_base)
    return os.path.abspath(os.getcwd())


def _ensure_keys_dir(base_dir: Optional[str] = None) -> str:
    keys_dir = os.path.join(_resolve_base_dir(base_dir), "keys")
    os.makedirs(keys_dir, exist_ok=True)
    return keys_dir


def alpaca_credential_paths(base_dir: Optional[str] = None) -> Tuple[str, str]:
    keys_dir = _ensure_keys_dir(base_dir)
    return (
        os.path.join(keys_dir, "alpaca_key_id.txt"),
        os.path.join(keys_dir, "alpaca_secret_key.txt"),
    )


def _alpaca_legacy_path_pairs(base_dir: Optional[str] = None) -> list[Tuple[str, str]]:
    base = _resolve_base_dir(base_dir)
    keys_dir = os.path.join(base, "keys")
    return [
        (
            os.path.join(base, "alpaca_key_id.txt"),
            os.path.join(base, "alpaca_secret_key.txt"),
        ),
        (
            os.path.join(keys_dir, "alpaca_api_key_id.txt"),
            os.path.join(keys_dir, "alpaca_api_secret_key.txt"),
        ),
        (
            os.path.join(base, "alpaca_api_key_id.txt"),
            os.path.join(base, "alpaca_api_secret_key.txt"),
        ),
        (
            os.path.join(keys_dir, "alpaca_api_key.txt"),
            os.path.join(keys_dir, "alpaca_api_secret.txt"),
        ),
        (
            os.path.join(base, "alpaca_api_key.txt"),
            os.path.join(base, "alpaca_api_secret.txt"),
        ),
    ]


def oanda_credential_paths(base_dir: Optional[str] = None) -> Tuple[str, str]:
    keys_dir = _ensure_keys_dir(base_dir)
    return (
        os.path.join(keys_dir, "oanda_account_id.txt"),
        os.path.join(keys_dir, "oanda_api_token.txt"),
    )


def _read_pair_files(path_a: str, path_b: str) -> Tuple[str, str]:
    try:
        with open(path_a, "r", encoding="utf-8") as fa:
            a = _clean(fa.read())
    except Exception:
        a = ""
    try:
        with open(path_b, "r", encoding="utf-8") as fb:
            b = _clean(fb.read())
    except Exception:
        b = ""
    return a, b


def get_alpaca_creds_from_files(base_dir: Optional[str] = None) -> Tuple[str, str]:
    key_path, secret_path = alpaca_credential_paths(base_dir)
    key, secret = _read_pair_files(key_path, secret_path)
    if key and secret:
        return key, secret
    for legacy_key_path, legacy_secret_path in _alpaca_legacy_path_pairs(base_dir):
        lk, ls = _read_pair_files(legacy_key_path, legacy_secret_path)
        if not key and lk:
            key = lk
        if not secret and ls:
            secret = ls
        if key and secret:
            break
    return key, secret


def get_oanda_creds_from_files(base_dir: Optional[str] = None) -> Tuple[str, str]:
    account_path, token_path = oanda_credential_paths(base_dir)
    return _read_pair_files(account_path, token_path)


def get_alpaca_creds(settings: Dict[str, Any], base_dir: Optional[str] = None) -> Tuple[str, str]:
    key = env_or_setting(
        settings,
        "alpaca_api_key_id",
        (
            "POWERTRADER_ALPACA_API_KEY_ID",
            "ALPACA_API_KEY_ID",
            "APCA_API_KEY_ID",
            "ALPACA_KEY_ID",
            "ALPACA_API_KEY",
        ),
    )
    secret = env_or_setting(
        settings,
        "alpaca_secret_key",
        (
            "POWERTRADER_ALPACA_SECRET_KEY",
            "ALPACA_SECRET_KEY",
            "APCA_API_SECRET_KEY",
            "ALPACA_API_SECRET_KEY",
            "ALPACA_SECRET",
        ),
    )
    if (not key) or (not secret):
        fk, fs = get_alpaca_creds_from_files(base_dir)
        if not key:
            key = fk
        if not secret:
            secret = fs
    return key, secret


def get_oanda_creds(settings: Dict[str, Any], base_dir: Optional[str] = None) -> Tuple[str, str]:
    account_id = env_or_setting(
        settings,
        "oanda_account_id",
        ("POWERTRADER_OANDA_ACCOUNT_ID", "OANDA_ACCOUNT_ID"),
    )
    token = env_or_setting(
        settings,
        "oanda_api_token",
        ("POWERTRADER_OANDA_API_TOKEN", "OANDA_API_TOKEN"),
    )
    if (not account_id) or (not token):
        fa, ft = get_oanda_creds_from_files(base_dir)
        if not account_id:
            account_id = fa
        if not token:
            token = ft
    return account_id, token


def twelvedata_credential_path(base_dir: Optional[str] = None) -> str:
    keys_dir = _ensure_keys_dir(base_dir)
    return os.path.join(keys_dir, "twelvedata_api_key.txt")


def get_twelvedata_api_key(settings: Dict[str, Any], base_dir: Optional[str] = None) -> str:
    key = env_or_setting(
        settings,
        "twelvedata_api_key",
        ("POWERTRADER_TWELVEDATA_API_KEY", "TWELVEDATA_API_KEY", "TWELVE_DATA_API_KEY"),
    )
    if key:
        return key
    path = twelvedata_credential_path(base_dir)
    try:
        with open(path, "r", encoding="utf-8") as f:
            key = _clean(f.read())
        if key:
            return key
    except Exception:
        pass
    return ""


def openai_credential_path(base_dir: Optional[str] = None) -> str:
    keys_dir = _ensure_keys_dir(base_dir)
    return os.path.join(keys_dir, "openai_api_key.txt")


def get_openai_api_key(settings: Optional[Dict[str, Any]] = None, base_dir: Optional[str] = None) -> str:
    """
    Resolve OpenAI API key from env first, then optional settings, then project key files.
    """
    env_key = _clean(os.environ.get("OPENAI_API_KEY") or os.environ.get("POWERTRADER_OPENAI_API_KEY"))
    if env_key:
        return env_key

    cfg = settings if isinstance(settings, dict) else {}
    cfg_key = _clean(cfg.get("openai_api_key", ""))
    if cfg_key:
        return cfg_key

    preferred = openai_credential_path(base_dir)
    legacy = os.path.join(_resolve_base_dir(base_dir), "openai_api_key.txt")
    for path in (preferred, legacy):
        try:
            with open(path, "r", encoding="utf-8") as f:
                key = _clean(f.read())
            if key:
                return key
        except Exception:
            continue
    return ""


def get_robinhood_creds_from_env() -> Tuple[str, str]:
    api_key = _clean(os.environ.get("POWERTRADER_RH_API_KEY") or os.environ.get("ROBINHOOD_API_KEY"))
    private_b64 = _clean(os.environ.get("POWERTRADER_RH_PRIVATE_B64") or os.environ.get("ROBINHOOD_PRIVATE_B64"))
    return api_key, private_b64


def robinhood_credential_paths(base_dir: str) -> Tuple[str, str]:
    base = os.path.abspath(str(base_dir or "."))
    keys_dir = os.path.join(base, "keys")
    os.makedirs(keys_dir, exist_ok=True)
    return os.path.join(keys_dir, "r_key.txt"), os.path.join(keys_dir, "r_secret.txt")


def get_robinhood_creds_from_files(base_dir: str) -> Tuple[str, str]:
    """
    Read Robinhood creds from preferred keys/ paths, then legacy root paths.
    """
    key_path, secret_path = robinhood_credential_paths(base_dir)
    legacy_key = os.path.join(os.path.abspath(str(base_dir or ".")), "r_key.txt")
    legacy_secret = os.path.join(os.path.abspath(str(base_dir or ".")), "r_secret.txt")
    for kp, sp in ((key_path, secret_path), (legacy_key, legacy_secret)):
        try:
            with open(kp, "r", encoding="utf-8") as f:
                k = _clean(f.read())
            with open(sp, "r", encoding="utf-8") as f:
                s = _clean(f.read())
            if k and s:
                return k, s
        except Exception:
            continue
    return "", ""


def normalize_start_allocation_pct(value: Any, default_pct: float = 0.5) -> float:
    """
    Normalize start allocation as a percent value.
    Legacy configs used tiny fractional values (e.g. 0.005 intended as 0.5%).
    For small positive values <= 0.01, treat input as fraction and convert to percent.
    """
    try:
        pct = float(str(value).replace("%", "").strip())
    except Exception:
        pct = float(default_pct)
    if pct < 0.0:
        pct = 0.0
    if 0.0 < pct <= 0.01:
        pct *= 100.0
    return pct


def key_file_permission_issues(base_dir: str) -> list[str]:
    base = os.path.abspath(str(base_dir or "."))
    keys_dir = os.path.join(base, "keys")
    checks = [
        os.path.join(keys_dir, "r_secret.txt"),
        os.path.join(keys_dir, "robinhood_private.pem"),
        os.path.join(base, "r_secret.txt"),  # legacy path
    ]
    issues: list[str] = []
    for path in checks:
        if not os.path.isfile(path):
            continue
        try:
            mode = stat.S_IMODE(os.stat(path).st_mode)
        except Exception:
            continue
        # Require owner-read/write only (0600) or stricter.
        if (mode & 0o077) != 0:
            issues.append(f"weak_key_permissions:{path}:{oct(mode)}")
    return issues


def key_rotation_reminder_issues(base_dir: str, max_age_days: int = 90) -> list[str]:
    base = os.path.abspath(str(base_dir or "."))
    max_days = max(7, int(max_age_days))
    max_age_s = float(max_days) * 86400.0
    try:
        import time as _t
        now = float(_t.time())
    except Exception:
        now = 0.0

    checks = [
        os.path.join(base, "keys", "r_secret.txt"),
        os.path.join(base, "keys", "robinhood_private.pem"),
        os.path.join(base, "keys", "r_key.txt"),
        os.path.join(base, "r_secret.txt"),
        os.path.join(base, "r_key.txt"),
    ]
    issues: list[str] = []
    for path in checks:
        if not os.path.isfile(path):
            continue
        try:
            age_s = max(0.0, now - float(os.path.getmtime(path)))
        except Exception:
            continue
        if age_s >= max_age_s:
            age_days = int(age_s // 86400.0)
            issues.append(f"key_rotation_due:{path}:{age_days}d")
    return issues
