from __future__ import annotations

import urllib.parse
from typing import Any, Dict, List, Tuple

ALPACA_PAPER_HOST = "paper-api.alpaca.markets"
ALPACA_LIVE_HOST = "api.alpaca.markets"
ALPACA_DATA_HOST = "data.alpaca.markets"

OANDA_PRACTICE_REST_HOST = "api-fxpractice.oanda.com"
OANDA_LIVE_REST_HOST = "api-fxtrade.oanda.com"
OANDA_PRACTICE_STREAM_HOST = "stream-fxpractice.oanda.com"
OANDA_LIVE_STREAM_HOST = "stream-fxtrade.oanda.com"


def _host_from_url(url: str) -> str:
    try:
        host = str(urllib.parse.urlparse(str(url or "")).netloc or "").strip().lower()
    except Exception:
        host = ""
    if ":" in host:
        host = host.split(":", 1)[0].strip()
    return host


def normalize_endpoint_url(raw: Any, default: str = "") -> Tuple[str, bool, str]:
    text = str(raw or "").strip()
    if not text:
        text = str(default or "").strip()
    if not text:
        return "", False, ""
    if "://" not in text:
        text = f"https://{text}"
    try:
        parsed = urllib.parse.urlparse(text)
    except Exception:
        return "", False, ""
    scheme = str(parsed.scheme or "").strip().lower()
    netloc = str(parsed.netloc or "").strip().lower()
    path = str(parsed.path or "").strip()
    if not netloc and path and "/" not in path:
        netloc = path.lower()
        path = ""
    if scheme not in {"http", "https"} or not netloc:
        return "", False, ""
    normalized = urllib.parse.urlunparse((scheme, netloc, path.rstrip("/"), "", "", ""))
    return normalized, True, _host_from_url(normalized)


def _issue(level: str, code: str, message: str, details: Dict[str, Any] | None = None) -> Dict[str, Any]:
    return {
        "level": str(level or "warning").strip().lower(),
        "code": str(code or "").strip(),
        "message": str(message or "").strip(),
        "details": dict(details or {}),
    }


def validate_alpaca_endpoints(base_url: Any, data_url: Any, paper_mode: bool = True) -> Dict[str, Any]:
    base_norm, base_ok, base_host = normalize_endpoint_url(base_url, default=f"https://{ALPACA_PAPER_HOST}")
    data_norm, data_ok, data_host = normalize_endpoint_url(data_url, default=f"https://{ALPACA_DATA_HOST}")
    issues: List[Dict[str, Any]] = []

    if not base_ok:
        issues.append(
            _issue(
                "critical",
                "alpaca_base_url_invalid",
                "Alpaca base URL is invalid.",
                {"value": str(base_url or "")},
            )
        )
    if not data_ok:
        issues.append(
            _issue(
                "critical",
                "alpaca_data_url_invalid",
                "Alpaca data URL is invalid.",
                {"value": str(data_url or "")},
            )
        )

    if base_ok:
        recognized = {ALPACA_PAPER_HOST, ALPACA_LIVE_HOST}
        if base_host not in recognized:
            issues.append(
                _issue(
                    "warning",
                    "alpaca_base_host_unrecognized",
                    "Alpaca base URL host is not one of the official paper/live API hosts.",
                    {"host": base_host},
                )
            )
        if bool(paper_mode) and base_host == ALPACA_LIVE_HOST:
            issues.append(
                _issue(
                    "warning",
                    "alpaca_paper_mode_uses_live_endpoint",
                    "Alpaca is set to paper mode but base URL points to live endpoint.",
                    {"host": base_host},
                )
            )
        if (not bool(paper_mode)) and base_host == ALPACA_PAPER_HOST:
            issues.append(
                _issue(
                    "warning",
                    "alpaca_live_mode_uses_paper_endpoint",
                    "Alpaca is set to live mode but base URL points to paper endpoint.",
                    {"host": base_host},
                )
            )

    if data_ok and data_host != ALPACA_DATA_HOST:
        issues.append(
            _issue(
                "warning",
                "alpaca_data_host_unexpected",
                "Alpaca data URL host differs from official data host.",
                {"host": data_host},
            )
        )

    return {
        "service": "alpaca",
        "valid": bool(base_ok and data_ok),
        "normalized_base_url": base_norm,
        "normalized_data_url": data_norm,
        "base_host": base_host,
        "data_host": data_host,
        "issues": issues,
    }


def validate_oanda_endpoints(rest_url: Any, stream_url: Any = "", practice_mode: bool = True) -> Dict[str, Any]:
    rest_default = f"https://{OANDA_PRACTICE_REST_HOST}" if bool(practice_mode) else f"https://{OANDA_LIVE_REST_HOST}"
    stream_default = f"https://{OANDA_PRACTICE_STREAM_HOST}" if bool(practice_mode) else f"https://{OANDA_LIVE_STREAM_HOST}"
    rest_norm, rest_ok, rest_host = normalize_endpoint_url(rest_url, default=rest_default)
    stream_norm, stream_ok, stream_host = normalize_endpoint_url(stream_url, default=stream_default)
    issues: List[Dict[str, Any]] = []

    if not rest_ok:
        issues.append(
            _issue(
                "critical",
                "oanda_rest_url_invalid",
                "OANDA REST URL is invalid.",
                {"value": str(rest_url or "")},
            )
        )
    if not stream_ok:
        issues.append(
            _issue(
                "critical",
                "oanda_stream_url_invalid",
                "OANDA stream URL is invalid.",
                {"value": str(stream_url or "")},
            )
        )

    if rest_ok:
        recognized_rest = {OANDA_PRACTICE_REST_HOST, OANDA_LIVE_REST_HOST}
        if rest_host not in recognized_rest:
            issues.append(
                _issue(
                    "warning",
                    "oanda_rest_host_unrecognized",
                    "OANDA REST URL host is not one of the official practice/live hosts.",
                    {"host": rest_host},
                )
            )
        if bool(practice_mode) and rest_host == OANDA_LIVE_REST_HOST:
            issues.append(
                _issue(
                    "warning",
                    "oanda_practice_mode_uses_live_endpoint",
                    "OANDA is set to practice mode but REST URL points to live endpoint.",
                    {"host": rest_host},
                )
            )
        if (not bool(practice_mode)) and rest_host == OANDA_PRACTICE_REST_HOST:
            issues.append(
                _issue(
                    "warning",
                    "oanda_live_mode_uses_practice_endpoint",
                    "OANDA is set to live mode but REST URL points to practice endpoint.",
                    {"host": rest_host},
                )
            )

    if stream_ok:
        recognized_stream = {OANDA_PRACTICE_STREAM_HOST, OANDA_LIVE_STREAM_HOST}
        if stream_host not in recognized_stream:
            issues.append(
                _issue(
                    "warning",
                    "oanda_stream_host_unrecognized",
                    "OANDA stream URL host is not one of the official practice/live stream hosts.",
                    {"host": stream_host},
                )
            )
        if bool(practice_mode) and stream_host == OANDA_LIVE_STREAM_HOST:
            issues.append(
                _issue(
                    "warning",
                    "oanda_practice_mode_uses_live_stream",
                    "OANDA is set to practice mode but stream URL points to live stream host.",
                    {"host": stream_host},
                )
            )
        if (not bool(practice_mode)) and stream_host == OANDA_PRACTICE_STREAM_HOST:
            issues.append(
                _issue(
                    "warning",
                    "oanda_live_mode_uses_practice_stream",
                    "OANDA is set to live mode but stream URL points to practice stream host.",
                    {"host": stream_host},
                )
            )

    return {
        "service": "oanda",
        "valid": bool(rest_ok and stream_ok),
        "normalized_rest_url": rest_norm,
        "normalized_stream_url": stream_norm,
        "rest_host": rest_host,
        "stream_host": stream_host,
        "issues": issues,
    }

