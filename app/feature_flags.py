from __future__ import annotations

from typing import Any, Dict, List

_FEATURE_FLAG_REGISTRY: List[Dict[str, str]] = [
    {
        "id": "paper_only_guard",
        "setting_key": "paper_only_unless_checklist_green",
        "label": "Paper-only live guard",
        "category": "safety",
        "description": "Blocks live execution unless runtime checklist is green.",
    },
    {
        "id": "stock_cached_scan_block",
        "setting_key": "stock_block_entries_on_cached_scan",
        "label": "Stocks cached-scan block",
        "category": "execution",
        "description": "Blocks stock entries while scanner is serving cached fallback data.",
    },
    {
        "id": "forex_cached_scan_block",
        "setting_key": "forex_block_entries_on_cached_scan",
        "label": "Forex cached-scan block",
        "category": "execution",
        "description": "Blocks forex entries while scanner is serving cached fallback data.",
    },
    {
        "id": "stock_data_quality_gate",
        "setting_key": "stock_require_data_quality_ok_for_entries",
        "label": "Stocks data-quality gate",
        "category": "scanner",
        "description": "Requires stock scanner data-quality health to be OK before entries.",
    },
    {
        "id": "forex_data_quality_gate",
        "setting_key": "forex_require_data_quality_ok_for_entries",
        "label": "Forex data-quality gate",
        "category": "scanner",
        "description": "Requires forex scanner data-quality health to be OK before entries.",
    },
    {
        "id": "autofix_live_apply",
        "setting_key": "autofix_allow_live_apply",
        "label": "Autofix live apply",
        "category": "autofix",
        "description": "Allows AI Assist patches to auto-apply in live_guarded mode.",
    },
    {
        "id": "stock_auto_trade",
        "setting_key": "stock_auto_trade_enabled",
        "label": "Stocks auto trade",
        "category": "execution",
        "description": "Enables autonomous stock order execution loop.",
    },
    {
        "id": "forex_auto_trade",
        "setting_key": "forex_auto_trade_enabled",
        "label": "Forex auto trade",
        "category": "execution",
        "description": "Enables autonomous forex order execution loop.",
    },
]


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    txt = str(value or "").strip().lower()
    if txt in {"1", "true", "yes", "on", "y", "t"}:
        return True
    if txt in {"0", "false", "no", "off", "n", "f"}:
        return False
    return bool(default)


def build_feature_flag_snapshot(settings: Dict[str, Any]) -> Dict[str, Any]:
    src = settings if isinstance(settings, dict) else {}
    rows: List[Dict[str, Any]] = []
    enabled = 0
    for row in _FEATURE_FLAG_REGISTRY:
        key = str(row.get("setting_key", "") or "").strip()
        val = _as_bool(src.get(key), default=False)
        if val:
            enabled += 1
        rows.append(
            {
                "id": str(row.get("id", "") or ""),
                "setting_key": key,
                "label": str(row.get("label", "") or ""),
                "category": str(row.get("category", "") or ""),
                "description": str(row.get("description", "") or ""),
                "enabled": bool(val),
            }
        )
    return {
        "flags": rows,
        "enabled_count": int(enabled),
        "total_count": int(len(rows)),
    }

