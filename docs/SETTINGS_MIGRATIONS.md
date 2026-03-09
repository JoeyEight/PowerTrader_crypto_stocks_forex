# Settings Schema & Migrations

## Current Version
- `settings_schema_version = 3`

## Version Notes
### v1 -> v2
- Migrates legacy root script paths to package paths:
  - `pt_thinker.py` -> `engines/pt_thinker.py`
  - `pt_trader.py` -> `engines/pt_trader.py`
  - `pt_trainer.py` -> `engines/pt_trainer.py`
  - `pt_markets.py` -> `runtime/pt_markets.py`
  - `pt_autopilot.py` -> `runtime/pt_autopilot.py`

### v2 -> v3
- Adds live safety defaults:
  - `paper_only_unless_checklist_green = true`
  - `key_rotation_warn_days = 90`

## Upgrade Notes Storage
- Recent migration notes are written to:
  - `settings_upgrade_notes` in `gui_settings.json`

## Pass 3 Additions (No Schema Bump)
- Added scanner cadence alert tuning keys:
  - `runtime_alert_cadence_warn_count`
  - `runtime_alert_cadence_crit_count`
  - `runtime_alert_cadence_late_warn_pct`
  - `runtime_alert_cadence_late_crit_pct`
  - `runtime_alert_cadence_min_samples`
  - `runtime_alert_cadence_cooldown_s`
- Added stocks session dampening keys:
  - `stock_scan_open_cooldown_minutes`
  - `stock_scan_close_cooldown_minutes`
  - `stock_scan_open_score_mult`
  - `stock_scan_close_score_mult`
- Added forex session weighting keys:
  - `forex_session_weight_enabled`
  - `forex_session_weight_floor`
  - `forex_session_weight_ceiling`
- Added broker retry-after cap key:
  - `broker_order_retry_after_cap_s`
- Added market chart cache sizing keys:
  - `market_chart_cache_symbols`
  - `market_chart_cache_bars`
- Added adaptive loss-streak sizing keys:
  - `stock_loss_streak_size_step_pct`
  - `stock_loss_streak_size_floor_pct`
  - `forex_loss_streak_size_step_pct`
  - `forex_loss_streak_size_floor_pct`
- Added runtime cached-fallback age controls:
  - `market_fallback_scan_max_age_s`
  - `market_fallback_snapshot_max_age_s`
- Added crypto feed resilience controls:
  - `kucoin_unsupported_cooldown_s`
  - `crypto_price_error_log_cooldown_s`
- Added stocks leader publication controls:
  - `stock_scan_publish_watch_leaders`
  - `stock_scan_watch_leaders_count`
- Added scanner leader-stability controls:
  - `stock_leader_stability_margin_pct`
  - `forex_leader_stability_margin_pct`
- Added cached-scan execution safety gates:
  - `stock_block_entries_on_cached_scan`
  - `forex_block_entries_on_cached_scan`
- Added cached-scan hard-age and reduced-size controls:
  - `stock_cached_scan_hard_block_age_s`
  - `stock_cached_scan_entry_size_mult`
  - `forex_cached_scan_hard_block_age_s`
  - `forex_cached_scan_entry_size_mult`
- Added scanner-quality entry gate controls:
  - `stock_require_data_quality_ok_for_entries`
  - `stock_require_reject_rate_max_pct`
  - `forex_require_data_quality_ok_for_entries`
  - `forex_require_reject_rate_max_pct`

## Pass 3 Scan Diagnostics Schema
- `scan_diagnostics.json` now carries `schema_version = 2`.
- New compatibility behavior:
  - Old payloads (without schema version) are auto-normalized to v2 by readers.
  - Compatibility marker `schema_compat_from` is set when legacy payloads are loaded.
- v2 baseline fields include:
  - `candidate_churn_pct`
  - `leader_churn_pct`
  - normalized `reject_summary`
  - normalized `candidate_symbols` / `leader_symbols`
