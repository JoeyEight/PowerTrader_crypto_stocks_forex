from __future__ import annotations

from typing import Any, Dict, Iterable, Tuple

from app.settings_migrations import CURRENT_SETTINGS_VERSION, migrate_settings

SANITIZER_DEFAULTS: Dict[str, Any] = {
    "settings_schema_version": CURRENT_SETTINGS_VERSION,
    "settings_upgrade_notes": [],
    "coins": ["BTC", "ETH", "XRP", "BNB", "DOGE"],
    "market_rollout_stage": "legacy",
    "settings_control_mode": "self_managed",
    "settings_profile": "balanced",
    "ui_role_mode": "basic",
    "ui_timestamp_mode": "local_24h",
    "market_panel_compact_mode": False,
    "ui_font_scale_preset": "normal",
    "ui_layout_preset": "auto",
    "stock_universe_mode": "all_tradable_filtered",
    "forex_session_mode": "all",
    "ui_refresh_seconds": 1.0,
    "chart_refresh_seconds": 10.0,
    "candles_limit": 120,
    "start_allocation_pct": 0.5,
    "dca_multiplier": 2.0,
    "max_dca_buys_per_24h": 2,
    "kucoin_min_interval_sec": 0.40,
    "kucoin_cache_ttl_sec": 2.5,
    "kucoin_stale_max_sec": 120.0,
    "kucoin_unsupported_cooldown_s": 21600.0,
    "crypto_price_error_log_cooldown_s": 120.0,
    "crypto_trader_loop_sleep_s": 1.0,
    "crypto_trader_error_sleep_s": 1.5,
    "crypto_dynamic_scan_interval_s": 300.0,
    "crypto_dynamic_target_count": 8,
    "crypto_dynamic_min_projected_edge_pct": 0.25,
    "crypto_dynamic_max_new_per_scan": 1,
    "crypto_dynamic_max_trainers": 1,
    "crypto_dynamic_rotation_cooldown_s": 900.0,
    "market_chart_cache_symbols": 8,
    "market_chart_cache_bars": 120,
    "market_fallback_scan_max_age_s": 7200.0,
    "market_fallback_snapshot_max_age_s": 1800.0,
    "market_bg_snapshot_interval_s": 15.0,
    "market_bg_stocks_interval_s": 15.0,
    "market_bg_forex_interval_s": 10.0,
    "market_intelligence_interval_s": 180.0,
    "stock_trader_step_interval_s": 18.0,
    "forex_trader_step_interval_s": 12.0,
    "runner_crash_lockout_s": 180.0,
    "stock_scan_max_symbols": 160,
    "stock_min_price": 5.0,
    "stock_max_price": 500.0,
    "stock_min_dollar_volume": 5_000_000.0,
    "stock_max_spread_bps": 40.0,
    "stock_min_bars_required": 24,
    "stock_min_valid_bars_ratio": 0.7,
    "stock_max_stale_hours": 6.0,
    "stock_scan_open_cooldown_minutes": 15,
    "stock_scan_close_cooldown_minutes": 15,
    "stock_scan_open_score_mult": 0.85,
    "stock_scan_close_score_mult": 0.90,
    "stock_scan_publish_watch_leaders": True,
    "stock_scan_watch_leaders_count": 6,
    "stock_leader_stability_margin_pct": 10.0,
    "stock_trade_notional_usd": 100.0,
    "stock_max_open_positions": 1,
    "stock_block_entries_on_cached_scan": True,
    "stock_cached_scan_hard_block_age_s": 1800,
    "stock_cached_scan_entry_size_mult": 0.60,
    "stock_require_data_quality_ok_for_entries": True,
    "stock_require_reject_rate_max_pct": 92.0,
    "stock_score_threshold": 0.2,
    "stock_replay_adaptive_enabled": True,
    "stock_replay_adaptive_weight": 0.35,
    "stock_replay_adaptive_step_cap_pct": 40.0,
    "stock_profit_target_pct": 0.35,
    "stock_trailing_gap_pct": 0.2,
    "stock_max_day_trades": 3,
    "stock_max_position_usd_per_symbol": 0.0,
    "stock_max_total_exposure_pct": 0.0,
    "stock_no_new_entries_mins_to_close": 15,
    "stock_live_guarded_score_mult": 1.2,
    "stock_min_calib_prob_live_guarded": 0.58,
    "stock_max_slippage_bps": 35.0,
    "stock_order_retry_count": 2,
    "stock_max_loss_streak": 3,
    "stock_loss_streak_size_step_pct": 0.15,
    "stock_loss_streak_size_floor_pct": 0.40,
    "stock_loss_cooldown_seconds": 1800,
    "stock_max_daily_loss_usd": 0.0,
    "stock_max_daily_loss_pct": 0.0,
    "stock_min_samples_live_guarded": 5,
    "stock_max_signal_age_seconds": 300,
    "stock_reject_drift_warn_pct": 65.0,
    "forex_universe_pairs": "",
    "forex_scan_max_pairs": 32,
    "forex_max_spread_bps": 8.0,
    "forex_min_volatility_pct": 0.01,
    "forex_min_bars_required": 24,
    "forex_min_valid_bars_ratio": 0.7,
    "forex_max_stale_hours": 8.0,
    "forex_session_weight_enabled": True,
    "forex_session_weight_floor": 0.85,
    "forex_session_weight_ceiling": 1.10,
    "forex_event_risk_enabled": True,
    "forex_event_cache_refresh_s": 1800.0,
    "forex_event_cache_stale_max_s": 86400.0,
    "forex_event_max_lookahead_minutes": 180,
    "forex_event_post_event_minutes": 30,
    "forex_event_block_high_impact_minutes": 45,
    "forex_event_score_mult_high": 0.70,
    "forex_event_score_mult_medium": 0.85,
    "forex_leader_stability_margin_pct": 12.0,
    "forex_trade_units": 1000,
    "forex_max_open_positions": 1,
    "forex_block_entries_on_cached_scan": True,
    "forex_cached_scan_hard_block_age_s": 1200,
    "forex_cached_scan_entry_size_mult": 0.65,
    "forex_require_data_quality_ok_for_entries": True,
    "forex_require_reject_rate_max_pct": 92.0,
    "forex_max_position_usd_per_pair": 0.0,
    "forex_score_threshold": 0.2,
    "forex_replay_adaptive_enabled": True,
    "forex_replay_adaptive_weight": 0.35,
    "forex_replay_adaptive_step_cap_pct": 40.0,
    "forex_profit_target_pct": 0.25,
    "forex_trailing_gap_pct": 0.15,
    "forex_max_total_exposure_pct": 0.0,
    "forex_live_guarded_score_mult": 1.15,
    "forex_min_calib_prob_live_guarded": 0.56,
    "forex_max_slippage_bps": 6.0,
    "forex_order_retry_count": 2,
    "forex_max_loss_streak": 3,
    "forex_loss_streak_size_step_pct": 0.15,
    "forex_loss_streak_size_floor_pct": 0.40,
    "forex_loss_cooldown_seconds": 1800,
    "forex_max_daily_loss_usd": 0.0,
    "forex_max_daily_loss_pct": 0.0,
    "forex_min_samples_live_guarded": 5,
    "forex_max_signal_age_seconds": 300,
    "forex_reject_drift_warn_pct": 65.0,
    "market_max_total_exposure_pct": 0.0,
    "runtime_alert_scan_reject_warn_pct": 65.0,
    "runtime_alert_scan_reject_crit_pct": 85.0,
    "runtime_alert_incident_warn_count": 8,
    "runtime_alert_incident_crit_count": 20,
    "runtime_alert_error_incident_warn_count": 2,
    "runtime_alert_error_incident_crit_count": 6,
    "runtime_alert_startup_warning_warn_count": 2,
    "runtime_alert_reject_spike_min_rate_pct": 25.0,
    "runtime_alert_reject_spike_delta_pct": 25.0,
    "runtime_alert_reject_spike_ratio": 2.0,
    "runtime_alert_reject_spike_min_samples": 6,
    "runtime_alert_drift_spike_warn_count": 1,
    "runtime_alert_drift_spike_crit_count": 3,
    "runtime_alert_cadence_warn_count": 1,
    "runtime_alert_cadence_crit_count": 2,
    "runtime_alert_cadence_late_warn_pct": 80.0,
    "runtime_alert_cadence_late_crit_pct": 180.0,
    "runtime_alert_cadence_min_samples": 3,
    "runtime_alert_cadence_cooldown_s": 300,
    "runtime_alert_market_loop_stale_s": 90.0,
    "runtime_alert_exposure_concentration_warn_pct": 55.0,
    "runtime_alert_exposure_concentration_crit_pct": 75.0,
    "runtime_api_quota_warn_15m": 4,
    "runtime_api_quota_crit_15m": 10,
    "runtime_incidents_max_lines": 25000,
    "runtime_events_max_lines": 50000,
    "broker_failure_disable_threshold": 4,
    "broker_failure_disable_cooldown_s": 900,
    "broker_order_retry_after_cap_s": 300.0,
    "autofix_enabled": True,
    "autofix_mode": "report_only",
    "autofix_allow_live_apply": False,
    "autofix_poll_interval_s": 45.0,
    "autofix_max_fixes_per_day": 2,
    "autofix_model": "gpt-5-mini",
    "autofix_api_base": "https://api.openai.com/v1",
    "autofix_request_timeout_s": 25.0,
    "autofix_test_command": "python -m unittest tests.test_settings_sanitize tests.test_runner_watchdog",
    "autofix_request_block_on_quota": True,
    "autofix_request_block_on_missing_key": True,
    "autofix_request_block_on_bad_request": True,
    "autofix_request_block_on_invalid_output": True,
    "autofix_request_block_on_no_patch": True,
    "adaptive_confidence_min_samples": 18,
    "adaptive_confidence_target_success_pct": 55.0,
    "replay_target_entries_stocks": 3,
    "replay_target_entries_forex": 4,
    "operator_notes_max_entries": 120,
    "paper_only_unless_checklist_green": True,
    "key_rotation_warn_days": 90,
    "data_cache_max_age_days": 14.0,
    "scanner_quality_max_age_days": 14.0,
    "data_cache_max_total_mb": 300,
    "global_max_drawdown_pct": 0.0,
    "global_drawdown_lookback_hours": 24,
    "global_drawdown_auto_resume_enabled": True,
    "global_drawdown_resume_cooloff_s": 14400,
    "global_drawdown_resume_recovery_buffer_pct": 0.25,
    "global_drawdown_require_manual_ack": True,
    "stock_symbol_cooldown_minutes": 30,
    "stock_symbol_cooldown_min_hits": 2,
    "stock_symbol_cooldown_reject_reasons": "data_quality,insufficient_bars,spread,liquidity",
    "forex_pair_cooldown_minutes": 20,
    "forex_pair_cooldown_min_hits": 2,
    "forex_pair_cooldown_reject_reasons": "data_quality,insufficient_bars,spread,low_volatility",
    "script_neural_runner2": "engines/pt_thinker.py",
    "script_neural_trainer": "engines/pt_trainer.py",
    "script_trader": "engines/pt_trader.py",
    "script_markets_runner": "runtime/pt_markets.py",
    "script_autopilot": "runtime/pt_autopilot.py",
    "script_autofix": "runtime/pt_autofix.py",
}

_BOOL_KEYS = {
    "auto_start_scripts",
    "auto_start_trading_when_all_trained",
    "crypto_dynamic_enabled",
    "crypto_dynamic_auto_train",
    "alpaca_paper_mode",
    "oanda_practice_mode",
    "stock_gate_market_hours_scan",
    "stock_scan_use_daily_when_closed",
    "stock_show_rejected_rows",
    "stock_scan_publish_watch_leaders",
    "stock_block_entries_on_cached_scan",
    "stock_require_data_quality_ok_for_entries",
    "stock_auto_trade_enabled",
    "stock_replay_adaptive_enabled",
    "stock_block_new_entries_near_close",
    "forex_auto_trade_enabled",
    "forex_block_entries_on_cached_scan",
    "forex_require_data_quality_ok_for_entries",
    "forex_replay_adaptive_enabled",
    "forex_show_rejected_rows",
    "forex_session_weight_enabled",
    "forex_event_risk_enabled",
    "autofix_enabled",
    "autofix_allow_live_apply",
    "autofix_request_block_on_quota",
    "autofix_request_block_on_missing_key",
    "autofix_request_block_on_bad_request",
    "autofix_request_block_on_invalid_output",
    "autofix_request_block_on_no_patch",
    "paper_only_unless_checklist_green",
    "market_panel_compact_mode",
    "global_drawdown_auto_resume_enabled",
    "global_drawdown_require_manual_ack",
}

_FLOAT_BOUNDS: Dict[str, Tuple[float, float, float]] = {
    "ui_refresh_seconds": (1.0, 0.2, 30.0),
    "chart_refresh_seconds": (10.0, 1.0, 300.0),
    "start_allocation_pct": (0.5, 0.0, 100.0),
    "dca_multiplier": (2.0, 0.0, 10.0),
    "kucoin_min_interval_sec": (0.40, 0.25, 5.0),
    "kucoin_cache_ttl_sec": (2.5, 0.5, 30.0),
    "kucoin_stale_max_sec": (120.0, 30.0, 3600.0),
    "kucoin_unsupported_cooldown_s": (21600.0, 300.0, 172800.0),
    "crypto_price_error_log_cooldown_s": (120.0, 5.0, 3600.0),
    "crypto_trader_loop_sleep_s": (1.0, 0.3, 10.0),
    "crypto_trader_error_sleep_s": (1.5, 0.5, 20.0),
    "crypto_dynamic_scan_interval_s": (300.0, 30.0, 3600.0),
    "crypto_dynamic_min_projected_edge_pct": (0.25, 0.0, 20.0),
    "crypto_dynamic_rotation_cooldown_s": (900.0, 30.0, 86400.0),
    "market_bg_snapshot_interval_s": (15.0, 5.0, 300.0),
    "market_bg_stocks_interval_s": (15.0, 8.0, 300.0),
    "market_bg_forex_interval_s": (10.0, 6.0, 300.0),
    "market_intelligence_interval_s": (180.0, 30.0, 3600.0),
    "market_fallback_scan_max_age_s": (7200.0, 60.0, 172800.0),
    "market_fallback_snapshot_max_age_s": (1800.0, 30.0, 86400.0),
    "market_loop_jitter_pct": (0.10, 0.0, 0.5),
    "market_settings_reload_interval_s": (8.0, 1.0, 120.0),
    "stock_trader_step_interval_s": (18.0, 8.0, 300.0),
    "forex_trader_step_interval_s": (12.0, 6.0, 300.0),
    "runner_crash_lockout_s": (180.0, 30.0, 3600.0),
    "stock_min_price": (5.0, 0.0, 10000.0),
    "stock_max_price": (500.0, 0.0, 20000.0),
    "stock_min_dollar_volume": (5_000_000.0, 0.0, 1_000_000_000_000.0),
    "stock_max_spread_bps": (40.0, 0.0, 5000.0),
    "stock_min_valid_bars_ratio": (0.7, 0.0, 1.0),
    "stock_max_stale_hours": (6.0, 0.5, 720.0),
    "stock_scan_open_score_mult": (0.85, 0.5, 1.0),
    "stock_scan_close_score_mult": (0.90, 0.5, 1.0),
    "stock_leader_stability_margin_pct": (10.0, 0.0, 100.0),
    "stock_cached_scan_entry_size_mult": (0.60, 0.10, 1.0),
    "stock_require_reject_rate_max_pct": (92.0, 0.0, 100.0),
    "stock_trade_notional_usd": (100.0, 1.0, 1_000_000.0),
    "stock_score_threshold": (0.2, 0.0, 5.0),
    "stock_replay_adaptive_weight": (0.35, 0.0, 1.0),
    "stock_replay_adaptive_step_cap_pct": (40.0, 5.0, 90.0),
    "stock_profit_target_pct": (0.35, 0.0, 100.0),
    "stock_trailing_gap_pct": (0.2, 0.0, 100.0),
    "stock_max_position_usd_per_symbol": (0.0, 0.0, 1_000_000_000.0),
    "stock_max_total_exposure_pct": (0.0, 0.0, 100.0),
    "stock_live_guarded_score_mult": (1.2, 0.5, 5.0),
    "stock_min_calib_prob_live_guarded": (0.58, 0.0, 1.0),
    "stock_max_slippage_bps": (35.0, 0.0, 5000.0),
    "stock_loss_streak_size_step_pct": (0.15, 0.0, 0.9),
    "stock_loss_streak_size_floor_pct": (0.40, 0.10, 1.0),
    "stock_max_daily_loss_usd": (0.0, 0.0, 1_000_000_000.0),
    "stock_max_daily_loss_pct": (0.0, 0.0, 100.0),
    "stock_reject_drift_warn_pct": (65.0, 10.0, 100.0),
    "forex_max_spread_bps": (8.0, 0.0, 500.0),
    "forex_min_volatility_pct": (0.01, 0.0, 100.0),
    "forex_min_valid_bars_ratio": (0.7, 0.0, 1.0),
    "forex_max_stale_hours": (8.0, 0.5, 720.0),
    "forex_session_weight_floor": (0.85, 0.5, 1.0),
    "forex_session_weight_ceiling": (1.10, 1.0, 2.0),
    "forex_event_cache_refresh_s": (1800.0, 60.0, 86400.0),
    "forex_event_cache_stale_max_s": (86400.0, 60.0, 604800.0),
    "forex_event_score_mult_high": (0.70, 0.10, 1.0),
    "forex_event_score_mult_medium": (0.85, 0.10, 1.0),
    "forex_leader_stability_margin_pct": (12.0, 0.0, 100.0),
    "forex_cached_scan_entry_size_mult": (0.65, 0.10, 1.0),
    "forex_require_reject_rate_max_pct": (92.0, 0.0, 100.0),
    "forex_max_position_usd_per_pair": (0.0, 0.0, 1_000_000_000.0),
    "forex_score_threshold": (0.2, 0.0, 5.0),
    "forex_replay_adaptive_weight": (0.35, 0.0, 1.0),
    "forex_replay_adaptive_step_cap_pct": (40.0, 5.0, 90.0),
    "forex_profit_target_pct": (0.25, 0.0, 100.0),
    "forex_trailing_gap_pct": (0.15, 0.0, 100.0),
    "forex_max_total_exposure_pct": (0.0, 0.0, 100.0),
    "forex_live_guarded_score_mult": (1.15, 0.5, 5.0),
    "forex_min_calib_prob_live_guarded": (0.56, 0.0, 1.0),
    "forex_max_slippage_bps": (6.0, 0.0, 500.0),
    "forex_loss_streak_size_step_pct": (0.15, 0.0, 0.9),
    "forex_loss_streak_size_floor_pct": (0.40, 0.10, 1.0),
    "forex_max_daily_loss_usd": (0.0, 0.0, 1_000_000_000.0),
    "forex_max_daily_loss_pct": (0.0, 0.0, 100.0),
    "forex_reject_drift_warn_pct": (65.0, 10.0, 100.0),
    "market_max_total_exposure_pct": (0.0, 0.0, 100.0),
    "runtime_alert_scan_reject_warn_pct": (65.0, 0.0, 100.0),
    "runtime_alert_scan_reject_crit_pct": (85.0, 0.0, 100.0),
    "runtime_alert_reject_spike_min_rate_pct": (25.0, 0.0, 100.0),
    "runtime_alert_reject_spike_delta_pct": (25.0, 0.0, 100.0),
    "runtime_alert_reject_spike_ratio": (2.0, 1.0, 20.0),
    "runtime_alert_cadence_late_warn_pct": (80.0, 10.0, 1000.0),
    "runtime_alert_cadence_late_crit_pct": (180.0, 20.0, 2000.0),
    "runtime_alert_market_loop_stale_s": (90.0, 10.0, 3600.0),
    "runtime_alert_exposure_concentration_warn_pct": (55.0, 0.0, 100.0),
    "runtime_alert_exposure_concentration_crit_pct": (75.0, 0.0, 100.0),
    "data_cache_max_age_days": (14.0, 1.0, 365.0),
    "scanner_quality_max_age_days": (14.0, 1.0, 365.0),
    "global_max_drawdown_pct": (0.0, 0.0, 100.0),
    "global_drawdown_resume_recovery_buffer_pct": (0.25, 0.0, 50.0),
    "broker_order_retry_after_cap_s": (300.0, 1.0, 3600.0),
    "autofix_poll_interval_s": (45.0, 5.0, 1800.0),
    "autofix_request_timeout_s": (25.0, 5.0, 120.0),
    "adaptive_confidence_target_success_pct": (55.0, 30.0, 90.0),
}

_INT_BOUNDS: Dict[str, Tuple[int, int, int]] = {
    "candles_limit": (120, 20, 500),
    "trade_start_level": (3, 1, 7),
    "max_dca_buys_per_24h": (2, 0, 24),
    "crypto_dynamic_target_count": (8, 1, 64),
    "crypto_dynamic_max_new_per_scan": (1, 1, 16),
    "crypto_dynamic_max_trainers": (1, 1, 8),
    "market_chart_cache_symbols": (8, 2, 32),
    "market_chart_cache_bars": (120, 40, 400),
    "stock_scan_max_symbols": (160, 8, 2000),
    "stock_scan_open_cooldown_minutes": (15, 0, 120),
    "stock_scan_close_cooldown_minutes": (15, 0, 120),
    "stock_scan_watch_leaders_count": (6, 1, 20),
    "stock_min_bars_required": (24, 8, 10000),
    "stock_max_open_positions": (1, 0, 500),
    "stock_cached_scan_hard_block_age_s": (1800, 30, 172800),
    "stock_max_day_trades": (3, 0, 100),
    "stock_no_new_entries_mins_to_close": (15, 0, 360),
    "stock_order_retry_count": (2, 1, 10),
    "stock_max_loss_streak": (3, 0, 100),
    "stock_loss_cooldown_seconds": (1800, 60, 86400),
    "stock_min_samples_live_guarded": (5, 0, 100000),
    "stock_max_signal_age_seconds": (300, 30, 86400),
    "forex_scan_max_pairs": (32, 4, 400),
    "forex_min_bars_required": (24, 8, 10000),
    "forex_event_max_lookahead_minutes": (180, 5, 1440),
    "forex_event_post_event_minutes": (30, 0, 240),
    "forex_event_block_high_impact_minutes": (45, 0, 240),
    "forex_trade_units": (1000, 1, 10_000_000),
    "forex_max_open_positions": (1, 0, 500),
    "forex_cached_scan_hard_block_age_s": (1200, 30, 172800),
    "forex_order_retry_count": (2, 1, 10),
    "forex_max_loss_streak": (3, 0, 100),
    "forex_loss_cooldown_seconds": (1800, 60, 86400),
    "forex_min_samples_live_guarded": (5, 0, 100000),
    "forex_max_signal_age_seconds": (300, 30, 86400),
    "runtime_alert_incident_warn_count": (8, 1, 5000),
    "runtime_alert_incident_crit_count": (20, 1, 5000),
    "runtime_alert_error_incident_warn_count": (2, 1, 5000),
    "runtime_alert_error_incident_crit_count": (6, 1, 5000),
    "runtime_alert_startup_warning_warn_count": (2, 0, 500),
    "runtime_alert_reject_spike_min_samples": (6, 3, 200),
    "runtime_alert_drift_spike_warn_count": (1, 1, 5000),
    "runtime_alert_drift_spike_crit_count": (3, 1, 5000),
    "runtime_alert_cadence_warn_count": (1, 1, 5000),
    "runtime_alert_cadence_crit_count": (2, 1, 5000),
    "runtime_alert_cadence_min_samples": (3, 2, 200),
    "runtime_alert_cadence_cooldown_s": (300, 30, 3600),
    "runtime_api_quota_warn_15m": (4, 1, 5000),
    "runtime_api_quota_crit_15m": (10, 1, 5000),
    "runtime_incidents_max_lines": (25000, 2000, 500000),
    "runtime_events_max_lines": (50000, 2000, 1000000),
    "broker_failure_disable_threshold": (4, 2, 50),
    "broker_failure_disable_cooldown_s": (900, 60, 86400),
    "autofix_max_fixes_per_day": (2, 0, 50),
    "adaptive_confidence_min_samples": (18, 6, 5000),
    "replay_target_entries_stocks": (3, 1, 20),
    "replay_target_entries_forex": (4, 1, 20),
    "operator_notes_max_entries": (120, 20, 2000),
    "key_rotation_warn_days": (90, 7, 3650),
    "data_cache_max_total_mb": (300, 32, 5000),
    "global_drawdown_lookback_hours": (24, 1, 168),
    "global_drawdown_resume_cooloff_s": (14400, 60, 604800),
    "stock_symbol_cooldown_minutes": (30, 1, 1440),
    "stock_symbol_cooldown_min_hits": (2, 1, 20),
    "forex_pair_cooldown_minutes": (20, 1, 1440),
    "forex_pair_cooldown_min_hits": (2, 1, 20),
}

_ENUMS: Dict[str, Iterable[str]] = {
    "market_rollout_stage": ("legacy", "scan_expanded", "risk_caps", "execution_v2", "shadow_only", "live_guarded"),
    "settings_control_mode": ("preset_managed", "self_managed"),
    "settings_profile": ("guarded", "balanced", "performance"),
    "ui_role_mode": ("basic", "advanced", "admin"),
    "ui_timestamp_mode": ("local_24h", "local_12h", "utc_24h"),
    "ui_font_scale_preset": ("small", "normal", "large"),
    "ui_layout_preset": ("auto", "compact", "normal", "wide"),
    "stock_universe_mode": ("core", "watchlist", "all_tradable_filtered"),
    "forex_session_mode": ("all", "london_ny", "london", "ny", "asia"),
    "autofix_mode": ("report_only", "manual", "shadow_apply"),
}


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "on", "y", "t"}:
        return True
    if text in {"0", "false", "no", "off", "n", "f"}:
        return False
    return bool(default)


def _bounded_float(value: Any, default: float, lo: float, hi: float) -> float:
    try:
        v = float(str(value).strip())
    except Exception:
        v = float(default)
    if v < lo:
        return float(lo)
    if v > hi:
        return float(hi)
    return float(v)


def _bounded_int(value: Any, default: int, lo: int, hi: int) -> int:
    try:
        v = int(float(str(value).strip()))
    except Exception:
        v = int(default)
    if v < lo:
        return int(lo)
    if v > hi:
        return int(hi)
    return int(v)


def _sanitize_coins(value: Any, default: Iterable[str]) -> list[str]:
    raw = value
    if isinstance(raw, str):
        seq = [p.strip() for p in raw.split(",")]
    elif isinstance(raw, (list, tuple, set)):
        seq = [str(p).strip() for p in raw]
    else:
        seq = [str(p).strip() for p in default]

    out = []
    seen = set()
    for part in seq:
        if not part:
            continue
        coin = part.upper()
        if not coin.isalnum():
            continue
        if coin in seen:
            continue
        seen.add(coin)
        out.append(coin)
    if not out:
        out = [str(p).upper().strip() for p in default if str(p).strip()]
    return out


def _sanitize_dca_levels(value: Any, default: Iterable[Any]) -> list[float]:
    src = value if isinstance(value, (list, tuple)) else list(default)
    out: list[float] = []
    for item in src:
        try:
            v = float(str(item).replace("%", "").strip())
        except Exception:
            continue
        if v >= 0.0:
            continue
        if v < -99.0:
            v = -99.0
        out.append(v)
    out = sorted(set(round(x, 4) for x in out), reverse=True)
    return out if out else [float(x) for x in default]


def _sanitize_script(value: Any, default: str) -> str:
    v = str(value or "").strip().replace("\\", "/")
    if not v:
        return str(default)
    while "//" in v:
        v = v.replace("//", "/")
    return v


def sanitize_settings(raw: Dict[str, Any] | None, defaults: Dict[str, Any] | None = None) -> Dict[str, Any]:
    migrated, notes, _from_v, _to_v = migrate_settings(raw if isinstance(raw, dict) else {})
    source = migrated if isinstance(migrated, dict) else {}
    base = dict(SANITIZER_DEFAULTS)
    if isinstance(defaults, dict):
        base.update(defaults)
    out = dict(base)
    out.update(source)

    out["coins"] = _sanitize_coins(out.get("coins"), base.get("coins", []))
    out["dca_levels"] = _sanitize_dca_levels(out.get("dca_levels"), base.get("dca_levels", [-2.5, -5.0, -10.0, -20.0]))

    for key in _BOOL_KEYS:
        out[key] = _as_bool(out.get(key), bool(base.get(key, False)))

    for key, (default, lo, hi) in _FLOAT_BOUNDS.items():
        fallback = float(base.get(key, default))
        out[key] = _bounded_float(out.get(key), fallback, lo, hi)

    # Legacy configs sometimes used fractional percent (0.005 meaning 0.5%).
    if 0.0 < float(out.get("start_allocation_pct", 0.0) or 0.0) <= 0.01:
        out["start_allocation_pct"] = float(out["start_allocation_pct"]) * 100.0

    for key, (default, lo, hi) in _INT_BOUNDS.items():
        fallback = int(base.get(key, default))
        out[key] = _bounded_int(out.get(key), fallback, lo, hi)

    for key, allowed in _ENUMS.items():
        cur = str(out.get(key, base.get(key, "")) or "").strip().lower()
        allowed_set = {str(v).strip().lower() for v in allowed}
        if cur not in allowed_set:
            cur = str(base.get(key, next(iter(allowed_set)))).strip().lower()
        out[key] = cur

    script_defaults = {
        "script_neural_runner2": str(base.get("script_neural_runner2", "engines/pt_thinker.py")),
        "script_neural_trainer": str(base.get("script_neural_trainer", "engines/pt_trainer.py")),
        "script_trader": str(base.get("script_trader", "engines/pt_trader.py")),
        "script_markets_runner": str(base.get("script_markets_runner", "runtime/pt_markets.py")),
        "script_autopilot": str(base.get("script_autopilot", "runtime/pt_autopilot.py")),
        "script_autofix": str(base.get("script_autofix", "runtime/pt_autofix.py")),
    }
    for key, dval in script_defaults.items():
        out[key] = _sanitize_script(out.get(key), dval)

    # Cross-field constraints.
    out["stock_max_price"] = max(float(out["stock_max_price"]), float(out["stock_min_price"]))
    out["forex_session_weight_ceiling"] = max(float(out["forex_session_weight_ceiling"]), float(out["forex_session_weight_floor"]))
    out["forex_event_cache_stale_max_s"] = max(
        float(out["forex_event_cache_stale_max_s"]),
        float(out["forex_event_cache_refresh_s"]),
    )
    out["runtime_alert_cadence_late_crit_pct"] = max(
        float(out["runtime_alert_cadence_late_crit_pct"]),
        float(out["runtime_alert_cadence_late_warn_pct"]),
    )
    out["runtime_alert_cadence_crit_count"] = max(
        int(out["runtime_alert_cadence_crit_count"]),
        int(out["runtime_alert_cadence_warn_count"]),
    )
    out["stock_loss_streak_size_floor_pct"] = max(0.10, min(1.0, float(out["stock_loss_streak_size_floor_pct"])))
    out["stock_loss_streak_size_step_pct"] = max(0.0, min(0.9, float(out["stock_loss_streak_size_step_pct"])))
    out["forex_loss_streak_size_floor_pct"] = max(0.10, min(1.0, float(out["forex_loss_streak_size_floor_pct"])))
    out["forex_loss_streak_size_step_pct"] = max(0.0, min(0.9, float(out["forex_loss_streak_size_step_pct"])))
    out["market_fallback_scan_max_age_s"] = max(
        float(out["market_fallback_scan_max_age_s"]),
        float(out["market_fallback_snapshot_max_age_s"]),
    )

    for pct_key in ("stock_max_total_exposure_pct", "forex_max_total_exposure_pct", "market_max_total_exposure_pct"):
        out[pct_key] = _bounded_float(out.get(pct_key), float(base.get(pct_key, 0.0)), 0.0, 100.0)

    out["autofix_model"] = str(out.get("autofix_model", base.get("autofix_model", "gpt-5-mini")) or "gpt-5-mini").strip() or "gpt-5-mini"
    out["autofix_api_base"] = (
        str(out.get("autofix_api_base", base.get("autofix_api_base", "https://api.openai.com/v1")) or "https://api.openai.com/v1").strip()
        or "https://api.openai.com/v1"
    )
    out["autofix_test_command"] = (
        str(
            out.get(
                "autofix_test_command",
                base.get("autofix_test_command", "python -m unittest tests.test_settings_sanitize tests.test_runner_watchdog"),
            )
            or "python -m unittest tests.test_settings_sanitize tests.test_runner_watchdog"
        ).strip()
        or "python -m unittest tests.test_settings_sanitize tests.test_runner_watchdog"
    )

    out["settings_schema_version"] = int(CURRENT_SETTINGS_VERSION)
    if notes:
        out["settings_upgrade_notes"] = list(notes[-20:])
    elif not isinstance(out.get("settings_upgrade_notes", []), list):
        out["settings_upgrade_notes"] = []

    return out
