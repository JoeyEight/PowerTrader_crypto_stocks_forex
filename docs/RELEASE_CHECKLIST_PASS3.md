# Release Checklist Pass 3

## Scope
- Scanner cadence drift detection and runtime alert surfacing.
- Stocks/Forex scanner quality reports and churn metrics.
- Stocks open/close session dampening and forex session-aware weighting.
- Retry-After lockout honoring for broker order retries.
- Dashboard/exports updates for scanner quality visibility.

## Acceptance Gates
1. `python -m unittest discover -s tests -p 'test_*.py'` passes.
1. `python runtime/tools/run_quality_suite.py` passes in a clean environment.
2. `python runtime/tools/check_pass3_artifacts.py` reports `ok: true` in active runtime sessions.
3. `runtime/smoke_test_all.py` report includes:
- `scanner_cadence_drift`
- `stock_universe_quality`
- `forex_universe_quality`
4. `hub_data/stocks/scan_diagnostics.json` includes:
- `candidate_churn_pct`
- `leader_churn_pct`
- `quality_summary`
 - `schema_version >= 2`
5. `hub_data/forex/scan_diagnostics.json` includes:
- `candidate_churn_pct`
- `leader_churn_pct`
- `session_context`
- `session_weighted_candidates`
 - `schema_version >= 2`
6. `hub_data/runtime_state.json` includes:
- `scan_cadence`
- cadence-aware alert reason when drift is active.
- `broker_backoff`
- `alerts.metrics.market_loop_age_s`
7. File menu includes `Export Scanner Quality JSON`.

## Operator Validation
1. Run quick diagnostics from the UI and confirm report writes to `hub_data/smoke_test_report.json`.
2. Confirm scanner quality export writes JSON under chart exports directory.
3. During degraded network conditions, verify order error messages include `retry_after=...s` when applicable.
