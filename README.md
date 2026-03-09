# PowerTrader AI

PowerTrader AI is a multi-market trading hub with three coordinated engines:
- Crypto (live-capable)
- Stocks (Alpaca-backed)
- Forex (OANDA-backed)

The desktop hub UI is in [ui/pt_hub.py](/Users/joeydelestre/PowerTrader_AI/ui/pt_hub.py), with runtime orchestration in [runtime/pt_runner.py](/Users/joeydelestre/PowerTrader_AI/runtime/pt_runner.py).

## Safety First
- This software can place real orders when configured for live mode.
- Run paper/practice/shadow validation first.
- Keep `paper_only_unless_checklist_green=true` unless you explicitly accept live risk.
- Review all broker credentials, risk caps, and exposure controls before enabling auto-trade.

## Current Repo Layout
- `app/` shared utilities (settings, paths, health, runtime helpers)
- `brokers/` Alpaca/OANDA broker adapters
- `engines/` thinker/trader/trainer modules
- `runtime/` process orchestration (`pt_runner`, `pt_markets`, `pt_autopilot`, `pt_autofix`)
- `ui/` Tkinter hub application
- `tests/` unit/integration tests
- `docs/` runbook/changelog/checklists
- `hub_data/` runtime output (status, logs, diagnostics, incidents)
- `<COIN>/` coin-specific model/data directories (crypto training/runtime)

## Install
```bash
python3 -m pip install -r requirements.txt
python3 -m pip install -r requirements-dev.txt
```

## Run
### Hub UI
```bash
python3 ui/pt_hub.py
```

### Runner (all background services)
```bash
python3 runtime/pt_runner.py
```

### Markets loop only (scanner + market trader steps)
```bash
python3 runtime/pt_markets.py
```

### Autopilot one-shot tune
```bash
python3 runtime/pt_autopilot.py --once
```

### Autofix overseer one-shot (safe dry-run)
```bash
python3 runtime/pt_autofix.py --once --dry-run
```

## Preflight (Run Before Shadow/Live)
Use the readiness checker before testing:
```bash
python3 runtime/tools/preflight_readiness.py
```

Strict mode fails on warnings and critical issues:
```bash
python3 runtime/tools/preflight_readiness.py --strict
```

Report output default:
- `hub_data/preflight_readiness.json`

## Quality Suite
```bash
python3 runtime/tools/run_quality_suite.py
```

Optional strict gates:
```bash
python3 runtime/tools/run_quality_suite.py --require-artifacts --require-stability --require-preflight
```

## Rollout Stages
`market_rollout_stage`:
1. `legacy`
2. `scan_expanded`
3. `risk_caps`
4. `execution_v2`
5. `shadow_only`
6. `live_guarded`

Set this in Hub Settings -> Advanced.

## Recommended Validation Path
### 1) Shadow validation
- Keep:
  - `alpaca_paper_mode=true`
  - `oanda_practice_mode=true`
  - `market_rollout_stage=shadow_only`
- Enable:
  - `stock_auto_trade_enabled=true`
  - `forex_auto_trade_enabled=true`
- Run runner/hub and verify:
  - scanners produce leaders
  - trader messages show simulated/shadow behavior
  - no stale fallback lockouts or health regressions

### 2) Controlled execution (paper/practice)
- Set `market_rollout_stage=execution_v2`
- Keep paper/practice modes enabled
- Verify entry/exit lifecycle and risk caps in logs/status

### 3) Live-guarded rollout
- Set `market_rollout_stage=live_guarded`
- Only after checklist is green, disable broker paper/practice mode if intended
- Keep strict risk controls enabled (cached-scan gates, data-quality gates, reject-pressure gates, exposure caps)

## Key Runtime Files
### Core status
- `hub_data/trader_status.json`
- `hub_data/runner_status.json`
- `hub_data/runtime_state.json`
- `hub_data/market_loop_status.json`

### Market status
- `hub_data/stocks/stock_thinker_status.json`
- `hub_data/stocks/stock_trader_status.json`
- `hub_data/forex/forex_thinker_status.json`
- `hub_data/forex/forex_trader_status.json`

### Diagnostics
- `hub_data/stocks/scan_diagnostics.json`
- `hub_data/forex/scan_diagnostics.json`
- `hub_data/stocks/universe_quality.json`
- `hub_data/forex/universe_quality.json`
- `hub_data/scanner_cadence_drift.json`
- `hub_data/runtime_events.jsonl`
- `hub_data/incidents.jsonl`
- `hub_data/autofix_status.json`
- `hub_data/autofix_state.json`
- `hub_data/autofix/tickets/*.json`

### Logs
- `hub_data/logs/runner.log`
- `hub_data/logs/markets.log`
- `hub_data/logs/autopilot.log`
- `hub_data/logs/autofix.log`
- `hub_data/logs/thinker.log`
- `hub_data/logs/trader.log`

## Autofix Modes
Configure in `gui_settings.json`:
- `autofix_enabled=true|false`
- `autofix_mode=report_only|manual|shadow_apply`
- `autofix_allow_live_apply=true|false` (default false)

Recommended:
1. Start with `report_only` and review generated tickets under `hub_data/autofix/tickets/`.
2. Use `shadow_apply` only in paper/practice stages after validating your test command.

## Credentials
### Crypto (Robinhood)
- Preferred files:
  - `keys/r_key.txt`
  - `keys/r_secret.txt`
- Or env:
  - `POWERTRADER_RH_API_KEY`
  - `POWERTRADER_RH_PRIVATE_B64`

### Stocks (Alpaca)
- Env or settings-backed values:
  - `POWERTRADER_ALPACA_API_KEY_ID`
  - `POWERTRADER_ALPACA_SECRET_KEY`

### Forex (OANDA)
- Env or settings-backed values:
  - `POWERTRADER_OANDA_ACCOUNT_ID`
  - `POWERTRADER_OANDA_API_TOKEN`

## Operator Notes
- Changelog: [docs/CHANGELOG.md](/Users/joeydelestre/PowerTrader_AI/docs/CHANGELOG.md)
- Runbook: [docs/RUNBOOK.md](/Users/joeydelestre/PowerTrader_AI/docs/RUNBOOK.md)
- Settings migration notes: [docs/SETTINGS_MIGRATIONS.md](/Users/joeydelestre/PowerTrader_AI/docs/SETTINGS_MIGRATIONS.md)

## Disclaimer
Use at your own risk. You are responsible for all broker/account configuration, risk limits, and any resulting trades.
