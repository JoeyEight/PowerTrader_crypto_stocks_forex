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
- `runtime/` process orchestration (`pt_runner`, `pt_markets`, `pt_autopilot`)
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

If you use the launcher, it will create `venv/` automatically and install missing core dependencies before opening the hub.

## Run
### Hub UI
```bash
./launch_powertrader.command
```

Alternative:
```bash
./venv/bin/python -m ui.pt_hub
```

Important runtime behavior:
- `Start Trades` launches the runtime supervisor as a detached background process.
- Closing the hub window does not stop trading by itself.
- Use `Stop Trades` first if you want crypto, stocks, and forex runtime activity to stop cleanly.
- If the computer goes to sleep, the Python processes pause and do not keep trading until the machine wakes again.

### Runner (all background services)
```bash
./venv/bin/python -m runtime.pt_runner
```

### Markets loop only (scanner + market trader steps)
```bash
./venv/bin/python -m runtime.pt_markets
```

### Autopilot one-shot tune
```bash
./venv/bin/python -m runtime.pt_autopilot --once
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

When a live Alpaca or live OANDA broker mode is saved in Settings, the hub will auto-promote non-executable rollout stages to `live_guarded`.

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

### Logs
- `hub_data/logs/runner.log`
- `hub_data/logs/markets.log`
- `hub_data/logs/autopilot.log`
- `hub_data/logs/thinker.log`
- `hub_data/logs/trader.log`

### UI behavior to expect
- Stocks and forex run from the same detached runtime supervisor used by crypto.
- Stocks and forex use the native in-app charts and watchlists; the old TradingView launch path is not part of the active UI flow.
- Notification Center reflects live runtime state plus recent unresolved incidents; stale resolved incidents are filtered out by the current app code.

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
