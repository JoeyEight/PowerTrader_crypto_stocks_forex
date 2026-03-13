# PowerTrader AI

PowerTrader AI is a multi-market trading hub with three coordinated engines:
- Crypto (live-capable)
- Stocks (Alpaca-backed)
- Forex (OANDA-backed)

The desktop hub UI is in [ui/pt_hub.py](/Users/joeydelestre/PowerTrader_AI/ui/pt_hub.py), with runtime orchestration in [runtime/pt_runner.py](/Users/joeydelestre/PowerTrader_AI/runtime/pt_runner.py).

## Safety First
- This software can place real orders when configured for live mode.
- Keep `paper_only_unless_checklist_green=true` until checklist is green.
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

## Preflight (Run Before Live)
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

### Stocks Data (Twelve Data, optional)
- Set `stock_data_provider` to `twelvedata` in Settings.
- Provide the API key via:
  - `POWERTRADER_TWELVEDATA_API_KEY`, or
  - `keys/twelvedata_api_key.txt`
- Rate guard knobs:
  - `twelvedata_api_credits_per_minute`
  - `twelvedata_daily_credits`
  - `twelvedata_scan_symbol_cap`

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
