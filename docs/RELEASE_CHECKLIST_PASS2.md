# Release Checklist Pass 2

- [ ] All unit and integration tests pass locally.
- [ ] `runtime/smoke_test_all.py` passes from hub-triggered quick diagnostics.
- [ ] `market_loop_status.json` updates continuously under runner control.
- [ ] Runtime state includes drawdown guard and stop-flag payloads.
- [ ] Stocks/Forex panel notes show loop cadence and session timing.
- [ ] Export flows validated:
  - [ ] current chart PNG
  - [ ] market status snapshot JSON
  - [ ] runtime summary TXT
  - [ ] diagnostics bundle ZIP
- [ ] Stop-flag recovery runbook validated in paper/practice modes.
- [ ] Drawdown-guard trigger path validated in simulation.
- [ ] Changelog and backlog pass-2 docs updated before tagging release.
