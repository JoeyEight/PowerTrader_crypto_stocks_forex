# Quality Backlog Pass 2 (100 Items)

Status legend: `[x] done`, `[ ] pending`

1. [x] Add JSONL line-trim helper for runtime files (`trim_jsonl_max_lines`).
2. [x] Add unit test coverage for JSONL line trimming.
3. [x] Add settings key `runtime_incidents_max_lines`.
4. [x] Add settings key `runtime_events_max_lines`.
5. [x] Add settings key `market_loop_jitter_pct`.
6. [x] Add settings key `market_settings_reload_interval_s`.
7. [x] Extend stock market awareness with countdown seconds.
8. [x] Extend stock market awareness with `next_open_ts` and `next_close_ts`.
9. [x] Extend stock holiday awareness calendar into 2027.
10. [x] Extend forex awareness with `next_session`.
11. [x] Extend forex awareness with `session_eta_s`.
12. [x] Add maintenance-awareness severity fields (`alpaca_level`, `oanda_level`).
13. [x] Add unit tests for market-awareness countdown/session payloads.
14. [x] Add runtime alert reason `drawdown_guard_triggered`.
15. [x] Add runtime alert reason `stop_flag_active`.
16. [x] Add quickfix mappings for drawdown and stop-flag conditions.
17. [x] Add health-rule unit test for drawdown-guard critical alert.
18. [x] Add health-rule unit test for stop-flag critical alert.
19. [x] Add stop-flag snapshot payload in runtime state.
20. [x] Add drawdown-guard snapshot payload in runtime state.
21. [x] Add market-loop snapshot ingestion in runtime state.
22. [x] Add incident-summary counts for 1h and 24h windows.
23. [x] Add runtime retention pass to trim incidents/events JSONL files.
24. [x] Add runtime event emission for JSONL trim operations.
25. [x] Add market-loop status output file (`hub_data/market_loop_status.json`).
26. [x] Add jittered scheduler for markets loop cadences.
27. [x] Add throttled settings reload in markets loop.
28. [x] Add snapshot cycle timing metadata for stocks/forex loop.
29. [x] Add scan/step cycle timing metadata for stocks loop.
30. [x] Add scan/step cycle timing metadata for forex loop.
31. [x] Add smoke harness summary for market-loop status.
32. [x] Include drawdown/stop-flag booleans in smoke runtime summary.
33. [x] Add smoke integration test assertion for market-loop status output.
34. [x] Add system safety label in Crypto dashboard (`lbl_runtime_guard`).
35. [x] Add checklist line with `inc1h` incident count.
36. [x] Surface stop-flag + drawdown-guard states in hub safety line.
37. [x] Surface market-loop freshness in hub safety line.
38. [x] Show loop age in Stocks/Forex system state text.
39. [x] Show stock market countdown minutes in Stocks state text.
40. [x] Show forex next-session name in Forex state text.
41. [x] Add forex signal text next-session ETA hint.
42. [x] Add loop cadence timing notes in Stocks/Forex Market Notes.
43. [x] Add session timing notes in Stocks/Forex Market Notes.
44. [x] Add helper for mixed ETA/age formatting in UI (`_market_eta_or_age`).
45. [x] Add File menu action: Export Market Status Snapshot JSON.
46. [x] Add File menu action: Export Runtime Summary TXT.
47. [x] Add shortcut `Ctrl+Shift+S` for status snapshot export.
48. [x] Add market-status snapshot export builder using hydration bundle.
49. [x] Add runtime-summary text export from runtime state and broker health.
50. [x] Add sanitize-settings test coverage for new retention/jitter keys.
51. [ ] Add dedicated runtime summary panel with compact cards in UI.
52. [ ] Add incident trend sparkline (1h/24h/7d) in dashboard.
53. [ ] Add scanner cadence drift detector (expected vs observed cycle timing).
54. [ ] Add forex session-aware scanner weighting.
55. [ ] Add stock open/close cooldown policy for first/last 15 minutes.
56. [ ] Add scanner symbol universe quality scoring report.
57. [ ] Add daily PnL decomposition (realized/unrealized/fees/slippage).
58. [ ] Add cross-market correlation exposure warnings.
59. [ ] Add strategy-by-strategy attribution report.
60. [ ] Add circuit-breaker cooldown countdown in UI badge.
61. [ ] Add auto-resume policy after drawdown guard cooloff.
62. [ ] Add manual safety acknowledgment flow for post-drawdown restart.
63. [ ] Add equity curve anomaly detector for sudden spikes/drops.
64. [ ] Add stale-history detector for frozen account-value stream.
65. [ ] Add per-market data-source reliability score in panel.
66. [ ] Add scanner candidate churn metric (turnover %) per cycle.
67. [ ] Add rejected-candidate replay tool for threshold tuning.
68. [ ] Add broker latency histogram persistence and dashboard.
69. [ ] Add order-fill quality comparison by time-of-day.
70. [ ] Add live-vs-paper discrepancy tracker by market.
71. [ ] Add structured feature-flag registry for rollout toggles.
72. [ ] Add migration test suite for settings schema upgrades.
73. [ ] Add resilient fallback loader for partially-corrupt settings files.
74. [ ] Add deterministic replay fixture for runner heartbeat lifecycle.
75. [ ] Add end-to-end paper-mode scenario test for stocks.
76. [ ] Add end-to-end practice-mode scenario test for forex.
77. [ ] Add deterministic mock broker layer for integration tests.
78. [ ] Add scanner throughput benchmark script.
79. [ ] Add UI render-time benchmark for heavy chart data.
80. [ ] Add chart panel split-layout persistence per tab.
81. [ ] Add market panel compact mode for smaller screens.
82. [ ] Add per-market notification center with severity filters.
83. [ ] Add one-click “safe defaults” reset for risk settings.
84. [ ] Add advanced settings profile import/export.
85. [ ] Add operator role modes (basic/advanced/admin) for UI complexity.
86. [ ] Add dashboard layout drag-and-save system.
87. [ ] Add command palette for key hub actions.
88. [ ] Add markdown notes editor with timestamped operator logs.
89. [ ] Add runbook deep-links from each alert reason.
90. [x] Add quick diagnostics checker button with pass/fail report.
91. [ ] Add market scanner “why not traded” explainer for top candidate.
92. [ ] Add adaptive confidence calibration retraining job.
93. [ ] Add rolling walk-forward validation report for all markets.
94. [ ] Add regime classifier (trend/range/high-vol) persisted by market.
95. [ ] Add shadow deployment scorecards before live-stage promotion.
96. [ ] Add versioned runtime-state schema with compatibility checks.
97. [ ] Add signed release artifact metadata in diagnostics bundle.
98. [ ] Add operator session audit trail for major UI actions.
99. [ ] Add localized timestamp formatting controls in settings.
100. [x] Add pass-2 release checklist and acceptance gates document.
