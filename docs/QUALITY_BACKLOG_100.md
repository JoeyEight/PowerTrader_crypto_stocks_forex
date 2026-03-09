# Quality Backlog (100 Items)

Status legend: `[x] done`, `[ ] pending`

1. [x] Normalize project package imports (`app/`, `brokers/`, `engines/`, `runtime/`, `ui/`)
2. [x] Remove root wrapper scripts and update launch/config paths
3. [x] Add autonomous runtime tuner (`runtime/pt_autopilot.py`)
4. [x] Integrate autopilot process into supervisor (`runtime/pt_runner.py`)
5. [x] Add Retry-After handling for Alpaca order retries
6. [x] Add Retry-After handling for OANDA order retries
7. [x] Add KuCoin throttle/caching runtime tuning keys
8. [x] Add smoke harness for all markets (`runtime/smoke_test_all.py`)
9. [x] Add dynamic crypto coin rotation settings
10. [x] Auto-start crypto trading when all training completes
11. [x] Add crypto action guidance in system panel
12. [x] Add Stocks/Forex Action Center controls
13. [x] Add Stocks/Forex chart focus controls + TradingView shortcut
14. [x] Improve Stocks/Forex overview chart rendering (candles + EMA + guides)
15. [x] Add Stocks/Forex auto-scan/auto-step UI toggles
16. [x] Use settings-driven scan/step intervals for Stocks/Forex loops
17. [x] Fix stock warmup lockout during market-open scans
18. [x] Fix stock reject summary math (unique-symbol rate + event count)
19. [x] Persist stock scan diagnostics JSON each cycle
20. [x] Persist forex scan diagnostics JSON each cycle
21. [x] Surface scan diagnostics in Stocks/Forex UI signal/action text
22. [x] Include scan diagnostics in smoke test report output
23. [x] Add structured JSON logging formatter for all runners
24. [x] Add log rotation policy for `hub_data/logs/*.log`
25. [x] Add log retention cleanup job by age/size budget
26. [x] Add per-market incident timeline file (`hub_data/incidents.jsonl`)
27. [x] Add startup self-check for required directories/permissions
28. [x] Add startup self-check for API credential presence/masks
29. [x] Add startup self-check for stale PID files
30. [x] Add guarded shutdown with child process flush timeout
31. [x] Add watchdog for hung subprocess heartbeats
32. [x] Add explicit crash-loop breaker with cooldown windows
33. [x] Add scanner SLA metrics (scan duration, success %, stale rates)
34. [x] Add trader SLA metrics (decision latency, order ack latency)
35. [x] Add broker API health scoreboard in UI header
36. [x] Add centralized alerting thresholds config
37. [x] Add deterministic config validation on settings load
38. [x] Add strict numeric bounds validation for risk settings
39. [x] Add settings migration/versioning with upgrade notes
40. [x] Add readonly runtime snapshot endpoint file (`hub_data/runtime_state.json`)
41. [x] Add forex reject-summary normalization (unique-symbol style)
42. [x] Add scanner drift detector for sudden reject-spikes
43. [x] Add adaptive feed-order fallback memory (per-market)
44. [x] Add stock scanner symbol-cooldown to avoid re-scoring dead symbols
45. [x] Add forex scanner pair-cooldown to reduce noisy thrashing
46. [x] Add shadow-vs-live divergence trend stats view
47. [x] Add execution slippage trend stats (p50/p95)
48. [x] Add signal-age trend stats by market
49. [x] Add portfolio exposure heatmap by symbol/pair
50. [x] Add standardized date/time formatting utility
51. [x] Add unit tests for stock reject summary and warmup logic
52. [x] Add unit tests for forex scan reject aggregation
53. [x] Add unit tests for retry-after parsing logic
54. [x] Add unit tests for path resolver/runtime path guards
55. [x] Add integration tests for runner child lifecycle
56. [x] Add integration tests for smoke harness outputs
57. [x] Add integration tests for UI status hydration from files
58. [x] Add regression test for missing-file/no-json robustness
59. [x] Add deterministic fixtures for scanner status files
60. [x] Add deterministic fixtures for trader status/history files
61. [x] Add strict type hints across runtime modules
62. [x] Add mypy config and baseline typing checks
63. [x] Add Ruff/flake8 lint config and baseline pass
64. [x] Add black/isort formatting config and CI step
65. [x] Add pre-commit hooks for lint/type/format checks
66. [x] Add secure secret redaction in all emitted logs
67. [x] Add explicit key-file permission check (0600 recommendation)
68. [x] Add key rotation reminder/state timestamp
69. [x] Add UI confirmation dialog for switching to live mode
70. [x] Add UI hard lock for live mode without full checklist pass
71. [x] Add “paper-only unless checklist green” feature flag
72. [x] Add max drawdown daily circuit-breaker (global)
73. [x] Add per-market daily loss guardrail dashboard tile
74. [x] Add automatic temporary disable after repeated broker failures
75. [x] Add managed exponential backoff policy object shared across brokers
76. [x] Add queue-based async writer for heavy JSONL writes
77. [x] Add consolidated data cache pruning policy
78. [x] Add cleanup command for stale coin artifacts
79. [x] Add archive command for old diagnostics/reports
80. [x] Add “health quickfix suggestions” map in UI
81. [x] Add onboarding wizard for first-time setup
82. [x] Add unified “system checklist” panel with pass/fail indicators
83. [x] Add contextual tooltips for risk controls in settings UI
84. [x] Add keyboard shortcuts for major hub actions
85. [x] Add improved empty-state visuals for all chart/table views
86. [x] Add responsive layout presets for desktop widths
87. [x] Add selectable font scaling preset for readability
88. [x] Add chart export PNG button for all market charts
89. [x] Add trade-history export CSV button
90. [x] Add diagnostics export bundle (json + logs manifest)
91. [x] Add markdown runbook for common incident playbooks
92. [x] Add operator handoff notes and recovery guide
93. [x] Add API quota monitor and warning thresholds
94. [x] Add broker maintenance window awareness
95. [x] Add market-holiday awareness and session notes
96. [x] Add forex session bias indicator
97. [x] Add stock market-hours guardrail dashboard line
98. [x] Add final release checklist document (paper->live)
99. [x] Add CI workflow for smoke + lint + type checks
100. [x] Add signed versioned changelog with release notes
