# Quality Backlog Pass 3 (100 Items)

Status legend: `[x] done`, `[ ] pending`

1. [x] Add scanner cadence drift detector for stocks cycles.
2. [x] Add scanner cadence drift detector for forex cycles.
3. [x] Persist cadence drift diagnostics to dedicated runtime file.
4. [x] Add cadence drift incident emission with cooldown handling.
5. [x] Surface cadence drift metrics in runner runtime state payload.
6. [x] Add health-rule warning reason for cadence drift pressure.
7. [x] Add health-rule quickfix guidance for cadence drift pressure.
8. [x] Add health-rule critical escalation for severe cadence drift.
9. [x] Add unit test coverage for cadence drift alert behavior.
10. [x] Add unit test coverage for cadence drift persistence helper.
11. [x] Add settings keys for cadence drift tolerances and thresholds.
12. [x] Add settings sanitize bounds for cadence drift keys.
13. [x] Add settings unit tests for cadence drift key clamping.
14. [x] Add stocks scanner candidate churn metric between cycles.
15. [x] Add forex scanner candidate churn metric between cycles.
16. [x] Add stocks scanner leader churn metric between cycles.
17. [x] Add forex scanner leader churn metric between cycles.
18. [x] Persist prior scan baseline snapshot for churn calculations.
19. [x] Include churn metrics in stocks scan diagnostics payload.
20. [x] Include churn metrics in forex scan diagnostics payload.
21. [x] Add stocks universe quality report JSON artifact.
22. [x] Add forex universe quality report JSON artifact.
23. [x] Add per-reason rejection percentages in universe quality reports.
24. [x] Add pass/fail gate summary in universe quality reports.
25. [x] Add data-source mix summary in stocks universe quality report.
26. [x] Add data-source mix summary in forex universe quality report.
27. [x] Add scanner diagnostics hints sourced from universe quality report.
28. [x] Add helper test for shared reject-summary percentage math.
29. [x] Add helper test for scanner churn computation logic.
30. [x] Add helper test for universe quality report builder.
31. [x] Add stocks opening-window score dampening policy.
32. [x] Add stocks closing-window score dampening policy.
33. [x] Add settings key for stock opening-window minutes.
34. [x] Add settings key for stock closing-window minutes.
35. [x] Add settings key for stock opening-window dampening multiplier.
36. [x] Add settings key for stock closing-window dampening multiplier.
37. [x] Append explicit open/close window notes to stock candidate reasons.
38. [x] Add diagnostics fields for stock window policy activity.
39. [x] Add stocks scanner unit test for opening-window dampening.
40. [x] Add stocks scanner unit test for closing-window dampening.
41. [x] Add forex session-aware score weighting function.
42. [x] Add settings key for forex session weighting enabled flag.
43. [x] Add settings key for forex session weight multiplier floor.
44. [x] Add settings key for forex session weight multiplier ceiling.
45. [x] Persist forex session context in candidate row diagnostics.
46. [x] Add forex scanner diagnostics field for session-weight usage rate.
47. [x] Add forex scanner unit test for session weighting application.
48. [x] Add forex scanner unit test for disabled session weighting path.
49. [x] Add Retry-After text parsing fallback on urllib HTTP error objects.
50. [x] Respect large server lockout Retry-After values in broker order retries.
51. [x] Add backoff policy setting for max retry-after cap.
52. [x] Add unit test ensuring Retry-After above exponential cap is honored.
53. [x] Add unit test for Retry-After parse fallback from HTTPError body text.
54. [x] Add stocks/forex order retry logs including selected wait seconds.
55. [x] Add runtime event emission for broker retry-after waits.
56. [x] Add runtime summary field for recent broker backoff durations.
57. [x] Add UI state-line snippet for scanner churn and cadence drift.
58. [x] Add UI market notes section for universe quality report summary.
59. [x] Add UI market notes section for cadence drift warnings.
60. [x] Add UI hint when stock open/close dampening is active.
61. [x] Add UI hint when forex session weighting is active.
62. [x] Add UI guard countdown emphasis for execution guard cooldowns.
63. [x] Add UI log line when cadence drift transitions to active.
64. [x] Add export action for scanner quality reports bundle.
65. [x] Add runtime smoke harness checks for new diagnostics files.
66. [x] Add smoke harness unit test assertions for cadence drift file.
67. [x] Add smoke harness unit test assertions for universe quality files.
68. [x] Add deterministic fixture for scanner churn comparisons.
69. [x] Add deterministic fixture for cadence drift state transitions.
70. [x] Add deterministic fixture for stock open/close policy scenarios.
71. [x] Add deterministic fixture for forex session weight scenarios.
72. [x] Add runner watchdog note when market loop status is stale.
73. [x] Add runtime alert for stale market-loop status file.
74. [x] Add runtime alert metrics for market-loop freshness age.
75. [x] Add tests for runner stale market-loop status detection.
76. [x] Add trend payload fields for scanner quality aggregates.
77. [x] Add trend payload fields for cadence drift aggregates.
78. [x] Add trend payload tests covering scanner quality fields.
79. [x] Add trend payload tests covering cadence drift fields.
80. [x] Add operator runbook section for scanner cadence troubleshooting.
81. [x] Add operator runbook section for universe quality tuning.
82. [x] Add operator runbook section for Retry-After lockout behavior.
83. [x] Add changelog entries for pass-3 scanner reliability improvements.
84. [x] Add changelog entries for pass-3 UI diagnostics additions.
85. [x] Add release checklist pass-3 for cadence/churn coverage.
86. [x] Add static typing pass for scanner diagnostic helper functions.
87. [x] Add lint baseline updates for new helper modules.
88. [x] Add schema version bump for scan diagnostics payload expansion.
89. [x] Add schema compatibility guard for old scan diagnostics readers.
90. [x] Add migration doc for new settings keys introduced in pass 3.
91. [x] Add stale-quality-report cleanup in retention maintenance.
92. [x] Add max-age setting for scanner quality report artifacts.
93. [x] Add docs on interpreting churn percentages in operator handoff.
94. [x] Add docs on interpreting cadence drift severities.
95. [x] Add docs on stock session dampening behavior and tradeoffs.
96. [x] Add docs on forex session-weighting behavior and tradeoffs.
97. [x] Add docs on Retry-After behavior and long lockout waits.
98. [x] Add self-check script to validate pass-3 artifact completeness.
99. [x] Add CI-like local command in docs to run all pass-3 tests quickly.
100. [x] Add pass-3 final acceptance checklist document.
