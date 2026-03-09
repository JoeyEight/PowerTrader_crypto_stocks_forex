# Operator Handoff Guide

## Shift Start Checklist
- Confirm `runtime_state.json` exists and updates.
- Confirm `alerts.severity`.
- Confirm broker modes (paper/practice vs live) in Settings.
- Confirm `execution_guard` is not active unless expected.

## During Shift
- Watch:
  - Scanner reject rate
  - Scanner churn (`candidate_churn_pct`, `leader_churn_pct`)
  - Scanner cadence level (`ok/warning/critical`)
  - Incident error counts
  - Exposure concentration
  - API quota pressure
- Use diagnostics export at each notable incident for traceability.

## Diagnostics Interpretation
- Churn:
  - `candidate_churn_pct > 65%` usually means unstable scanner inputs or cadence pressure.
  - `leader_churn_pct > 50%` means top picks are rotating quickly; execution confidence should be reduced.
- Cadence drift:
  - `warning`: monitor closely, avoid tightening scan intervals.
  - `critical`: prioritize stability remediation before expanding execution.
- Stocks session dampening:
  - Opening/closing windows apply score multipliers; this reduces false positives during volatile session edges.
  - If leader count collapses only at open/close, adjust dampening multipliers before changing base thresholds.
- Forex session weighting:
  - Session bias can boost trend signals (`TREND`) or dampen range conditions (`RANGE/MEAN-REV`).
  - If signal flips are frequent during low-liquidity periods, keep weighting enabled and reduce aggressiveness.
- Retry-After lockouts:
  - Long lockouts are expected when broker APIs throttle hard.
  - Confirm wait telemetry in `runtime_state.broker_backoff`; avoid manual spam retries.

## Shift End
- Export diagnostics bundle.
- Add summary:
  - Time window
  - Markets impacted
  - Actions taken
  - Remaining risks
- Link incident IDs/events from `incidents.jsonl`.

## Escalation Triggers
- Repeated `execution_temporarily_disabled` events.
- `alerts.severity == critical`.
- Persistent broker `ERROR` state beyond cooldown window.
