# Paper-to-Live Release Checklist

## Preflight
- [ ] All tests pass locally.
- [ ] Smoke harness returns `ok: true`.
- [ ] No critical runtime alerts for 24h in paper/practice.

## Risk Controls
- [ ] Daily loss controls configured.
- [ ] Exposure caps configured.
- [ ] Live-mode checklist gate enabled (`paper_only_unless_checklist_green=true`).

## Broker Readiness
- [ ] Credentials present and tested.
- [ ] Key rotation status reviewed (`due_count == 0` preferred).
- [ ] API quota stable in paper/practice.

## UI/Operator Readiness
- [ ] Dashboard checklist panel all green.
- [ ] Runbook and handoff docs reviewed.
- [ ] Export paths verified (charts/trade-history/diagnostics).

## Go/No-Go
- [ ] Final review completed.
- [ ] Live toggle confirmed by operator.
- [ ] Post-go monitoring plan in place.
