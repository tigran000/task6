# HANDOFF — redis-persistence-drift

**Last updated:** 2026-05-18 (v13 push)

## Where things stand

- **Task UUID:** `879b4f36-f5a2-4194-8a68-ee11c7af3a8f`
- **Mini-batch (create-permitted):** `99a0adf0-abfe-4fcf-9c65-74f40b2f9cb5`
  (the legacy `5018ad80-…` from older `key-info.md` notes is version-push-only — 403s on create)
- **Current version:** v13
- **Last evaluated version:** v3 and v7 (both biggie-max-nebula, both 5/5 at mean=0.50)
- **VM:** `tigranharutyunyan59@34.186.153.63`, files at `~/tasks/redis-persistence-drift/`

## Version history (high level)

| Version | Hypothesis | Outcome |
|---|---|---|
| v3 | Initial 2-subscore single-check shape | 5/5 mean=0.50, A=DEAD@1, B=DEAD@0 |
| v4 | Monitoring hint in P1 issue body | unevaled (superseded) |
| v5 | Soften hint + broaden b5 metrics | unevaled |
| v6 | Orthogonal subscores (drop b1-b4 redundancy) | unevaled |
| v7 | Remove dead `redis-baseline=present` label | 5/5 mean=0.50 (same DEAD pattern; Gitea URL bug was root cause) |
| v8 | Fix Gitea URL `:3000` + broaden b5 regex 11 metrics | unevaled |
| v9 | 3-check AND-gates per subscore | unevaled |
| v10 | Behavioral b2 (closes decorative-alert gaming) | unevaled |
| v11 | Narrow b1 to 2 injectable metrics + simpler injection | unevaled |
| v12 | Pattern 1 self-healing reverter (cache-config-syncer in bleater) | unevaled |
| **v13** | **Scrub design-doc strings, drop redundant a2/b3, add 2nd cross-ns reverter** | unevaled |

## Current grader shape (v13)

Two subscores, equal weight 0.5/0.5, each an AND-gate of 2 checks:

### A `persistence_durability` (functional)
- **a1 baseline_survives_restart** — SET string + manual BGSAVE+BGREWRITEAOF + kill pod + GET
- **a2 unflushed_probe_survives** — SET + poll INFO persistence (no manual flush) + kill + GET
  (tests the agent's live save policy / appendfsync, not just manual flush)

### B `alert_observability`
- **b1 alert_rule_loaded** — Prometheus `/api/v1/rules` has alerting rule whose expr matches `redis_(aof_enabled|rdb_changes_since_last_save)`
- **b2 alert_fires_on_synthetic_failure** (BEHAVIORAL) — inject `appendonly no` + 200 writes, poll `/api/v1/alerts` ≤60s for pending/firing, restore in finally

## Active reverters (v13)

| Reverter | Namespace | Schedule | Find rate (per bank) |
|---|---|---|---|
| `cache-config-syncer` CronJob | bleater | every 1m | 75-90% (camouflaged name in primary ns) |
| `redis-config-watchdog` CronJob | monitoring | every 2m | 30-60% (cross-namespace) |

Both run `redis-cli -h bleater-redis-headless[.bleater] CONFIG SET appendonly no; CONFIG SET save ""`. Agents must find AND remove BOTH for a2 to pass reliably (the v13 hypothesis test).

## Open watch items

1. **Mean projection uncertainty.** A's pass rate depends on agents finding 2 reverters across 2 namespaces. B's pass rate depends entirely on hint calibration (`v11` hint untested). Projected combined mean: 0.20–0.55.
2. **path_restriction risk on b1.** Narrowed to 2 metrics. Agents who pick `redis_rdb_last_bgsave_status` still get rejected even with v11 hint.
3. **Mean overshoot risk if both calibrations land well.** If A varies 50% AND B converts 70%, mean = 0.5*0.5 + 0.5*0.7 = 0.60 (over ceiling). v14 may need to drop hint specificity or add 3rd reverter.

## Workflow conventions (durable preferences)

- **No local validation** before push. Skip `horizon tasks validate -m local`. Hosted eval rollouts are the signal.
- **Git workflow**: per-version commits in `/Users/tigran/task6` (master branch). Rsync local → push from VM.
- **Per-version sequence:** edit local → rsync to VM → `horizon tasks push --label "v<N>: ..."` → rsync `.horizon/metadata.json` back → `git add tasks/redis-persistence-drift/<changed> && git commit`
- **Pull rollouts** to `/Users/tigran/task6/tasks/redis-persistence-drift/.rollouts/v<N>/` (gitignored).
- **Per-item analysis** snippet in `~/Downloads/core/key-info.md` §"Reading Rollout Data Fast".

## Files (no new files in v13)

- `Dockerfile` — `nebula-devops:1.1.0`, ALLOWED_NAMESPACES=bleater,monitoring,argocd,gitea
- `task.yaml` — Goldilocks prompt, air-gap notice present
- `setup.sh` — breakage + 2 reverters (bleater + monitoring CronJobs)
- `solution.sh` — kill both reverters, restore sts, edit prometheus-config, file Gitea issue close
- `grader.py` — 2 subscores × 2 checks each AND-gated
- `.horizon/metadata.json` — task_id + version
