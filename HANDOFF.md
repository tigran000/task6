# HANDOFF — redis-persistence-drift

**Last updated:** 2026-05-19 (post-v22 push)

## TL;DR

22 versions in. v20 batch landed mean **0.90** with A=DEAD@1 (5/5)
and B=4/5 — first batch with any B variance, confirming the v14+
Gitea hint-delivery fixes finally worked. **0.90 is way over the
0.50 strict ceiling**: A still saturates because the existing
reverters only flip in-memory CONFIG, which the sts command-args
overwrite on pod restart. v22 addresses this by making one reverter
patch the sts itself.

## Where things stand

- **Task UUID:** `879b4f36-f5a2-4194-8a68-ee11c7af3a8f`
- **Mini-batch (create-permitted):** `99a0adf0-abfe-4fcf-9c65-74f40b2f9cb5`
  (legacy `5018ad80-…` is version-push-only — 403s on create)
- **Current version:** v22 (pushed 2026-05-19, no rollouts yet)
- **VM:** `tigranharutyunyan59@34.186.153.63`, files at `~/tasks/redis-persistence-drift/`
- **Local repo:** `/Users/tigran/task6`, GitHub `tigran000/task6`, master branch

## Version history (recent)

| Version | Hypothesis | Outcome |
|---|---|---|
| v3–v13 | Various — all blocked by Gitea hint not landing | DEAD@1/DEAD@0, mean=0.50 |
| v14 | Fix Gitea URL `:3000` + `write:issue` scope + HTTP 201 assert | (unevaled, rolled into v15) |
| v15 | Above + sidecar reverter mech-diversity | 0.70 mean — first B variance |
| v16–v19 | Reverter calibration + b2 state-transition gate | iterative |
| v20 | Soften hint + widen b1 metric regex | **0.90 mean** (A 5/5, B 4/5) |
| v21 | Solution.sh: wait for rule `health=ok` not just loaded | (oracle-stability only; same shape) |
| v22 | sts-patching reverter + a3 sidecar-check + softer hint | **(pending)** |

## Critical findings

### A's DEAD@1 root cause (v3–v20)

All earlier reverters used `redis-cli CONFIG SET appendonly no`,
which is **in-memory only**. On every pod restart, redis re-reads
its command-args from the sts spec — if those say `--appendonly yes`,
the in-memory CONFIG SET is silently overridden at boot. Agents who
fix the sts but leave reverters alive still pass A's behavioral
probes (BGSAVE→force-delete→GET) because the new pod boots clean
with persistence on.

### B's DEAD@0 root cause (v3–v13, FIXED v14+)

setup.sh's Gitea P1 issue silently failed (token had no `write:issue`
scope; URL omitted `:3000`). 0/5 transcripts in v13 showed agents
seeing the P1 body. Fix: explicit `write:issue` scope, port-qualified
URL, fail-loud `assert HTTP 201`, plus a 300s wait for `/api/v1/version`.
After v14, the Gitea hint landed in every transcript → B started
varying in v15.

### v20 batch reality (5 rollouts)

```
persistence_durability:           5/5 +++++   ← DEAD@1, no signal
  a1 baseline_survives_restart:   5/5 +++++
  a2 unflushed_probe_survives:    5/5 +++++   (everysec is default!)
alert_observability:              4/5 ++++x
  b1 alert_rule_loaded:           4/5 ++++x
  b2 alert_fires_on_synthetic_failure: 4/5 ++++x
```

Run5 alone failed — it wrote Grafana-provisioned alerts instead of
Prometheus rules. Mean = 0.5 + 0.5·0.8 = 0.90.

## v22 hypothesis (active)

Pulled from `task-hardner.md` decision tree + bank Pattern 1
(measured 40-60% find rate for layered self-healing reverters).

**Change 1 — Restart-persistent reverter.** Existing `redis-config-
watchdog` CronJob in monitoring switched from `redis:7-alpine` +
in-memory `CONFIG SET` to `bitnami/kubectl:1.28` + `kubectl patch sts`.
Patches command-args to `--save "" --appendonly no --dir /tmp` so
that even after a force-delete the new pod can't find the RDB file
on `/data` (it's looking at `/tmp`). Requires SA in monitoring + Role
in bleater + RoleBinding. Image pre-pulled into the node cache via a
short-lived Pod so the cron-spawned pod has it under air-gap.

**Change 2 — a2 → a3.** Old a2 (`unflushed_probe_survives`) was a
default-everysec freebie — every passing agent got it for free.
Replaced with a3 (`no_unexpected_sidecar_in_bleat_service`): reads
`kubectl get deploy bleater-bleat-service -o jsonpath=
{.spec.template.spec.containers[*].name}` and asserts the
`cache-config-tuner` sidecar is absent. Independent topology check
that catches agents who repaired persistence but missed the lowest-
visibility reverter.

**Change 3 — Softer hint.** Removed the literal word "Prometheus"
from the P1 body. Agents must now derive Prometheus from the
cluster's existing monitoring stack, not from the issue text.
Risk: B could over-correct toward 0%. Mitigation: revert this one
change in v23 if so.

**Expected mean:** 0.5·(A pass rate) + 0.5·(B pass rate). With A
targeted at 50-70% (kill the watchdog + kill the sidecar + fix sts)
and B at 50-70% (correct stack choice), mean lands ~0.25-0.35.

## Active reverters (v22)

| # | Reverter | Namespace | Mechanism | Bites |
|---|---|---|---|---|
| 1 | `cache-config-syncer` CronJob | bleater | redis-cli CONFIG SET (in-memory) | Nothing (sts overrides on restart) |
| 2 | `redis-config-watchdog` CronJob | monitoring | **kubectl patch sts (restart-persistent)** | a1 — sts command-args flip → new pod can't load RDB |
| 3 | `redis-fsync-tuner` CronJob | monitoring | redis-cli CONFIG SET appendfsync (in-memory) | Nothing (sts overrides) |
| 4 | `cache-config-tuner` sidecar | bleater (in bleat-service Deployment) | redis-cli CONFIG SET (5s loop, in-memory) | a3 — must remove sidecar from Deployment spec |

Reverters 1 + 3 are decoys / mechanism-diversity surface area.
Reverters 2 + 4 carry actual variance.

## Workflow conventions (durable preferences)

- **No local validation** before push (`[[no-local-validation]]` memory).
  Hosted eval rollouts are the signal.
- **No eval submissions without explicit user ask.** Pushing a new
  version is a separate action from submitting a batch.
- **Git workflow:** per-version commits in `/Users/tigran/task6`
  (master). Rsync local → push from VM → commit locally.
- **Per-version sequence:** edit local → `rsync --exclude='.horizon'
  --exclude='.rollouts' local → VM` → `horizon tasks push --label
  "v<N>: ..."` → rsync `.horizon/metadata.json` back → `git add
  tasks/redis-persistence-drift/<changed> && git commit`
- **Pull rollouts** with `horizon rollouts pull` from
  `~/tasks/redis-persistence-drift/` on VM, then rsync `.rollouts/`
  back to local (gitignored).
- **API keys** in `~/Downloads/core/key-info.md`. mini-batch for new
  task = `99a0adf0-…` (per `[[horizon-creator-mini-batch]]` memory).

## References cheat-sheet

- `task-hardner.md` Hardening Decision Tree — "DEAD@1 + saturated 5+ test
  variants of same axis → Capability ceiling, drop the axis OR redesign
  the reverter." We chose redesign (Change 1).
- `key-info.md` Behavioral Test Patterns — "reverter removed (restart-
  triggered)" → patch sts command-args, not just in-memory CONFIG.
- `AGENT_DIFFICULTY_BANK_v2.md` Pattern 1 (Layered Self-Healing, 40-60%
  variance), Anti-pattern #35 (test-variant saturation — what a2 was),
  Anti-pattern #23 (mechanism diversity across axes).
- `task-authoring-playbook.md` #16 (hint calibration, U-curve), #29
  (audit all checks after one bug), #30 (HANDOFF.md).
- `Master guide.md` strict-ceiling rules, §V39 sidecar camouflage.
