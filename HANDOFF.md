# HANDOFF — redis-persistence-drift

**Last updated:** 2026-05-30 (post-v63 push: a2 hardened with --dir /data +
PVC-Bound checks, closing an ephemeral-data bypass QA found in v62).

## TL;DR

v63 is the current live version. **v63** closed a follow-up QA finding
(warning) on v62: `_a2` was still spec-shape only — it checked
`--appendonly`/`--save`/the `data` VCT but never `--dir`, so an agent who
flipped the flags in place while leaving the watchdog reverter's `--dir /tmp`
(`setup.sh:395`) passed `a2` while AOF/RDB wrote to ephemeral `/tmp` (durable
flags, ephemeral data). It also never confirmed the `data` PVC actually
Bound. **v63 adds:** `a2` rejects `--dir` ≠ `/data`, and asserts PVC
`data-bleater-redis-0` is `Bound`. (Deliberately skipped: `availableReplicas`
on the deployments and functional `CONFIG GET` in A — both rollout-timing
flaky. Oracle still 1.0.) Local commit `d76a0cb`. Not yet eval-validated.

**v62** (the prior fix) addressed two defects a QA pass found in **v61**
(problem version `df78eb4d`), where the whole of subscore A had collapsed to a
single reverter-audit atom (`a1`) and was awarding full credit on a platform
that was never actually fixed:

1. **(error) A scored 1.0 on an unfixed/deleted platform.** `a1` only scans
   for reverter-*shaped* containers. The real breakage —
   `redis-server --save "" --appendonly no --dir /tmp` on an emptyDir with no
   PVC — is the main container's *startup command*, not a reverter loop, so
   `a1` passed on a fully-broken cluster. Deleting workloads outright also
   passed because `_resource_containers` returned `"absent"` → PASS. QA
   reproduced 1.0 with Redis still ephemeral, and again with bleat-service /
   timeline-service deleted.
2. **(warning) b3 auto-passed Prometheus-store rules.** `_b3_route_is_pageable`
   returned `True` immediately for `source == "prometheus"` ("route check
   skipped"), so an alert that can notify nobody (no Alertmanager on this
   snapshot) got the same credit a working-but-blackholed Grafana rule was
   denied.

**v62 fix:** subscore A is now a 2-atom AND-gate (`a1` + a positive
`a2 redis_persistence_restored`), `a1` treats deletion of a *required*
workload as FAIL, and `b3` fails closed for Prometheus rules unless a live
Alertmanager is wired. The oracle still scores 1.0 (it restores persistence
and uses a Grafana rule with a non-blackhole receiver). **Not yet
eval-validated** — awaiting a v62 batch.

## Where things stand

- **Task UUID:** `879b4f36-f5a2-4194-8a68-ee11c7af3a8f`
- **Mini-batch (create-permitted):** `99a0adf0-abfe-4fcf-9c65-74f40b2f9cb5`
  (legacy `5018ad80-…` is version-push-only — 403s on create)
- **Current version:** **v64** (pushed 2026-05-30). Local commit `2287fdb`.
  Prior: v63 `d76a0cb`, v62 `239c9b1`.
- **Latest empirical data (daydream, 11 rollouts across v62+v63):** A 6/11
  (~55%, legitimate sidecar-hunt variance — failures are agents missing the
  `cache-config-tuner`/`redis-pool-sizer` in-deployment sidecars, confirmed via
  2/2-container pods in run4/5); B 2/11 (~18%, alive but low — only
  Grafana+receiver rules pass b3). Mean v62 0.42 / v63 0.30, both in band. v64
  targets B's low rate via disclosure (grader unchanged).
- **VM:** `tigranharutyunyan59@34.186.153.63`, task files at
  **`~/tasks/redis-persistence-drift/`** (NOT `~/task/` — the old HANDOFF was
  wrong and it cost a session's worth of confusion). horizon CLI lives at
  `~/horizon_env/bin/horizon` on the VM.
- **Local repo:** **`/Users/tigran/tasks/task6`** (the old HANDOFF said
  `/Users/tigran/task6` — also wrong). GitHub `tigran000/task6`, master branch.
  Task files under `tasks/redis-persistence-drift/`.
- **Runtime:** biggie-max-nebula, strict `0 < X < 0.50` ceiling.

## Current grader structure (v62)

Two equal-weight (1/2 each) binary subscores. `grade()` runs A first, then B.

- **A persistence_durability** — AND-gate of 2 atoms:
  - `a1 no_reverter_sidecar_in_bleat_service` — behavior-based spec audit over
    6 resources (`_REVERTER_SIDECAR_RESOURCES`, now 4-tuples carrying a
    `classification`). Each container's command/args is matched against
    reverter-shaped patterns (redis-cli CONFIG SET disabling persistence;
    kubectl-patch loops). **Resources are classified `"required"` vs
    `"reverter"`:** deleting a `required` workload (bleat-service,
    timeline-service, the redis sts) is now a FAIL (`deleted_required`);
    deleting a `reverter` CronJob is still PASS.
  - `a2 redis_persistence_restored` — **positive** check on the live
    bleater-redis sts: redis container command has `--appendonly yes` (not
    `no`), a non-empty `--save`, `--dir` at `/data` (rejects `/tmp` etc.;
    absent OK since image WORKDIR=/data), a `volumeClaimTemplate` named
    `data`, AND that PVC `data-bleater-redis-0` is actually `Bound` (v63).
    This is the atom that re-couples A to the actual incident. Measured
    against the agent's left-behind state (A runs before B's isolation
    harness). NB: still no pod-Ready / availableReplicas check — those were
    skipped for rollout-timing flakiness; B's harness needing redis up caps
    a non-startable spec at 0.5.
- **B alert_observability** — AND-gate of 3 atoms:
  - `b1 alert_rule_loaded` — three-store discovery (Prometheus
    `/api/v1/rules`, Grafana file-provisioning CM, Grafana runtime API).
    Metric whitelist `_b_metric_pattern` (redis_aof_enabled,
    rdb_changes_since_last_save, rdb_last_bgsave_status, aof_last_write_status).
  - `b2 alert_fires_on_synthetic_failure` — behavioral. Inside the isolation
    harness (suspend reverter CronJobs, scale bleat-service to 0, strip sts
    sidecars, patch sts to known-good), inject a failure and assert state
    transition. **This is the only durable variance lever** (see findings).
  - `b3 alert_routes_to_pageable_receiver` — Grafana policy-tree walk for a
    non-blackhole receiver. **Prometheus-store rules now fail CLOSED** unless
    Prometheus reports an active Alertmanager
    (`_prometheus_has_active_alertmanager` via `/api/v1/alertmanagers`); this
    snapshot wires none.

The prior `a2 argocd_application_synced` and `a3 source_repo_aligned` atoms
(and setup.sh's Gitea manifest-corruption block) were **removed** as confirmed
dead weight — do not resurrect them without new data.

## Oracle (solution.sh) — how it earns 1.0

1. Deletes reverter CronJobs (+ RBAC) and strips the two in-deploy sidecars.
2. Rebuilds the redis sts: command
   `--save "3600 1 300 100 60 10000" --appendonly yes --appendfsync everysec
   --dir /data` + a `data` vct (RWO 2Gi). Uses scale-0 → delete → apply
   (vct is immutable). → passes `a1` + `a2`.
3. Creates a **Grafana** runtime alert rule on `redis_aof_enabled` (datasource
   uid `prometheus`, threshold refId `C`) and sets the notification policy
   root receiver to a non-blackhole contact point. → passes `b1`/`b2`/`b3`
   via the Grafana path (so the b3 Prometheus fail-closed change does not
   touch the oracle).

## Key empirical findings (still valid)

- **b2 is the only durable variance lever.** Long track record of varying
  2/5–5/5. PromQL composition (label filters, threshold ops against filtered
  scalars, noDataState) is genuinely error-prone.
- **Every config-presence atom saturates within 1–2 versions.**
  `baseline_survives_restart`, `no_orphan_watchdog_rbac`, `alert_rule_loaded`
  (post-v32), `prune=true AND selfHeal=true` (v44) — all went DEAD@1. This is
  why `a2_argocd`/`a3` were dropped. **Watch `a2 redis_persistence_restored`
  for the same fate** — it is a spec-presence check; if v62 data shows it
  5/5, it adds no variance and the real signal is still b2.
- **Behavioral atoms without a real skill axis saturate too.**
  `data_survives_pod_restart` went DEAD@1 (agents rebuild the sts wholesale,
  dropping any planted initContainer as a side effect). A v42 behavioral
  drift-injection on a2 caught 0/5 (downstream of the same precondition as the
  spec check).
- **A-side has historically had no durable variance lever.** Spec/ArgoCD-shaped
  checks on the live cluster all saturate. The v62 `a2` is a *correctness*
  guard (closes the false-positive QA found), not necessarily a variance
  source — its job is to stop A rewarding a broken cluster, not to vary.

## Version trajectory (compressed)

| v   | A_atoms                      | Mean / status     | Outcome |
| --- | ---------------------------- | ----------------- | ------- |
| v44 | a1 + a2_prune_strict         | 0.60              | prune-tightening caught 0/5; A too easy |
| v45–v53 | a1 + a2 + a3_source_repo | invalid           | a3 (Gitea-manifest GitOps lever) — oracle stuck at 0.5 / 0.0 across 9 versions chasing ArgoCD OutOfSync (immutable-vct CSA→SSA, setup races, RespectIgnoreDifferences unsupported). Abandoned. |
| v54 | a1 + a2(no-Synced) + a3      | 0.50              | a3 saturated 5/5; b3 dropped to 2/5 (new variance). Dist `0,0.5,0.5,0.5,1.0` |
| v55 | a1 + a2(no-Synced) + a3      | pending           | closed answer-leak `/tmp/bleater-manifests-original.b64`; solution.sh restores via Gitea commits API |
| v56 | a1 + a2(no-Synced) + a3      | n/a               | VM cleanup (stale subfolder) |
| v57–v60 | a1 + a2(no-Synced) + a3  | invalid           | Dockerfile build/timeout fights: dead `bitnami/kubectl:1.28` (Bitnami deprecated free docker.io images), then uncapped curl/crane hangs. v60 bounded all network ops with `timeout`. |
| **v61** | **a1 only (single atom)** | (QA-reviewed)   | A collapsed to single reverter-audit atom; a2_argocd/a3 dropped as dead weight. **QA found A awards 1.0 on an unfixed/deleted platform** (problem version `df78eb4d`) + b3 auto-passes Prometheus rules. |
| **v62** | **a1 + a2_redis_persistence_restored** | pushed 2026-05-29 | re-coupled A to the incident (positive persistence check + deletion-of-required = FAIL); b3 fail-closed for Prometheus rules. Oracle = 1.0. QA then found a2 was still spec-shape only (see v63). |
| **v63** | **a1 + a2 (+ --dir /data + PVC Bound)** | daydream 5-run: **0.30** (A 3/5, B 0/5) | closed the "durable flags, ephemeral data" bypass: a2 rejects `--dir`≠/data and requires PVC Bound. Oracle = 1.0. B 0/5 was sample noise (see v62). |
| **v62** (batched) | a1 + a2_redis_persistence_restored | daydream 6-run: **0.42** (A 3/6, B 2/6) | b3 fail-closed first shipped here. B=2/6 proves B is alive (Grafana+receiver passes; Prometheus-only fails). |
| **v64** | a1 + a2; **P1 issue discloses "alert must page a human"** | pushed 2026-05-30, **not yet batched** | grader UNCHANGED. setup.sh-only: sharpened P1 Notes to disclose (symptom-level) that the alert must reach on-call, closing the b3 under-disclosure that pinned B at ~18%. Target: B → ~40-60%. |

## What to watch on the v62 batch

- **Mean band:** A may now genuinely fail for agents who suppress reverters but
  leave Redis ephemeral, and b3 now fails Prometheus-only alerts → both push
  the mean *down* from v61. If mean < ~0.20, soften (e.g. drop the `--save`
  sub-condition in `a2`, or accept Prometheus rules in b3). If `a2` is 5/5 it's
  saturated-but-harmless (it's a correctness guard, not the variance source).
- **b3 fairness:** confirm agents *can* discover that a Prometheus rule can't
  page on this snapshot (no Alertmanager). The P1 issue's "no alerting on this
  layer … got blindsided" + "land somewhere they will stick" is the
  symptom-level pointer. If 0/N agents use Grafana, the hint may be too oblique
  → consider a clearer nudge rather than reverting b3.
- Pull ≥10 rollouts (2 batches) before reacting to any single 0/5 on an
  AND-gated atom — sample variance is ±2/5.

## Workflow conventions (durable)

- **VM is authoritative for versions.** Local git history runs *behind* the
  VM — v55–v61 were pushed from the VM but never committed locally, so the
  v62 commit (`239c9b1`) jumped local history straight from v60. Always check
  `~/tasks/redis-persistence-drift/.horizon/metadata.json` on the VM for the
  real version. The local `.horizon/metadata.json` is stale (`version: 35`)
  and is gitignored/excluded from rsync — ignore it.
- **Per-version sequence:** edit local → rsync local → VM → `horizon tasks
  push` on the VM → commit locally. (key-info.md says commit-after-push; the
  order isn't load-bearing.)
- **Exact rsync** (note the `~/tasks/<name>/` path and the excludes —
  excluding `.horizon` is mandatory so you don't clobber the VM's real version
  metadata with the stale local one):
  ```
  rsync -avz --exclude='.git' --exclude='.horizon' --exclude='.rollouts' \
    --exclude='.validation' \
    /Users/tigran/tasks/task6/tasks/redis-persistence-drift/ \
    tigranharutyunyan59@34.186.153.63:~/tasks/redis-persistence-drift/
  ```
  Then `ssh … 'cd ~/tasks/redis-persistence-drift && horizon tasks push'`.
  Watch for a stray `__pycache__/` getting rsync'd if you ran `py_compile`
  locally — delete it on both sides.
- **PERMISSION-CLASSIFIER GOTCHA (this session's main time-sink):** in auto
  mode, the safety classifier blocks `ssh`/`rsync` to this VM (raw public IP,
  not a configured git remote → treated as exfiltration of the answer-key
  files), AND blocks the agent from writing its own allow-rule. A verbal "go
  ahead" in chat does NOT clear it. To let the agent push directly, the **user
  must** add to `/Users/tigran/tasks/task6/.claude/settings.local.json`:
  ```json
  { "permissions": { "allow": ["Bash(rsync:*)",
    "Bash(ssh tigranharutyunyan59@34.186.153.63:*)"] } }
  ```
  Otherwise the user runs the rsync/push themselves (e.g. `!`-prefixed in the
  prompt). Don't try to route around the block with scp/`ssh tee` — that's a
  guardrail bypass.
- **No eval submissions without explicit user ask.** Pushing a version ≠
  submitting a batch.
- **No local validation** before push (hosted rollouts are the signal).
- **Dead-code cleanup before push** — audit for orphaned `_a2_*`/`_c1_*`
  helpers + constants from removed atoms. v62 is clean (scripted unused-symbol
  check passed). Note grader.py docstrings still carry historical version refs
  ("biggie-max-nebula", "v25/v30/v43", "daydream") — harmless (grader is not
  copied into the agent image), but the pre-push smell-test in key-info.md
  greps for them, so don't be alarmed.
- **Pre-push checks:** `python3 -c "import ast; ast.parse(open('grader.py').read())"`,
  `bash -n setup.sh solution.sh`, and grep task.yaml/setup.sh (agent-visible)
  for leak strings.
- **API keys / VM creds / horizon CLI examples:** `~/Downloads/core/key-info.md`
  (also mirrored at `/Users/tigran/tasks/core/key-info.md`). This file is the
  authoritative source for the VM paths — trust it over older HANDOFFs.

## References cheat-sheet

- `key-info.md` — VM creds, exact rsync/horizon commands, DEAD@N triage,
  subscore-independence checklist, "in band" numbers.
- `task-hardner.md` — Hardening Decision Tree, U-curve hint calibration.
- `task-authoring-playbook.md` — 30-item pre-push audit, regex/PromQL traps.
- `nebula-task-reviewer-v3.md` — 8-phase static review procedure (this is the
  lens QA used on v61).
- `nebula-batch-qc-feedback.md` — 20-point batch QC reformat.
- `AGENT_DIFFICULTY_BANK_v2.md` — difficulty levers, hint-disclosure U-curve.
- `Master guide.md` — strict-ceiling rules, AND-gate non-functional rule.
