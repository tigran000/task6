# HANDOFF — redis-persistence-drift

**Last updated:** 2026-05-27 (post-v60 push, Dockerfile network ops bounded with timeouts)

## TL;DR

45 versions in. v44 batch landed mean **0.60** (kill-criterion triggered:
v41=v42=v44=0.60 across three different A-side experiments). v45
implements Option 1 from the v44 decision: a new `a3
source_repo_aligned` atom that audits the bleater-redis manifest in
the bleater-manifests Gitea repo (the source ArgoCD pulls from).
Setup.sh corrupts the manifest in master via the Gitea contents API;
agents who only fix the live cluster fail a3 because ArgoCD selfHeal
would regress the cluster on the next reconcile. Projected mean
**~0.42** (a1=0.60 × a2=1.0 × a3=~0.40 ≈ 0.24 for A; B unchanged
~0.60). Awaiting v45 rollout data.

## Where things stand

- **Task UUID:** `879b4f36-f5a2-4194-8a68-ee11c7af3a8f`
- **Mini-batch (create-permitted):** `99a0adf0-abfe-4fcf-9c65-74f40b2f9cb5`
  (legacy `5018ad80-…` is version-push-only — 403s on create)
- **Current version:** v60 (pushed 2026-05-27, Dockerfile RUN step wrapped in `timeout` + curl `--connect-timeout` flags; VM `.horizon/stash/` cleared)
- **VM:** `tigranharutyunyan59@34.186.153.63`, files at `~/task/`
- **Local repo:** `/Users/tigran/task6`, GitHub `tigran000/task6`, master branch
- **Runtime:** biggie-max-nebula, strict `0 < X < 0.50` ceiling

## Current grader structure (v45)

- **A persistence_durability** (1/2): AND-gate of
  - `a1 no_reverter_sidecar_in_bleat_service` — behavior-based spec audit
    across 6 reverter resource locations
  - `a2 argocd_application_synced` — `selfHeal=true AND prune=true AND
    status.sync.status=Synced`
  - `a3 source_repo_aligned` — bleater-redis sts manifest in
    bleater-manifests `templates/infrastructure.yaml` has
    `--appendonly yes` AND non-empty `volumeClaimTemplates` named
    `data`. Setup.sh corrupts via Gitea contents API; solution.sh
    restores via the same API before re-enabling ArgoCD auto-sync.
- **B alert_observability** (1/2): AND-gate of
  - `b1 alert_rule_loaded` — three-store discovery
  - `b2 alert_fires_on_synthetic_failure` — behavioral injection +
    state-transition under cluster isolation
  - `b3 alert_routes_to_pageable_receiver` — Grafana policy-tree walk

## Key empirical findings (v37–v44)

- **b2 is the only durable variance lever.** 15-version track record
  of varying 2/5 to 5/5. PromQL composition (label filters, threshold
  ops against filtered scalars, noDataState) is genuinely error-prone.
- **Every config-presence atom saturates within 1–2 versions.**
  `baseline_survives_restart`, `no_orphan_watchdog_rbac`, `alert_rule_loaded`
  (post-v32), v44 `prune=true AND selfHeal=true` — all DEAD@1.
- **Behavioral atoms that don't have a real skill axis saturate too.**
  `data_survives_pod_restart` went DEAD@1 because agents rebuild sts
  wholesale (dropping initContainer as side effect). v42 behavioral
  drift-injection on a2 caught 0/5 — it was downstream of the same
  precondition as the spec check.
- **A-side has no remaining variance lever.** Without source-side
  breakage (setup.sh modifying the Gitea manifest repo), every
  ArgoCD-shaped check on the live cluster saturates.

## v37–v45 trajectory (compressed)

| v | A_atoms | B_atoms | Mean | Outcome |
|---|---|---|---|---|
| v37 | a1+a2_data_survives | b1+b2+b3 | 0.40 | sample-variance in-band |
| v38 | a1+a2 | b1+b2+b3 + range-op fix | 0.70 | calibration removal saturated b1 |
| v39 | a1+a2 / c1 (3-subscore) | b1+b2+b3 | 0.73 | c1 (ArgoCD config) DEAD@1 at 9/10 |
| v40 | a1 only | b1+b2+b3 | not batched | dropped DEAD@1 a2, softened P1 body |
| v41 | a1+a2_argocd | b1+b2+b3 | 0.60 | collapsed C into A |
| v42 | a1+a2_drift_behavioral | b1+b2+b3 | 0.60 | behavioral drift caught 0/5 |
| v43 | a1+a2 | b1+b2+b3+b4_for+b5_severity | not batched | preempted as redundant |
| v44 | a1+a2_prune_strict | b1+b2+b3 | 0.60 | prune-tightening caught 0/5 |
| v45 | a1+a2+a3_source_repo | b1+b2+b3 | invalid | solution.sh scored 0.5 locally — a2 OutOfSync due to git/live vct shape mismatch |
| v46 | a1+a2+a3_source_repo | b1+b2+b3 | invalid | solution.sh STILL 0.5 — a2 OutOfSync. vct-shape guess was wrong root cause |
| v47 | a1+a2+a3_source_repo | b1+b2+b3 | invalid | a2 still OutOfSync. Diagnostic revealed REAL root cause: CSA→SSA migration fails on immutable vct. v44 only "passed" a2 via stale status (no sync was triggered) |
| v48 | a1+a2+a3_source_repo | b1+b2+b3 | invalid | a2 still OutOfSync. Diagnostic revealed: ArgoCD created sts from corrupted git in 30s window before our restore; then update path hit immutable vct |
| v49 | a1+a2+a3_source_repo | b1+b2+b3 | invalid | Aggressive delete + Replace=true + force=true + retrigger loop. Same OutOfSync — race persists; sts delete still happens before git restore |
| v50 | a1+a2+a3_source_repo | b1+b2+b3 | invalid | a2 still OutOfSync. Full diagnostic showed: "Retrying attempt #5" of vct immutable update. ignoreDifferences only affects diff visibility, not apply behavior |
| v51 | a1+a2+a3_source_repo | b1+b2+b3 | invalid | No-Op validation failed at SETUP. Setup.sh's `op:remove /spec/syncPolicy/automated` silently failed when path absent; Argo auto-sync stayed on; Argo recreated sts during setup's delete-then-apply window |
| v52 | a1+a2+a3_source_repo | b1+b2+b3 | invalid | Setup.sh now succeeds (hardening worked) but a2 OutOfSync persisted. Diagnostic confirmed `Replace=true` IS in effect ("error when replacing /dev/shm/...") but kubectl replace ≠ kubectl replace --force; immutable error continues |
| v53 | a1+a2+a3_source_repo | b1+b2+b3 | invalid | a2 still OutOfSync (RespectIgnoreDifferences likely not supported in this Argo version) AND b2 newly broke (SSA pre-apply with argocd-controller manager conflicted with grader's default-manager patches). Score 0.0 |
| v54 | a1+a2(no-Synced)+a3 | b1+b2+b3 | 0.50 mean | 5 rollouts. a3 5/5 (saturated), b3 dropped to 2/5 (new variance). Score distribution `0,0.5,0.5,0.5,1.0` |
| v55 | a1+a2(no-Synced)+a3 | b1+b2+b3 | pending | Closed reviewer-flagged answer-leak: `/tmp/bleater-manifests-original.b64`. solution.sh now restores via Gitea commits API (fetches previous-commit content). |
| v56 | a1+a2(no-Synced)+a3 | b1+b2+b3 | n/a | VM cleanup only: deleted stale `~/task/redis-persistence-drift/` subfolder that was getting pushed with every version. |
| v57 | a1+a2(no-Synced)+a3 | b1+b2+b3 | invalid | Validator failed at Dockerfile build: `GET https://index.docker.io/v2/bitnami/kubectl/manifests/1.28:` — Bitnami deprecated free docker.io images in 2024, the `bitnami/kubectl:1.28` tag is no longer pullable. Confirms why v55's runtime pull silently failed too. |
| v58 | a1+a2(no-Synced)+a3 | b1+b2+b3 | invalid | Parallel push (hardened setup.sh: removed runtime-pull fallback, made image-import failure LOUD) but still referenced the dead `bitnami/kubectl:1.28` |
| v59 | a1+a2(no-Synced)+a3 | b1+b2+b3 | invalid (timeout) | No-op validation hit the 90-min timeout sweep. Root cause: Dockerfile RUN had uncapped `curl` + `crane pull` — either could hang indefinitely on slow/flaky network instead of fail-fast. |
| v60 | a1+a2(no-Synced)+a3 | b1+b2+b3 | pending | Bounded the Dockerfile network ops: `timeout 120 curl --connect-timeout 10 --max-time 90 --retry 2` for crane download, `timeout 300 crane pull` for image pull. If either hangs, build fails within minutes with a clear error. Also: cleaned `.horizon/stash/` on VM. |

## v44 per-item (most recent batched data)

| Item | Pass | Pattern | Notes |
|---|---|---|---|
| a1 no_reverter_sidecar | 3/5 | `++xx+` | runs 3,4 missed in-deploy sidecars (cache-config-tuner, redis-pool-sizer) |
| a2 argocd_synced | 5/5 | `+++++` | DEAD@1 — all 5 set prune+selfHeal |
| b1 alert_rule_loaded | 5/5 | `+++++` | DEAD@1 — mostly Grafana, some Prometheus |
| b2 alert_fires | 4/5 | `+++x+` | run3 used label filter that returned empty vector |
| b3 routes_pageable | 4/5 | `++++x` | run5 hit pre-existing blackhole receiver |

## v45 hypothesis

`a3 source_repo_aligned` adds a new variance lever that the v44 data
shows has not been exhausted. Setup.sh now PUTs a corrupted
templates/infrastructure.yaml to bleater-manifests master (Gitea
contents API). With ArgoCD auto-sync still disabled by setup.sh,
the corruption sits dormant until an agent re-enables Argo.
Agents who only fix the live cluster pass a1/a2 but fail a3 — the
P1 hint "make sure your changes land somewhere they will stick" is
the symptom-level pointer for this distinction.

Projected: a1=0.60, a2=1.0, a3=~0.30-0.50 → A joint ≈ 0.24 →
mean ≈ 0.5×0.24 + 0.5×0.60 = **~0.42** (in band).

Risks: if a3 also saturates at 1.0 (agents reflexively edit git too)
the mean stays at 0.60. If a3 lands at 0% (too oblique hint), mean
drops below 0.25 and a1 reverter audit needs softening too.

## Workflow conventions (durable preferences)

- **No local validation** before push (`[[no-local-validation]]` memory).
  Hosted eval rollouts are the signal.
- **No eval submissions without explicit user ask.** Pushing a new
  version is a separate action from submitting a batch.
- **Git workflow:** per-version commits in `/Users/tigran/task6` (master).
  Rsync local → push from VM → commit locally.
- **Per-version sequence:** edit local → `rsync --exclude='.git'
  --exclude='.horizon' --exclude='.rollouts' --exclude='.validation'
  local → VM` → `horizon tasks push` → commit locally.
- **Pull rollouts** with `horizon rollouts pull <uuid>` from `~/task/`
  on VM, then rsync `.rollouts/` back to local (gitignored).
- **API keys** in `~/Downloads/core/key-info.md`.
- **Dead-code cleanup before push** — audit for stale `_a2_*`, `_c1_*`,
  orphaned constants from removed atoms before every `horizon tasks push`.

## References cheat-sheet

- `task-hardner.md` — Hardening Decision Tree, U-curve calibration
- `AGENT_DIFFICULTY_BANK_v2.md` — Pattern 1 (layered self-healing),
  Anti-pattern #23 (same-shape mechanism diversity), Anti-pattern #27
  (atoms that the model has reliably internalized)
- `task-authoring-playbook.md` — Multiplication Trick math table,
  Hint Disclosure U-curve specifics
- `Master guide.md` — strict-ceiling rules, AND-gate non-functional rule
- `nebula-task-reviewer-v3.md` — 7-phase review procedure
- `nebula-batch-qc-feedback.md` — 20-point QC reformat
