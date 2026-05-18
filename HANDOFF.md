# HANDOFF — redis-persistence-drift

**Last updated:** 2026-05-18 (post-v13-rollout-pull)

## TL;DR

13 versions in, mean stuck at exactly **0.50** across every batch (v3, v7, v11, v13). A=DEAD@1 always, B=DEAD@0 always. Mean = 0.5×1.0 + 0.5×0.0 = 0.50, sitting on the strict 0.50 ceiling boundary. **My v8-v13 "fixes" did not move the needle because (a) Gitea hint never reached agents in any version and (b) persistence-survival is at biggie-max-nebula capability ceiling.** Next moves must address those two root causes, not iterate on the grader structure further.

## Where things stand

- **Task UUID:** `879b4f36-f5a2-4194-8a68-ee11c7af3a8f`
- **Mini-batch (create-permitted):** `99a0adf0-abfe-4fcf-9c65-74f40b2f9cb5`
  (legacy `5018ad80-…` is version-push-only — 403s on create)
- **Current version:** v13
- **VM:** `tigranharutyunyan59@34.186.153.63`, files at `~/tasks/redis-persistence-drift/`
- **Local repo:** `/Users/tigran/task6`, GitHub `tigran000/task6`, master branch

## Version history

| Version | Hypothesis | Outcome |
|---|---|---|
| v3 | Initial 2-subscore single-check shape | **5/5 mean=0.50**, A=DEAD@1, B=DEAD@0 |
| v4 | Monitoring hint in P1 issue body | unevaled |
| v5 | Soften hint + broaden b5 metrics | unevaled |
| v6 | Orthogonal subscores | unevaled |
| v7 | Remove dead `redis-baseline=present` label | **5/5 mean=0.50** (same pattern) |
| v8 | Fix Gitea URL `:3000` + broaden b5 regex | unevaled |
| v9 | 3-check AND-gates per subscore | **0/5 success** (infra failure, no scores) |
| v10 | Behavioral b2 | unevaled |
| v11 | Narrow b1 to 2 metrics + simpler injection | **5/5 mean=0.50** (same pattern) |
| v12 | Single CronJob reverter | unevaled |
| v13 | Scrub design refs + drop a2/b3 + 2nd cross-ns reverter | **4/5 mean=0.50** (same pattern) |

## Rollout breakdown (most recent versions)

### v13 per-item (4 scored runs)
```
baseline_survives_restart:     4/4 ++++   ← A's a1
unflushed_probe_survives:      4/4 ++++   ← A's a2 (reverter didn't bite)
alert_rule_loaded:             0/4 xxxx   ← B's b1 (agents not writing matching alerts)
alert_fires_on_synthetic_failure: 0/4 xxxx ← B's b2 (gated on b1)
```

### v11 per-item (5 runs, old 3-check structure)
```
string_survives_restart:   5/5 +++++
hash_survives_restart:     5/5 +++++
unflushed_probe_survives:  5/5 +++++
alert_rule_loaded:         0/5 xxxxx
alert_fires_on_synthetic_failure: 0/5 xxxxx
alert_is_actionable:       0/5 xxxxx
```

## Critical findings from v13 transcripts

### B's DEAD@0 root cause: Gitea hint NEVER REACHED AGENTS
Transcript grep across all 5 v13 runs:
```
P1 text seen by agent:      0/5 runs
Gitea issues API queries:   2-9 per run (agents WERE looking)
Monitoring hint seen:       0/5 runs
```

**The v8 "fix" of `GITEA_URL=http://gitea.devops.local:3000` is unverified and apparently broken in hosted env.** Every "hint calibration" iteration since v4 has been against a hint that never landed. All evidence consistent with: setup.sh's Gitea issue-filing block silently fails in hosted eval, agents see `open_issues_count: 0` on `bleater-app`.

**Untested locally.** I tested on `apex_rs` (review container) and observed setup.sh's `"Filing P1 incident..."` log but never confirmed the issue actually appeared via API call afterward.

### A's DEAD@1 root cause: capability ceiling on persistence-survival axis
Per `task-hardner.md` decision tree: "Saturated across 5+ test variants of the same axis → Capability ceiling. Drop the axis or grade end-state instead of behavior."

My a1/a2/a3 variants (v9, v11, v13) all saturate at 4-5/5 because:
- Fixing persistence is the *visible primary task* — every capable agent does it
- The v12/v13 reverter is structurally inert: it only flips IN-MEMORY CONFIG via `CONFIG SET`, which the StatefulSet's `--appendonly yes` command-args overrides on every pod restart
- Adding more "data survives" variants is exactly the anti-pattern the bank warns against (Anti-pattern #35 test-variant saturation)

## Current grader shape (v13)

Two subscores, equal weight 0.5/0.5, each AND-gate of 2:

| Subscore | Check | Type |
|---|---|---|
| A `persistence_durability` | a1 baseline_survives_restart | behavioral E2E probe |
| | a2 unflushed_probe_survives | behavioral, tests live save/appendfsync |
| B `alert_observability` | b1 alert_rule_loaded | structural |
| | b2 alert_fires_on_synthetic_failure | behavioral, injection |

## Active reverters (v13)

| Reverter | Namespace | Schedule | Status |
|---|---|---|---|
| `cache-config-syncer` CronJob | bleater | every 1m | **structurally inert** — only flips in-memory CONFIG |
| `redis-config-watchdog` CronJob | monitoring | every 2m | same |

Both run `redis-cli -h bleater-redis-headless CONFIG SET appendonly no; CONFIG SET save ""`. After ANY pod restart (including grader's a2 force-delete), StatefulSet command args reassert `--appendonly yes` → reverter's effect is gone. **They don't actually bite a2.** Empirically confirmed by v13 a2: 4/4 pass.

## What v14 must address (root causes, not symptoms)

### Priority 1: Verify Gitea hint reaches agents (CHEAP, no eval cost)
Spin a fresh `horizon setup` container, run setup.sh, then:
```
docker exec <container> curl -s http://gitea.devops.local:3000/api/v1/repos/root/bleater-app/issues
```
- If response shows the P1 issue → setup.sh works, my v11 hint just isn't converting. Then the calibration problem is real and tune the hint.
- If response shows `[]` → setup.sh's issue-filing block is broken. Debug WHY (token auth? endpoint? network namespace?). Possible fixes: use `bleater-app/issues` directly without token, or use a different delivery channel (`/home/ubuntu/INCIDENT.md`, wiki page).

### Priority 2: Redesign A's axis OR redesign the reverter
**Per `task-hardner.md` decision tree on capability ceiling: DROP the axis or grade end-state.**
- Option A: Accept A=DEAD@1, focus all variance on B. Mean = 0.5 + 0.5·B = 0.5 to 1.0. Even B at 0% leaves us at-ceiling. Not viable for approval.
- Option B: Redesign reverter to actually bite. Per `key-info.md` "Behavioral Test Patterns" → "reverter removed (restart-triggered)": modify the StatefulSet command-args directly, not just in-memory CONFIG. Reverter would `kubectl patch sts bleater-redis` to flip `--appendonly yes` → `--appendonly no` in command args. Persists across pod restarts. Restart-triggered reverter pattern from bank.
- Option C: Move A's axis entirely. e.g., A becomes "ArgoCD bleater-platform Application is Synced+Healthy" (orthogonal to persistence-fix). v3-v7 transcripts showed 4-5/5 agents DO re-enable selfHeal, so this might also DEAD@1.

### Priority 3: HANDOFF.md discipline
This file (per playbook #30). Update on every push going forward.

## What NOT to do for v14

- Don't iterate on grader check structure further. Adding/removing AND-gate items hasn't moved the needle in 13 versions and won't.
- Don't tune the hint wording further until we confirm it reaches agents.
- Don't push another version without testing the hypothesis being measured.

## Workflow conventions (durable preferences)

- **No local validation** before push (`[[no-local-validation]]` memory). Hosted eval rollouts are the signal.
- **Git workflow**: per-version commits in `/Users/tigran/task6` (master). Rsync local → push from VM → commit locally.
- **Per-version sequence:** edit local → `rsync --exclude='.horizon' --exclude='.rollouts' local → VM` → `horizon tasks push --label "v<N>: ..."` → rsync `.horizon/metadata.json` back → `git add tasks/redis-persistence-drift/<changed> && git commit`
- **Pull rollouts** with `horizon rollouts pull` from `~/tasks/redis-persistence-drift/` on VM, then rsync `.rollouts/` back to local (gitignored).
- **Per-item analysis** snippet in `~/Downloads/core/key-info.md` §"Reading Rollout Data Fast".
- **API keys** in `~/Downloads/core/key-info.md`. mini-batch for new task = `99a0adf0-…` (per `[[horizon-creator-mini-batch]]` memory).

## Files (no new files in v13)

- `Dockerfile` — `nebula-devops:1.1.0`, ALLOWED_NAMESPACES=`bleater,monitoring,argocd,gitea`
- `task.yaml` — Goldilocks prompt, air-gap notice present, terse "[TASK] Investigate and resolve" framing
- `setup.sh` — disables ArgoCD auto-sync, replaces sts with broken command + emptyDir, deletes orphan PVC, plants 2 reverters, files P1 + 2 decoys in Gitea, prewarms decoy keys
- `solution.sh` — kills both reverters, restores sts with PVC, scale 0→1 Prometheus, edits `prometheus-config` ConfigMap to add `alerts.yml` + `rule_files`, closes Gitea issue with RCA
- `grader.py` — 2 subscores × 2 AND-gated checks each (4 total)
- `.horizon/metadata.json` — task_id + current version

## References cheat-sheet

- `task-hardner.md` Hardening Decision Tree — directly applies to our DEAD@1/DEAD@0
- `key-info.md` "Behavioral Test Patterns" table — the right reverter design pattern
- `AGENT_DIFFICULTY_BANK_v2.md` Pattern 1, Anti-pattern #35 (test-variant saturation), Anti-pattern #23 (mechanism diversity)
- `task-authoring-playbook.md` #16 (hint calibration), #29 (audit all checks after one bug), #30 (HANDOFF.md)
- `Master guide.md` strict ceiling rules, §V39 sidecar camouflage
