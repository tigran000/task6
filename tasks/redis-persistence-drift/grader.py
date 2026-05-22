"""Grader for redis-persistence-drift.

Two equal-weight, binary, orthogonal subscores. Each is an AND-gate of
one or more related checks. Within-subscore checks share a theme;
between-subscore checks are independent code paths.

  A persistence_durability  (weight 1/2) — AND-gate of 3 atoms.
      a1 no_reverter_sidecar_in_bleat_service  (BEHAVIOR-BASED SPEC AUDIT)
         Per-resource scan of _REVERTER_SIDECAR_RESOURCES — Deployments,
         the bleater-redis StatefulSet, and CronJob templates across the
         bleater and monitoring namespaces. Each container's command/args
         is matched against two reverter-shaped patterns: redis-cli CONFIG
         SET disabling persistence, and kubectl-patch loops flipping the
         bleater-redis sts command-args back. Catches quiesced reverters
         (CronJob spec.suspend=true, scale-to-0, host-Deployment rolled)
         AND reverters planted on the Redis sts itself.
      a2 argocd_application_synced  (CLUSTER-STATE AUDIT)
         The bleater-platform ArgoCD Application must have
         spec.syncPolicy.automated.selfHeal == true AND .prune == true
         AND status.sync.status == 'Synced'. selfHeal and prune are
         semantically distinct: selfHeal reverts modifications, prune
         removes orphaned out-of-band resources. Agents who set only
         selfHeal leave the planted reverter CronJobs/Deployments
         unowned by the Application — they linger across the next
         reconcile. Setup.sh strips automated sync entirely; a correct
         cleanup restores both flags.
      a3 source_repo_aligned  (GITOPS SOURCE-OF-TRUTH AUDIT)
         The bleater-redis StatefulSet manifest inside the bleater-
         manifests Gitea repo (which ArgoCD pulls from) must have
         --appendonly yes AND a non-empty volumeClaimTemplates entry
         named 'data'. Setup.sh corrupts this file in master so that
         agents who only fix the LIVE cluster leave a time bomb: the
         moment ArgoCD selfHeal reconciles from the still-broken
         manifest, the cluster regresses. The "hotfixes get quietly
         rolled back by the platform reconciliation loop — make sure
         your changes land somewhere they will stick" hint in the P1
         body is the symptom-level pointer for this atom; agents who
         miss the git-vs-live distinction fail here regardless of how
         well they cleaned the cluster.

  B alert_observability     (weight 1/2) — AND-gate of 3 atoms.
      b1 alert_rule_loaded
         Three-store rule discovery (Prometheus /api/v1/rules, Grafana
         file-provisioning CM, Grafana runtime API). Reports the matched
         rule and metric. b2 and b3 cascade to "no rule to test" if b1
         fails, preserving cascade semantics without collapsing two
         distinct skills (rule discovery + rule firing) into one atom.
      b2 alert_fires_on_synthetic_failure  (BEHAVIORAL)
         Runs the discovered rule inside isolation (suspend reverters,
         scale bleat-service to 0, patch sts to known-good, wait for
         redis_aof_enabled=1), injects a synthetic failure, and asserts
         state transition (inactive→pending/firing for Prometheus rules;
         threshold-eval pre/post-injection for Grafana rules). The pre-
         state gate catches decorative always-firing rules; the post-
         injection check catches stuck-off rules.
      b3 alert_routes_to_pageable_receiver  (CONFIG-WALK, NON-DESTRUCTIVE)
         Walks Grafana's notification policy tree top-down against the
         matched rule's labels (curl /api/v1/provisioning/policies from
         inside the Grafana pod). The matched receiver must not be in
         the blackhole set. Prometheus-store rules fail-open (this
         snapshot has no Alertmanager wired).

A and B are fully independent. A is measured against the cluster state
the agent left behind. Before B starts measuring, the grader takes
temporary control of the persistence layer for the duration of b2's
measurement window — suspends the reverter CronJobs, scales the
bleat-service Deployment to 0 (so the cache-config-tuner sidecar pod
cannot interfere), strips all sidecar containers from the bleater-redis
sts pod template (so any agent-left in-pod reverter such as redis-
metrics-exporter cannot fight us), and patches the sts back to a known-
good command — then restores the agent's last-set state in a finally
block. This means the outcome of B depends only on whether the agent
built a working alert, not on whether they also killed all of A's
reverters. All four cells of the joint distribution are reachable AND
equally likely given the agent's actual work.
"""

import json
import re
import subprocess
import time
import urllib.parse

NS = "bleater"
PROM_NS = "monitoring"
POD_LABEL = "app=bleater-redis"
KUBECTL = ["kubectl", "-n", NS]


def log(msg):
    print(msg, flush=True)


def run(cmd, timeout=60):
    try:
        out = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout, check=False
        )
        return out.returncode, out.stdout, out.stderr
    except Exception as e:
        return 1, "", str(e)


def kubectl_jsonpath(args, jsonpath):
    cmd = KUBECTL + list(args) + ["-o", "jsonpath=" + jsonpath]
    _, out, _ = run(cmd)
    return out.strip()


def redis_pod():
    name = kubectl_jsonpath(
        ["get", "pod", "-l", POD_LABEL],
        "{.items[0].metadata.name}",
    )
    return name or None


def redis_cli(pod, *args, timeout=15):
    if not pod:
        return ""
    cmd = KUBECTL + ["exec", pod, "--", "redis-cli"] + list(args)
    _, out, _ = run(cmd, timeout=timeout)
    return out.strip()


def wait_for_redis(timeout=180):
    start = time.time()
    while time.time() - start < timeout:
        pod = redis_pod()
        if pod:
            phase = kubectl_jsonpath(["get", "pod", pod], "{.status.phase}")
            if phase == "Running":
                pong = redis_cli(pod, "PING", timeout=5)
                if pong.upper() == "PONG":
                    save_out = redis_cli(pod, "CONFIG", "GET", "save", timeout=5)
                    if save_out:
                        return pod
        time.sleep(3)
    return None


# Resources where setup.sh plants reverters. The a3 spec audit visits
# every entry; agents must clean ALL of them. Tuple = (kind, name,
# namespace). Resources of kind "cronjob" are caught even when suspended
# (suspending preserves the spec, so a reverter-shaped container in the
# jobTemplate still indicates the reverter is one Suspend=false away
# from running). "absent" status (resource entirely deleted) counts as
# PASS for that entry — full deletion is the strongest cleanup. Adding
# bleater-redis catches sidecars planted on the Redis sts itself, which
# the v25-v29 audit missed.
_REVERTER_SIDECAR_RESOURCES = [
    ("deploy", "bleater-bleat-service", NS),
    ("deploy", "bleater-timeline-service", NS),
    ("sts", "bleater-redis", NS),
    ("cronjob", "cache-config-syncer", NS),
    ("cronjob", "redis-fsync-tuner", PROM_NS),
    ("cronjob", "redis-config-watchdog", PROM_NS),
]


def _resource_containers(kind, name, namespace):
    """Return the list of container dicts inside the named resource's
    pod template. Handles Deployment / StatefulSet (containers under
    .spec.template.spec.containers) and CronJob (containers under
    .spec.jobTemplate.spec.template.spec.containers). Returns:
      list[dict] — the containers
      "absent"   — resource is fully deleted (PASS for the audit)
      None       — resource exists but couldn't be parsed (audit-fail)"""
    rc, out, _ = run(
        ["kubectl", "-n", namespace, "get", kind, name, "-o", "json"],
        timeout=15,
    )
    out = (out or "").strip()
    if rc != 0 or not out:
        return "absent"
    try:
        d = json.loads(out)
    except Exception:
        return None
    if kind == "cronjob":
        pod_spec = (d.get("spec", {})
                     .get("jobTemplate", {})
                     .get("spec", {})
                     .get("template", {})
                     .get("spec", {}))
    else:
        pod_spec = (d.get("spec", {})
                     .get("template", {})
                     .get("spec", {}))
    return pod_spec.get("containers", [])


def _is_reverter_shaped(container):
    """Behavior-based detection of a reverter container. Catches both:
      A. redis-cli CONFIG SET loops disabling persistence (appendonly no,
         appendfsync no, save "")
      B. kubectl patch loops flipping the bleater-redis StatefulSet
         command-args back to a non-durable state (the redis-config-
         watchdog mechanism)
    Behavior-based so renames don't bypass."""
    parts = (container.get("command") or []) + (container.get("args") or [])
    joined = " ".join(parts).lower()
    # Pattern A: redis-cli CONFIG SET disabling persistence.
    # Whitespace-tolerant regexes (catches `appendonly\tno`, `appendonly  no`,
    # etc.) instead of a literal `" no"` substring match.
    if "config set" in joined:
        if re.search(r"appendonly\s+no\b", joined):
            return True
        if re.search(r"appendfsync\s+no\b", joined):
            return True
        # `CONFIG SET save ""` — the empty string survives .lower() as
        # just two quotes; look for the disabling pattern directly.
        if re.search(r"\bsave\b", joined) and ('""' in joined or "''" in joined):
            return True
    # Pattern B: kubectl patch on the bleater-redis StatefulSet that
    # flips its command-args back to a non-durable state. Detect via
    # behavior signature (kubectl + patch + statefulset + persistence-
    # disabling args) without requiring the literal sts name, so an
    # agent who renames the sts as a workaround doesn't bypass the audit.
    if ("kubectl" in joined and "patch" in joined and
            ("statefulset" in joined or " sts " in joined)):
        if re.search(r"appendonly[^a-z]+no\b", joined):
            return True
        if re.search(r"\bsave\b", joined) and ('""' in joined or "''" in joined):
            return True
    return False


def _a1_spec_audit_no_reverter_sidecar():
    """Behavior-based spec audit across Deployments, StatefulSets, and
    CronJob templates. For each entry in _REVERTER_SIDECAR_RESOURCES,
    fetch the live spec and fail if ANY container in the pod template
    looks like a reverter. Resources entirely deleted count as PASS.
    Catches:
      - quiesced reverters (CronJob spec.suspend=true) the live poll misses
      - reverters planted on the Redis sts itself (covers the
        StatefulSet container list directly, not only Deployments)
      - kubectl-patch-shaped watchdog containers (catches redis-config-
        watchdog in its CronJob jobTemplate)
    """
    per_resource = []
    overall_bad = []
    unreadable = []
    for kind, name, namespace in _REVERTER_SIDECAR_RESOURCES:
        containers = _resource_containers(kind, name, namespace)
        if containers is None:
            unreadable.append("%s/%s in %s" % (kind, name, namespace))
            continue
        if containers == "absent":
            per_resource.append((kind, name, namespace, "absent", []))
            continue
        reverter_names = [c.get("name", "?") for c in containers
                          if _is_reverter_shaped(c)]
        all_names = [c.get("name", "?") for c in containers]
        per_resource.append((kind, name, namespace, all_names, reverter_names))
        if reverter_names:
            overall_bad.append((kind, name, namespace, reverter_names))
    if unreadable:
        return False, ("could not parse spec for resource(s): %s" % unreadable)
    if overall_bad:
        return False, ("reverter-shaped container(s) still present: %s" %
                       [(k, n, ns, names) for k, n, ns, names in overall_bad])
    return True, ("no reverter-shaped container in any audited resource "
                  "(%s)" % [(k, n, ns) for k, n, ns, _, _ in per_resource])


def subscore_a_persistence_durability():
    """AND-gate of 3 atoms answering: 'Will the cluster stay durably
    persistent — free of latent reverter mechanisms, with the GitOps
    reconciliation loop owning the live state, AND with the source-of-
    truth manifest matching the desired persistent shape?'
    a1 no_reverter_sidecar_in_bleat_service — Behavior-based spec audit
                                              across Deployments, the
                                              bleater-redis StatefulSet,
                                              and CronJob templates. No
                                              container's command/args
                                              may match a reverter
                                              pattern.
    a2 argocd_application_synced            — Cluster-state audit. The
                                              bleater-platform ArgoCD
                                              Application has
                                              spec.syncPolicy.automated
                                              set AND status.sync.status
                                              == 'Synced'. Both ensure
                                              the next drift event
                                              auto-reconciles.
    a3 source_repo_aligned                  — GitOps source-of-truth
                                              audit. The bleater-redis
                                              StatefulSet manifest in
                                              bleater-manifests
                                              (templates/infrastructure.
                                              yaml) has --appendonly yes
                                              AND non-empty
                                              volumeClaimTemplates named
                                              'data'. Catches agents
                                              who fix only the live
                                              cluster while leaving the
                                              git manifest corrupted —
                                              ArgoCD selfHeal would
                                              regress them on next
                                              reconcile.
    A prior behavioral data_survives_pod_restart atom was dropped:
    agents reliably rebuild the redis sts from scratch, which drops any
    setup-planted initContainer as a side effect — saturating the atom
    in practice."""
    a1_ok, a1_detail = _a1_spec_audit_no_reverter_sidecar()
    a2_ok, a2_detail = _a2_argocd_reconciled()
    a3_ok, a3_detail = _a3_source_repo_aligned()
    return [int(a1_ok), int(a2_ok), int(a3_ok)], [
        ("no_reverter_sidecar_in_bleat_service", a1_ok, a1_detail),
        ("argocd_application_synced", a2_ok, a2_detail),
        ("source_repo_aligned", a3_ok, a3_detail),
    ]


_ARGOCD_NS = "argocd"
_ARGOCD_APP = "bleater-platform"


def _a2_argocd_reconciled():
    """Verify the bleater-platform ArgoCD Application is back in a healthy
    self-reconciling state. Three binary conditions all required:
      1. spec.syncPolicy.automated.selfHeal == true (mods get reverted).
      2. spec.syncPolicy.automated.prune == true (orphans get removed).
      3. status.sync.status == 'Synced' (deployed state matches manifests).
    selfHeal and prune are semantically distinct — agents commonly set
    selfHeal but forget prune, leaving orphaned reverter workloads
    around even though the sts itself reconciles. The v42 behavioral
    drift-injection variant was dropped: it caught zero agents in 5
    rollouts (5/5 passed), confirming the behavioral step is downstream
    of the same precondition as the spec check.
    Returns (ok, detail)."""
    rc_a, auto, _ = run(
        ["kubectl", "-n", _ARGOCD_NS, "get", "application", _ARGOCD_APP,
         "-o", "json"],
        timeout=15,
    )
    if rc_a != 0:
        return False, ("could not read ArgoCD Application %s/%s "
                       "(rc=%s)" % (_ARGOCD_NS, _ARGOCD_APP, rc_a))
    try:
        app = json.loads(auto)
    except Exception:
        return False, "ArgoCD Application JSON unparseable"
    automated = ((app.get("spec") or {})
                 .get("syncPolicy") or {}).get("automated") or {}
    self_heal = bool(automated.get("selfHeal"))
    prune = bool(automated.get("prune"))
    sync = (((app.get("status") or {}).get("sync") or {})
            .get("status") or "").strip()
    if not self_heal:
        return False, ("ArgoCD %s spec.syncPolicy.automated.selfHeal is not "
                       "true — modifications will not auto-revert, so a "
                       "future Helm-upgrade-style drift will not be "
                       "corrected" % _ARGOCD_APP)
    if not prune:
        return False, ("ArgoCD %s spec.syncPolicy.automated.prune is not "
                       "true — orphaned out-of-band resources (like the "
                       "planted reverter CronJobs/Deployments) will not "
                       "be auto-removed on the next reconcile" %
                       _ARGOCD_APP)
    if sync != "Synced":
        return False, ("ArgoCD %s status.sync.status=%r (expected 'Synced') "
                       "— live cluster diverges from GitOps manifests" %
                       (_ARGOCD_APP, sync))
    return True, ("ArgoCD %s is Synced with selfHeal=true AND prune=true" %
                  _ARGOCD_APP)


_GITEA_BASE = "http://gitea.devops.local:3000"
_GITEA_USER = "root"
_GITEA_PASS = "Admin@123456"
_MANIFEST_REPO = "bleater-manifests"
_MANIFEST_PATH = "templates/infrastructure.yaml"


def _a3_source_repo_aligned():
    """Verify the bleater-redis StatefulSet manifest inside the
    bleater-manifests Gitea repo (the chart ArgoCD pulls from) has
    persistence enabled. Setup.sh corrupts this file to --appendonly no
    + empty volumeClaimTemplates; a correct fix must commit a restored
    manifest back to master, otherwise ArgoCD selfHeal will reconcile
    the cluster back to the broken shape on the next refresh.

    Two binary conditions all required (read from the file in master,
    not the live cluster):
      1. The redis container's command/args has --appendonly yes AND
         does NOT have --appendonly no or --save "".
      2. spec.volumeClaimTemplates contains an entry named 'data'
         (no entry == emptyDir == ephemeral cache).

    Uses HTTP Basic auth against the Gitea contents API. The file
    response shape is {"sha": "...", "content": "<base64>", ...}.

    Returns (ok, detail)."""
    import base64
    cmd = ["curl", "-s", "--connect-timeout", "5", "--max-time", "20",
           "-u", "%s:%s" % (_GITEA_USER, _GITEA_PASS),
           "%s/api/v1/repos/%s/%s/contents/%s" % (
               _GITEA_BASE, _GITEA_USER, _MANIFEST_REPO, _MANIFEST_PATH)]
    rc, out, _ = run(cmd, timeout=30)
    if rc != 0 or not out:
        return False, ("could not fetch %s from %s/%s via Gitea API (rc=%s)" %
                       (_MANIFEST_PATH, _GITEA_USER, _MANIFEST_REPO, rc))
    try:
        resp = json.loads(out)
    except Exception:
        return False, ("Gitea API returned non-JSON for %s" % _MANIFEST_PATH)
    content_b64 = resp.get("content", "")
    if not content_b64:
        return False, ("Gitea API response missing 'content' for %s "
                       "(repo or path may not exist)" % _MANIFEST_PATH)
    try:
        text = base64.b64decode(content_b64).decode("utf-8", "replace")
    except Exception:
        return False, ("templates/infrastructure.yaml content_base64 "
                       "could not be decoded")
    try:
        import yaml
    except Exception:
        return False, "yaml module unavailable in grader environment"
    try:
        docs = list(yaml.safe_load_all(text))
    except Exception as e:
        return False, ("%s in git is not valid YAML: %s" %
                       (_MANIFEST_PATH, e))
    sts = None
    for doc in docs:
        if not isinstance(doc, dict):
            continue
        if (doc.get("kind") == "StatefulSet"
                and (doc.get("metadata") or {}).get("name") == "bleater-redis"):
            sts = doc
            break
    if sts is None:
        return False, ("%s in git has no StatefulSet named bleater-redis "
                       "(file may have been deleted or renamed)" % _MANIFEST_PATH)
    spec = sts.get("spec") or {}
    containers = (((spec.get("template") or {}).get("spec") or {})
                  .get("containers") or [])
    redis_c = next((c for c in containers if c.get("name") == "redis"), None)
    if redis_c is None:
        return False, ("bleater-redis StatefulSet in git has no container "
                       "named 'redis'")
    cmd_args = ((redis_c.get("command") or []) +
                (redis_c.get("args") or []))
    # List-position check: redis-server CLI uses positional pairs like
    # ['--save', ''] and ['--appendonly', 'no'] / ['--appendonly', 'yes'].
    # Joining-and-regex misses the empty-string case because the quotes
    # are YAML-level (the parser strips them). Scan pairs instead.
    appendonly_value = None
    save_value = None
    for i, arg in enumerate(cmd_args):
        token = str(arg).strip().lower()
        if token == "--appendonly" and i + 1 < len(cmd_args):
            appendonly_value = str(cmd_args[i + 1]).strip().lower()
        elif token == "--save" and i + 1 < len(cmd_args):
            save_value = str(cmd_args[i + 1]).strip()
    if appendonly_value == "no":
        return False, ("bleater-redis StatefulSet in git still has "
                       "--appendonly no — live cluster will regress "
                       "to no-AOF on next ArgoCD reconcile (command=%r)"
                       % cmd_args)
    if save_value is not None and save_value in ("", '""', "''"):
        return False, ("bleater-redis StatefulSet in git still has "
                       "--save \"\" — RDB snapshots disabled at the "
                       "source (command=%r)" % cmd_args)
    if appendonly_value != "yes":
        return False, ("bleater-redis StatefulSet in git command does "
                       "not enable --appendonly yes (saw appendonly=%r, "
                       "command=%r)" % (appendonly_value, cmd_args))
    vcts = spec.get("volumeClaimTemplates") or []
    data_pvc = next((v for v in vcts
                     if (v.get("metadata") or {}).get("name") == "data"),
                    None)
    if data_pvc is None:
        return False, ("bleater-redis StatefulSet in git has no "
                       "volumeClaimTemplate named 'data' — emptyDir "
                       "leaves the cache ephemeral across pod restarts")
    return True, ("bleater-redis manifest in %s/%s is correct "
                  "(--appendonly yes + PVC named 'data')" %
                  (_MANIFEST_REPO, _MANIFEST_PATH))


def _prom_query(path, timeout=15):
    """Hit the prometheus HTTP API via kubectl exec. Returns (json_dict, err)."""
    cmd = [
        "kubectl", "-n", PROM_NS, "exec", "deploy/prometheus", "--",
        "wget", "-qO-", "http://localhost:9090" + path,
    ]
    _, out, _ = run(cmd, timeout=timeout)
    if not out:
        return None, "no response from prometheus" + path
    try:
        return json.loads(out), None
    except Exception:
        return None, "non-json from prometheus" + path


def _find_matching_prometheus_rule():
    """Scan Prometheus /api/v1/rules for an alerting rule whose expr references
    a redis-exporter persistence metric. Returns (rule_dict, metric, err)."""
    data, err = _prom_query("/api/v1/rules")
    if data is None:
        return None, None, err
    for g in data.get("data", {}).get("groups", []):
        for r in g.get("rules", []):
            if r.get("type") != "alerting":
                continue
            if r.get("health") not in (None, "ok"):
                continue
            expr = r.get("query", "") or r.get("expr", "")
            m = _b_metric_pattern.search(expr)
            if m:
                return r, m.group(0), None
    return None, None, "no Prometheus rule references a redis-exporter persistence metric"


def _read_grafana_provisioned_rules():
    """Read the cluster's Grafana UnifiedAlerting provisioning ConfigMap
    (monitoring/grafana-alerting-provisioning, key alert-rules.yaml) and
    return the list of rule dicts (flattened across all groups), or [] if
    the CM or key is missing / unparseable."""
    _, out, _ = run(
        ["kubectl", "-n", PROM_NS, "get", "cm",
         "grafana-alerting-provisioning", "-o",
         r"jsonpath={.data.alert-rules\.yaml}"],
        timeout=15,
    )
    text = (out or "").strip()
    if not text:
        return []
    # Lazy-import yaml — apex base image ships it but be defensive.
    try:
        import yaml
    except Exception:
        return []
    try:
        doc = yaml.safe_load(text) or {}
    except Exception:
        return []
    rules = []
    for g in (doc.get("groups") or []):
        for r in (g.get("rules") or []):
            rules.append(r)
    return rules


def _grafana_rule_matching_expr(rule):
    """Scan every prometheus-datasource data item in `rule` and return
    (expr, metric) for the FIRST one whose `model.expr` matches the
    redis-exporter persistence metric whitelist (`_b_metric_pattern`).
    Returns ("", None) if no such item exists.

    Grafana UnifiedAlerting rules carry a `data` array of refIds, and the
    convention is refId A = query, B = reduce, C = threshold. That is
    convention only, not contract — multi-query rules with the redis
    metric in B or later are valid. Scanning every refId (rather than
    just refId A) prevents b1 from silently missing those.

    Both `_find_matching_grafana_rule` (for b1 discovery) and
    `_b2_grafana_path` (for the threshold eval) call this with the same
    rule, so the expr they pick is guaranteed consistent.
    """
    for item in (rule.get("data") or []):
        if item.get("datasourceUid") != "prometheus":
            continue
        model = item.get("model") or {}
        expr = (model.get("expr") or "").strip()
        if not expr:
            continue
        m = _b_metric_pattern.search(expr)
        if m:
            return expr, m.group(0)
    return "", None


def _grafana_rule_threshold(rule):
    """Find the rule's `condition`-refId data item (the threshold step) and
    return (op, value) extracted from its first evaluator. op ∈
    {gt, lt, gte, lte, eq, within_range, outside_range}. For scalar ops the
    value is a float; for range ops it is a (lower, upper) float tuple so
    `_evaluate_threshold` can apply the inclusive-range semantics correctly.
    Returns (None, None) if not parseable."""
    cond_refid = rule.get("condition")
    if not cond_refid:
        return None, None
    for item in (rule.get("data") or []):
        if item.get("refId") != cond_refid:
            continue
        model = item.get("model") or {}
        if model.get("type") != "threshold":
            continue
        conditions = model.get("conditions") or []
        if not conditions:
            continue
        ev = (conditions[0] or {}).get("evaluator") or {}
        op = ev.get("type")
        params = ev.get("params") or []
        if op and params:
            try:
                if op in ("within_range", "outside_range") and len(params) >= 2:
                    return op, (float(params[0]), float(params[1]))
                return op, float(params[0])
            except (TypeError, ValueError):
                return None, None
    return None, None


def _find_matching_grafana_rule():
    """Scan the Grafana provisioning CM for an alert rule with ANY
    prometheus-datasource expression that references a redis-exporter
    persistence metric (every refId scanned, not just the first).
    Returns (rule_dict, metric, err).

    Calibration / decoration is verified behaviorally in `_b2_grafana_path`'s
    pre/post-injection gate, which queries live Prometheus and composes
    PromQL filter semantics correctly. A discovery-time calibration check
    cannot model inline-comparison expressions (e.g. `redis_aof_enabled
    == 0`) or range-based threshold operators (`within_range` /
    `outside_range`), so it is omitted here in favor of the behavioral path.

    Note: this cluster has no prometheus-operator (its prometheus.yml
    does not declare a `kube_state` or `kube_apiserver` config and the
    `alertmanagers:` stanza is commented out, per the v23 run1 transcript
    dump). PrometheusRule CRD objects therefore have nowhere to reconcile
    to, so we do not probe for them. If a future setup.sh installs the
    operator, add a third store check next to this one."""
    rules = _read_grafana_provisioned_rules()
    if not rules:
        return None, None, "no rules in grafana-alerting-provisioning"
    for r in rules:
        expr, metric = _grafana_rule_matching_expr(r)
        if metric is None:
            continue
        return r, metric, None
    return None, None, ("no Grafana rule references a redis-exporter "
                        "persistence metric")


def _read_grafana_api_rules():
    """Read Grafana's runtime alert-rule store via
    /api/v1/provisioning/alert-rules. This catches rules an agent created
    via the Grafana API (e.g., `POST /api/v1/provisioning/alert-rules`
    with `X-Disable-Provenance: true`) — those land in Grafana's runtime
    DB but NOT in the file-provisioning ConfigMap, so they are invisible
    to `_read_grafana_provisioned_rules`. v25 run1's agent did exactly
    this and was unfairly rejected by b1.

    Authentication: try the wiki-documented `admin:admin123` first, then
    fall back to Grafana's built-in default `admin:admin`. v25 transcripts
    show agents successfully calling this endpoint with both; the cluster's
    actual configured password is in the `grafana-ini-config` ConfigMap.
    If neither works (e.g., agent rotated the password), the probe falls
    through quietly — acceptable since the rule would then also be
    invisible to ops.

    Returns a list of rule dicts (same shape as file-provisioned rules:
    `data`, `condition`, `title`, `uid`), or [] on any failure / no rules.
    """
    for pwd in ("admin123", "admin"):
        cmd = [
            "kubectl", "-n", PROM_NS, "exec", "deploy/grafana", "--",
            "sh", "-c",
            (r"curl -s -w '\n%{http_code}' -u admin:" + pwd + " "
             "http://localhost:3000/api/v1/provisioning/alert-rules"),
        ]
        _, out, _ = run(cmd, timeout=15)
        text = (out or "").strip()
        if not text:
            continue
        # Last line is the HTTP status code (curl -w '\n%{http_code}').
        idx = text.rfind("\n")
        if idx == -1:
            continue
        body, code = text[:idx], text[idx + 1:].strip()
        if code != "200":
            continue
        try:
            rules = json.loads(body)
        except Exception:
            continue
        if isinstance(rules, list):
            return rules
    return []


def _find_matching_grafana_api_rule():
    """Scan Grafana's runtime API alert-rule store for a rule whose ANY
    prometheus-datasource expression references a redis-exporter persistence
    metric. Same parser as the file-provisioning path — runtime rule shape
    is identical to YAML provisioning rule shape. Calibration / decoration
    is verified behaviorally in `_b2_grafana_path` (see
    `_find_matching_grafana_rule` docstring for rationale).
    Returns (rule_dict, metric, err)."""
    rules = _read_grafana_api_rules()
    if not rules:
        return None, None, "no rules in grafana runtime API store"
    for r in rules:
        expr, metric = _grafana_rule_matching_expr(r)
        if metric is None:
            continue
        return r, metric, None
    return None, None, ("no Grafana API rule references a redis-exporter "
                        "persistence metric")


def _find_matching_alert_rule():
    """Path-store-agnostic discovery. Returns (rule, metric, source, err),
    where source ∈ {"prometheus", "grafana", "grafana_api"}.

    Preference order is the chain agents typically take from cheapest to
    discover: Prometheus rule_files (everyone sees prometheus-config), then
    Grafana file-provisioning (visible via `kubectl get cm -n monitoring`),
    then Grafana runtime API (only visible to agents who hit the Grafana
    HTTP API directly). Returning the FIRST hit means a Prometheus rule
    wins over a Grafana rule even if both exist, preserving v20 baseline
    behavior."""
    r, m, err_p = _find_matching_prometheus_rule()
    if r is not None:
        return r, m, "prometheus", None
    r, m, err_g = _find_matching_grafana_rule()
    if r is not None:
        return r, m, "grafana", None
    r, m, err_ga = _find_matching_grafana_api_rule()
    if r is not None:
        return r, m, "grafana_api", None
    return None, None, None, ("%s; %s; %s" %
                              (err_p or "?", err_g or "?", err_ga or "?"))


def _evaluate_threshold(op, threshold_value, observed_value):
    """Apply a Grafana threshold evaluator to an observed metric value.
    For scalar ops (gt/lt/gte/lte/eq), threshold_value is a float. For
    range ops (within_range/outside_range), threshold_value is a
    (lower, upper) tuple matching Grafana's two-param evaluator shape."""
    if observed_value is None or threshold_value is None or op is None:
        return None
    if op == "gt":
        return observed_value > threshold_value
    if op == "lt":
        return observed_value < threshold_value
    if op == "gte":
        return observed_value >= threshold_value
    if op == "lte":
        return observed_value <= threshold_value
    if op == "eq":
        return observed_value == threshold_value
    if op == "within_range":
        if isinstance(threshold_value, tuple) and len(threshold_value) == 2:
            lo, hi = threshold_value
            return lo <= observed_value <= hi
        return None
    if op == "outside_range":
        if isinstance(threshold_value, tuple) and len(threshold_value) == 2:
            lo, hi = threshold_value
            return observed_value < lo or observed_value > hi
        return None
    return None


def _prom_instant_value(expr, timeout=15):
    """Evaluate a PromQL expression via Prometheus /api/v1/query and return
    the first-series scalar value as a float, or None if the result is empty
    / unparseable."""
    q = urllib.parse.quote(expr, safe="")
    data, _ = _prom_query("/api/v1/query?query=" + q, timeout=timeout)
    if not data:
        return None
    result = (data.get("data") or {}).get("result") or []
    if not result:
        return None
    val_pair = result[0].get("value") or [None, None]
    try:
        return float(val_pair[1])
    except (TypeError, ValueError):
        return None


# Receivers that silently swallow notifications. Empty string covers the
# (surprisingly common) case where the policy has no receiver set at all.
_BLACKHOLE_RECEIVER_NAMES = {
    "", "blackhole", "null", "noop", "discard", "drop", "silenced",
}


def _read_grafana_notification_policies():
    """Read Grafana's runtime notification-policy tree via
    /api/v1/provisioning/policies. Same auth handling as the rule store
    probe (try admin:admin123 first, fall back to admin:admin). Returns
    the policy root dict, or None on failure."""
    for pwd in ("admin123", "admin"):
        cmd = [
            "kubectl", "-n", PROM_NS, "exec", "deploy/grafana", "--",
            "sh", "-c",
            (r"curl -s -w '\n%{http_code}' -u admin:" + pwd + " "
             "http://localhost:3000/api/v1/provisioning/policies"),
        ]
        _, out, _ = run(cmd, timeout=15)
        text = (out or "").strip()
        if not text:
            continue
        idx = text.rfind("\n")
        if idx == -1:
            continue
        body, code = text[:idx], text[idx + 1:].strip()
        if code != "200":
            continue
        try:
            return json.loads(body)
        except Exception:
            continue
    return None


def _route_matches_labels(route, labels):
    """Check if a Grafana route matches a label set, supporting all four
    matcher shapes Grafana has shipped over the years: `match` (kv map),
    `match_re` (regex kv map), `matchers` (list of strings like
    `"k=\"v\""`), `object_matchers` (list of [name, op, value] tuples)."""
    # Legacy `match` (exact equality kv map)
    for k, v in (route.get("match") or {}).items():
        if labels.get(k) != v:
            return False
    # Legacy `match_re` (regex kv map)
    for k, v in (route.get("match_re") or {}).items():
        if not re.search(v, labels.get(k, "") or ""):
            return False
    # Current `matchers` (list of strings like `severity="critical"`)
    for m in (route.get("matchers") or []):
        # parse "name=value" / "name!=value" / "name=~regex" / "name!~regex"
        mm = re.match(r'^\s*([A-Za-z_][A-Za-z_0-9]*)\s*(=~|!=|!~|=)\s*"?([^"]*)"?\s*$', m or "")
        if not mm:
            continue
        name, op, val = mm.group(1), mm.group(2), mm.group(3)
        actual = labels.get(name, "") or ""
        if op == "=" and actual != val:
            return False
        if op == "!=" and actual == val:
            return False
        if op == "=~" and not re.search(val, actual):
            return False
        if op == "!~" and re.search(val, actual):
            return False
    # Current `object_matchers` (list of [name, op, value] triples)
    for triple in (route.get("object_matchers") or []):
        if not isinstance(triple, (list, tuple)) or len(triple) < 3:
            continue
        name, op, val = triple[0], triple[1], triple[2]
        actual = labels.get(name, "") or ""
        if op == "=" and actual != val:
            return False
        if op == "!=" and actual == val:
            return False
        if op == "=~" and not re.search(val, actual):
            return False
        if op == "!~" and re.search(val, actual):
            return False
    return True


def _resolve_route_receiver(policy_root, labels):
    """Walk the notification-policy tree top-down, first-match-wins, to
    determine which receiver the rule's labels would route to. Returns the
    receiver name (lowercased + stripped) or "" if the policy is empty."""
    if not policy_root:
        return ""
    # Walk children depth-first, first match wins. If no child matches,
    # the current node's receiver is effective.
    current = policy_root
    while True:
        children = current.get("routes") or []
        next_match = None
        for child in children:
            if _route_matches_labels(child, labels):
                next_match = child
                break
        if next_match is None:
            break
        current = next_match
        # Continue if this matched route allows further descent.
        if not (current.get("routes") or []):
            break
    receiver = current.get("receiver") or ""
    return str(receiver).strip().casefold()


def _b3_route_is_pageable(rule, source):
    """Check that the matched alert rule's notification path terminates at
    a non-blackhole receiver. For Prometheus-store rules this snapshot has
    no Alertmanager wired, so the check fails open (returns True). For
    Grafana-store rules (file or runtime API), walk the Grafana
    notification-policy tree."""
    if source == "prometheus":
        return True, "alertmanager not deployed on this snapshot; route check skipped"
    policy = _read_grafana_notification_policies()
    if not policy:
        return False, ("could not read Grafana notification policies "
                       "(auth or endpoint unreachable)")
    labels = rule.get("labels") or {}
    receiver = _resolve_route_receiver(policy, labels)
    if receiver in _BLACKHOLE_RECEIVER_NAMES:
        return False, ("rule routes to blackhole-shaped receiver %r "
                       "(rule labels=%s — route silently drops notifications)"
                       % (receiver, labels))
    return True, ("rule routes to receiver %r (non-blackhole)" % receiver)


# Accepted metric families for a "Redis persistence regressed" alert.
# Two are reliably triggered by the grader's synthetic injection
# (appendonly no + writes flips aof_enabled and grows rdb_changes_since_
# last_save), two are not (status metrics only flip when redis actually
# attempts AND fails a write — the grader can't force those in Redis 7+
# because dir/dbfilename are protected config). Including the _status
# metrics anyway, so agents reaching for the canonical "snapshot failing"
# or "AOF write failing" alerts pass b1 by metric choice. They will then
# be evaluated by b2 on whether their expression actually fires under
# the failure the grader can inject — that's a real correctness signal,
# not a path restriction on metric name.
_b_metric_pattern = re.compile(
    r"redis_("
    r"aof_enabled"
    r"|rdb_changes_since_last_save"
    r"|rdb_last_bgsave_status"
    r"|aof_last_write_status"
    r")"
)


def _snapshot_persistence_config(pod):
    """Capture live persistence CONFIG values for restoration."""
    out = {}
    for key in ("save", "appendonly"):
        raw = redis_cli(pod, "CONFIG", "GET", key)
        lines = raw.splitlines()
        out[key] = lines[1].strip() if len(lines) >= 2 else ""
    return out


def _inject_persistence_failure(pod):
    """Flip the two reliably-injectable redis-exporter metrics into failure
    states. `dir` and `dbfilename` are protected in Redis 7+, so we use the
    two metrics we CAN control via CONFIG SET:
      - redis_aof_enabled  → 0  (CONFIG SET appendonly no)
      - redis_rdb_changes_since_last_save → grows (writes + save policy off)
    Caller must call _restore_persistence_config in finally."""
    redis_cli(pod, "CONFIG", "SET", "save", "")
    redis_cli(pod, "CONFIG", "SET", "appendonly", "no")
    # 200 writes with save="" guarantees redis_rdb_changes_since_last_save
    # exceeds any plausible alert threshold.
    for i in range(200):
        redis_cli(pod, "SET", "grader:inject:%d" % i, "v%d" % i, timeout=5)


def _restore_persistence_config(pod, snapshot):
    for key in ("appendonly", "save"):
        val = snapshot.get(key, "")
        if val:
            redis_cli(pod, "CONFIG", "SET", key, val)
    # Force a save so rdb_changes_since_last_save resets and the agent's
    # alert (if any) returns to inactive.
    redis_cli(pod, "BGSAVE")


def _current_alert_state(rule_name):
    """Return the alert state for this rule name right now, or 'inactive'
    if no alert instance is present (Prometheus omits inactive rules from
    /api/v1/alerts)."""
    data, _ = _prom_query("/api/v1/alerts")
    if not data:
        return "unknown"
    for a in data.get("data", {}).get("alerts", []):
        labels = a.get("labels") or {}
        if labels.get("alertname") == rule_name:
            return a.get("state", "unknown")
    return "inactive"


def _poll_alert_pending_or_firing(rule_name, timeout=60):
    """Poll /api/v1/alerts looking for an alert instance with this rule name
    in pending or firing state. Pending counts as proof-of-life; the rule's
    `for:` clause length therefore does not matter."""
    start = time.time()
    while time.time() - start < timeout:
        state = _current_alert_state(rule_name)
        if state in ("pending", "firing"):
            return True, state
        time.sleep(3)
    return False, None


# --- B/A isolation: temporary cluster control for b2's measurement window ---
#
# Without isolation, B's `b2` is implicitly coupled to A. If an agent fails A
# by leaving the sts-patching reverter (`redis-config-watchdog`) alive, the
# live cluster is stuck with `redis_aof_enabled == 0` and any agent alert on
# that metric is firing pre-injection → b2's "must be inactive" pre-state
# gate rejects it as decorative → B fails because A failed. The cells
# {fail A, pass B} and {fail A, fail B} get conflated.
#
# To make B's outcome depend only on whether the agent built a real alert,
# b2 takes temporary control of the persistence-related cluster state for
# the duration of its measurement window: suspend reverter CronJobs, scale
# the bleat-service Deployment to 0 (killing the cache-config-tuner sidecar
# pod), patch the sts back to a known-good command. Everything is restored
# in a finally block so A's prior measurement is unaffected (A always runs
# before B) and the cluster ends up in the agent's last-set state.

_REVERTER_CRONJOBS = [
    ("monitoring", "redis-config-watchdog"),
    ("monitoring", "redis-fsync-tuner"),
    ("bleater", "cache-config-syncer"),
]
_BLEAT_SERVICE_DEPLOY = "bleater-bleat-service"
_GOOD_STS_COMMAND = [
    "redis-server",
    "--save", "3600 1 300 100 60 10000",
    "--appendonly", "yes",
    "--appendfsync", "everysec",
    "--dir", "/data",
]


def _suspend_reverter_cronjobs():
    """Patch each known reverter CronJob to spec.suspend=true. Returns the
    list of (ns, name) we actually changed so caller can restore."""
    suspended = []
    for ns, name in _REVERTER_CRONJOBS:
        rc, _, _ = run(
            ["kubectl", "-n", ns, "patch", "cronjob", name, "--type=merge",
             "-p", '{"spec":{"suspend":true}}'],
            timeout=15,
        )
        if rc == 0:
            suspended.append((ns, name))
    return suspended


def _unsuspend_reverter_cronjobs(suspended):
    for ns, name in suspended:
        run(["kubectl", "-n", ns, "patch", "cronjob", name, "--type=merge",
             "-p", '{"spec":{"suspend":false}}'], timeout=15)


def _bleat_service_replicas():
    out = kubectl_jsonpath(["get", "deploy", _BLEAT_SERVICE_DEPLOY], "{.spec.replicas}")
    try:
        return int(out)
    except Exception:
        return None


def _scale_bleat_service(replicas):
    run(KUBECTL + ["scale", "deploy", _BLEAT_SERVICE_DEPLOY,
                    "--replicas=%d" % replicas], timeout=30)


def _wait_for_deploy_replicas(deploy, target, timeout=90):
    start = time.time()
    while time.time() - start < timeout:
        out = kubectl_jsonpath(["get", "deploy", deploy], "{.status.replicas}")
        try:
            if int(out or "0") == target:
                return True
        except Exception:
            pass
        time.sleep(3)
    return False


def _snapshot_sts_command():
    _, out, _ = run(
        KUBECTL + ["get", "sts", "bleater-redis", "-o",
                    "jsonpath={.spec.template.spec.containers[0].command}"],
        timeout=15,
    )
    out = (out or "").strip()
    if not out:
        return None
    try:
        return json.loads(out)
    except Exception:
        return None


def _patch_sts_command(command_args):
    if not command_args:
        return
    patch = json.dumps([{"op": "replace",
                          "path": "/spec/template/spec/containers/0/command",
                          "value": command_args}])
    run(KUBECTL + ["patch", "sts", "bleater-redis",
                    "--type=json", "-p", patch], timeout=30)


def _snapshot_sts_containers():
    """Return the full containers list from the bleater-redis sts pod template,
    or None if unreadable. Used by b2's isolation harness to capture the
    agent's cluster state (including any sidecars they failed to remove)
    so it can be restored after b2 completes."""
    _, out, _ = run(
        KUBECTL + ["get", "sts", "bleater-redis", "-o",
                    "jsonpath={.spec.template.spec.containers}"],
        timeout=15,
    )
    out = (out or "").strip()
    if not out:
        return None
    try:
        return json.loads(out)
    except Exception:
        return None


def _patch_sts_containers(containers):
    """Replace the bleater-redis sts pod-template containers list wholesale."""
    if not containers:
        return
    patch = json.dumps([{"op": "replace",
                          "path": "/spec/template/spec/containers",
                          "value": containers}])
    run(KUBECTL + ["patch", "sts", "bleater-redis",
                    "--type=json", "-p", patch], timeout=30)


def _wait_for_metric_value(metric, target, timeout=90):
    """Wait for Prometheus to observe `metric` with integer value `target`."""
    start = time.time()
    while time.time() - start < timeout:
        data, _ = _prom_query("/api/v1/query?query=" + metric)
        if data:
            for r in (data.get("data", {}).get("result", []) or []):
                try:
                    if int(float(r["value"][1])) == target:
                        return True
                except Exception:
                    pass
        time.sleep(3)
    return False


def _isolate_cluster_for_b2():
    """Drive the persistence layer to a known-good state for b2's window.
    Returns a restoration-state dict; pass it to _restore_cluster_after_b2
    inside a finally block.

    Snapshot the full containers list AND strip to redis-only for the
    measurement window so an agent-left sidecar (e.g. redis-metrics-exporter
    planted by setup.sh on the bleater-redis sts itself) cannot re-assert
    CONFIG SET appendonly no from inside the pod. The agent's last-set
    containers list is restored in _restore_cluster_after_b2 so a1's
    measurement (which has already completed by the time we run) is
    unaffected and the post-b2 cluster matches the agent's final state."""
    state = {
        "suspended_cronjobs": _suspend_reverter_cronjobs(),
        "bleat_service_replicas": _bleat_service_replicas(),
        "sts_command": _snapshot_sts_command(),
        "sts_containers": _snapshot_sts_containers(),
    }
    # Scale bleat-service to 0 so the cache-config-tuner sidecar pod (if
    # still attached) cannot keep re-asserting CONFIG SET appendonly no
    # via redis-cli during our measurement window.
    if state["bleat_service_replicas"] and state["bleat_service_replicas"] > 0:
        _scale_bleat_service(0)
        _wait_for_deploy_replicas(_BLEAT_SERVICE_DEPLOY, 0, timeout=60)
    # Strip any sidecar containers from the sts pod template so an
    # in-pod reverter (e.g. redis-metrics-exporter) cannot fight us.
    containers = state.get("sts_containers") or []
    redis_only = [c for c in containers if c.get("name") == "redis"]
    if redis_only and len(containers) > len(redis_only):
        _patch_sts_containers(redis_only)
    # Patch sts to known-good. Retry to absorb the race window where an
    # already-in-flight reverter Job re-broke the sts after we suspended
    # its parent CronJob.
    for _ in range(3):
        _patch_sts_command(_GOOD_STS_COMMAND)
        time.sleep(8)
        cur = _snapshot_sts_command() or []
        if "--appendonly" in cur and "yes" in cur and "--dir" in cur and "/data" in cur:
            break
    # Wait for the new pod the sts-controller is rolling, then wait for
    # the metric to reach 1 and the alert evaluator to settle.
    new_pod = wait_for_redis(timeout=180)
    if not new_pod:
        return state, None
    _wait_for_metric_value("redis_aof_enabled", 1, timeout=90)
    # One rule-eval interval (~15s) past metric stabilization so the alert
    # state has had a chance to transition to inactive in /api/v1/alerts.
    time.sleep(20)
    return state, new_pod


def _restore_cluster_after_b2(state):
    if state is None:
        return
    # Restore the agent's last-set containers list BEFORE the command,
    # because _patch_sts_containers replaces containers[0] too; restoring
    # the command afterward lands cleanly on the redis container.
    if state.get("sts_containers"):
        _patch_sts_containers(state["sts_containers"])
    if state.get("sts_command"):
        _patch_sts_command(state["sts_command"])
    rep = state.get("bleat_service_replicas")
    if rep and rep > 0:
        _scale_bleat_service(rep)
    _unsuspend_reverter_cronjobs(state.get("suspended_cronjobs") or [])


def _b2_prometheus_path(rule, pod):
    """Behavioral b2 for a Prometheus-loaded rule. Verify state TRANSITIONS
    from inactive to pending/firing under injection. Returns (ok, detail).

    Audit (item 6, post-v25): the Grafana path had an empty-vector
    misclassification (None pre_val treated as eval failure). The Prometheus
    path does NOT have the same bug because the mechanism is different: we
    poll Prometheus's own /api/v1/alerts endpoint, and Prometheus omits
    inactive rules from that response. _current_alert_state returns
    "inactive" when the rule's alertname is absent, "unknown" only when
    /api/v1/alerts cannot be reached at all. Both states are interpreted
    correctly (inactive → not firing; unknown → sanity-fail). No empty-
    result-of-PromQL-expression path runs here, so there is no analogous
    misclassification to fix."""
    rule_name = rule.get("name", "")
    pre_state = _current_alert_state(rule_name)
    if pre_state == "unknown":
        return False, ("could not determine pre-injection alert state "
                       "(prometheus /api/v1/alerts unavailable)")
    if pre_state in ("pending", "firing"):
        return False, ("alert %s was already %s in a known-good cluster "
                       "(decorative / always-fires rule)" %
                       (rule_name, pre_state))
    snapshot = _snapshot_persistence_config(pod)
    try:
        _inject_persistence_failure(pod)
        fired, state = _poll_alert_pending_or_firing(rule_name, timeout=60)
        if fired:
            return True, ("alert %s transitioned %s -> %s under injection" %
                          (rule_name, pre_state, state))
        return False, ("alert %s did not transition out of %s within 60s "
                       "of synthetic failure injection" % (rule_name, pre_state))
    finally:
        _restore_persistence_config(pod, snapshot)


def _b2_grafana_path(rule, pod):
    """Behavioral b2 for a Grafana-provisioned rule. Grafana's alert state
    machinery requires authenticated access to the Grafana HTTP API and is
    awkward to poll from inside the grader. Instead, evaluate the rule's
    primary prometheus expression in both pre- and post-injection cluster
    states and apply the rule's threshold condition manually. This catches
    decoration (rule fires in a known-good cluster) and stuck-off rules
    (rule does not fire when persistence is broken) the same way the
    Prometheus path does."""
    rule_title = rule.get("title") or rule.get("uid") or "?"
    expr, _ = _grafana_rule_matching_expr(rule)
    if not expr:
        return False, ("rule %s has no prometheus-datasource expression "
                       "matching the metric whitelist" % rule_title)
    op, threshold_val = _grafana_rule_threshold(rule)
    if op is None:
        return False, ("rule %s has no parseable threshold step "
                       "(condition refId or evaluator missing)" % rule_title)

    # Pre-injection: known-good cluster. Rule must NOT fire.
    # PromQL semantics: a vector comparison like `redis_aof_enabled == 0`
    # returns an empty vector when no series matches the filter, which
    # _prom_instant_value reports as None. The correct interpretation is
    # "filter matched no series → rule is not firing" — NOT "could not
    # evaluate." v25 run2 was a false-negative under the old logic
    # (agent's expr was `redis_aof_enabled{...} == 0` and the empty
    # pre-injection result was misclassified as eval failure).
    pre_val = _prom_instant_value(expr)
    pre_fires = (False if pre_val is None
                 else bool(_evaluate_threshold(op, threshold_val, pre_val)))
    if pre_fires:
        return False, ("rule %s would fire in a known-good cluster "
                       "(pre-injection value=%s threshold=%s %s) — "
                       "decorative / always-fires" %
                       (rule_title, pre_val, op, threshold_val))

    # Inject failure and wait for the metric to actually flip in Prometheus
    # (one scrape interval, typically 15-30s).
    snapshot = _snapshot_persistence_config(pod)
    try:
        _inject_persistence_failure(pod)
        _wait_for_metric_value("redis_aof_enabled", 0, timeout=60)
        # One extra second of polling so the latest scrape lands and the
        # instant query reflects the post-injection value.
        time.sleep(2)
        post_val = _prom_instant_value(expr)
        post_fires = (False if post_val is None
                      else bool(_evaluate_threshold(op, threshold_val, post_val)))
        if post_fires:
            return True, ("rule %s transitioned (pre=%s post=%s threshold=%s %s)"
                          % (rule_title, pre_val, post_val, op, threshold_val))
        return False, ("rule %s did not fire under injection "
                       "(pre=%s post=%s threshold=%s %s) — "
                       "expression does not respond to the failure" %
                       (rule_title, pre_val, post_val, op, threshold_val))
    finally:
        _restore_persistence_config(pod, snapshot)


def subscore_b_alert_observability():
    """AND-gate of 3 atoms, isolated from A so subscore independence holds.
    Path-store-agnostic: accepts rules from Prometheus rule_files, Grafana
    file-provisioning ConfigMap, or Grafana runtime API.
      b1 alert_rule_loaded                      — three-store rule discovery.
      b2 alert_fires_on_synthetic_failure       — behavioral; verifies the
                                                  discovered rule transitions
                                                  under injection inside the
                                                  cluster isolation harness.
      b3 alert_routes_to_pageable_receiver      — walks Grafana notification
                                                  policies to confirm the
                                                  rule's labels resolve to a
                                                  non-blackhole receiver.
                                                  Prometheus-store rules fail
                                                  open (no Alertmanager).
    b2/b3 cascade to "no rule to test" when b1 fails.

    v43 added b4 (for-duration) and b5 (severity-label) as quality gates,
    but historical config-presence atoms in this task have all saturated
    to 5/5 within 1-2 versions. Dropped to keep B's variance signal on
    its behavioral atom (b2) rather than diluting with structural checks.
    """
    rule, metric, source, err = _find_matching_alert_rule()
    b1_ok = rule is not None
    if b1_ok:
        if source == "prometheus":
            rule_label = rule.get("name", "?")
        else:
            rule_label = rule.get("title") or rule.get("uid") or "?"
        b1_detail = ("matched %s [%s] on metric %s" %
                     (rule_label, source, metric or "?"))
    else:
        b1_detail = err or "no rule"

    # b3 is config-only — cheap, no cluster mutation, runs outside the
    # isolation harness. Cascades when b1 fails.
    if not b1_ok:
        b3_ok, b3_detail = False, "no rule to test (b1 failed)"
    else:
        b3_ok, b3_detail = _b3_route_is_pageable(rule, source)

    isolation_state = None
    try:
        isolation_state, pod = _isolate_cluster_for_b2()

        if not b1_ok:
            b2_ok = False
            b2_detail = "no rule to test (b1 failed)"
        elif not pod:
            b2_ok = False
            b2_detail = ("could not bring redis to known-good state for b2 "
                         "(isolation setup failed)")
        elif source == "prometheus":
            b2_ok, b2_detail = _b2_prometheus_path(rule, pod)
        else:
            b2_ok, b2_detail = _b2_grafana_path(rule, pod)
    finally:
        _restore_cluster_after_b2(isolation_state)

    return [int(b1_ok), int(b2_ok), int(b3_ok)], [
        ("alert_rule_loaded", b1_ok, b1_detail),
        ("alert_fires_on_synthetic_failure", b2_ok, b2_detail),
        ("alert_routes_to_pageable_receiver", b3_ok, b3_detail),
    ]


def grade(transcript=None):
    weights = {
        "persistence_durability": 0.5,
        "alert_observability": 0.5,
    }

    a_items, a_details = subscore_a_persistence_durability()
    b_items, b_details = subscore_b_alert_observability()

    a_pass = all(x == 1 for x in a_items)
    b_pass = all(x == 1 for x in b_items)

    subscores = {
        "persistence_durability": 1.0 if a_pass else 0.0,
        "alert_observability": 1.0 if b_pass else 0.0,
    }
    total = sum(subscores[k] * weights[k] for k in subscores)

    feedback_lines = []
    feedback_lines.append(
        ("+" if a_pass else "x") + " persistence_durability:"
    )
    for name, ok, msg in a_details:
        feedback_lines.append("    " + ("+" if ok else "x") + " " + name + ": " + str(msg))
    feedback_lines.append(
        ("+" if b_pass else "x") + " alert_observability:"
    )
    for name, ok, msg in b_details:
        feedback_lines.append("    " + ("+" if ok else "x") + " " + name + ": " + str(msg))
    feedback = "\n".join(feedback_lines)

    log("GRADER_FEEDBACK_BEGIN")
    for line in feedback.splitlines():
        log("FEEDBACK: " + line)
    log("GRADER_FEEDBACK_END")

    try:
        from apex_arena._types import GradingResult  # type: ignore
    except Exception:
        try:
            from horizon.grader import GradingResult  # type: ignore
        except Exception:
            GradingResult = None
    if GradingResult is not None:
        return GradingResult(
            score=total,
            subscores=subscores,
            weights=weights,
            feedback=feedback,
        )
    return {
        "score": total,
        "subscores": subscores,
        "weights": weights,
        "feedback": feedback,
    }


if __name__ == "__main__":
    print(grade())
