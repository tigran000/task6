"""Grader for redis-persistence-drift.

Two equal-weight, binary, orthogonal subscores. Each is an AND-gate of two
related but independent checks. Within-subscore checks share a theme;
between-subscore checks are independent code paths.

  A persistence_durability  (weight 0.5)
      a1 baseline_survives_restart
         STRING key with manual BGSAVE+BGREWRITEAOF, force-delete the pod,
         and GET it back. Also asserts /data is on a persistentVolumeClaim
         (so emptyDir-with-AOF-only does not pass). The grader's force-
         delete causes the pod to come up with whatever command-args the
         live sts currently has — so if an sts-patching reverter (e.g.,
         redis-config-watchdog) is still alive and has flipped the sts
         to `--appendonly no` and `--save ""`, the new pod loads from
         RDB but writes after the last BGSAVE are lost on the next cycle.
      a3 no_reverter_sidecar_in_bleat_service
         The bleater-bleat-service Deployment must contain no container
         whose command/args look like a redis-config reverter — anything
         calling `CONFIG SET appendonly no`, `CONFIG SET save ""`, or
         `CONFIG SET appendfsync no` in a loop. Behavior-based, so an
         agent who renames the cache-config-tuner sidecar to something
         benign still fails this check.

  B alert_observability     (weight 0.5)
      b1 alert_rule_loaded
         Path-store-agnostic across THREE stores. Accepts any of:
           (i) Prometheus /api/v1/rules — file-based rule_files config.
           (ii) Grafana file-provisioning ConfigMap
                `monitoring/grafana-alerting-provisioning` (alert-rules.yaml).
           (iii) Grafana runtime API
                 `/api/v1/provisioning/alert-rules` — picks up rules created
                 via POST with X-Disable-Provenance: true that never land in
                 (ii). Default cluster auth is admin/admin.
         Any rule whose primary prometheus expression references the
         redis-exporter persistence metric whitelist passes. Tests the
         realistic capability ("wire an alert visible to the platform's
         monitoring stack"), not "guess which store the grader prefers."
      b2 alert_fires_on_synthetic_failure  (BEHAVIORAL)
         Same isolation as v23: suspend reverters, scale bleat-service
         to 0, patch sts to known-good, wait for redis_aof_enabled=1.
         Then dispatch by rule source:
           Prometheus rule  → inject failure, poll /api/v1/alerts up
                              to 60s for pending|firing. Pre-state
                              must be inactive (catches decoration).
           Grafana rule     → query Prometheus instant API for the
                              rule's primary model.expr in both pre-
                              and post-injection states, apply the
                              rule's threshold condition. Pre-state
                              must NOT trigger (catches decoration);
                              post-state MUST trigger.
         State restored in finally regardless of source.

A and B are fully independent. A is measured against the cluster state
the agent left behind. Before B starts measuring, the grader takes
temporary control of the persistence layer for the duration of b2's
measurement window — suspends the reverter CronJobs, scales the
bleat-service Deployment to 0 (so the cache-config-tuner sidecar pod
cannot interfere), and patches the sts back to a known-good command —
then restores the agent's last-set state in a finally block. This means
the outcome of B depends only on whether the agent built a working
alert, not on whether they also killed all of A's reverters. All four
cells of the joint distribution are reachable AND equally likely given
the agent's actual work.
"""

import json
import re
import subprocess
import time
import urllib.parse
import uuid

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


def _poll_bgsave_done(pod, timeout=30):
    last_save_before = redis_cli(pod, "LASTSAVE")
    redis_cli(pod, "BGSAVE")
    start = time.time()
    while time.time() - start < timeout:
        cur = redis_cli(pod, "LASTSAVE")
        if cur and cur != last_save_before:
            return
        info = redis_cli(pod, "INFO", "persistence")
        if "rdb_bgsave_in_progress:0" in info and "loading:0" in info:
            return
        time.sleep(2)


def _poll_aof_rewrite_done(pod, timeout=15):
    redis_cli(pod, "BGREWRITEAOF")
    start = time.time()
    while time.time() - start < timeout:
        info = redis_cli(pod, "INFO", "persistence")
        if "aof_rewrite_in_progress:0" in info:
            return
        time.sleep(1)


def _data_mount_is_pvc(pod_name):
    """Return True if the live pod has /data backed by a persistentVolumeClaim
    (not emptyDir). Used to gate a1 so that probe-survival via manual flush
    alone is not enough — durable storage must also be wired."""
    _, out, _ = run(KUBECTL + ["get", "pod", pod_name, "-o", "json"])
    try:
        spec = json.loads(out).get("spec", {})
    except Exception:
        return False
    volumes = {v["name"]: v for v in spec.get("volumes", [])}
    for c in spec.get("containers", []):
        for m in c.get("volumeMounts", []):
            if m.get("mountPath") == "/data":
                vol = volumes.get(m.get("name"), {})
                return "persistentVolumeClaim" in vol
    return False


# Deployments where setup.sh plants in-app reverter sidecars. a3 audits
# every entry — agents must clean ALL of them, not just the obvious
# bleat-service one. v27 added bleater-timeline-service after v25 batch
# showed agents reliably finding the single bleat-service sidecar
# (saturated a3 at 5/5).
_REVERTER_SIDECAR_DEPLOYMENTS = [
    "bleater-bleat-service",
    "bleater-timeline-service",
]


def _deploy_containers(deploy_name):
    """Return the list of container dicts in the named Deployment's spec,
    or None if the Deployment is unreadable / absent."""
    _, out, _ = run(
        KUBECTL + ["get", "deploy", deploy_name, "-o", "json"],
        timeout=15,
    )
    out = (out or "").strip()
    if not out:
        return None
    try:
        d = json.loads(out)
        return (d.get("spec", {})
                 .get("template", {})
                 .get("spec", {})
                 .get("containers", []))
    except Exception:
        return None


def _is_reverter_shaped(container):
    """Behavior-based detection of a redis-config reverter container.
    Catches the cache-config-tuner sidecar AND any rename of it: anything
    whose command/args looks like a redis-cli loop that flips persistence
    config (`CONFIG SET appendonly no` or `CONFIG SET save ""`)."""
    parts = (container.get("command") or []) + (container.get("args") or [])
    joined = " ".join(parts).lower()
    if "config set" not in joined:
        return False
    if "appendonly" in joined and " no" in joined:
        return True
    if "appendfsync" in joined and " no" in joined:
        return True
    # `CONFIG SET save ""` — the empty string survives the .lower() pipeline
    # as just two quotes; look for the disabling pattern directly.
    if " save " in joined and ('""' in joined or "''" in joined):
        return True
    return False


def subscore_a_persistence_durability():
    """AND-gate of 2 independent checks.
    a1 baseline_survives_restart           — write+BGSAVE+BGREWRITEAOF, force-
                                              delete the pod, GET back. Also
                                              asserts /data is on a PVC. The
                                              new pod boots from the LIVE sts
                                              spec, so if the sts-patching
                                              reverter (`redis-config-watchdog`)
                                              is still alive it will have
                                              flipped the command to
                                              `--save "" --appendonly no
                                              --dir /tmp`. With `--dir /tmp`
                                              the new pod can no longer find
                                              the RDB file we BGSAVEd onto
                                              the `/data` PVC, so the round-
                                              trip fails. Agents must kill
                                              the watchdog AND its RBAC for
                                              this check to pass.
    a3 no_reverter_sidecar_in_bleat_service —
                                              Independent topology check.
                                              The bleater-bleat-service
                                              Deployment must not contain
                                              any container whose command/
                                              args call `CONFIG SET appendonly
                                              no` / `save ""` / `appendfsync
                                              no` in a loop (behavior-based,
                                              so renames don't bypass).
                                              Catches agents who repaired
                                              persistence but missed the
                                              camouflaged 5s sidecar — the
                                              lowest-visibility reverter,
                                              hidden inside another app's
                                              container list.
    """
    pod = wait_for_redis(timeout=120)
    if not pod:
        return [0, 0], [
            ("baseline_survives_restart", False, "no redis pre-probe"),
            ("no_reverter_sidecar_in_bleat_service", False,
             "redis unavailable; cannot validate cluster state"),
        ]

    # a1: probe key, then force a manual flush via BGSAVE+BGREWRITEAOF, then
    # force-delete the pod. On restart the pod uses whatever command-args
    # the LIVE sts has at that moment — that is the failure mode the
    # sts-patching reverter (redis-config-watchdog) exploits.
    a1_key = "grader:base:" + uuid.uuid4().hex
    a1_val = uuid.uuid4().hex
    redis_cli(pod, "SET", a1_key, a1_val)
    _poll_bgsave_done(pod, timeout=30)
    _poll_aof_rewrite_done(pod, timeout=15)

    run(KUBECTL + ["delete", "pod", pod, "--force", "--grace-period=0"], timeout=60)

    new_pod = wait_for_redis(timeout=180)
    if not new_pod:
        a1_ok = False
        a1_detail = "redis did not recover after force-delete"
    else:
        a1_got = redis_cli(new_pod, "GET", a1_key)
        a1_value_ok = a1_got == a1_val
        a1_pvc_ok = _data_mount_is_pvc(new_pod)
        a1_ok = a1_value_ok and a1_pvc_ok
        a1_detail = ("round-trip + /data on PVC" if a1_ok else
                     "value_ok=%s pvc_mount=%s got=%r" % (a1_value_ok, a1_pvc_ok, a1_got))

    # a3: behavior-based topology check on every deployment that setup.sh
    # plants a reverter sidecar into. Reads each Deployment's spec,
    # classifies each container by what its command/args actually DO (not
    # by name), and fails if ANY deployment still has a reverter-shaped
    # container. Closes both the rename-bypass (behavior-based detection)
    # AND the "only audit the obvious deployment" path (multi-deployment).
    per_deploy = []
    overall_bad = []
    unreadable = []
    for deploy in _REVERTER_SIDECAR_DEPLOYMENTS:
        containers = _deploy_containers(deploy)
        if containers is None:
            unreadable.append(deploy)
            continue
        reverter_names = [c.get("name", "?") for c in containers
                          if _is_reverter_shaped(c)]
        all_names = [c.get("name", "?") for c in containers]
        per_deploy.append((deploy, all_names, reverter_names))
        if reverter_names:
            overall_bad.append((deploy, reverter_names))
    if unreadable:
        a3_ok = False
        a3_detail = ("could not read deployment(s): %s" % unreadable)
    elif overall_bad:
        a3_ok = False
        a3_detail = ("reverter-shaped sidecar(s) still attached: %s" %
                     [(d, names) for d, names in overall_bad])
    else:
        a3_ok = True
        a3_detail = ("no reverter-shaped sidecar in any audited deployment "
                     "(%s)" % [(d, names) for d, names, _ in per_deploy])

    return [int(a1_ok), int(a3_ok)], [
        ("baseline_survives_restart", a1_ok, a1_detail),
        ("no_reverter_sidecar_in_bleat_service", a3_ok, a3_detail),
    ]


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
    {gt, lt, gte, lte, eq, within_range, outside_range}; value is float.
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
                return op, float(params[0])
            except (TypeError, ValueError):
                return None, None
    return None, None


def _find_matching_grafana_rule():
    """Scan the Grafana provisioning CM for an alert rule with ANY
    prometheus-datasource expression that references a redis-exporter
    persistence metric (every refId scanned, not just the first), AND
    whose threshold can distinguish a known-good metric from a known-bad
    one (rejects decorative `redis_aof_enabled > 9999999`-style rules
    at discovery time). Returns (rule_dict, metric, err).

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
        op, threshold_val = _grafana_rule_threshold(r)
        if not _is_plausibly_calibrated_threshold(metric, op, threshold_val):
            # Skip decorative rules — keep scanning for a real one.
            continue
        return r, metric, None
    return None, None, ("no Grafana rule references a redis-exporter "
                        "persistence metric with a plausibly-calibrated threshold")


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
    metric AND whose threshold is plausibly calibrated. Same parser as the
    file-provisioning path — runtime rule shape is identical to YAML
    provisioning rule shape.
    Returns (rule_dict, metric, err)."""
    rules = _read_grafana_api_rules()
    if not rules:
        return None, None, "no rules in grafana runtime API store"
    for r in rules:
        expr, metric = _grafana_rule_matching_expr(r)
        if metric is None:
            continue
        op, threshold_val = _grafana_rule_threshold(r)
        if not _is_plausibly_calibrated_threshold(metric, op, threshold_val):
            continue
        return r, metric, None
    return None, None, ("no Grafana API rule references a redis-exporter "
                        "persistence metric with a plausibly-calibrated threshold")


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


# Known-good vs known-bad metric values per redis-exporter persistence
# metric, used by `_is_plausibly_calibrated_threshold` to reject Grafana
# rules whose threshold cannot distinguish a healthy cluster from a broken
# one (e.g., `redis_aof_enabled > 9999999` always-off, or `< 9999999`
# always-on). The reviewer's polish item 5 — kills the silliest gaming
# at discovery time rather than relying solely on b2's pre-state gate.
_METRIC_GOOD_BAD_TEST = {
    "redis_aof_enabled": (1.0, 0.0),
    "redis_rdb_changes_since_last_save": (0.0, 1000.0),
    "redis_rdb_last_bgsave_status": (1.0, 0.0),
    "redis_aof_last_write_status": (1.0, 0.0),
}


def _is_plausibly_calibrated_threshold(metric, op, threshold_value):
    """Return True iff the rule's threshold distinguishes a known-good
    metric value (rule should NOT fire) from a known-bad one (rule SHOULD
    fire). Returns True for metrics we don't have test values for (don't
    block on unknown shapes). Returns True when threshold_value is None
    (can't evaluate, defer to b2)."""
    if threshold_value is None or op is None:
        return True
    test = _METRIC_GOOD_BAD_TEST.get(metric)
    if test is None:
        return True
    good_val, bad_val = test
    good_fires = _evaluate_threshold(op, threshold_value, good_val)
    bad_fires = _evaluate_threshold(op, threshold_value, bad_val)
    return (good_fires is False) and (bad_fires is True)


def _evaluate_threshold(op, threshold_value, observed_value):
    """Apply a Grafana threshold evaluator to an observed metric value."""
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
    inside a finally block."""
    state = {
        "suspended_cronjobs": _suspend_reverter_cronjobs(),
        "bleat_service_replicas": _bleat_service_replicas(),
        "sts_command": _snapshot_sts_command(),
    }
    # Scale bleat-service to 0 so the cache-config-tuner sidecar pod (if
    # still attached) cannot keep re-asserting CONFIG SET appendonly no
    # via redis-cli during our measurement window.
    if state["bleat_service_replicas"] and state["bleat_service_replicas"] > 0:
        _scale_bleat_service(0)
        _wait_for_deploy_replicas(_BLEAT_SERVICE_DEPLOY, 0, timeout=60)
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
    """AND-gate of 2 checks, isolated from A so subscore independence holds.
    Path-store-agnostic: accepts rules from Prometheus rule_files or from
    Grafana UnifiedAlerting provisioning.
      b1 alert_rule_loaded                  — rule exists with accepted metric
                                              in ANY of three stores: Prometheus
                                              /api/v1/rules, Grafana file-
                                              provisioning ConfigMap, or
                                              Grafana runtime API
                                              (/api/v1/provisioning/alert-rules).
      b2 alert_fires_on_synthetic_failure   — behavioral; dispatches on rule
                                              source (Prometheus polls alerts
                                              API; Grafana file OR API uses
                                              the same logical-eval path
                                              since the rule shapes are
                                              identical).
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

    isolation_state = None
    try:
        isolation_state, pod = _isolate_cluster_for_b2()

        if not rule:
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

    return [int(b1_ok), int(b2_ok)], [
        ("alert_rule_loaded", b1_ok, b1_detail),
        ("alert_fires_on_synthetic_failure", b2_ok, b2_detail),
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
