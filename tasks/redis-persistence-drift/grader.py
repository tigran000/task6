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
      a3 no_unexpected_sidecar_in_bleat_service
         The bleater-bleat-service Deployment must contain ONLY its
         legitimate containers — specifically, no `cache-config-tuner`
         sidecar (the aggressive 5s in-memory CONFIG SET loop installed
         by setup). Without this check, agents who repaired the sts but
         missed the sidecar would still appear to pass A, because the
         sidecar's effect doesn't survive a pod restart (in-memory only).

  B alert_observability     (weight 0.5)
      b1 alert_rule_loaded
         Prometheus /api/v1/rules has an alerting rule (type=alerting,
         health=ok) whose expr references a redis-exporter persistence
         metric in the accepted set.
      b2 alert_fires_on_synthetic_failure  (BEHAVIORAL)
         Inject failure (flip appendonly to no, disable save policy,
         write 200 keys), poll /api/v1/alerts up to 60s expecting the
         agent's alert to reach pending or firing. Pending counts; the
         for: clause length does not matter. State restored in finally.
         A decorative `expr: redis_aof_enabled == 1` would not fire,
         because the injection flips the metric to 0.

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
import uuid

NS = "bleater"
PROM_NS = "monitoring"
POD_LABEL = "app=bleater-redis"
POD_NAME = "bleater-redis-0"
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


_BLEAT_SERVICE_FORBIDDEN_SIDECARS = {"cache-config-tuner"}


def _bleat_service_container_names():
    """Return the list of container names currently in the bleater-bleat-service
    Deployment, or None if the Deployment is unreadable / absent."""
    _, out, _ = run(
        KUBECTL + [
            "get", "deploy", "bleater-bleat-service",
            "-o", "jsonpath={.spec.template.spec.containers[*].name}",
        ],
        timeout=15,
    )
    text = (out or "").strip()
    if not text:
        return None
    return text.split()


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
    a3 no_unexpected_sidecar_in_bleat_service —
                                              Independent topology check.
                                              The bleater-bleat-service
                                              Deployment must not contain a
                                              `cache-config-tuner` sidecar
                                              (the camouflaged 5s reverter).
                                              Catches agents who repaired
                                              persistence but missed the
                                              sidecar — the lowest-visibility
                                              reverter, hidden inside another
                                              app's container list.
    """
    pod = wait_for_redis(timeout=120)
    if not pod:
        return [0, 0], [
            ("baseline_survives_restart", False, "no redis pre-probe"),
            ("no_unexpected_sidecar_in_bleat_service", False,
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

    # a3: topology check on bleater-bleat-service. Independent code path
    # from a1 — does not depend on Redis at all, only on the Deployment
    # spec. Reads container names; fails if any forbidden sidecar is still
    # attached.
    names = _bleat_service_container_names()
    if names is None:
        a3_ok = False
        a3_detail = ("could not read bleater-bleat-service deployment "
                     "(missing or inaccessible)")
    else:
        bad = [n for n in names if n in _BLEAT_SERVICE_FORBIDDEN_SIDECARS]
        a3_ok = len(bad) == 0
        a3_detail = ("no forbidden sidecar attached (containers=%s)" % names
                     if a3_ok else
                     "forbidden sidecar(s) still attached: %s (containers=%s)"
                     % (bad, names))

    return [int(a1_ok), int(a3_ok)], [
        ("baseline_survives_restart", a1_ok, a1_detail),
        ("no_unexpected_sidecar_in_bleat_service", a3_ok, a3_detail),
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


def _find_matching_alert_rule():
    """Scan /api/v1/rules for an alerting rule whose expr references a
    redis-exporter persistence metric. Returns (rule_dict, matched_metric,
    err_msg)."""
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
    return None, None, "no alerting rule references a redis-exporter persistence metric"


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


def subscore_b_alert_observability():
    """AND-gate of 2 checks, isolated from A so subscore independence holds:
      b1 alert_rule_loaded                  — rule exists with accepted metric.
      b2 alert_fires_on_synthetic_failure   — behavioral; inject + poll alerts
                                              in a controlled cluster state
                                              (reverters suspended, sidecar
                                              quiesced, sts patched to known
                                              good). Restored in finally.
    """
    rule, metric, err = _find_matching_alert_rule()
    b1_ok = rule is not None
    b1_detail = ("matched " + (rule.get("name", "?") if rule else "?")
                 + " on metric " + (metric or "?")) if b1_ok else (err or "no rule")

    isolation_state = None
    try:
        isolation_state, pod = _isolate_cluster_for_b2()

        # b2: BEHAVIORAL — verify the alert TRANSITIONS from inactive to
        # pending/firing under synthetic injection, in a known-good cluster.
        # Pre-state gate catches always-firing decorative alerts; post-
        # injection poll catches stuck-off alerts. Independent of A because
        # the isolation reset the cluster persistence state regardless of
        # how A's reverters were left.
        if not rule:
            b2_ok = False
            b2_detail = "no rule to test (b1 failed)"
        elif not pod:
            b2_ok = False
            b2_detail = ("could not bring redis to known-good state for b2 "
                         "(isolation setup failed)")
        else:
            rule_name = rule.get("name", "")
            pre_state = _current_alert_state(rule_name)
            if pre_state == "unknown":
                b2_ok = False
                b2_detail = ("could not determine pre-injection alert state "
                             "(prometheus /api/v1/alerts unavailable)")
            elif pre_state in ("pending", "firing"):
                b2_ok = False
                b2_detail = ("alert %s was already %s in a known-good cluster "
                             "(decorative / always-fires rule)" %
                             (rule_name, pre_state))
            else:
                snapshot = _snapshot_persistence_config(pod)
                try:
                    _inject_persistence_failure(pod)
                    fired, state = _poll_alert_pending_or_firing(rule_name, timeout=60)
                    b2_ok = fired
                    b2_detail = (
                        ("alert %s transitioned %s -> %s under injection" %
                         (rule_name, pre_state, state))
                        if fired else
                        "alert %s did not transition out of %s within 60s "
                        "of synthetic failure injection" % (rule_name, pre_state)
                    )
                finally:
                    _restore_persistence_config(pod, snapshot)
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
