"""Grader for redis-persistence-drift.

Two equal-weight, binary, orthogonal subscores. Each is an AND-gate of two
related but independent checks. Within-subscore checks share a theme;
between-subscore checks are independent code paths.

  A persistence_durability  (weight 0.5)
      a1 baseline_survives_restart
         STRING key with manual BGSAVE+BGREWRITEAOF, kill pod, GET.
         Passes if PVC is mounted and either persistence mechanism works.
      a2 unflushed_probe_survives
         SET key with NO manual flush. Poll INFO persistence for
         aof_pending_bio_fsync:0 or rdb_changes_since_last_save:0 so the
         agent's own save policy / appendfsync is what makes the write
         durable. Kill pod, GET. Fails if the agent's live config does
         not actually persist writes between save events.

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

A and B do not share state: an agent can pass A while failing B (fixes
redis, skips monitoring) or pass B while failing A (writes alert, does
not fix redis). All four cells of the joint distribution are reachable.
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


def subscore_a_persistence_durability():
    """AND-gate of 2 probes, both within ONE pod restart cycle.
    a1 baseline_survives_restart    — manual BGSAVE+BGREWRITEAOF, kill, GET.
                                       ALSO asserts /data is on a PVC, so
                                       emptyDir-with-AOF-only does not pass.
    a2 unflushed_probe_survives     — write probe, lapse a 2-second timing
                                       window (no manual flush, no polling
                                       for redis-internal state), kill, GET.
                                       With a working appendfsync setting,
                                       the OS flushes within the window;
                                       with appendfsync=no the write is
                                       still in the OS page-cache and is
                                       lost on force-delete.
    """
    pod = wait_for_redis(timeout=120)
    if not pod:
        return [0, 0], [
            ("baseline_survives_restart", False, "no redis pre-probe"),
            ("unflushed_probe_survives", False, "no redis pre-probe"),
        ]

    # a1: probe key, then force a manual flush via BGSAVE+BGREWRITEAOF.
    a1_key = "grader:base:" + uuid.uuid4().hex
    a1_val = uuid.uuid4().hex
    redis_cli(pod, "SET", a1_key, a1_val)
    _poll_bgsave_done(pod, timeout=30)
    _poll_aof_rewrite_done(pod, timeout=15)

    # a2: probe key written AFTER the manual flush. We do NOT call BGSAVE or
    # BGREWRITEAOF — survival depends on the agent's live appendfsync setting
    # getting this write through the OS within the 2-second window.
    a2_key = "grader:noflush:" + uuid.uuid4().hex
    a2_val = uuid.uuid4().hex
    redis_cli(pod, "SET", a2_key, a2_val)
    # 2-second timing window — polling-style loop, not a substitute for
    # waiting on an event. The window is the test: appendfsync=everysec
    # flushes within ~1s, appendfsync=always flushes immediately, and
    # appendfsync=no leaves the write in OS page-cache (default Linux
    # writeback interval is 30s, well beyond this window).
    a2_deadline = time.time() + 2.0
    while time.time() < a2_deadline:
        time.sleep(0.5)

    # Single force-delete exercises both probes at once.
    run(KUBECTL + ["delete", "pod", pod, "--force", "--grace-period=0"], timeout=60)

    new_pod = wait_for_redis(timeout=180)
    if not new_pod:
        return [0, 0], [
            ("baseline_survives_restart", False, "redis did not recover"),
            ("unflushed_probe_survives", False, "redis did not recover"),
        ]

    a1_got = redis_cli(new_pod, "GET", a1_key)
    a2_got = redis_cli(new_pod, "GET", a2_key)

    a1_value_ok = a1_got == a1_val
    a1_pvc_ok = _data_mount_is_pvc(new_pod)
    a1_ok = a1_value_ok and a1_pvc_ok
    a2_ok = a2_got == a2_val

    a1_detail = ("round-trip + /data on PVC" if a1_ok else
                 "value_ok=%s pvc_mount=%s got=%r" % (a1_value_ok, a1_pvc_ok, a1_got))
    a2_detail = ("survived 2s window" if a2_ok else
                 "value lost (got=%r)" % a2_got)
    return [int(a1_ok), int(a2_ok)], [
        ("baseline_survives_restart", a1_ok, a1_detail),
        ("unflushed_probe_survives", a2_ok, a2_detail),
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


def subscore_b_alert_observability():
    """AND-gate of 2 checks:
      b1 alert_rule_loaded                  — rule exists with accepted metric.
      b2 alert_fires_on_synthetic_failure   — behavioral; inject + poll alerts.
    """
    rule, metric, err = _find_matching_alert_rule()
    b1_ok = rule is not None
    b1_detail = ("matched " + (rule.get("name", "?") if rule else "?")
                 + " on metric " + (metric or "?")) if b1_ok else (err or "no rule")

    # b2: BEHAVIORAL — verify the alert TRANSITIONS from inactive to
    # pending/firing under synthetic injection. Two-sided check:
    #   1. Pre-injection: alert must be inactive. Always-firing decorative
    #      alerts (e.g., `expr: redis_aof_enabled < 999`) are already
    #      firing when redis is healthy → fail this gate.
    #   2. Post-injection: alert must reach pending/firing within 60s.
    # An alert that's stuck on or stuck off fails one side. Only an alert
    # whose expression genuinely flips with the metric passes both.
    if not rule:
        b2_ok = False
        b2_detail = "no rule to test (b1 failed)"
    else:
        pod = redis_pod()
        if not pod:
            b2_ok = False
            b2_detail = "no redis pod to inject failure into"
        else:
            rule_name = rule.get("name", "")
            pre_state = _current_alert_state(rule_name)
            if pre_state == "unknown":
                # Sanity-fail rather than vacuously pass. An unknown
                # pre-state means we couldn't read /api/v1/alerts cleanly,
                # so the transition gate would be meaningless.
                b2_ok = False
                b2_detail = ("could not determine pre-injection alert state "
                             "(prometheus /api/v1/alerts unavailable)")
            elif pre_state in ("pending", "firing"):
                b2_ok = False
                b2_detail = ("alert %s was already %s before injection "
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
