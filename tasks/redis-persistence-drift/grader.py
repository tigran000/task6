"""Grader for redis-persistence-drift.

Two equal-weight, binary, orthogonal subscores. Each is an AND-gate of 3
related-but-non-redundant checks (Master Guide §3 + AGENT_DIFFICULTY_BANK_v2
Pattern 6 "AND-Gate with Mixed Difficulty"). Within-subscore checks are
related by theme; between-subscore checks are independent code paths.

  A persistence_durability  (FUNCTIONAL AND-gate, weight 0.5)
      a1 string_survives_restart   — STRING key, BGSAVE+BGREWRITEAOF, kill,
                                     GET. Baseline behavioral probe; passes
                                     if any persistence + PVC is wired up.
      a2 hash_survives_restart     — HSET hash field in the SAME restart
                                     cycle; HGET on the new pod. Tests data
                                     type breadth.
      a3 unflushed_probe_survives  — SET key with NO manual BGSAVE /
                                     BGREWRITEAOF. Poll for aof_pending_bio_
                                     fsync:0 or rdb_changes_since_last_save:0
                                     (i.e., the agent's own save policy /
                                     appendfsync caused durability). Kill,
                                     GET. Distinguishes agents who configured
                                     real persistence policy from agents who
                                     would only pass under manual flushes.

  B alert_observability     (NON-FUNCTIONAL AND-gate, weight 0.5)
      b1 alert_rule_loaded         — Prometheus /api/v1/rules has an alerting
                                     rule (type=alerting, health=ok) whose
                                     expr references a redis-exporter
                                     persistence metric.
      b2 alert_fires_on_synthetic_failure (BEHAVIORAL) — grader injects a
                                     persistence failure (flips appendonly
                                     to no, points dir at a nonexistent
                                     path, triggers BGSAVE which fails),
                                     then polls /api/v1/alerts for ≤60s
                                     expecting the agent's alert to reach
                                     pending or firing state. Restores state
                                     in try/finally. Pending is acceptable
                                     proof-of-life per playbook §4 — means
                                     the `for:` clause length does not matter.
                                     Closes the "decorative alert" gaming
                                     vector (e.g., `expr: redis_aof_enabled
                                     == 1` would never fire because injection
                                     flips the metric to 0).
      b3 alert_is_actionable       — the rule has at least one of:
                                     labels.severity, annotations.summary,
                                     annotations.description. Basic SRE
                                     hygiene; an unannotated rule pages
                                     nobody useful.

A and B do not share underlying state: agent can pass A while failing B
(fixes redis, skips monitoring) or pass B while failing A (writes alert,
doesn't fix redis). Within each subscore, AND-gate multiplies individual
pass rates (per difficulty bank §"Multiplication Trick Math Table"), giving
each subscore variance even on capable agents.
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


def _poll_unflushed_durable(pod, timeout=15):
    """Wait for the agent's live save policy / appendfsync to make recently-
    written data durable WITHOUT us calling BGSAVE or BGREWRITEAOF. Returns
    True if at least one of: AOF buffer fsynced (aof_pending_bio_fsync:0) OR
    save triggered (rdb_changes_since_last_save back to 0)."""
    start = time.time()
    while time.time() - start < timeout:
        info = redis_cli(pod, "INFO", "persistence")
        if "aof_pending_bio_fsync:0" in info:
            return True
        m = re.search(r"^rdb_changes_since_last_save:0\s*$", info, re.M)
        if m:
            return True
        time.sleep(1)
    return False


def subscore_a_persistence_durability():
    """AND-gate of 3 behavioral probes, all within ONE pod restart cycle."""
    pod = wait_for_redis(timeout=120)
    if not pod:
        return [0, 0, 0], [
            ("string_survives_restart", False, "no redis pre-probe"),
            ("hash_survives_restart", False, "no redis pre-probe"),
            ("unflushed_probe_survives", False, "no redis pre-probe"),
        ]

    # a1: string key (flushed via BGSAVE+BGREWRITEAOF below)
    a1_key = "grader:str:" + uuid.uuid4().hex
    a1_val = uuid.uuid4().hex
    redis_cli(pod, "SET", a1_key, a1_val)

    # a2: hash key (same flush path)
    a2_key = "grader:hash:" + uuid.uuid4().hex
    a2_field = "probe"
    a2_val = uuid.uuid4().hex
    redis_cli(pod, "HSET", a2_key, a2_field, a2_val)

    # Force-flush a1 + a2 via manual BGSAVE + BGREWRITEAOF (polling, no bare sleep).
    _poll_bgsave_done(pod, timeout=30)
    _poll_aof_rewrite_done(pod, timeout=15)

    # a3: probe key written AFTER the manual flush. We do NOT call BGSAVE or
    # BGREWRITEAOF again — survival depends on the agent's live save policy
    # and/or appendfsync getting this write to disk before the kill.
    a3_key = "grader:noflush:" + uuid.uuid4().hex
    a3_val = uuid.uuid4().hex
    redis_cli(pod, "SET", a3_key, a3_val)
    a3_durable = _poll_unflushed_durable(pod, timeout=15)

    # Single force-delete exercises all three probes at once.
    run(KUBECTL + ["delete", "pod", pod, "--force", "--grace-period=0"], timeout=60)

    new_pod = wait_for_redis(timeout=180)
    if not new_pod:
        return [0, 0, 0], [
            ("string_survives_restart", False, "redis did not recover"),
            ("hash_survives_restart", False, "redis did not recover"),
            ("unflushed_probe_survives", False, "redis did not recover"),
        ]

    a1_got = redis_cli(new_pod, "GET", a1_key)
    a2_got = redis_cli(new_pod, "HGET", a2_key, a2_field)
    a3_got = redis_cli(new_pod, "GET", a3_key)

    a1_ok = a1_got == a1_val
    a2_ok = a2_got == a2_val
    a3_ok = a3_got == a3_val and a3_durable

    a3_detail = "survived" if a3_ok else (
        "value lost (durability poll=%s, got=%r)" % (a3_durable, a3_got)
    )
    return [int(a1_ok), int(a2_ok), int(a3_ok)], [
        ("string_survives_restart", a1_ok,
         "string round-trip" if a1_ok else "got=%r" % a1_got),
        ("hash_survives_restart", a2_ok,
         "hash round-trip" if a2_ok else "got=%r" % a2_got),
        ("unflushed_probe_survives", a3_ok, a3_detail),
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


# Narrowed to two metrics that respond reliably to live CONFIG SET changes
# AND are the right alert family for this incident class. The original
# breakage was 'save policy disabled, appendonly no' — that doesn't cause
# BGSAVE failures (no save is attempted at all), so _status metrics would
# never fire and wouldn't have caught the original incident. The right
# alerts are on CONFIG STATE, not on failure events:
#   - redis_aof_enabled               → 0 means AOF was turned off
#   - redis_rdb_changes_since_last_save > N → save policy isn't snapshotting
# Both metrics are also reliably flippable via CONFIG SET without needing
# protected-config workarounds (dir/dbfilename are locked in Redis 7+).
_b_metric_pattern = re.compile(
    r"redis_(aof_enabled|rdb_changes_since_last_save)"
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


def _poll_alert_pending_or_firing(rule_name, timeout=60):
    """Poll /api/v1/alerts looking for an alert instance with this rule name
    in pending or firing state. Pending is acceptable proof-of-life per
    playbook §4 — agent's `for:` clause length doesn't matter."""
    start = time.time()
    while time.time() - start < timeout:
        data, _ = _prom_query("/api/v1/alerts")
        if data:
            for a in data.get("data", {}).get("alerts", []):
                labels = a.get("labels") or {}
                if labels.get("alertname") == rule_name:
                    state = a.get("state", "")
                    if state in ("pending", "firing"):
                        return True, state
        time.sleep(3)
    return False, None


def subscore_b_alert_observability():
    """AND-gate of 3 alert-quality checks: rule loaded + alert actually fires
    on synthetic failure (behavioral) + rule is actionable."""
    rule, metric, err = _find_matching_alert_rule()
    b1_ok = rule is not None
    b1_detail = ("matched " + (rule.get("name", "?") if rule else "?")
                 + " on metric " + (metric or "?")) if b1_ok else (err or "no rule")

    # b2: BEHAVIORAL — inject synthetic persistence failure, verify the
    # agent's alert actually responds to it. Catches decorative alerts whose
    # expression would never fire (e.g., `redis_aof_enabled == 1`).
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
            snapshot = _snapshot_persistence_config(pod)
            try:
                _inject_persistence_failure(pod)
                # Wait scrape (~15s) + eval (~15s) + a margin for state propagation.
                fired, state = _poll_alert_pending_or_firing(rule_name, timeout=60)
                b2_ok = fired
                b2_detail = ("alert %s reached %s" % (rule_name, state)
                             if fired else
                             "alert %s did not reach pending/firing within 60s "
                             "of synthetic failure injection" % rule_name)
            finally:
                _restore_persistence_config(pod, snapshot)

    # b3: the matched rule is minimally actionable — has labels.severity OR
    # annotations.summary OR annotations.description.
    if rule:
        labels = rule.get("labels") or {}
        ann = rule.get("annotations") or {}
        has_sev = bool(labels.get("severity"))
        has_summary = bool(ann.get("summary"))
        has_desc = bool(ann.get("description"))
        b3_ok = has_sev or has_summary or has_desc
        b3_detail = ("severity=%s summary=%s description=%s" %
                     (labels.get("severity"), ann.get("summary"),
                      ann.get("description"))) if b3_ok else "no severity / summary / description"
    else:
        b3_ok = False
        b3_detail = "no rule to inspect (b1 failed)"

    return [int(b1_ok), int(b2_ok), int(b3_ok)], [
        ("alert_rule_loaded", b1_ok, b1_detail),
        ("alert_fires_on_synthetic_failure", b2_ok, b2_detail),
        ("alert_is_actionable", b3_ok, b3_detail),
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
