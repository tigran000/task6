"""Grader for redis-persistence-drift.

Two equal-weight, binary, orthogonal subscores:

  A persistence_survives_restart  (FUNCTIONAL)
      End-to-end probe: SET a unique key in bleater-redis-0, force RDB+AOF
      flush (polling, not bare sleep), kill the pod, wait for recovery,
      GET the key. Passes iff the value survived the restart. This implicitly
      requires CONFIG save/appendonly to be set AND /data to be on a Bound PVC
      mounted at /data — so the agent's persistence-fix work is fully covered
      by A's outcome, not by separate structural checks.

  B alert_rule_present            (OBSERVABILITY)
      Live Prometheus has an alerting rule (type=alerting, health=ok) whose
      PromQL expression references one of the redis-exporter persistence
      metrics. Single check, not AND-gated — by design the only B-item that
      is independent of A.

Per key-info.md "Subscore Independence Verification", A and B do not share
underlying state: agent can pass A while failing B (fixes redis, skips
monitoring) or pass B while failing A (writes the alert rule, doesn't fix
redis). All four cells of the joint distribution are reachable.
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


def subscore_a_persistence_survives_restart():
    pod = wait_for_redis(timeout=120)
    if not pod:
        return 0, "redis not reachable pre-probe"

    probe_key = "grader:probe:" + uuid.uuid4().hex
    probe_val = uuid.uuid4().hex
    set_out = redis_cli(pod, "SET", probe_key, probe_val)
    if "OK" not in set_out.upper():
        return 0, "could not SET probe key"

    last_save_before = redis_cli(pod, "LASTSAVE")
    redis_cli(pod, "BGSAVE")
    poll_start = time.time()
    while time.time() - poll_start < 30:
        cur = redis_cli(pod, "LASTSAVE")
        if cur and cur != last_save_before:
            break
        in_prog = redis_cli(pod, "INFO", "persistence")
        if "rdb_bgsave_in_progress:0" in in_prog and "loading:0" in in_prog:
            break
        time.sleep(2)

    redis_cli(pod, "BGREWRITEAOF")
    poll_start = time.time()
    while time.time() - poll_start < 15:
        in_prog = redis_cli(pod, "INFO", "persistence")
        if "aof_rewrite_in_progress:0" in in_prog:
            break
        time.sleep(1)

    run(KUBECTL + ["delete", "pod", pod, "--force", "--grace-period=0"], timeout=60)

    new_pod = wait_for_redis(timeout=180)
    if not new_pod:
        return 0, "redis did not recover after restart"

    got = redis_cli(new_pod, "GET", probe_key)
    if got == probe_val:
        return 1, "probe key survived pod restart"
    return 0, "probe key lost after restart (got %r)" % got


def subscore_b_alert_rule_loaded():
    """Query the live Prometheus /api/v1/rules via kubectl exec into the
    prometheus pod. Match on metric NAME in the rule's expression, not on
    the alert's user-chosen title. No PrometheusRule CRD exists on this
    snapshot, so there is no CRD fallback."""
    # Accept any of the four canonical redis-exporter persistence-health
    # metrics. An alert keyed on any of them is a legitimate "page when
    # durability regresses" signal.
    metric_pattern = re.compile(
        r"redis_("
        r"rdb_last_bgsave_status"          # RDB BGSAVE failures
        r"|aof_last_write_status"          # AOF write failures
        r"|rdb_last_save_timestamp_seconds"  # snapshots not happening
        r"|rdb_changes_since_last_save"    # save policy broken / not snapshotting
        r")"
    )
    cmd = [
        "kubectl", "-n", PROM_NS, "exec", "deploy/prometheus", "--",
        "wget", "-qO-", "http://localhost:9090/api/v1/rules",
    ]
    _, out, _ = run(cmd, timeout=15)
    if not out:
        return False, "could not query prometheus /api/v1/rules"
    try:
        data = json.loads(out)
    except Exception:
        return False, "non-json from prometheus rules api"
    for g in data.get("data", {}).get("groups", []):
        for r in g.get("rules", []):
            if r.get("type") != "alerting":
                continue
            if r.get("health") not in (None, "ok"):
                continue
            expr = r.get("query", "") or r.get("expr", "")
            if metric_pattern.search(expr):
                return True, "prometheus alert match: " + r.get("name", "?")
    return False, "no alerting rule references redis_rdb/aof status metric"


def grade(transcript=None):
    weights = {
        "persistence_survives_restart": 0.5,
        "alert_rule_present": 0.5,
    }

    a_score, a_detail = subscore_a_persistence_survives_restart()
    b_pass, b_detail = subscore_b_alert_rule_loaded()
    b_score = 1 if b_pass else 0

    subscores = {
        "persistence_survives_restart": float(a_score),
        "alert_rule_present": float(b_score),
    }
    total = sum(subscores[k] * weights[k] for k in subscores)

    feedback = (
        ("+" if a_score else "x") + " persistence_survives_restart: " + a_detail + "\n"
        + ("+" if b_score else "x") + " alert_rule_present: " + b_detail
    )

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
