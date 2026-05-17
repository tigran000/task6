"""Grader for redis-persistence-drift.

Two equal-weight binary subscores:
  A persistence_survives_restart      (FUNCTIONAL)
      End-to-end probe: SET a unique key in bleater-redis-0, force RDB+AOF
      flush (polling, not bare sleep), kill the pod, wait for recovery,
      GET the key. Passes iff the value survived the restart.

  B config_and_observability_combined (NON-FUNCTIONAL AND-gate of 5 items)
      b1 save_non_empty             — CONFIG GET save is not empty
      b2 appendonly_yes             — CONFIG GET appendonly == yes
      b3 pvc_bound                  — PVC backing the live pod's /data is Bound
      b4 pvc_mounted_at_data        — /data on the live pod is a PVC, not emptyDir
      b5 alert_rule_present         — Prometheus has an alerting rule (type=alerting,
                                       health=ok) whose expression references
                                       redis_rdb_last_bgsave_status or
                                       redis_aof_last_write_status. Verified via
                                       live /api/v1/rules (no PrometheusRule CRD
                                       exists on this snapshot).
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


def b1_save_set(pod):
    out = redis_cli(pod, "CONFIG", "GET", "save")
    lines = out.splitlines()
    val = lines[1].strip() if len(lines) >= 2 else ""
    return bool(val) and val != '""', val


def b2_aof_on(pod):
    out = redis_cli(pod, "CONFIG", "GET", "appendonly")
    lines = out.splitlines()
    val = lines[1].strip().lower() if len(lines) >= 2 else ""
    return val == "yes", val


def _data_volume_claim_for_current_pod():
    """Return the PVC name backing /data on the live Redis pod, or None."""
    pod = redis_pod()
    if not pod:
        return None, None
    _, out, _ = run(KUBECTL + ["get", "pod", pod, "-o", "json"])
    try:
        data = json.loads(out)
    except Exception:
        return None, None
    spec = data.get("spec", {})
    volumes = {v["name"]: v for v in spec.get("volumes", [])}
    target_volume = None
    for c in spec.get("containers", []):
        for m in c.get("volumeMounts", []):
            if m.get("mountPath") == "/data":
                target_volume = m.get("name")
                break
        if target_volume:
            break
    if not target_volume:
        return None, None
    vol = volumes.get(target_volume, {})
    pvc = vol.get("persistentVolumeClaim", {})
    return pvc.get("claimName"), vol


def b3_pvc_bound():
    """Tied to the PVC actually claimed by the live Redis pod (not any orphan)."""
    claim_name, _ = _data_volume_claim_for_current_pod()
    if not claim_name:
        return False, "current redis pod has no PVC at /data"
    phase = kubectl_jsonpath(["get", "pvc", claim_name], "{.status.phase}")
    if phase == "Bound":
        return True, claim_name + ":Bound"
    return False, "%s:%s" % (claim_name, phase or "missing")


def b4_pvc_mounted_at_data():
    claim_name, vol = _data_volume_claim_for_current_pod()
    if claim_name:
        return True, "/data backed by PVC " + claim_name
    return False, "/data not backed by PVC (volume=%r)" % (vol or {})


def b5_alert_rule_loaded():
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


def subscore_b_combined():
    pod = wait_for_redis(timeout=60)
    if pod:
        r1, v1 = b1_save_set(pod)
        r2, v2 = b2_aof_on(pod)
    else:
        r1, v1 = False, "no redis pod"
        r2, v2 = False, "no redis pod"
    r3, v3 = b3_pvc_bound()
    r4, v4 = b4_pvc_mounted_at_data()
    r5, v5 = b5_alert_rule_loaded()
    items = [
        ("save_non_empty", r1, v1),
        ("appendonly_yes", r2, v2),
        ("pvc_bound", r3, v3),
        ("pvc_mounted_at_/data", r4, v4),
        ("alert_rule_present", r5, v5),
    ]
    passed = all(ok for _, ok, _ in items)
    detail_lines = [
        ("+" if ok else "x") + " " + name + ": " + str(detail)
        for name, ok, detail in items
    ]
    return (1 if passed else 0), "\n".join(detail_lines)


def grade(transcript=None):
    subscores = {}
    weights = {
        "persistence_survives_restart": 0.5,
        "config_and_observability_combined": 0.5,
    }

    a_score, a_detail = subscore_a_persistence_survives_restart()
    subscores["persistence_survives_restart"] = float(a_score)

    b_score, b_detail = subscore_b_combined()
    subscores["config_and_observability_combined"] = float(b_score)

    total = sum(subscores[k] * weights[k] for k in subscores)

    feedback_lines = []
    feedback_lines.append(
        ("+" if a_score else "x")
        + " persistence_survives_restart: "
        + a_detail
    )
    feedback_lines.append(
        ("+" if b_score else "x")
        + " config_and_observability_combined:"
    )
    for line in b_detail.splitlines():
        feedback_lines.append("    " + line)

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
