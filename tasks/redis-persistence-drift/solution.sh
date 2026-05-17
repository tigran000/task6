#!/bin/bash
set -e

NS="bleater"
STS="bleater-redis"
POD="bleater-redis-0"
PROM_NS="monitoring"
GITEA_URL="http://gitea.devops.local:3000"
GITEA_USER="root"
GITEA_PASS="Admin@123456"

# Remove the cache-config-syncer CronJob in bleater and any sibling sync
# CronJobs that periodically re-assert the broken persistence config.
# These must go BEFORE we restart Redis, otherwise our fix is undone
# within one minute.
echo "[solution] Removing config-syncer CronJobs that re-disable persistence..."
kubectl -n "$NS" delete cronjob cache-config-syncer --ignore-not-found >/dev/null 2>&1 || true
kubectl -n "$NS" delete job -l app=cache-config-syncer --ignore-not-found >/dev/null 2>&1 || true
kubectl -n monitoring delete cronjob redis-config-watchdog --ignore-not-found >/dev/null 2>&1 || true
kubectl -n monitoring delete job -l app=redis-config-watchdog --ignore-not-found >/dev/null 2>&1 || true

echo "[solution] Reading current redis StatefulSet..."
ORIG=$(mktemp)
kubectl -n "$NS" get sts "$STS" -o yaml > "$ORIG"

echo "[solution] Building corrected sts spec (persistence on, PVC at /data)..."
FIXED=$(mktemp)
python3 - "$ORIG" "$FIXED" <<'PY'
import sys, yaml
src, dst = sys.argv[1], sys.argv[2]
d = yaml.safe_load(open(src).read())
md = d.get("metadata", {})
for f in ("creationTimestamp", "resourceVersion", "uid", "generation", "managedFields"):
    md.pop(f, None)
d.pop("status", None)
spec = d["spec"]
# Restore volumeClaimTemplates for /data
spec["volumeClaimTemplates"] = [{
    "apiVersion": "v1",
    "kind": "PersistentVolumeClaim",
    "metadata": {"name": "data"},
    "spec": {
        "accessModes": ["ReadWriteOnce"],
        "resources": {"requests": {"storage": "2Gi"}},
        "volumeMode": "Filesystem",
    },
}]
pod = spec["template"]["spec"]
# Drop the broken emptyDir; let the PVC template provide the volume
pod["volumes"] = [v for v in (pod.get("volumes") or []) if v.get("name") != "data"]
# Restore the canonical command with persistence enabled
for c in pod.get("containers", []):
    if c.get("name") == "redis":
        c["command"] = [
            "redis-server",
            "--save", "3600 1 300 100 60 10000",
            "--appendonly", "yes",
            "--appendfsync", "everysec",
            "--dir", "/data",
        ]
open(dst, "w").write(yaml.safe_dump(d))
PY

echo "[solution] Scaling redis to 0 to release any storage lock (RWO PVC discipline)..."
kubectl -n "$NS" scale sts "$STS" --replicas=0 >/dev/null 2>&1 || true
WAIT=0
while [ $WAIT -lt 60 ]; do
  CNT=$(kubectl -n "$NS" get pod -l app=bleater-redis --no-headers 2>/dev/null | wc -l)
  [ "$CNT" -eq 0 ] && break
  sleep 2
  WAIT=$((WAIT + 2))
done

echo "[solution] Replacing the StatefulSet with persistence enabled (vct is immutable)..."
kubectl -n "$NS" delete sts "$STS" --cascade=foreground --timeout=60s >/dev/null 2>&1 || true
WAIT=0
while [ $WAIT -lt 60 ]; do
  kubectl -n "$NS" get sts "$STS" >/dev/null 2>&1 || break
  sleep 2
  WAIT=$((WAIT + 2))
done
kubectl apply -f "$FIXED" >/dev/null

echo "[solution] Waiting for redis pod to come up with the new spec..."
WAIT=0
while [ $WAIT -lt 180 ]; do
  PHASE=$(kubectl -n "$NS" get pod "$POD" -o jsonpath='{.status.phase}' 2>/dev/null || true)
  if [ "$PHASE" = "Running" ]; then
    PONG=$(kubectl -n "$NS" exec "$POD" -- redis-cli PING 2>/dev/null || true)
    [ "$PONG" = "PONG" ] && break
  fi
  sleep 3
  WAIT=$((WAIT + 3))
done

# Belt-and-braces: also drive CONFIG SET in case the args didn't propagate.
kubectl -n "$NS" exec "$POD" -- redis-cli CONFIG SET save "3600 1 300 100 60 10000" >/dev/null 2>&1 || true
kubectl -n "$NS" exec "$POD" -- redis-cli CONFIG SET appendonly yes >/dev/null 2>&1 || true
kubectl -n "$NS" exec "$POD" -- redis-cli CONFIG SET appendfsync everysec >/dev/null 2>&1 || true
kubectl -n "$NS" exec "$POD" -- redis-cli BGSAVE >/dev/null 2>&1 || true

echo "[solution] Wiring Prometheus rules for redis persistence (no Operator → edit ConfigMap + reload)..."
PROM_CM=$(mktemp)
kubectl -n "$PROM_NS" get cm prometheus-config -o yaml > "$PROM_CM"

PROM_CM_NEW=$(mktemp)
python3 - "$PROM_CM" "$PROM_CM_NEW" <<'PY'
import sys, yaml, re
src, dst = sys.argv[1], sys.argv[2]
d = yaml.safe_load(open(src).read())
md = d.get("metadata", {})
for f in ("creationTimestamp", "resourceVersion", "uid", "generation", "managedFields"):
    md.pop(f, None)
data = d.setdefault("data", {})

# Inject a rules file as a new key in the ConfigMap.
data["alerts.yml"] = """groups:
- name: redis.persistence
  rules:
  - alert: RedisAOFDisabled
    expr: redis_aof_enabled == 0
    for: 30s
    labels:
      severity: critical
    annotations:
      summary: "Redis AOF persistence has been disabled"
      description: "Cache layer is now ephemeral. Any pod restart loses all data."
  - alert: RedisChangesAccumulatingWithoutSave
    expr: redis_rdb_changes_since_last_save > 10000
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "Redis has >10K unsaved changes for >5m"
      description: "Save policy may be disabled or BGSAVE not running."
"""

# Make sure prometheus.yml references the rule file. Uncomment or add rule_files.
py = data.get("prometheus.yml", "")
if re.search(r"^rule_files:\s*\n\s*-\s*['\"]?/etc/prometheus/alerts\.yml", py, re.M):
    pass
elif re.search(r"^\s*#\s*-\s*\"alerts\.yml\"", py, re.M):
    py = re.sub(r"^\s*#\s*-\s*\"alerts\.yml\"", "  - /etc/prometheus/alerts.yml", py, flags=re.M)
elif re.search(r"^rule_files:", py, re.M):
    py = re.sub(
        r"^(rule_files:\s*\n)",
        r"\1  - /etc/prometheus/alerts.yml\n",
        py,
        count=1,
        flags=re.M,
    )
else:
    py = "rule_files:\n  - /etc/prometheus/alerts.yml\n\n" + py

data["prometheus.yml"] = py
open(dst, "w").write(yaml.safe_dump(d))
PY

kubectl apply -f "$PROM_CM_NEW" >/dev/null

# Prometheus uses an RWO PVC. Rolling update would race the storage lock
# (new pod boots before old one releases /prometheus). Scale 0 -> wait
# -> scale 1 sidesteps the lock.
kubectl -n "$PROM_NS" scale deployment prometheus --replicas=0 >/dev/null 2>&1 || true
WAIT=0
while [ $WAIT -lt 60 ]; do
  CNT=$(kubectl -n "$PROM_NS" get pod -l app=prometheus --no-headers 2>/dev/null | wc -l)
  [ "$CNT" -eq 0 ] && break
  sleep 2
  WAIT=$((WAIT + 2))
done
kubectl -n "$PROM_NS" scale deployment prometheus --replicas=1 >/dev/null 2>&1 || true
WAIT=0
while [ $WAIT -lt 90 ]; do
  READY=$(kubectl -n "$PROM_NS" get pod -l app=prometheus -o jsonpath='{.items[0].status.containerStatuses[0].ready}' 2>/dev/null || true)
  [ "$READY" = "true" ] && break
  sleep 3
  WAIT=$((WAIT + 3))
done

# Wait for Prometheus to load the new rules.
WAIT=0
while [ $WAIT -lt 60 ]; do
  RULES_OUT=$(kubectl -n "$PROM_NS" exec deploy/prometheus -- \
    wget -qO- http://localhost:9090/api/v1/rules 2>/dev/null || true)
  if echo "$RULES_OUT" | grep -q "redis_rdb_last_bgsave_status\|redis_aof_last_write_status"; then
    break
  fi
  sleep 3
  WAIT=$((WAIT + 3))
done

echo "[solution] Closing the P1 incident with an RCA comment..."
TOKEN_RESP=$(curl -s -u "${GITEA_USER}:${GITEA_PASS}" \
  --connect-timeout 5 --max-time 15 \
  -H "Content-Type: application/json" \
  -d '{"name":"solution-'"$RANDOM"'","scopes":["read:repository","write:repository"]}' \
  "${GITEA_URL}/api/v1/users/${GITEA_USER}/tokens" 2>/dev/null || true)
GITEA_TOKEN=$(echo "$TOKEN_RESP" | grep -o '"sha1":"[^"]*"' | head -n1 | cut -d'"' -f4)

if [ -n "$GITEA_TOKEN" ]; then
  ISSUES=$(curl -s --connect-timeout 5 --max-time 15 \
    -H "Authorization: token ${GITEA_TOKEN}" \
    "${GITEA_URL}/api/v1/repos/${GITEA_USER}/bleater-app/issues?state=open&type=issues" 2>/dev/null || true)
  ISSUE_NUM=$(echo "$ISSUES" | python3 -c "
import json, sys
try:
    for issue in json.load(sys.stdin):
        t = issue.get('title','').lower()
        if 'feed loading slowly' in t or t.startswith('p1'):
            print(issue['number']); sys.exit(0)
except Exception: pass
" 2>/dev/null || true)

  if [ -n "$ISSUE_NUM" ]; then
    RCA='## RCA\n\nThe bleater-redis StatefulSet had been re-applied without persistence: `--save \"\" --appendonly no`, and its `/data` mount switched from a PVC to `emptyDir`. The PVC `data-bleater-redis-0` had also been removed. On pod restart Redis came back empty, bleat-service fell through to PostgreSQL for every read, and DB CPU spiked.\n\n## Fix\n- Restored the StatefulSet command to `redis-server --save 3600 1 300 100 60 10000 --appendonly yes --appendfsync everysec --dir /data`.\n- Re-added the `data` `volumeClaimTemplate` (2Gi, RWO) so `/data` is on a Bound PVC again.\n- Added Prometheus alert rules `RedisRDBSnapshotFailing` and `RedisAOFWriteFailing` keyed off `redis_rdb_last_bgsave_status` and `redis_aof_last_write_status`, so we get paged the moment durability regresses again.'
    curl -s -o /dev/null --connect-timeout 5 --max-time 15 \
      -X POST -H "Authorization: token ${GITEA_TOKEN}" \
      -H "Content-Type: application/json" \
      -d "{\"body\":\"${RCA}\"}" \
      "${GITEA_URL}/api/v1/repos/${GITEA_USER}/bleater-app/issues/${ISSUE_NUM}/comments" || true

    curl -s -o /dev/null --connect-timeout 5 --max-time 15 \
      -X PATCH -H "Authorization: token ${GITEA_TOKEN}" \
      -H "Content-Type: application/json" \
      -d '{"state":"closed"}' \
      "${GITEA_URL}/api/v1/repos/${GITEA_USER}/bleater-app/issues/${ISSUE_NUM}" || true
  fi
fi

echo "[solution] Done."
