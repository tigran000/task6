#!/bin/bash
set -e

# ------- [DO NOT CHANGE ANYTHING BELOW]------- #
if ! supervisorctl status &>/dev/null; then
  echo "Starting supervisord..."
  /usr/bin/supervisord -c /etc/supervisor/supervisord.conf
  sleep 5
fi

export KUBECONFIG=/etc/rancher/k3s/k3s.yaml

echo "Waiting for k3s to be ready..."
MAX_WAIT=180
ELAPSED=0
until kubectl get nodes &>/dev/null; do
  if [ $ELAPSED -ge $MAX_WAIT ]; then
    echo "Error: k3s is not ready after ${MAX_WAIT} seconds"
    exit 1
  fi
  sleep 2
  ELAPSED=$((ELAPSED + 2))
done
echo "k3s is ready!"
# ------- [DO NOT CHANGE ANYTHING ABOVE]------- #

NS="bleater"
STS="bleater-redis"
POD="bleater-redis-0"
PVC="data-bleater-redis-0"
GITEA_URL="http://gitea.devops.local:3000"
GITEA_USER="root"
GITEA_PASS="Admin@123456"

echo "[setup] Waiting for ${NS} namespace + redis to be reachable..."
WAIT=0
while [ $WAIT -lt 600 ]; do
  if kubectl -n "$NS" get sts "$STS" >/dev/null 2>&1; then
    break
  fi
  sleep 5
  WAIT=$((WAIT + 5))
done
[ $WAIT -ge 600 ] && { echo "ERROR: ${STS} not present"; exit 1; }

# Wait for the existing redis pod to be ready before we tamper with it.
WAIT=0
while [ $WAIT -lt 300 ]; do
  PHASE=$(kubectl -n "$NS" get pod "$POD" -o jsonpath='{.status.phase}' 2>/dev/null || true)
  [ "$PHASE" = "Running" ] && break
  sleep 3
  WAIT=$((WAIT + 3))
done

echo "[setup] Disabling bleater-platform ArgoCD auto-sync so the breakage sticks..."
# Without this, ArgoCD's selfHeal will revert our sts patch within ~3 minutes.
kubectl -n argocd patch application bleater-platform --type=json \
  -p='[{"op":"remove","path":"/spec/syncPolicy/automated"}]' >/dev/null 2>&1 || true
# Also strip the resource-level tracking annotation as belt-and-braces.
kubectl -n "$NS" annotate sts "$STS" argocd.argoproj.io/tracking-id- >/dev/null 2>&1 || true

echo "[setup] Capturing original sts for restore-ability..."
ORIG_STS_YAML=$(mktemp)
kubectl -n "$NS" get sts "$STS" -o yaml > "$ORIG_STS_YAML"

echo "[setup] Building broken sts spec (no persistence, emptyDir)..."
BROKEN_STS_YAML=$(mktemp)
python3 - "$ORIG_STS_YAML" "$BROKEN_STS_YAML" <<'PY'
import sys, yaml
src, dst = sys.argv[1], sys.argv[2]
d = yaml.safe_load(open(src).read())
md = d.get("metadata", {})
for f in ("creationTimestamp", "resourceVersion", "uid", "generation", "managedFields"):
    md.pop(f, None)
d.pop("status", None)
spec = d["spec"]
# Wipe volumeClaimTemplates so the StatefulSet uses emptyDir
spec["volumeClaimTemplates"] = []
pod = spec["template"]["spec"]
# Replace the container command with persistence DISABLED
for c in pod.get("containers", []):
    if c.get("name") == "redis":
        c["command"] = ["redis-server", "--save", "", "--appendonly", "no"]
# Replace any existing "data" volume with emptyDir
vols = [v for v in (pod.get("volumes") or []) if v.get("name") != "data"]
vols.append({"name": "data", "emptyDir": {}})
pod["volumes"] = vols
open(dst, "w").write(yaml.safe_dump(d))
PY

echo "[setup] Scaling redis to 0 to release the PVC lock cleanly..."
kubectl -n "$NS" scale sts "$STS" --replicas=0 >/dev/null 2>&1 || true
WAIT=0
while [ $WAIT -lt 60 ]; do
  CNT=$(kubectl -n "$NS" get pod -l app=bleater-redis --no-headers 2>/dev/null | wc -l)
  [ "$CNT" -eq 0 ] && break
  sleep 2
  WAIT=$((WAIT + 2))
done

echo "[setup] Deleting orphan PVC..."
kubectl -n "$NS" delete pvc "$PVC" --ignore-not-found --timeout=30s >/dev/null 2>&1 || true
kubectl -n "$NS" patch pvc "$PVC" -p '{"metadata":{"finalizers":null}}' --type=merge >/dev/null 2>&1 || true

echo "[setup] Replacing StatefulSet (volumeClaimTemplates is immutable on update)..."
kubectl -n "$NS" delete sts "$STS" --cascade=foreground --timeout=60s >/dev/null 2>&1 || true
WAIT=0
while [ $WAIT -lt 60 ]; do
  kubectl -n "$NS" get sts "$STS" >/dev/null 2>&1 || break
  sleep 2
  WAIT=$((WAIT + 2))
done
kubectl apply -f "$BROKEN_STS_YAML" >/dev/null

echo "[setup] Waiting for new pod to come up with broken config..."

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

# Pre-warm "hot cache" so the agent sees realistic key churn (NOT graded).
if [ "$PHASE" = "Running" ]; then
  for i in 1 2 3 4 5 6 7 8; do
    kubectl -n "$NS" exec "$POD" -- redis-cli SET "bleat:hot:feed:user${i}" "warm" >/dev/null 2>&1 || true
    kubectl -n "$NS" exec "$POD" -- redis-cli SET "timeline:sorted:user${i}" "warm" >/dev/null 2>&1 || true
  done
fi

# Periodic config-syncer in the bleater namespace. Every minute it
# re-asserts a "lean cache" configuration (appendonly off, no save).
# Naming and labels match a legitimate platform config syncer; uses the
# same redis:7-alpine image already pulled for bleater-redis.
echo "[setup] Installing cache-config-syncer CronJob (bleater namespace)..."
cat <<'YAML' | kubectl apply -f - >/dev/null 2>&1 || true
apiVersion: batch/v1
kind: CronJob
metadata:
  name: cache-config-syncer
  namespace: bleater
  labels:
    app: cache-config-syncer
    app.kubernetes.io/managed-by: platform-config
spec:
  schedule: "* * * * *"
  concurrencyPolicy: Forbid
  successfulJobsHistoryLimit: 1
  failedJobsHistoryLimit: 1
  jobTemplate:
    spec:
      backoffLimit: 0
      template:
        spec:
          restartPolicy: Never
          containers:
          - name: sync
            image: redis:7-alpine
            imagePullPolicy: IfNotPresent
            command:
            - sh
            - -c
            - |
              redis-cli -h bleater-redis-headless --connect-timeout 3 \
                CONFIG SET appendonly no >/dev/null 2>&1 || true
              redis-cli -h bleater-redis-headless --connect-timeout 3 \
                CONFIG SET save "" >/dev/null 2>&1 || true
YAML

# Second config-syncer in the monitoring namespace on a different cadence
# (every 2 minutes). Mimics a SRE/monitoring-team watchdog reasserting
# "expected" cache config from a config catalog. Cross-namespace placement
# means agents who only audit `bleater` cronjobs miss this one.
echo "[setup] Installing redis-config-watchdog CronJob (monitoring namespace)..."
cat <<'YAML' | kubectl apply -f - >/dev/null 2>&1 || true
apiVersion: batch/v1
kind: CronJob
metadata:
  name: redis-config-watchdog
  namespace: monitoring
  labels:
    app: redis-config-watchdog
    app.kubernetes.io/managed-by: monitoring-team
spec:
  schedule: "*/2 * * * *"
  concurrencyPolicy: Forbid
  successfulJobsHistoryLimit: 1
  failedJobsHistoryLimit: 1
  jobTemplate:
    spec:
      backoffLimit: 0
      template:
        spec:
          restartPolicy: Never
          containers:
          - name: watchdog
            image: redis:7-alpine
            imagePullPolicy: IfNotPresent
            command:
            - sh
            - -c
            - |
              redis-cli -h bleater-redis-headless.bleater --connect-timeout 3 \
                CONFIG SET appendonly no >/dev/null 2>&1 || true
              redis-cli -h bleater-redis-headless.bleater --connect-timeout 3 \
                CONFIG SET save "" >/dev/null 2>&1 || true
YAML

echo "[setup] Filing P1 incident + decoys in Gitea..."
GITEA_TOKEN=""
TOKEN_RESP=$(curl -s -u "${GITEA_USER}:${GITEA_PASS}" \
  --connect-timeout 5 --max-time 15 \
  -H "Content-Type: application/json" \
  -d '{"name":"setup-bootstrap-'"$RANDOM"'","scopes":["write:repository","write:user","write:admin"]}' \
  "${GITEA_URL}/api/v1/users/${GITEA_USER}/tokens" 2>/dev/null || true)
GITEA_TOKEN=$(echo "$TOKEN_RESP" | grep -o '"sha1":"[^"]*"' | head -n1 | cut -d'"' -f4)

if [ -n "$GITEA_TOKEN" ]; then
  P1_BODY=$(cat <<'EOF'
## Incident Report
**Reported by:** On-call SRE
**Severity:** P1

### Symptoms
- Bleater home feed taking 2-5s to load (was <200ms)
- Postgres primary CPU at 95%+, query rate ~10x normal
- bleat-service logs show "cache miss" on nearly every request
- The cache appears to lose everything any time a pod cycles

### What we have tried
- Restarted bleat-service pods — no change
- Scaled up Postgres read replicas — temporary relief only

### Notes
- We have no alerting on this layer at all and got blindsided
- Please also wire up Prometheus alerts that page us if the cache layer
  persistence configuration regresses again — e.g., if AOF gets turned off
  or if unsaved changes start piling up. We want to learn about it from a
  page, not from user complaints
- Please make sure whatever caused this cannot recur
EOF
)
  P1_JSON=$(python3 -c "import json,sys; print(json.dumps({'title':'P1 — Bleater feed loading slowly, DB CPU pegged','body':sys.stdin.read()}))" <<<"$P1_BODY")
  curl -s -o /dev/null --connect-timeout 5 --max-time 15 \
    -X POST -H "Authorization: token ${GITEA_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "$P1_JSON" \
    "${GITEA_URL}/api/v1/repos/${GITEA_USER}/bleater-app/issues" || true

  curl -s -o /dev/null --connect-timeout 5 --max-time 15 \
    -X POST -H "Authorization: token ${GITEA_TOKEN}" \
    -H "Content-Type: application/json" \
    -d '{"title":"MinIO uploads slower than usual after office hours","body":"Reports of slow media uploads between 22:00 and 02:00 UTC. Probably network."}' \
    "${GITEA_URL}/api/v1/repos/${GITEA_USER}/bleater-app/issues" || true

  curl -s -o /dev/null --connect-timeout 5 --max-time 15 \
    -X POST -H "Authorization: token ${GITEA_TOKEN}" \
    -H "Content-Type: application/json" \
    -d '{"title":"Grafana login sporadically fails","body":"A few engineers report getting 401 on first login attempt. Retry works. Low priority."}' \
    "${GITEA_URL}/api/v1/repos/${GITEA_USER}/bleater-app/issues" || true
fi

kubectl delete events -A --all >/dev/null 2>&1 || true

echo "[setup] Done. bleater-redis is running with persistence disabled and PVC removed."
