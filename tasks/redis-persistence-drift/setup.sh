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

# Third reverter, mechanism-diverse: sidecar inside bleater-bleat-service.
# Bleat-service is the legitimate consumer of redis; a "cache config tuner"
# sidecar there is realistic (some teams do exactly this to coax cache
# behaviour from the consumer side). The sidecar reverter is aggressive
# (every 5s) so it reasserts the broken config faster than a CronJob.
# Find rate is lower than a CronJob (agents rarely audit other apps'
# container lists) so this mechanism carries A's real variance.
echo "[setup] Installing redis-fsync-tuner CronJob (monitoring namespace)..."
cat <<'YAML' | kubectl apply -f - >/dev/null 2>&1 || true
apiVersion: batch/v1
kind: CronJob
metadata:
  name: redis-fsync-tuner
  namespace: monitoring
  labels:
    app: redis-fsync-tuner
    app.kubernetes.io/managed-by: platform-perf
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
          - name: tuner
            image: redis:7-alpine
            imagePullPolicy: IfNotPresent
            command:
            - sh
            - -c
            - |
              redis-cli -h bleater-redis-headless.bleater --connect-timeout 3 \
                CONFIG SET appendfsync no >/dev/null 2>&1 || true
YAML

echo "[setup] Installing cache-config-tuner sidecar in bleater-bleat-service..."
kubectl -n bleater patch deployment bleater-bleat-service --type=json -p='[
  {"op":"add","path":"/spec/template/spec/containers/-","value":{
    "name":"cache-config-tuner",
    "image":"redis:7-alpine",
    "imagePullPolicy":"IfNotPresent",
    "command":["sh","-c"],
    "args":["while true; do redis-cli -h bleater-redis-headless --connect-timeout 3 CONFIG SET appendonly no >/dev/null 2>&1 || true; redis-cli -h bleater-redis-headless --connect-timeout 3 CONFIG SET save \"\" >/dev/null 2>&1 || true; sleep 5; done"]
  }}
]' >/dev/null 2>&1 || true
kubectl -n bleater rollout status deploy/bleater-bleat-service --timeout=90s >/dev/null 2>&1 || true

# Cross-namespace SRE watchdog. Unlike the in-memory CONFIG SET reverters
# above, this one patches the StatefulSet's command-args directly via the
# Kubernetes API, so the broken configuration SURVIVES pod restarts (the
# in-memory CONFIG SETs are overwritten by the sts command-args on every
# Redis boot — patching the sts itself is the only way to make persistence
# regress across a force-delete). Needs RBAC (SA in monitoring, Role in
# bleater granting patch on statefulsets, RoleBinding stitching them).
echo "[setup] Installing redis-config-watchdog RBAC + CronJob (monitoring namespace, sts-patch)..."
cat <<'YAML' | kubectl apply -f - >/dev/null 2>&1 || true
apiVersion: v1
kind: ServiceAccount
metadata:
  name: redis-config-watchdog
  namespace: monitoring
  labels:
    app: redis-config-watchdog
    app.kubernetes.io/managed-by: monitoring-team
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: redis-config-watchdog
  namespace: bleater
  labels:
    app: redis-config-watchdog
    app.kubernetes.io/managed-by: monitoring-team
rules:
- apiGroups: ["apps"]
  resources: ["statefulsets"]
  verbs: ["get", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: redis-config-watchdog
  namespace: bleater
  labels:
    app: redis-config-watchdog
    app.kubernetes.io/managed-by: monitoring-team
subjects:
- kind: ServiceAccount
  name: redis-config-watchdog
  namespace: monitoring
roleRef:
  kind: Role
  name: redis-config-watchdog
  apiGroup: rbac.authorization.k8s.io
---
apiVersion: batch/v1
kind: CronJob
metadata:
  name: redis-config-watchdog
  namespace: monitoring
  labels:
    app: redis-config-watchdog
    app.kubernetes.io/managed-by: monitoring-team
spec:
  schedule: "*/1 * * * *"
  concurrencyPolicy: Forbid
  successfulJobsHistoryLimit: 1
  failedJobsHistoryLimit: 1
  jobTemplate:
    spec:
      backoffLimit: 0
      template:
        spec:
          serviceAccountName: redis-config-watchdog
          restartPolicy: Never
          containers:
          - name: watchdog
            image: bitnami/kubectl:1.28
            imagePullPolicy: IfNotPresent
            command:
            - sh
            - -c
            - |
              kubectl -n bleater patch statefulset bleater-redis --type=json -p='[{"op":"replace","path":"/spec/template/spec/containers/0/command","value":["redis-server","--save","","--appendonly","no","--dir","/tmp"]}]' >/dev/null 2>&1 || true
YAML

# Pre-pull bitnami/kubectl:1.28 into k3s's containerd cache so the watchdog
# CronJob can spawn its pod during the agent's air-gapped session without
# needing to reach a registry. We trigger the pull via a short-lived Pod
# and wait for it to Succeed; if it fails, the watchdog degrades gracefully
# to ImagePullBackOff (no setup crash, just a less-effective reverter).
echo "[setup] Pre-pulling bitnami/kubectl:1.28 into the node image cache..."
kubectl -n monitoring run kubectl-prepull \
  --image=bitnami/kubectl:1.28 --restart=Never --command \
  -- sh -c "exit 0" >/dev/null 2>&1 || true
PRE_WAIT=0
while [ $PRE_WAIT -lt 180 ]; do
  PRE_PHASE=$(kubectl -n monitoring get pod kubectl-prepull -o jsonpath='{.status.phase}' 2>/dev/null || true)
  if [ "$PRE_PHASE" = "Succeeded" ] || [ "$PRE_PHASE" = "Failed" ]; then
    break
  fi
  sleep 3
  PRE_WAIT=$((PRE_WAIT + 3))
done
kubectl -n monitoring delete pod kubectl-prepull --ignore-not-found --force --grace-period=0 >/dev/null 2>&1 || true

# Wait for Gitea API to be reachable before attempting token creation.
# Previous versions skipped this and silently passed an empty token to the
# issue-creation block — agents then saw an empty issues list and never
# encountered the monitoring hint.
echo "[setup] Waiting for Gitea API at ${GITEA_URL}..."
GITEA_WAIT=0
while [ $GITEA_WAIT -lt 300 ]; do
  GITEA_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
    --connect-timeout 3 --max-time 5 \
    "${GITEA_URL}/api/v1/version" 2>/dev/null || echo "000")
  if [ "$GITEA_CODE" = "200" ]; then
    echo "[setup] Gitea API ready (HTTP 200)"
    break
  fi
  sleep 5
  GITEA_WAIT=$((GITEA_WAIT + 5))
done
if [ "$GITEA_CODE" != "200" ]; then
  echo "ERROR: Gitea API not ready after 300s (last code: ${GITEA_CODE})"
  exit 1
fi

echo "[setup] Filing P1 incident + decoys in Gitea..."
# Token creation: NOT silenced — we want a hard failure with a visible error
# if Gitea rejects the auth, instead of falling through with an empty token.
# Scopes MUST include write:issue — write:repository alone does NOT grant
# issue creation. Without write:issue, all issue POSTs return 403 and the
# setup completes "successfully" but agents see open_issues_count: 0.
TOKEN_RESP=$(curl -s -u "${GITEA_USER}:${GITEA_PASS}" \
  --connect-timeout 5 --max-time 15 \
  -H "Content-Type: application/json" \
  -d '{"name":"setup-bootstrap-'"$RANDOM"'","scopes":["write:repository","write:user","write:admin","write:issue"]}' \
  "${GITEA_URL}/api/v1/users/${GITEA_USER}/tokens") \
  || { echo "ERROR: gitea token-create curl failed"; exit 1; }
GITEA_TOKEN=$(echo "$TOKEN_RESP" | grep -o '"sha1":"[^"]*"' | head -n1 | cut -d'"' -f4)

if [ -z "$GITEA_TOKEN" ]; then
  echo "ERROR: Gitea token creation returned no sha1. Response was:"
  echo "$TOKEN_RESP"
  exit 1
fi
echo "[setup] Got Gitea token (len ${#GITEA_TOKEN})"

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
- We had no alerting on this layer at all and got blindsided. We need
  to learn about regressions like this from a page, not from user
  complaints — please make sure on-call gets notified next time
- Please make sure whatever caused this cannot recur
EOF
)
  P1_JSON=$(python3 -c "import json,sys; print(json.dumps({'title':'P1 — Bleater feed loading slowly, DB CPU pegged','body':sys.stdin.read()}))" <<<"$P1_BODY")
  # P1 issue: NOT silenced. We need this issue to land — it carries the
  # monitoring hint. Fail loudly if the POST is rejected.
  P1_HTTP=$(curl -s -o /tmp/setup-p1-resp -w "%{http_code}" \
    --connect-timeout 5 --max-time 15 \
    -X POST -H "Authorization: token ${GITEA_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "$P1_JSON" \
    "${GITEA_URL}/api/v1/repos/${GITEA_USER}/bleater-app/issues")
  if [ "$P1_HTTP" != "201" ]; then
    echo "ERROR: P1 issue POST returned HTTP ${P1_HTTP}. Body:"
    cat /tmp/setup-p1-resp
    exit 1
  fi
  echo "[setup] P1 issue filed (HTTP 201)"

  # Decoys: silent fallback OK — they're noise, not load-bearing.
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
