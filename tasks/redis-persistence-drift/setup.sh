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
# Use JSON Merge Patch (RFC 7396) with explicit null to delete the field
# whether or not it exists. The prior op:remove approach failed silently
# when the path was already absent in the snapshot, leaving auto-sync
# enabled and racing the sts delete-then-apply below (Argo would
# recreate the sts from the chart between our delete and apply, and
# kubectl apply would then fail with the immutable-vct error because
# Argo's recreated sts has vct=[{data}] while BROKEN_STS_YAML has vct=[]).
kubectl -n argocd patch application bleater-platform --type=merge \
  -p='{"spec":{"syncPolicy":{"automated":null}}}' >/dev/null 2>&1 || true
# Verify the disable actually took effect; if not, fall back to JSON Patch
# op:replace which works regardless of prior state.
AUTO_CHECK=$(kubectl -n argocd get app bleater-platform -o jsonpath='{.spec.syncPolicy.automated}' 2>/dev/null || echo "")
if [ -n "$AUTO_CHECK" ]; then
  echo "[setup] WARN: merge-null didn't clear automated ('${AUTO_CHECK}'); retrying with op:remove"
  kubectl -n argocd patch application bleater-platform --type=json \
    -p='[{"op":"remove","path":"/spec/syncPolicy/automated"}]' >/dev/null 2>&1 || true
  AUTO_CHECK=$(kubectl -n argocd get app bleater-platform -o jsonpath='{.spec.syncPolicy.automated}' 2>/dev/null || echo "")
  if [ -n "$AUTO_CHECK" ]; then
    echo "ERROR: could not disable ArgoCD auto-sync (still='${AUTO_CHECK}'); setup would race Argo's reconciler"
    exit 1
  fi
fi
echo "[setup] ArgoCD auto-sync verified disabled (spec.syncPolicy.automated absent)"
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
# Aggressive delete: --force --grace-period=0 hard-kills the sts and its
# pods, --cascade=foreground waits for them. The wait loop below strips
# finalizers if delete still hangs after 20s.
kubectl -n "$NS" delete sts "$STS" --cascade=foreground --grace-period=0 --force --timeout=60s 2>/dev/null || true
WAIT=0
while [ $WAIT -lt 90 ]; do
  if ! kubectl -n "$NS" get sts "$STS" >/dev/null 2>&1; then
    break
  fi
  if [ $WAIT -ge 20 ]; then
    kubectl -n "$NS" patch sts "$STS" --type=merge \
      -p '{"metadata":{"finalizers":null}}' >/dev/null 2>&1 || true
  fi
  sleep 2
  WAIT=$((WAIT + 2))
done
# Belt-and-braces: explicit pod delete in case the sts cascade left orphans.
kubectl -n "$NS" delete pod -l app=bleater-redis --grace-period=0 --force --ignore-not-found >/dev/null 2>&1 || true

# Verify sts is truly absent before recreate. If still present, the
# subsequent apply will hit the immutable-vct error.
if kubectl -n "$NS" get sts "$STS" >/dev/null 2>&1; then
  echo "ERROR: sts $STS still present after delete attempts; cannot proceed"
  kubectl -n "$NS" get sts "$STS" -o yaml | head -40
  exit 1
fi

# Use kubectl create so this is treated as a fresh resource (no apply
# annotation lookup, no patch attempt). If create races with something
# that recreated the sts (e.g., Argo despite the disable), fall back to
# `replace --force` which does delete+create in one operation.
if ! kubectl create -f "$BROKEN_STS_YAML" >/dev/null 2>&1; then
  echo "[setup] WARN: create failed (likely race), trying replace --force"
  if ! kubectl replace --force -f "$BROKEN_STS_YAML" >/dev/null 2>&1; then
    echo "ERROR: could not create OR replace the broken sts"
    kubectl -n "$NS" get sts "$STS" -o yaml 2>&1 | head -40
    exit 1
  fi
fi

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

# Second in-app reverter sidecar, installed in a DIFFERENT Deployment
# (bleater-timeline-service). Different name (redis-pool-sizer) to look
# like an unrelated connection-pool tuner. Same behavioral shape
# (CONFIG SET appendonly no loop). v30 grader's behavioral a3 probe
# detects any live reverter regardless of which deployment hosts it,
# so the second placement is purely to increase the chance that some
# reverter survives the agent's cleanup pass (agents who only audit
# bleat-service's container list miss this one entirely).
echo "[setup] Installing redis-pool-sizer sidecar in bleater-timeline-service..."
kubectl -n bleater patch deployment bleater-timeline-service --type=json -p='[
  {"op":"add","path":"/spec/template/spec/containers/-","value":{
    "name":"redis-pool-sizer",
    "image":"redis:7-alpine",
    "imagePullPolicy":"IfNotPresent",
    "command":["sh","-c"],
    "args":["while true; do redis-cli -h bleater-redis-headless --connect-timeout 3 CONFIG SET appendonly no >/dev/null 2>&1 || true; redis-cli -h bleater-redis-headless --connect-timeout 3 CONFIG SET save \"\" >/dev/null 2>&1 || true; sleep 7; done"]
  }}
]' >/dev/null 2>&1 || true
kubectl -n bleater rollout status deploy/bleater-timeline-service --timeout=90s >/dev/null 2>&1 || true

# Lowest-visibility placement: sidecar attached to the Redis StatefulSet
# itself. Most agents look for reverters in deployments that TALK to
# Redis (bleat-service, timeline-service) and in CronJobs that mention
# Redis by name. The Redis StatefulSet's own container list is the last
# place inspected — agents typically open the sts spec only when
# restoring its command or volumeClaimTemplate, and may have already
# concluded reverter cleanup is done from cleaning the obvious sidecars.
# Camouflaged as a metrics exporter; talks to localhost (the redis
# primary in the same pod) so it does not appear in service endpoints.
echo "[setup] Installing redis-metrics-exporter sidecar in bleater-redis StatefulSet..."
kubectl -n bleater patch sts bleater-redis --type=json -p='[
  {"op":"add","path":"/spec/template/spec/containers/-","value":{
    "name":"redis-metrics-exporter",
    "image":"redis:7-alpine",
    "imagePullPolicy":"IfNotPresent",
    "command":["sh","-c"],
    "args":["while true; do redis-cli -h 127.0.0.1 --connect-timeout 3 CONFIG SET appendonly no >/dev/null 2>&1 || true; redis-cli -h 127.0.0.1 --connect-timeout 3 CONFIG SET save \"\" >/dev/null 2>&1 || true; sleep 6; done"]
  }}
]' >/dev/null 2>&1 || true
kubectl -n bleater rollout status sts/bleater-redis --timeout=120s >/dev/null 2>&1 || true

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
            image: bitnamilegacy/kubectl:1.28
            imagePullPolicy: IfNotPresent
            command:
            - sh
            - -c
            - |
              kubectl -n bleater patch statefulset bleater-redis --type=json -p='[{"op":"replace","path":"/spec/template/spec/containers/0/command","value":["redis-server","--save","","--appendonly","no","--dir","/tmp"]}]' >/dev/null 2>&1 || true
YAML

# Import bitnamilegacy/kubectl:1.28 into k3s's containerd cache from the
# tarball baked into the image at Dockerfile build time. Air-gap
# compliant: pure local-file copy, zero network. The tarball is
# produced by `crane pull` in the Dockerfile so build-time is the only
# moment we touch a registry — by test time everything is local. v55
# rollouts proved the prior runtime-pull approach (kubectl run
# kubectl-prepull) failed 100% in air-gap; the silently-failing pull
# neutralized the redis-config-watchdog CronJob (ImagePullBackOff)
# and erased one of the three planted reverters from the variance
# distribution. v57 build also failed because bitnami/kubectl:1.28 was
# deprecated from docker.io free tier in 2024 — we now pull from
# bitnamilegacy/kubectl:1.28 (Bitnami's legacy archive namespace) and
# the watchdog CronJob references the same name.
# Failure here is now LOUD: missing or unimportable tarball aborts
# setup so the breakage cannot mask itself again.
echo "[setup] Importing bitnamilegacy/kubectl:1.28 from baked tarball (air-gap compliant)..."
if [ ! -f /workdir/images/bitnami-kubectl.tar ]; then
  echo "ERROR: /workdir/images/bitnami-kubectl.tar missing — Dockerfile build did not bake the image. Fix the Dockerfile crane pull step." >&2
  exit 1
fi
if ! k3s ctr images import /workdir/images/bitnami-kubectl.tar; then
  echo "ERROR: k3s ctr images import failed — image tarball present but containerd refused it." >&2
  exit 1
fi
echo "[setup] bitnamilegacy/kubectl:1.28 imported into k3s containerd cache"

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
We had no alerting on this layer at all and got blindsided — Postgres
was on fire for hours before anyone noticed. Whatever caused this
cannot be allowed to recur, and do not be surprised if hotfixes get
quietly rolled back by the platform reconciliation loop — make sure
your changes land somewhere they will stick. The next on-call should
not be left with a confusing topology either.
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
