## Scenario

A recent Helm upgrade of the Redis chart for the bleater platform silently overrode the Redis persistence configuration. The upgrade applied default chart values that set save '' (empty, disabling RDB snapshots) and appendonly no, effectively making Redis fully ephemeral. This was not caught because the CI pipeline only validates that the Helm upgrade succeeds, not that resulting Redis configuration matches the desired persistence spec. When the Redis pod was subsequently restarted due to a node memory pressure event, all cached data was lost — including bleat-service hot cache entries and the timeline-service sorted sets. bleat-service fell back to PostgreSQL for all reads, causing a 10x query spike that degraded database performance. The agent must diagnose the persistence misconfiguration by inspecting current Redis CONFIG GET output, compare against the intended Helm values stored in the GitOps repository, identify the drift introduced by the upgrade, restore correct persistence settings (RDB with sensible save intervals and AOF enabled), validate that a PersistentVolumeClaim is properly mounted for Redis data, and implement a Prometheus alert to detect future persistence configuration drift.

## What the Agent Must Accomplish

Confirm Redis persistence is disabled by querying CONFIG GET save and CONFIG GET appendonly. Identify the Helm values drift between the GitOps repo and deployed state via ArgoCD diff or helm get values. Correct the Helm values to re-enable RDB snapshots (e.g., save 900 1 300 10 60 10000) and AOF persistence. Verify the Redis pod has a PVC mounted at /data and that the PVC is bound. Apply the corrected Helm values via helm upgrade and validate persistence config via CONFIG GET post-upgrade. Confirm bleat-service cache hit rate recovers and PostgreSQL query rate returns to baseline. Add a Prometheus alert rule that fires when redis_rdb_last_bgsave_status is not ok or when persistence config is detected as disabled.

## What Is Broken

- Redis RDB snapshots disabled (save set to empty string) after Helm upgrade applied default values
- Redis AOF persistence disabled (appendonly no) leaving all cache data ephemeral
- Redis PVC may be unmounted or unused due to persistence being disabled in chart values
- bleat-service cache miss rate at 100% after Redis pod restart — all hot cache data lost
- PostgreSQL experiencing 10x query spike as bleat-service falls through to database for every cache miss
- No Prometheus alert exists for Redis persistence misconfiguration

## Success Criteria

- Redis CONFIG GET save returns non-empty save intervals (e.g., 900 1 300 10 60 10000)
- Redis CONFIG GET appendonly returns yes
- kubectl get pvc in bleater namespace shows Redis PVC in Bound state with correct capacity
- Redis pod mounts PVC at /data — confirmed via kubectl describe pod showing volume mount
- bleat-service cache hit rate recovers to above 70% within 15 minutes post-fix as cache warms
- PostgreSQL queries per second returns within 20% of pre-incident baseline in Grafana
- Prometheus alert rule for redis_rdb_last_bgsave_status != ok present and active in AlertManager
- ArgoCD shows no drift between GitOps repo Redis Helm values and deployed state

## Metadata

- **Category:** platform-eng
- **Type:** hybrid
- **Difficulty:** medium
- **Domains:** redis, cache, platform-eng
- **Components:** Redis, bleat-service, timeline-service, Helm, ArgoCD, PersistentVolumeClaim, PostgreSQL, Prometheus
- **Estimated Horizon:** 1 day
