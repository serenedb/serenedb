# SereneDB Helm Chart

Deploys a **single-node** SereneDB server: a StatefulSet (1 replica) with a
persistent data volume, client and headless Services, generated superuser
credentials, and a flagfile ConfigMap.

SereneDB is currently a single-machine database, so this chart intentionally
has no `replicaCount` -- there is exactly one database pod. Upgrades and config
changes restart that pod; expect a short downtime window (graceful shutdown +
startup/recovery). Clients should reconnect on connection loss.

## Installing

```bash
helm install mydb ./serenedb
helm test mydb                       # runs SELECT 1 against the service
```

Connect (see the printed NOTES for the password):

```bash
psql -h mydb-serenedb.<namespace>.svc -p 7890 -U postgres -d postgres
```

## Upgrading

```bash
helm upgrade mydb ./serenedb --set image.tag=<new version>
```

The pod is recreated on the same PersistentVolumeClaim: serened shuts down
gracefully (checkpoints, drains background tasks) and the new version opens the
same data directory. The `terminationGracePeriodSeconds` default (120s) exists
for that shutdown; raise it for large datadirs.

## Passwords

`auth.password` (or the generated random password) is passed to serened as
`POSTGRES_PASSWORD` and is only honored on the **first boot with an empty data
directory**. Changing it later happens in SQL (`ALTER ROLE postgres PASSWORD
...`), not through chart values. The release Secret carries
`helm.sh/resource-policy: keep` so the password survives uninstall/reinstall
together with the data PVC.

**GitOps users (Argo CD, Flux, `helm template` pipelines):** the
generated-password fallback uses `lookup`, which requires cluster access at
render time. Without it a new random password is rendered on every sync. Set
`auth.password` or `auth.existingSecret` explicitly.

## Data lifecycle

`helm uninstall` keeps the data PVC (`data-<release>-serenedb-0`) by default;
delete it explicitly to wipe data, or set
`persistence.retentionPolicy.whenDeleted=Delete` (k8s >= 1.27) to couple data
lifetime to the release.

## Values

| Key | Default | Description |
|---|---|---|
| `image.repository` | `serenedb/serenedb` | Image repository |
| `image.tag` | `""` (= appVersion) | Image tag; pin for production |
| `image.pullPolicy` | `IfNotPresent` | |
| `auth.password` | `""` (generate) | Initial `postgres` password, first boot only |
| `auth.existingSecret` | `""` | Existing Secret with key `postgres-password` |
| `listeners.postgres.port` | `7890` | pg-wire listener port |
| `listeners.postgres.sslmode` | `""` | Per-listener `?sslmode=` (e.g. `require`) |
| `listeners.http.enabled` | `false` | Elasticsearch-compatible HTTP listener |
| `listeners.http.port` | `9200` | |
| `listeners.http.corsOrigins` | `""` | `--http_cors_origins` |
| `tls.enabled` | `false` | Enable TLS; requires `tls.existingSecret` |
| `tls.existingSecret` | `""` | `kubernetes.io/tls` Secret (cert-manager compatible) |
| `tls.minVersion` | `"1.2"` | |
| `config.logLevel` | `info` | |
| `config.maxConnections` | `0` | 0 = unlimited |
| `config.cpuThreads` / `ioThreads` / `backgroundThreads` | `0` | 0 = auto-detect (sees node cores, not pod limits -- set explicitly with small CPU limits) |
| `config.authTimeout` | `30s` | |
| `config.idleSessionTimeout` | `75s` | |
| `config.pgMaxMessageBytes` | `0` | 0 = built-in default |
| `config.hba` | `""` | pg_hba.conf content; empty = engine default |
| `config.extraFlags` | `[]` | Extra serened flags, verbatim |
| `persistence.size` | `20Gi` | Data volume size |
| `persistence.storageClass` | `""` | `""` = cluster default |
| `persistence.existingClaim` | `""` | Use a pre-created PVC |
| `persistence.mountPath` | `/var/lib/serenedb` | Data directory |
| `persistence.retentionPolicy.whenDeleted` | `Retain` | PVC fate on uninstall (k8s >= 1.27) |
| `resources` | `{}` | Recommended: requests = limits, >= 2 CPU / 4Gi |
| `terminationGracePeriodSeconds` | `120` | Graceful-shutdown budget |
| `podSecurityContext` | `fsGroup: 0` | Image uses group-0-writable datadir (OpenShift-friendly) |
| `containerSecurityContext` | non-root, no caps | Drops all capabilities; NUMA pinning in the entrypoint no-ops without `SYS_NICE` |
| `automountServiceAccountToken` | `false` | |
| `updateStrategy.type` | `RollingUpdate` | |
| `readinessProbe` / `livenessProbe` / `startupProbe` | enabled | `pg_isready`-based; startup probe is lenient because the listener binds only after indexes load |
| `service.type` | `ClusterIP` | |
| `service.nodePort` | `""` | Pin NodePort when `service.type=NodePort` |
| `networkPolicy.enabled` | `false` | Ingress restricted to serving ports |
| `extraEnv` | `[]` | Extra container env vars |
| `initContainers` / `sidecars` | `[]` | Rendered verbatim into the pod |
| `extraVolumes` / `extraVolumeMounts` | `[]` | |
| `extraObjects` | `[]` | Additional manifests (templated) |
| `podAnnotations` / `podLabels` / `nodeSelector` / `tolerations` / `affinity` / `priorityClassName` | | Standard passthroughs |
