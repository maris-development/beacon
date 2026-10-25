# Beacon Helm chart

Deploy the [Beacon](https://github.com/maris-development/beacon) data lake query
engine on Kubernetes.

Beacon is a query engine server: point it at an archive of scientific files
(NetCDF, Zarr, Parquet, CSV, …) on a local volume or an S3 bucket, and query it
over SQL / JSON (HTTP API + admin UI on port `5001`) and Arrow Flight SQL
(port `32011`).

## Install

Install the chart from GHCR. The chart version is the Beacon server version:

```bash
helm install beacon oci://ghcr.io/maris-development/charts/beacon \
  --version 2.0.2 --namespace beacon --create-namespace
```

To install from a clone of the repository, use the chart folder:

```bash
helm install beacon ./charts/beacon \
  --namespace beacon --create-namespace
```

Then port-forward and open the admin UI:

```bash
kubectl -n beacon port-forward svc/beacon 5001:5001
# http://localhost:5001/admin
```

Fetch the generated admin password:

```bash
kubectl -n beacon get secret beacon-admin \
  -o jsonpath='{.data.BEACON_ADMIN_PASSWORD}' | base64 -d; echo
```

## Configuration

All settings live under `values.yaml`. The chart maps values to the server's
`BEACON_*` / `AWS_*` environment variables (see the
[configuration reference](https://maris-development.github.io/beacon/)).

| Key | Description | Default |
|-----|-------------|---------|
| `replicaCount` | Replicas: `1`, or `0` to stop Beacon | `1` |
| `kind` | `Deployment` or `StatefulSet` | `Deployment` |
| `updateStrategy` | Deployment `strategy` / StatefulSet `updateStrategy` | `Recreate` with persistence |
| `image.repository` / `image.tag` | Beacon image | `ghcr.io/maris-development/beacon` / chart `appVersion` |
| `beacon.port` | HTTP API + admin UI port | `5001` |
| `beacon.basePath` | URL prefix for all routes | `""` |
| `beacon.logLevel` | `trace`…`error`/`off` | `info` |
| `beacon.dataDir` | Data root in-container | `/beacon/data` |
| `beacon.admin.adminUsername` | Admin user | `admin` |
| `beacon.admin.adminPassword` | Admin password (random if empty) | `""` |
| `beacon.admin.existingSecret` | Secret with `BEACON_ADMIN_USERNAME/PASSWORD` | `""` |
| `beacon.secretsKey.*` | `BEACON_SECRETS_KEY` from a value or an existing Secret | unset |
| `beacon.flightSql.*` | Arrow Flight SQL endpoint | enabled, `32011` |
| `beacon.oidc.*` | OIDC auth delegation | disabled |
| `beacon.s3.*` | Back datasets with an S3 bucket | disabled |
| `beacon.pluginSources` | `BEACON_PLUGIN_SOURCES`: allowed remote plugin source prefixes, comma list | `""` |
| `beacon.plugins` | `BEACON_PLUGINS`: plugins to install at startup, `name=source` comma list | `""` |
| `beacon.extraEnv` / `extraEnvFrom` | Extra env / envFrom | `[]` |
| `persistence.*` | PVC for `dataDir` (datasets + catalog) | enabled, 20Gi |
| `service.*` | Service type and ports | `ClusterIP` |
| `ingress.*` | Ingress | disabled |
| `resources` | Container resources | `{}` |
| `livenessProbe` / `readinessProbe` | Probes; an empty `httpGet.path` checks `{basePath}/api/health` | enabled |

### Persistence and replicas

Beacon keeps its catalog (`beacon.db`) under `dataDir/tables`. Beacon takes an
exclusive lock on that file, so only one Beacon process can run against it. A
`ReadWriteMany` volume does not change this. The chart fails when
`replicaCount` is above `1`, and it has no autoscaler.

A Deployment with persistence updates with `Recreate`. The old pod stops and
releases the volume before the new pod starts, so an upgrade has a short
downtime.

`kind: StatefulSet` gives the pod a volume from `volumeClaimTemplates`. With
`persistence.existingClaim` or `persistence.enabled: false`, it mounts that
claim or an `emptyDir` instead.

### Admin password and GitOps

When `beacon.admin.adminPassword` and `beacon.admin.existingSecret` are empty,
the chart generates a password. On `helm upgrade` it reads the existing Secret
and keeps that password. Argo CD, Flux and `helm template` do not let the chart
read the cluster, so they get a new password on every render. With those tools,
set `adminPassword` or `existingSecret`.

### Secrets key

`BEACON_SECRETS_KEY` encrypts the stored credentials of external SQL database
tables. Without it, Beacon rejects a database table with a password. Beacon
does not generate the key. Create one and keep it in a Secret:

```bash
kubectl -n beacon create secret generic beacon-secrets-key --from-literal=BEACON_SECRETS_KEY="$(openssl rand -base64 32)"
```

```yaml
beacon:
  secretsKey:
    existingSecret: beacon-secrets-key
```

A changed or lost key makes stored credentials unreadable. You must then
recreate those tables.

### S3-backed datasets

Set `beacon.s3.enabled: true` and `beacon.s3.bucket`. Credentials follow the
standard AWS chain — prefer **IRSA** or **EKS Pod Identity** (annotate the
service account via `serviceAccount.annotations`) over static keys. The catalog
still lives on the local persistent volume.

```yaml
serviceAccount:
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::<account>:role/<role>
beacon:
  s3:
    enabled: true
    bucket: my-datasets-bucket
    region: us-west-2
```

`beacon.s3.allowHttp` is `false` in the chart, but the server default is
`true`. Set it to `true` for an `http://` endpoint, for example a local MinIO.

### OIDC

```yaml
beacon:
  oidc:
    enabled: true
    issuer: https://issuer.example.com/
    audience: beacon
    jwksUrl: https://issuer.example.com/.well-known/jwks.json
    rolesClaim: realm_access.roles
    usernameClaim: preferred_username
```

`jwksUrl` is required. Beacon does not discover it from the issuer.

## Site overlays

Keep `values.yaml` generic. Provide a site-specific overlay with `-f my-values.yaml`
for your ingress host, storage class, bucket, resource sizing and (if used)
service-account IAM annotations. Do not commit environment-specific
infrastructure details into the chart defaults.
