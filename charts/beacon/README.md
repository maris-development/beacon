# Beacon Helm chart

Deploy the [Beacon](https://github.com/maris-development/beacon) data lake query
engine on Kubernetes.

Beacon is a query engine server: point it at an archive of scientific files
(NetCDF, Zarr, Parquet, CSV, …) on a local volume or an S3 bucket, and query it
over SQL / JSON (HTTP API + admin UI on port `5001`) and Arrow Flight SQL
(port `32011`).

## Install

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
| `replicaCount` | Replicas (keep at 1 unless `tables` is RWX) | `1` |
| `kind` | `Deployment` or `StatefulSet` | `Deployment` |
| `image.repository` / `image.tag` | Beacon image | `ghcr.io/maris-development/beacon` / `latest` |
| `beacon.port` | HTTP API + admin UI port | `5001` |
| `beacon.basePath` | URL prefix for all routes | `""` |
| `beacon.logLevel` | `trace`…`error`/`off` | `info` |
| `beacon.dataDir` | Data root in-container | `/beacon/data` |
| `beacon.admin.adminUsername` | Admin user | `admin` |
| `beacon.admin.adminPassword` | Admin password (random if empty) | `""` |
| `beacon.admin.existingSecret` | Secret with `BEACON_ADMIN_USERNAME/PASSWORD` | `""` |
| `beacon.flightSql.*` | Arrow Flight SQL endpoint | enabled, `32011` |
| `beacon.oidc.*` | OIDC auth delegation | disabled |
| `beacon.s3.*` | Back datasets with an S3 bucket | disabled |
| `beacon.extraEnv` / `extraEnvFrom` | Extra env / envFrom | `[]` |
| `persistence.*` | PVC for `dataDir` (datasets + catalog) | enabled, 20Gi |
| `service.*` | Service type and ports | `ClusterIP` |
| `ingress.*` | Ingress | disabled |
| `resources` | Container resources | `{}` |
| `autoscaling.*` | HPA (Deployment only) | disabled |

### Persistence and replicas

Beacon keeps its catalog (`beacon.db`) under `dataDir/tables`. Running more than
one replica requires a `ReadWriteMany` volume for that path (or an external
store). Otherwise keep `replicaCount: 1`, or use `kind: StatefulSet` with a
single replica for a stable per-pod volume.

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

## Site overlays

Keep `values.yaml` generic. Provide a site-specific overlay with `-f my-values.yaml`
for your ingress host, storage class, bucket, resource sizing and (if used)
service-account IAM annotations. Do not commit environment-specific
infrastructure details into the chart defaults.
