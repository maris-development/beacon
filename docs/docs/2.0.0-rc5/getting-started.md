---
description: Deploy a Beacon server with Docker, on local disk or on S3 object storage. Then register datasets, secure it and expose it to clients.
---

# Getting Started

This guide deploys a Beacon node with Docker. The **[Quick Start](#quick-start)** below is the
fastest path: start the server, add data, then explore it in the admin UI. The **[Local](#local)**
and **[S3](#s3-compatible-object-storage)** sections show production-shaped Docker Compose setups.
The [beacon-example repository](https://github.com/maris-development/beacon-example) holds ready-made
examples with MinIO and sample datasets.

Running a node is four jobs, in this order:

1. **Deploy** it, on this page.
2. **[Configure](/docs/2.0.0-rc5/server/configuration)** the ports, the datasets store and the
   resource limits.
3. **[Register your data](/docs/2.0.0-rc5/server/)** as tables and views, so clients query names
   instead of paths.
4. **[Secure and expose](/docs/2.0.0-rc5/security/access-control)** it, then point clients at it.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/), for the setups below

## Quick Start

Start the server and run your first query in a few minutes.

### 1. Run Beacon

Open the folder that holds your data. Then run:

```bash
docker run -d \
  --name beacon \
  -p 5001:5001 \
  -e BEACON_ADMIN_USERNAME=admin \
  -e BEACON_ADMIN_PASSWORD=securepassword \
  -v ./datasets:/beacon/data/datasets \
  -v ./tables:/beacon/data/tables \
  ghcr.io/maris-development/beacon:latest
```

Beacon now serves on <http://localhost:5001>.

### 2. Add data

Copy supported files into the `./datasets` folder. Supported files include `.parquet`, `.nc`,
`.zarr` and `.csv`. Beacon finds them automatically. There is no import step.

### 3. Explore in the Admin UI

Open <http://localhost:5001/admin>. Sign in with the admin user name and password from step 1
(`admin` / `securepassword`). The server and the Docker image include the
[admin web UI](/docs/2.0.0-rc5/connect/web-admin-ui). You deploy nothing extra. The UI gives you:

- **Query editor**: write SQL, run it (⌘/Ctrl + Enter), read the results and download CSV or Parquet.
- **Datasets**: browse the files that Beacon found and inspect their schemas.
- **Tables**: create and manage tables over your datasets.
- **Crawlers and external tables**: automate discovery and register external sources.
- **Server**: runtime information, health and the available functions.

### 4. Or query over HTTP

Every request goes to one endpoint. Beacon streams back a file in the format that you ask for:

```bash
curl -X POST http://localhost:5001/api/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT * FROM read_parquet([\"**/*.parquet\"]) LIMIT 10",
    "output": { "format": "csv" }
  }'
```

The interactive API docs are at <http://localhost:5001/swagger/>.

## Local

Write a `docker-compose.yml` for a repeatable setup. The longer `docker run` command below does the
same. Point the volume paths at your datasets:

::: code-group

```bash [docker run]
docker run -d \
    --name beacon \
    --restart unless-stopped \
    -p 5001:5001 \
    -p 32011:32011 \
    -e BEACON_ADMIN_USERNAME=admin \
    -e BEACON_ADMIN_PASSWORD=securepassword \
    -v ./datasets:/beacon/data/datasets \
    -v ./tables:/beacon/data/tables \
    -v ./logs:/beacon/logs \
    ghcr.io/maris-development/beacon:latest
```

```yaml [docker-compose.yml]
services:
    beacon:
        image: ghcr.io/maris-development/beacon:latest
        container_name: beacon
        restart: unless-stopped
        ports:
            - "5001:5001"   # HTTP API
            - "32011:32011" # Arrow Flight SQL
        environment:
            - BEACON_ADMIN_USERNAME=admin
            - BEACON_ADMIN_PASSWORD=securepassword
        volumes:
            - ./datasets:/beacon/data/datasets
            - ./tables:/beacon/data/tables
            - ./logs:/beacon/logs
```

:::

For Compose, run `docker compose up -d`. Beacon now runs. Open the
[admin UI](/docs/2.0.0-rc5/connect/web-admin-ui) at `http://localhost:5001/admin` to explore and
query. Open `http://localhost:5001/swagger` for the API docs. You can query any file in `./datasets`
at once.

::: tip Log files
The `./logs` volume writes the log files to your machine. Beacon starts one file each day, for
example `beacon.log.2026-08-19`. Without the volume the files stay in the container. See
[Log files](/docs/2.0.0-rc5/server/configuration#log-files).
:::

::: tip Two ways to connect
Beacon exposes two endpoints. The **HTTP API** on port `5001` serves SQL and JSON queries, the admin
UI and the OpenAPI docs. The **Arrow Flight SQL** server on port `32011` uses a columnar protocol
with high throughput. Clients such as
[JetBrains DataGrip](/docs/2.0.0-rc5/connect/datagrip) and the
[Python ADBC driver](/docs/2.0.0-rc5/connect/python-adbc) use it. Flight SQL authenticates with a
bearer token. Tune it or switch it off with the `BEACON_FLIGHT_SQL_*`
[settings](/docs/2.0.0-rc5/server/configuration#arrow-flight-sql).
:::

::: warning Secure your instance
The `BEACON_ADMIN_*` credentials protect the admin UI and all write operations. **Change them from
the defaults** before you expose Beacon. To control who reads data, switch on
[access control](/docs/2.0.0-rc5/security/access-control) with `BEACON_AUTH_ENFORCE=true`.
:::

## S3-Compatible Object Storage

Add the S3 environment variables. Then remove the datasets volume:

::: code-group

```bash [docker run]
docker run -d \
    --name beacon \
    --restart unless-stopped \
    -p 5001:5001 \
    -p 32011:32011 \
    -e BEACON_ADMIN_USERNAME=admin \
    -e BEACON_ADMIN_PASSWORD=securepassword \
    -e AWS_ENDPOINT=https://s3.amazonaws.com \
    -e AWS_ACCESS_KEY_ID=your-access-key \
    -e AWS_SECRET_ACCESS_KEY=your-secret-key \
    -e BEACON_S3_BUCKET=your-bucket-name \
    -e BEACON_S3_DATASETS=true \
    -v ./tables:/beacon/data/tables \
    -v ./logs:/beacon/logs \
    ghcr.io/maris-development/beacon:latest
```

```yaml [docker-compose.yml]
services:
    beacon:
        image: ghcr.io/maris-development/beacon:latest
        container_name: beacon
        restart: unless-stopped
        ports:
            - "5001:5001"
            - "32011:32011"
        environment:
            - BEACON_ADMIN_USERNAME=admin
            - BEACON_ADMIN_PASSWORD=securepassword
            - AWS_ENDPOINT=https://s3.amazonaws.com
            - AWS_ACCESS_KEY_ID=your-access-key
            - AWS_SECRET_ACCESS_KEY=your-secret-key
            - BEACON_S3_BUCKET=your-bucket-name
            - BEACON_S3_DATASETS=true
        volumes:
            - ./tables:/beacon/data/tables
            - ./logs:/beacon/logs
```

:::

:::tip Anonymous / public buckets
For a public bucket, remove `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. Add
`AWS_SKIP_SIGNATURE=true` instead.
:::

For Compose, run `docker compose up -d`. You can query the files in the S3 bucket at once. The
`./tables` volume keeps the external tables and views that you create.

## Next steps

**Configure the node**

| | |
| - | - |
| **Every setting** | [Configuration](/docs/2.0.0-rc5/server/configuration) |
| **Put the datasets on a bucket** | [Object Storage](/docs/2.0.0-rc5/data-sources/object-storage) |
| **Memory, concurrency, caches** | [Performance Tuning](/docs/2.0.0-rc5/server/performance-tuning) |

**Register the data**

| | |
| - | - |
| **Name a set of files** | [External Tables](/docs/2.0.0-rc5/data-sources/external-tables) |
| **Save a query** | [Views](/docs/2.0.0-rc5/server/view) · [Materialized Views](/docs/2.0.0-rc5/sql/create-materialized-view) |
| **Register a large tree on a schedule** | [Crawlers](/docs/2.0.0-rc5/server/crawlers) |
| **Own the rows yourself** | [Managed Tables](/docs/2.0.0-rc5/sql/managed-tables) |
| **Reach another node or a database** | [ATTACH](/docs/2.0.0-rc5/data-sources/attach) · [SQL Databases](/docs/2.0.0-rc5/data-sources/sql-databases) |

**Expose it**

| | |
| - | - |
| **Decide who reads what** | [Access Control](/docs/2.0.0-rc5/security/access-control) |
| **Explore in the browser** | [Admin Web UI](/docs/2.0.0-rc5/connect/web-admin-ui) |
| **Point clients at it** | [Python](/docs/2.0.0-rc5/connect/python) · [TypeScript](/docs/2.0.0-rc5/connect/typescript) · [CLI](/docs/2.0.0-rc5/connect/cli) · [DataGrip](/docs/2.0.0-rc5/connect/datagrip) · [Python ADBC](/docs/2.0.0-rc5/connect/python-adbc) |
| **Document the query API** | [REST API](/docs/2.0.0-rc5/api/) |

**When something is wrong**

[Troubleshooting](/docs/2.0.0-rc5/troubleshooting) · [FAQ](/docs/2.0.0-rc5/faq)
