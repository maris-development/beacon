---
description: Run Beacon with Docker in minutes — locally or against S3-compatible object storage — explore it in the bundled admin UI, then query your files over SQL or JSON.
---

# Getting Started

This guide gets a Beacon instance running with Docker. The **[Quick Start](#quick-start)** below is the fastest path — run, add data, and explore in the bundled admin UI. The **[Local](#local)** and **[S3](#s3-compatible-object-storage)** sections that follow cover reproducible Docker Compose setups. For ready-made examples including MinIO and sample datasets, see the [beacon-example repository](https://github.com/maris-development/beacon-example).

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/) — for the reproducible setups below

## Quick Start

Get running and querying in a couple of minutes.

### 1. Run Beacon

From a folder where you want your data to live:

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

That's it — Beacon is now serving on <http://localhost:5001>.

### 2. Add data

Drop any supported files (e.g. `.parquet`, `.nc`, `.zarr`, `.csv`) into the `./datasets` folder you just mounted. Beacon discovers them automatically — there is no import step.

### 3. Explore in the Admin UI

Open <http://localhost:5001/admin> and sign in with the admin username and password you set above (`admin` / `securepassword`). Beacon bundles an [admin web UI](./connect/web-admin-ui.md) into the server and Docker image — nothing extra to deploy. From it you can:

- **Query editor** — write SQL, run it (⌘/Ctrl + Enter), view results, and download CSV/Parquet.
- **Datasets** — browse discovered files and inspect their schemas.
- **Tables** — register and manage queryable tables over your datasets.
- **Crawlers & external tables** — automate discovery and register external sources.
- **Server** — runtime info, health, and the available functions.

### 4. Or query over HTTP

Every request goes to a single endpoint and streams back a file in the format you ask for:

```bash
curl -X POST http://localhost:5001/api/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT * FROM read_parquet([\"datasets/**/*.parquet\"]) LIMIT 10",
    "output": { "format": "csv" }
  }'
```

Interactive API docs are at <http://localhost:5001/swagger/>.

## Local

For a reproducible setup, define a `docker-compose.yml` (or use the fuller `docker run` below). Either way, adjust the volume paths to point at your datasets:

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
```

:::

If you used Compose, start it with `docker compose up -d`. Beacon is now running. Open the [admin UI](./connect/web-admin-ui.md) at `http://localhost:5001/admin` to explore and query, or `http://localhost:5001/swagger` for the API docs. Any files placed in `./datasets` are immediately available for querying.

::: tip Two ways to connect
Beacon exposes two endpoints. The **HTTP API** on port `5001` serves SQL/JSON queries, the admin UI, and the OpenAPI docs. The **Arrow Flight SQL** server on port `32011` is a high-throughput, columnar protocol used by clients such as [JetBrains DataGrip](./connect/jetbrains-datagrip.md) and the [Python ADBC driver](./connect/python-adbc.md). Flight SQL uses bearer-token authentication and can be tuned or disabled via the `BEACON_FLIGHT_SQL_*` [settings](./data-lake/configuration.md#arrow-flight-sql).
:::

::: warning Secure your instance
The `BEACON_ADMIN_*` credentials gate the admin UI and all write/management operations — **change them from the defaults** before exposing Beacon. To restrict who can read data, enable [access control](./security/access-control.md) (`BEACON_AUTH_ENFORCE=true`).
:::

## S3-Compatible Object Storage

Add the S3 environment variables and remove the datasets volume:

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
    -e BEACON_S3_DATA_LAKE=true \
    -v ./tables:/beacon/data/tables \
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
            - BEACON_S3_DATA_LAKE=true
        volumes:
            - ./tables:/beacon/data/tables
```

:::

:::tip Anonymous / public buckets
For publicly accessible buckets, omit `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` and add `AWS_SKIP_SIGNATURE=true` instead.
:::

If you used Compose, start it with `docker compose up -d`. Files already in the S3 bucket are available for querying immediately. The `./tables` volume persists any external tables or views you create.

## Next steps

| | |
| - | - |
| **Explore in the browser** | [Admin Web UI](./connect/web-admin-ui.md) |
| **Connect a client** | [JetBrains DataGrip](./connect/jetbrains-datagrip.md) · [Python ADBC](./connect/python-adbc.md) · [TypeScript SDK](./connect/beacon-typescript-sdk.md) |
| **Register datasets as SQL tables** | [External Tables](./data-lake/external-tables.md) · [Views](./data-lake/view.md) |
| **Write queries** | [SQL Guide](./sql/index.md) |
| **Secure access** | [Authentication & Access Control](./security/access-control.md) |
| **Tune performance** | [Performance Tuning](./data-lake/performance-tuning.md) |
