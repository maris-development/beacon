---
description: Start Beacon with Docker in minutes, on local disk or on S3 object storage. Explore it in the admin UI, then query your files.
---

# Getting Started

This guide starts a Beacon server with Docker. The **[Quick Start](#quick-start)** below is the
fastest path. Start the server, add data, then explore the data in the admin UI. The
**[Local](#local)** and **[S3](#s3-compatible-object-storage)** sections show Docker Compose setups.
The [beacon-example repository](https://github.com/maris-development/beacon-example) holds ready-made
examples with MinIO and sample datasets.

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
[admin web UI](/docs/2.0.0-rc2/connect/web-admin-ui). You deploy nothing extra. The UI gives you:

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
    "sql": "SELECT * FROM read_parquet([\"datasets/**/*.parquet\"]) LIMIT 10",
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

For Compose, run `docker compose up -d`. Beacon now runs. Open the
[admin UI](/docs/2.0.0-rc2/connect/web-admin-ui) at `http://localhost:5001/admin` to explore and
query. Open `http://localhost:5001/swagger` for the API docs. You can query any file in `./datasets`
at once.

::: tip Two ways to connect
Beacon exposes two endpoints. The **HTTP API** on port `5001` serves SQL and JSON queries, the admin
UI and the OpenAPI docs. The **Arrow Flight SQL** server on port `32011` uses a columnar protocol
with high throughput. Clients such as
[JetBrains DataGrip](/docs/2.0.0-rc2/connect/jetbrains-datagrip) and the
[Python ADBC driver](/docs/2.0.0-rc2/connect/python-adbc) use it. Flight SQL authenticates with a
bearer token. Tune it or switch it off with the `BEACON_FLIGHT_SQL_*`
[settings](/docs/2.0.0-rc2/data-lake/configuration#arrow-flight-sql).
:::

::: warning Secure your instance
The `BEACON_ADMIN_*` credentials protect the admin UI and all write operations. **Change them from
the defaults** before you expose Beacon. To control who reads data, switch on
[access control](/docs/2.0.0-rc2/security/access-control) with `BEACON_AUTH_ENFORCE=true`.
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
For a public bucket, remove `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. Add
`AWS_SKIP_SIGNATURE=true` instead.
:::

For Compose, run `docker compose up -d`. You can query the files in the S3 bucket at once. The
`./tables` volume keeps the external tables and views that you create.

## Next steps

| | |
| - | - |
| **Explore in the browser** | [Admin Web UI](/docs/2.0.0-rc2/connect/web-admin-ui) |
| **Connect a client** | [JetBrains DataGrip](/docs/2.0.0-rc2/connect/jetbrains-datagrip) · [Python ADBC](/docs/2.0.0-rc2/connect/python-adbc) · [TypeScript SDK](/docs/2.0.0-rc2/connect/beacon-typescript-sdk) |
| **Register datasets as SQL tables** | [External Tables](/docs/2.0.0-rc2/beacondb/data-sources/external-tables) · [Views](/docs/2.0.0-rc2/data-lake/view) |
| **Write queries** | [SQL Guide](/docs/2.0.0-rc2/beacondb/sql/) |
| **Secure access** | [Authentication & Access Control](/docs/2.0.0-rc2/security/access-control) |
| **Tune performance** | [Performance Tuning](/docs/2.0.0-rc2/data-lake/performance-tuning) |
