---
description: Run Beacon with Docker in minutes — locally or against S3-compatible object storage — then point it at your files and query them over SQL or JSON.
---

# Getting Started

This guide gets a Beacon instance running with Docker Compose. For ready-made examples including MinIO and sample datasets, see the [beacon-example repository](https://github.com/maris-development/beacon-example).

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

## Local

Start Beacon with a single `docker run`, or define a `docker-compose.yml` for a reproducible setup. Either way, adjust the volume paths to point at your datasets:

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

If you used Compose, start it with `docker compose up -d`. Beacon is now running. Open `http://localhost:5001/swagger` to verify and explore the API. Any files placed in `./datasets` are immediately available for querying.

::: tip Two ways to connect
Beacon exposes two endpoints. The **HTTP API** on port `5001` serves SQL/JSON queries and the OpenAPI docs. The **Arrow Flight SQL** server on port `32011` is a high-throughput, columnar protocol used by clients such as [JetBrains DataGrip](./connect/jetbrains-datagrip.md) and the [Python ADBC driver](./connect/python-adbc.md). Flight SQL uses bearer-token authentication and can be tuned or disabled via the `BEACON_FLIGHT_SQL_*` [settings](./data-lake/configuration.md#arrow-flight-sql).
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
| **Connect a client** | [JetBrains DataGrip](./connect/jetbrains-datagrip.md) · [Python ADBC](./connect/python-adbc.md) |
| **Register datasets as SQL tables** | [External Tables](./data-lake/external-tables.md) · [Views](./data-lake/view.md) |
| **Write queries** | [SQL Guide](./sql/index.md) |
| **Tune performance** | [Performance Tuning](./data-lake/performance-tuning.md) |
