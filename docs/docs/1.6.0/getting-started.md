# Getting Started

This guide gets a Beacon instance running with Docker Compose. For ready-made examples including MinIO and sample datasets, see the [beacon-example repository](https://github.com/maris-development/beacon-example).

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

## Local

Create a `docker-compose.yml` file and adjust the volume path to point at your datasets:

```yaml
services:
    beacon:
        image: ghcr.io/maris-development/beacon:latest
        container_name: beacon
        restart: unless-stopped
        ports:
            - "8080:8080"   # HTTP API
            - "32011:32011" # Arrow Flight SQL
        environment:
            - BEACON_ADMIN_USERNAME=admin
            - BEACON_ADMIN_PASSWORD=securepassword
            - BEACON_VM_MEMORY_SIZE=4096
            - BEACON_HOST=0.0.0.0
            - BEACON_PORT=8080
        volumes:
            - ./datasets:/beacon/data/datasets
            - ./tables:/beacon/data/tables
```

Start Beacon:

```bash
docker compose up -d
```

Beacon is now running. Open `http://localhost:8080/swagger` to verify and explore the API. Any files placed in `./datasets` are immediately available for querying.

## S3-Compatible Object Storage

Add the S3 environment variables to your compose file and remove the datasets volume:

```yaml
services:
    beacon:
        image: ghcr.io/maris-development/beacon:latest
        container_name: beacon
        restart: unless-stopped
        ports:
            - "8080:8080"
            - "32011:32011"
        environment:
            - BEACON_ADMIN_USERNAME=admin
            - BEACON_ADMIN_PASSWORD=securepassword
            - BEACON_VM_MEMORY_SIZE=4096
            - BEACON_HOST=0.0.0.0
            - BEACON_PORT=8080
            - AWS_ENDPOINT=https://s3.amazonaws.com
            - AWS_ACCESS_KEY_ID=your-access-key
            - AWS_SECRET_ACCESS_KEY=your-secret-key
            - BEACON_S3_BUCKET=your-bucket-name
            - BEACON_S3_DATA_LAKE=true
        volumes:
            - ./tables:/beacon/data/tables
```

:::tip Anonymous / public buckets
For publicly accessible buckets, omit `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` and add `AWS_SKIP_SIGNATURE=true` instead.
:::

Start Beacon the same way:

```bash
docker compose up -d
```

Files already in the S3 bucket are available for querying immediately. The `./tables` volume persists any external tables or views you create.

## Next steps

| | |
| - | - |
| **Connect a client** | [JetBrains DataGrip](./connect/jetbrains-datagrip.md) · [Python ADBC](./connect/python-adbc.md) |
| **Register datasets as SQL tables** | [External Tables](./data-lake/external-tables.md) · [Views](./data-lake/view.md) |
| **Write queries** | [SQL Guide](./sql/index.md) |
| **Tune performance** | [Performance Tuning](./data-lake/performance-tuning.md) |
