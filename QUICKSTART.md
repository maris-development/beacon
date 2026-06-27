# Beacon — Quick Start

Get a Beacon query engine running and query your first dataset in a couple of
minutes. For the full picture, see the [README](README.md) and the
[documentation](https://maris-development.github.io/beacon/).

## 1. Run Beacon

You need [Docker](https://docs.docker.com/get-docker/). From a folder where you
want your data to live:

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

## 2. Add data

Drop any supported files (e.g. `.parquet`, `.nc`, `.zarr`, `.csv`) into the
`./datasets` folder you just mounted. Beacon discovers them automatically — no
import step.

## 3. Explore in the Admin UI

Open <http://localhost:5001/admin> and sign in with the admin username and
password you set above (`admin` / `securepassword`).

From the UI you can:

- **Query editor** — write SQL, run it (⌘/Ctrl + Enter), view results, and
  download CSV/Parquet.
- **Datasets** — browse discovered files and inspect their schemas.
- **Tables** — register and manage queryable tables over your datasets.
- **Crawlers & external tables** — automate discovery and register external sources.
- **Server** — runtime info, health, and available functions.

## 4. Or query over HTTP

Every request goes to a single endpoint and streams back a file in the format
you ask for:

```bash
curl -X POST http://localhost:5001/api/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT * FROM read_parquet([\"datasets/**/*.parquet\"]) LIMIT 10",
    "output": { "format": "csv" }
  }'
```

Interactive API docs are at <http://localhost:5001/swagger/>.

## Next steps

- [README](README.md) — features, configuration, and query examples.
- [Documentation](https://maris-development.github.io/beacon/) — full data model and API reference.
- [Configuration reference](https://maris-development.github.io/beacon/docs/1.8.0/data-lake/configuration.html) — all `BEACON_*` settings.
- Community [Slack](https://beacontechnic-wwa5548.slack.com/join/shared_invite/zt-2dp1vv56r-tj_KFac0sAKNuAgUKPPDRg).
