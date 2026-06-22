# Beacon integration tests

Black-box, end-to-end tests that **build the Beacon Docker image**, run the container,
and exercise the HTTP API: querying generated Parquet in place, creating external
tables, and the full managed-table write lifecycle (`CREATE` / `INSERT` / `UPDATE` /
`DELETE` / `DROP`).

These are intentionally **local / opt-in** — they are not wired into CI, because a full
image build compiles the Rust workspace in release mode and takes a while.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) (the `docker` CLI must be on `PATH`;
  the whole suite is skipped if it is not).
- Python 3.10+.

## Running

```powershell
# Windows
./run.ps1
```

```bash
# macOS / Linux
./run.sh
```

The runner creates a `.venv`, installs `requirements.txt`, and invokes `pytest`. Extra
arguments pass through to pytest:

```bash
./run.sh -v -k external      # only the external-table tests, verbose
```

To run pytest directly in an existing environment:

```bash
pip install -r requirements.txt
pytest -v
```

## Configuration

| Env var | Default | Effect |
| ------- | ------- | ------ |
| `BEACON_IMAGE` | _(unset)_ | If set, run this image instead of building the local `Dockerfile`. Use `ghcr.io/maris-development/beacon:latest` for fast reruns. |

> **Note:** The published image will **fail** the external- and managed-table tests
> until the HTTP write-path change in `beacon-api` (admin basic-auth → super-user on
> `POST /api/query`) ships in a release. Build the local image to validate the full
> suite. This mismatch is itself a useful regression signal.

## What runs

| File | Covers |
| ---- | ------ |
| `test_health.py` | `/api/health`, `/swagger`, `beacon_version()`, `/api/datasets`, `list_datasets()` |
| `test_queries_parquet.py` | `read_parquet()` counts/aggregates over a multi-file glob; equivalent JSON DSL; malformed-query 400 |
| `test_external_tables.py` | `CREATE EXTERNAL TABLE` (admin SQL + `POST /api/admin/external-tables`); `SHOW TABLES` / `/api/tables`; no-auth → 400/401, bad-auth → 401 |
| `test_admin_crawlers.py` | admin crawler CRUD + run (`/api/admin/crawlers*`): create/list/get/run/drop, 404 for unknown, 401 without auth |
| `test_managed_tables.py` | managed-table `CREATE`/`INSERT`/`SELECT`/`UPDATE`/`DELETE`/`DROP`; writes need admin |

## How it works

`conftest.py` provides session-scoped fixtures that:

1. Build (or reuse) the image.
2. Generate small deterministic Parquet files (plus a CSV) into a temp datasets dir with
   `pyarrow`, and an empty temp tables dir for managed-table storage.
3. `docker run` Beacon with those dirs mounted at `/beacon/data/datasets` and
   `/beacon/data/tables`, admin credentials set, and the HTTP port published to a random
   host port (resolved via `docker port`).
4. Poll `/api/health` until ready, then hand tests a `BeaconHTTPClient`.

On teardown the container is removed; if any test failed, its `docker logs` are dumped
first to aid debugging.
