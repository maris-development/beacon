# Beacon integration tests

Black-box, end-to-end tests that **build the Beacon Docker image**, run the container,
and exercise the HTTP API: querying generated Parquet/CSV in place, multiple output
formats (CSV / Arrow / Parquet, decoded with `pyarrow`), the JSON DSL, introspection
and query-plan/metrics endpoints, external tables, and the full managed-table
lifecycle on the Lance engine — including a complete ETL pipeline (`CREATE TABLE AS
SELECT` / `ALTER` / `UPDATE` / `DELETE` / aggregate mart).

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
| `BEACON_BASE_URL` | _(unset)_ | If set, the RBAC tests (`test_rbac.py`) run against this already-running Beacon instead of Docker — handy for iterating against a local debug build. Also reads `BEACON_ADMIN_USERNAME`/`BEACON_ADMIN_PASSWORD` (default `admin`/`securepassword`). Set `BEACON_AUTH_ENFORCE=true` to also run the enforcement test (it must match the server's config). |

> **Note:** The published image will **fail** the external- and managed-table tests
> until the HTTP write-path change in `beacon-api` (admin basic-auth → super-user on
> `POST /api/query`) ships in a release. Build the local image to validate the full
> suite. This mismatch is itself a useful regression signal.

## What runs

| File | Covers |
| ---- | ------ |
| `test_health.py` | `/api/health`, `/swagger`, `beacon_version()`, `/api/datasets`, `list_datasets()` |
| `test_queries_parquet.py` | `read_parquet()` counts/aggregates over a multi-file glob; equivalent JSON DSL; malformed-query 400 |
| `test_output_formats.py` | CSV / Arrow IPC / Parquet output formats decoded with `pyarrow` and cross-checked; content-type, download filename, and `x-beacon-query-id` headers |
| `test_query_dsl.py` | structured JSON DSL: `filter` tree (`gt`/`eq`/`and`/`or`/`is_not_null`), `sort_by` (`Asc`/`Desc`), `limit`/`offset`, aliased & function `select` |
| `test_csv_reader.py` | `read_csv()`, `read_schema()`, the JSON DSL `csv` source, and a CSV↔Parquet cross-format join |
| `test_introspection.py` | `/api/info`, `/api/list-datasets`, `/api/total-datasets`, `/api/dataset-schema`, `/api/tables-with-schema`, `/api/table-schema`, `/api/functions`, `/api/table-functions` |
| `test_query_plan.py` | `/api/parse-query`, `/api/explain-query`, `/api/explain-analyze-query`, `EXPLAIN`, and the `/api/query/metrics/{id}` round-trip via the `x-beacon-query-id` header |
| `test_external_tables.py` | `CREATE EXTERNAL TABLE` (admin SQL + `POST /api/admin/external-tables`); `SHOW TABLES` / `/api/tables`; no-auth → 400/401, bad-auth → 401 |
| `test_admin_crawlers.py` | admin crawler CRUD + run (`/api/admin/crawlers*`): create/list/get/run/drop, 404 for unknown, 401 without auth |
| `test_managed_tables.py` | managed-table `CREATE`/`INSERT`/`SELECT`/`UPDATE`/`DELETE`/`DROP`; writes need admin |
| `test_lance_tables.py` | Lance engine (the default): engine identity via `/api/admin/table-config`, `ALTER TABLE` add/rename/drop column, `CREATE TABLE AS SELECT`, `INSERT ... SELECT`, `UPDATE`/`DELETE` semantics |
| `test_etl_pipeline.py` | full ETL on Lance tables: extract (Parquet+CSV) → load (CTAS join) → transform (`ALTER`/`UPDATE`/`DELETE`) → publish (aggregate mart) → verify; anonymous writes rejected |
| `test_sql_databases.py` | external `STORED AS POSTGRES` tables: federated query/pushdown, cross-source joins, credential redaction, restart survival (PostgreSQL sidecar) |
| `test_mysql.py` | external `STORED AS MYSQL` tables: query, filter pushdown (federated plan), credential redaction (MySQL sidecar) |
| `test_nd_formats.py` | NetCDF & Zarr (`read_netcdf`/`read_zarr`, `SELECT *` dimension auto-selection, explicit dims); flat & ND `netcdf` output round-tripped through the reader |
| `test_spatial.py` | `st_within_point`, `st_geojson_as_wkt`, the GeoJSON DSL filter, and GeoParquet output round-tripped through `read_geoparquet` |
| `test_materialized_views.py` | `CREATE MATERIALIZED VIEW` / `REFRESH` / drop; snapshot-until-refresh semantics; admin gate |
| `test_indexes.py` | `CREATE INDEX` (btree/bitmap/inverted) / `SHOW INDEXES` / `DROP INDEX` on Lance; results unchanged; rejected on external tables |
| `test_sql_features.py` | window functions, CTEs, `HAVING`, `min/max/avg/stddev`, `count(DISTINCT)`, `CASE`, string functions, numeric DSL `BETWEEN` |
| `test_concurrency.py` | many simultaneous queries return consistent results |
| `test_errors.py` | unknown table/column, type mismatch, empty glob, bad output format, empty body → 4xx |
| `test_persistence.py` | managed Lance table, external table, and crawler definition survive a container restart |
| `test_iceberg_tables.py` | managed **Iceberg** engine (dedicated `BEACON_DEFAULT_TABLE_ENGINE=iceberg` container): create/insert/update/delete/alter/CTAS, engine identity |
| `test_sql_disabled.py` | dedicated `BEACON_ENABLE_SQL=false` container: SQL rejected (400) but the JSON DSL still works |
| `test_crawlers_advanced.py` | `format_filter` exclusion, `leaf_prefix` vs `crawler_prefixed` naming, re-run idempotency, SQL `CREATE`/`RUN`/`SHOW`/`DROP CRAWLER` |
| `test_delta.py` | external Delta Lake (`read_delta`, `STORED AS DELTA`, INSERT) over a hand-written `_delta_log` (no `deltalake` dependency) |
| `test_flight_sql.py` | the Arrow **Flight SQL** transport over ADBC: auth handshake, queries, Arrow results, anonymous rejection (optional dep — skipped if `adbc-driver-flightsql` is absent) |
| `test_remote_federation.py` | beacon-to-beacon `STORED AS REMOTE` federation against a second container (anonymous Flight SQL); pushdown + cross-source join |

Several suites spin up **extra containers** from the same image (a second Beacon with a
different engine / SQL disabled / anonymous Flight, plus PostgreSQL and MySQL sidecars)
via `run_beacon_container` in `conftest.py`, all on the shared docker network.

To run the Flight SQL suite, also install the optional deps:

```bash
pip install -r requirements-optional.txt
```

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
