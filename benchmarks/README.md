# Beacon query-engine benchmark

Reproducible, single-node benchmark comparing **Beacon** against **PostgreSQL**,
**Trino**, **Presto**, and **DuckDB** on the same data.

Beacon's pitch is "query files where they live — no ETL". This benchmark tests that
two ways:

- **Fair comparison** — every engine queries the *same generated Parquet*, so query
  latency is apples-to-apples.
- **ETL-overhead** — Beacon also reads the source **NetCDF** natively (`beacon-netcdf`),
  while the SQL engines first need that data converted to Parquet. We measure that
  conversion cost (the work Beacon skips).

Metrics: **query latency** (cold + warm p50/p95), **ingestion time** (make data
query-ready), and **resource usage** (peak memory / mean CPU via `docker stats`).

📊 **Read the write-up:** [Too big for memory — Beacon vs PostgreSQL, Trino, Presto & DuckDB at 250M rows](https://maris-development.github.io/beacon/docs/benchmarks/beacon-vs-sql-engines-250m)
(source: [`docs/docs/benchmarks/`](../docs/docs/benchmarks/beacon-vs-sql-engines-250m.md)).

```
benchmarks/
  docker-compose.yml      # beacon, postgres, trino, presto, duckdb (+ hive metastore)
  .env.example            # ports, scale, image tags, resource caps
  data-gen/generate.py    # synthesize the dataset (parquet + netcdf)
  engines/                # per-engine config (postgres init, trino/presto catalogs)
  harness/                # python benchmark runner
  results/                # generated results.json + report.md
```

## Why these engines

Trino reads files in object storage in place — the closest analog to Beacon. Presto is
Trino's predecessor (near-identical; included for direct comparison). Postgres is the
row-store baseline that must load data first. **DuckDB** is a columnar engine that queries
Parquet directly — philosophically the closest model to Beacon; here it's run in a matched
container reading the same volume for an equal comparison. To add another engine, add a
`Client` subclass in `harness/clients.py` (and a `docker-compose.yml` service) — the query
suite is engine-agnostic.

| Engine | How it reads the data |
|---|---|
| **Beacon** | Queries the Parquet/NetCDF files in place over HTTP SQL |
| **DuckDB** | Columnar engine; `read_parquet()` over the files (no ingest), run in a matched container |
| **Trino / Presto** | Hive EXTERNAL tables over `file://` via a Derby metastore |
| **PostgreSQL** | Data `COPY`-loaded into a table first |

## Prerequisites

- Docker Desktop (Windows/Mac) or Docker Engine + compose v2.
- Python 3.10+ on the host.
- ~3× the chosen Parquet size in free disk (parquet + netcdf + container layers).

## 1. Generate data

```bash
cd benchmarks
python -m pip install -r data-gen/requirements.txt
python data-gen/generate.py --scale small        # ~100 MB; use medium (~2 GB) for the real run
```

Writes `data/parquet/*.parquet` and `data/netcdf/*.nc` (+ a `manifest.txt`). Add
`--formats parquet,netcdf,zarr` to also emit Zarr (note: Beacon reads Zarr **v3**;
xarray+`zarr<3` writes v2, so the native-Zarr query needs a v3-capable `zarr`).

Parquet is written with **zstd** compression by default (`--compression zstd|snappy|lz4|gzip|none`),
the common choice for cloud-optimized scientific data.

### Data layout: `--sort` (row-group pruning)

By default rows are **random** — the worst case for columnar pruning (every Parquet
row-group's min/max spans the full range, so filters must scan everything). Real
ARCO/scientific data is laid out far more deliberately. Two ordered layouts mirror that:

```bash
# time-ordered: tight, non-overlapping row-group time ranges (helps time filters + top-N)
python data-gen/generate.py --scale medium --sort time

# time + spatial: as above, plus Morton(lon,lat) ordering within each chunk so row-groups
# also get tight lat/lon bounds (helps spatial filters). Use small row-groups for fine pruning:
python data-gen/generate.py --scale medium --sort time-geo --row-group-size 131072
```

On sorted data the columnar engines prune row-groups they can't match — e.g. Beacon's
spatial-box filter reads ~2M of 5M rows instead of all 5M, and runs ~34% faster.
**Trade-off:** `time-geo` scrambles time *within* a chunk to gain spatial locality, so
time pruning drops to chunk granularity (use `--sort time` for the tightest time pruning).
And very small row-groups help pruning but add per-group overhead — Presto in particular
degrades sharply with `--row-group-size 131072`; 1Mi (the default) is safer for it.

## 2. Copy data into the native volume

The engines read Parquet from a **native Docker volume** (ext4 in the Linux VM), not the
Windows/Mac bind mount — bind mounts go through a slow gRPC-FUSE/virtiofs layer that would
dominate I/O-bound reads. After generating (or regenerating) data, sync it into the volume:

```bash
./sync-data-to-volume.sh        # copies ./data -> the beaconbench_benchdata volume
```

## 3. Start the engines

```bash
cp .env.example .env          # then edit if needed
docker compose -p beaconbench up -d
docker compose -p beaconbench ps     # wait until all are healthy
```

The fixed project name `beaconbench` makes container names predictable
(`beaconbench-beacon-1`, …) which the resource sampler relies on. The Postgres ingest step
in the harness reads `./data` directly on the host, so only the file-querying engines use
the volume. **Re-run `sync-data-to-volume.sh` (and restart the engines) whenever you
regenerate the dataset.**

## 4. Run the benchmark

```bash
python -m pip install -r harness/requirements.txt
python harness/run_benchmark.py --scale small --warm 3
```

Outputs `results/results.json` and `results/report.md`, and prints a cross-engine
correctness check (every query's row count must agree).

Useful flags:

```bash
python harness/run_benchmark.py --engines beacon,postgres   # subset
python harness/run_benchmark.py --warm 5 --no-etl           # more warm runs, skip conversion timing
python harness/run_benchmark.py --engines duckdb            # DuckDB only
```

**DuckDB** runs as a container (compose service `duckdb`) reading the same `benchdata`
volume under the same CPU/memory caps as every other engine — an equal comparison. DuckDB
has no native client/server protocol, so a tiny HTTP query server
([`engines/duckdb/server.py`](engines/duckdb/server.py)) wraps one persistent connection;
the harness talks to it at `--duckdb-host` / `--duckdb-port` (default `localhost:8500`).

## Query suite

Eight analytical queries, defined once in [`harness/queries.py`](harness/queries.py) with a
`{table}` placeholder, run verbatim on every engine (only the table reference differs:
`obs` for Postgres, `hive.bench.obs` for Trino/Presto, `read_parquet('parquet/*.parquet')`
for Beacon, `read_parquet('…/data/parquet/*.parquet')` for DuckDB). Each is wrapped as
`SELECT count(*) FROM ( … ) sub` to measure execution (not result transfer) and to give a
cross-engine row-count check — except `count_all`, which runs as-is to return the row total.

```sql
-- count_all: full-scan count (column-touching, forces a real scan)
SELECT count(temperature) AS n FROM <table>;

-- filter_temp: projection + single range filter
SELECT time, latitude, longitude, temperature FROM <table>
WHERE temperature BETWEEN 10 AND 12;

-- filter_multi: multi-column BETWEEN filter
SELECT time, latitude, longitude, temperature, salinity FROM <table>
WHERE temperature BETWEEN 5 AND 15 AND latitude BETWEEN -10 AND 10;

-- agg_by_platform: GROUP BY with aggregates
SELECT platform, avg(temperature) AS avg_temp, avg(salinity) AS avg_sal, count(*) AS n
FROM <table> GROUP BY platform ORDER BY n DESC;

-- spatial_box: bounding-box filter on lon/lat
SELECT time, longitude, latitude, temperature FROM <table>
WHERE longitude BETWEEN -10 AND 10 AND latitude BETWEEN 30 AND 60;

-- time_window: ~30-day time-range filter (time is epoch seconds)
SELECT time, latitude, longitude, temperature FROM <table>
WHERE time BETWEEN 1656625600 AND 1659217600;

-- topn_recent: top-N by time
SELECT time, platform, temperature, salinity FROM <table>
ORDER BY time DESC LIMIT 1000;

-- distinct_platform: distinct values
SELECT DISTINCT platform FROM <table> ORDER BY platform;
```

## 5. Tear down

```bash
docker compose -p beaconbench down -v    # -v also removes the benchdata volume
```

## Notes & fairness

- `time` is stored as `int64` epoch seconds (identical type in every engine) to avoid
  Parquet-timestamp portability differences across Hive engines.
- Each query is executed as `SELECT count(*) FROM (<query>) sub`, forcing full execution
  on every engine while returning one integer — isolating engine execution from
  result-serialization, and giving a built-in correctness check.
- `ENGINE_CPUS` / `ENGINE_MEM` in `.env` apply equal caps to every engine container.
- Trino/Presto read `data/parquet` as Hive **external** tables over `file://` locations
  via a Derby-backed metastore — no S3/MinIO needed, but it's the heaviest part of the
  stack; if an image tag misbehaves, pin a different one in `.env`.
- **Beacon version (1.7.0 / 1.7.1):** the benchmark targets the **1.7.x** line. It runs by
  default on the published **1.7.0** image (`ghcr.io/maris-development/beacon:v.1.7.0`) —
  the newest release tag on ghcr (`latest` is stale at v1.5.0; the v2.x nightlies need a
  `BEACON_TOKEN`). **1.7.1** is the current source release and shares the same query engine;
  run it by setting `BEACON_IMAGE` in `.env` to a 1.7.1 image (or a locally built
  `beacon:1.7.1`). Either way the harness records the engine's real version via
  `beacon_version()` and prints it in `results/report.md`.
- `beacon-netcdf` (the native, no-ETL query path) is **opt-in** — add it to `--engines`.
  It's kept out of the default correctness-checked suite because some Beacon builds'
  NetCDF reader can return inflated row counts. The no-ETL advantage is still captured
  regardless, via the per-engine ingestion column and the measured NetCDF→Parquet
  conversion time.
- Start at `--scale small` to validate the whole pipeline, then re-run at `medium`.
