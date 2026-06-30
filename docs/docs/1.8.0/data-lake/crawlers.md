# Crawlers

```sql
CREATE CRAWLER argo
ON 'argo/'
WITH ('format' 'parquet', 'schedule' '15m');

RUN CRAWLER argo;
```

A **crawler** automatically discovers datasets in Beacon's storage and registers them as [external tables](./external-tables.md) — so you don't have to write one `CREATE EXTERNAL TABLE` per dataset. It scans a prefix, groups files by format, detects Hive-style partitions, infers each table's schema, and registers a table for every dataset it finds. As new files land, a crawler keeps the catalog up to date on a schedule or in response to storage events.

This is Beacon's equivalent of an AWS Glue crawler. The tables it produces are ordinary external tables: they persist across restarts, and you `SELECT`, `JOIN`, and `DROP` them like any other table.

:::tip When to use a crawler
Reach for a crawler when you have **many** datasets under a prefix (often partitioned by date/region) and want them registered — and kept current — without maintaining DDL by hand. For a one-off table, a plain [`CREATE EXTERNAL TABLE`](../sql/create-table.md) is simpler.
:::

Crawler DDL can be submitted through any of Beacon's SQL surfaces:

- **HTTP** — `POST /api/query` with `{ "sql": "CREATE CRAWLER ..." }`
- **Arrow Flight SQL** — any Flight SQL client (DataGrip, ADBC, DBeaver, …)

:::info
SQL must be enabled (`BEACON_ENABLE_SQL=true`) to run DDL over the HTTP API. Arrow Flight SQL does not require this flag.
:::

## CREATE CRAWLER

```sql
CREATE CRAWLER <name>
[ ON '<prefix>' ]
[ WITH ( '<key>' '<value>' [, ...] ) ]
```

- **`<name>`** — a unique crawler name.
- **`ON '<prefix>'`** — the storage prefix to scan, relative to the datasets root (e.g. `argo/`). Equivalent to the `target_prefix` option.
- **`WITH (...)`** — key/value options, using the same shape as `CREATE EXTERNAL TABLE … OPTIONS (…)`.

### Options

| Option | Default | Description |
|---|---|---|
| `format` | all formats | Comma-separated list restricting which formats to discover, e.g. `'parquet,nc'`. |
| `detect_partitions` | `true` | Detect Hive-style `key=value/` partitions and map them to partition columns. |
| `schedule` | _none_ | Periodic re-crawl interval: `30s`, `15m`, `2h`, `1d`, or a bare number of seconds. Omit for no timer. |
| `event_driven` | `false` | Re-crawl when storage events fire under the prefix (see [Triggers](#triggers)). |
| `table_naming` | `leaf_prefix` | `leaf_prefix` (use the prefix leaf) or `crawler_prefixed` (`<crawler>_<leaf>`). |

Any **other** key in `WITH (...)` is forwarded verbatim into every discovered table's format `OPTIONS` — for example `'read_dimensions' 'lat,lon'` for NetCDF.

```sql
CREATE CRAWLER profiles
ON 'argo/'
WITH (
  'format'            'parquet,nc',
  'detect_partitions' 'true',
  'schedule'          '15m',
  'event_driven'      'true',
  'read_dimensions'   'lat,lon'
);
```

Defining a crawler **persists** it (it reloads on restart) and starts its triggers, but does not crawl immediately — issue `RUN CRAWLER` for the first pass, or wait for the first scheduled tick.

## RUN CRAWLER

```sql
RUN CRAWLER <name>
```

Runs a crawl once, on demand. Each run scans the prefix and creates or updates the tables it owns. Runs are idempotent — re-running registers no duplicates.

## SHOW CRAWLERS

```sql
SHOW CRAWLERS
```

Lists every defined crawler with its prefix, format filter, schedule, partition and event settings, and naming policy.

## DROP CRAWLER

```sql
DROP CRAWLER <name>
```

Removes the crawler definition and stops its triggers. **The tables it created are left in place** — dropping a crawler never deletes data or tables. Drop the tables separately with `DROP TABLE` if you want them gone.

## Partition detection

With `detect_partitions` enabled (the default), the crawler recognises Hive-style directory partitioning:

```
argo/year=2024/month=01/part-0.parquet
argo/year=2024/month=02/part-1.parquet
argo/year=2025/month=01/part-2.parquet
```

becomes a single table partitioned by `year` and `month`. The partition columns are added to the table schema and support partition pruning:

```sql
SELECT count(*) FROM argo WHERE year = '2024' AND month = '01';
```

Files that share a format and a base prefix are grouped into one table; the partition columns are read from the `key=value` path segments.

## Table naming

A discovered group is named after the **leaf** of its base prefix. For example, files under `data/argo_floats/…` produce a table named `argo_floats`. Use `table_naming` `crawler_prefixed` to prefix the crawler name (`<crawler>_argo_floats`). Names are slugified to valid SQL identifiers, and collisions are disambiguated deterministically.

## Ownership

Crawled tables are tagged internally as owned by the crawler that created them. This guarantees two things:

- A crawl **never overwrites a hand-created table** (or one owned by a different crawler) — such tables are skipped.
- Re-running a crawler **updates its own tables** (new files, new partitions, schema changes) rather than creating duplicates.

The ownership marker is internal and is hidden from the table config API.

## Triggers

Crawlers can keep the catalog current automatically through two mechanisms:

### Scheduled

When `schedule` is set, Beacon runs the crawl on that interval. Scheduling re-lists the prefix every tick and reconciles new files, new partitions, and schema changes. This is the baseline mechanism and works on every storage backend, including S3.

### Event-driven

When `event_driven` is `true` **and** storage events are available, Beacon subscribes to change events under the prefix and runs an incremental crawl shortly after new or changed files appear (debounced to coalesce bursts). This gives lower latency than polling.

:::warning Event availability
Filesystem events require `BEACON_ENABLE_FS_EVENTS=true` (disabled by default; enable it for the local backend). On S3, change events are not yet wired up. If a crawler requests `event_driven` where events cannot fire **and** it has no `schedule`, Beacon falls back to a default poll interval so the crawler still makes progress rather than sitting idle — see [Configuration](#configuration).
:::

## Configuration

| Environment variable | Default | Description |
|---|---|---|
| `BEACON_CRAWLER_ENABLE` | `true` | Master switch for crawler background triggers. When `false`, crawlers can still be defined and run on demand, but no scheduled/event tasks are spawned. |
| `BEACON_CRAWLER_DEFAULT_INTERVAL_SECS` | `900` | Fallback poll interval applied to an `event_driven` crawler when storage events are unavailable and no explicit schedule is set. |

## Admin REST API

Besides the SQL DDL above, crawlers can be managed over HTTP. All of these
endpoints require admin basic auth (the same credentials used for other write
operations) and back the admin web UI:

| Method & path | Purpose |
| --- | --- |
| `POST /api/admin/crawlers` | Define (or replace) a crawler and start its triggers. |
| `GET /api/admin/crawlers` | List all defined crawlers. |
| `GET /api/admin/crawlers/{name}` | Fetch one crawler's definition (404 if unknown). |
| `POST /api/admin/crawlers/{name}/run` | Run a crawler once on demand and return its report. |
| `DELETE /api/admin/crawlers/{name}` | Remove a crawler and stop its triggers (crawled tables are left in place). |

## Supported formats and limitations

The crawler discovers **file-per-dataset** formats whose filename extension exactly equals the format identifier: `parquet`, `geoparquet`, `csv`, `nc` (NetCDF), `bbf`, `arrow`, and `tiff`. Alias extensions are **not** crawled — a file must use the canonical extension. In particular `.tsv` (read as CSV), `.feather` (read as Arrow), and `.tif` (read as TIFF) are skipped by the crawler even though those formats can read them directly; register such files with an explicit table function or `CREATE EXTERNAL TABLE`.

Directory/marker-based stores — **Zarr** (`*.zarr/zarr.json`) and **Atlas** (`atlas.json`) — are **skipped** by the crawler. They are not registered as external tables through the listing-table path (Zarr is read via [`read_zarr`](../sql/table-functions.md#read_zarr)); a crawl that encounters them ignores them and continues with the other datasets. Register these with an explicit table function or `CREATE EXTERNAL TABLE` instead.

GeoParquet files are matched by the `.geoparquet` extension and crawled as GeoParquet tables (with GeoArrow geometry decoding). A plain `.parquet` file — even one carrying `geo` metadata — is crawled as an ordinary Parquet table; to get geometry decoding for such a file, give it the `.geoparquet` extension or register a GeoParquet external table explicitly.

**Delta Lake** tables — directories containing a `_delta_log/` — are also **not** auto-crawled. Register them explicitly with [`CREATE EXTERNAL TABLE ... STORED AS DELTA`](./delta-lake.md).

## See also

- [External Tables](./external-tables.md) — the tables a crawler produces
- [`CREATE EXTERNAL TABLE`](../sql/create-table.md) — the manual equivalent, including `PARTITIONED BY`
- [Configuration](./configuration.md) — all Beacon settings
