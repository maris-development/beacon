# Crawlers

```sql
CREATE CRAWLER argo
ON 'argo/'
WITH ('format' 'parquet', 'schedule' '15m');

RUN CRAWLER argo;
```

A **crawler** finds the datasets in the storage of Beacon. It registers them as
[external tables](/docs/2.0.0-rc5/data-sources/external-tables). You therefore write no
`CREATE EXTERNAL TABLE` for each dataset. A crawler scans a prefix. It groups the files by format.
It detects Hive-style partitions. It infers the schema of each table. It then registers a table for
each dataset. A crawler also keeps the catalog current when new files arrive. It runs on a schedule
or after a storage event.

A Beacon crawler works like an AWS Glue crawler. It produces ordinary external tables. Those tables
survive a restart. You run `SELECT`, `JOIN` and `DROP` on them, like any other table.

:::tip When to use a crawler
Use a crawler when you have **many** datasets under a prefix. Such datasets often use a partition by
date or by region. The crawler registers them and keeps them current. You write no DDL by hand. For
one table, a plain [`CREATE EXTERNAL TABLE`](/docs/2.0.0-rc5/sql/create-external-table) is simpler.
:::

You can send the crawler DDL through any SQL interface of Beacon:

- **HTTP**: `POST /api/query` with `{ "sql": "CREATE CRAWLER ..." }`
- **Arrow Flight SQL**: any Flight SQL client, such as DataGrip, ADBC or DBeaver

:::info
DDL over the HTTP API needs the SQL interface. That interface is on by default
(`BEACON_ENABLE_SQL`). Arrow Flight SQL does not need this flag.
:::

## CREATE CRAWLER

```sql
CREATE CRAWLER <name>
[ ON '<prefix>' ]
[ WITH ( '<key>' '<value>' [, ...] ) ]
```

- **`<name>`**: a unique crawler name.
- **`ON '<prefix>'`**: the storage prefix to scan, relative to the datasets root, for example
  `argo/`. It equals the `target_prefix` option.
- **`WITH (...)`**: the key and value options. They use the same form as
  `CREATE EXTERNAL TABLE … OPTIONS (…)`.

### Options

| Option | Default | Description |
|---|---|---|
| `format` | all formats | A list with commas. It limits the formats that the crawler finds, for example `'parquet,nc'`. |
| `detect_partitions` | `true` | Detect Hive-style `key=value/` partitions. Map them to partition columns. |
| `schedule` | _none_ | The interval between two crawls: `30s`, `15m`, `2h`, `1d`, or a number of seconds. Omit it for no timer. |
| `event_driven` | `false` | Crawl again after a storage event under the prefix. See [Triggers](#triggers). |
| `table_naming` | `leaf_prefix` | `leaf_prefix` uses the last part of the prefix. `crawler_prefixed` gives `<crawler>_<leaf>`. |

The crawler copies any **other** key in `WITH (...)` into the format `OPTIONS` of each table that it
finds. `'read_dimensions' 'lat,lon'` for NetCDF is an example.

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

`CREATE CRAWLER` **stores** the crawler. Beacon reloads it after a restart. The statement also starts
the triggers. It does not crawl at once. Run `RUN CRAWLER` for the first pass, or wait for the first
scheduled run.

## RUN CRAWLER

```sql
RUN CRAWLER <name>
```

Runs one crawl on demand. Each run scans the prefix. It then creates or updates the tables that it
owns. A run is idempotent. A second run registers no duplicate.

## SHOW CRAWLERS

```sql
SHOW CRAWLERS
```

Lists every crawler. Each row gives the prefix, the format filter, the schedule, the partition and
event settings, and the name policy.

## DROP CRAWLER

```sql
DROP CRAWLER <name>
```

Removes the crawler definition and stops its triggers. **Beacon keeps the tables that it created.**
`DROP CRAWLER` never deletes data or a table. Use `DROP TABLE` to remove a table.

## Partition detection

`detect_partitions` is on by default. The crawler then recognizes a Hive-style directory partition:

```
argo/year=2024/month=01/part-0.parquet
argo/year=2024/month=02/part-1.parquet
argo/year=2025/month=01/part-2.parquet
```

These files become one table with a partition by `year` and `month`. The crawler adds the partition
columns to the table schema. Beacon can then prune the partitions:

```sql
SELECT count(*) FROM argo WHERE year = '2024' AND month = '01';
```

The crawler groups the files with the same format and the same base prefix into one table. It reads
the partition columns from the `key=value` parts of the path.

## Table naming

The crawler names a group after the **last part** of its base prefix. The files under
`data/argo_floats/…` therefore give a table named `argo_floats`. Set `table_naming` to
`crawler_prefixed` to add the crawler name, as in `<crawler>_argo_floats`. The crawler converts each
name into a valid SQL identifier. It also resolves a name conflict in the same way each time.

## Ownership

Beacon marks each crawled table with the crawler that created it. This gives two guarantees:

- A crawl **never overwrites a table that you created by hand**. It also never overwrites a table of
  another crawler. The crawler skips those tables.
- A second run of a crawler **updates its own tables**. It adds new files, new partitions and schema
  changes. It creates no duplicate.

The owner marker is internal. The table config API does not show it.

## Triggers

A crawler keeps the catalog current in two ways:

### Scheduled

With a `schedule`, Beacon runs the crawl on that interval. Each run lists the prefix again. It then
adds the new files, the new partitions and the schema changes. This is the basic mechanism. It works
on every storage backend, also on S3.

### Event-driven

Set `event_driven` to `true`. Storage events must also be available. Beacon then subscribes to the
change events under the prefix. It runs a small crawl soon after a new or changed file appears. A
debounce merges a burst of events. This gives a lower latency than a schedule.

:::warning Event availability
No backend supports storage change events today. An `event_driven` crawler therefore does not react
to a file change. A crawler with `event_driven` and **no** `schedule` gets a default poll interval.
The crawler then still makes progress. See [Configuration](#configuration).
:::

## Configuration

| Environment variable | Default | Description |
|---|---|---|
| `BEACON_CRAWLER_ENABLE` | `true` | The main switch for the background triggers. With `false`, you can still define a crawler and run it on demand. Beacon starts no scheduled task and no event task. |
| `BEACON_CRAWLER_DEFAULT_INTERVAL_SECS` | `900` | The default poll interval of an `event_driven` crawler. Beacon uses it when storage events are absent and the crawler has no schedule. |

## Admin REST API

You can also manage a crawler over HTTP, next to the SQL DDL above. Each endpoint needs admin basic
auth. It uses the same credentials as the other write operations. The admin web UI uses these
endpoints:

| Method & path | Purpose |
| --- | --- |
| `POST /api/admin/crawlers` | Define or replace a crawler. Start its triggers. |
| `GET /api/admin/crawlers` | List every crawler. |
| `GET /api/admin/crawlers/{name}` | Return the definition of one crawler. Returns 404 for an unknown name. |
| `POST /api/admin/crawlers/{name}/run` | Run a crawler once on demand. Return its report. |
| `DELETE /api/admin/crawlers/{name}` | Remove a crawler and stop its triggers. Beacon keeps the crawled tables. |

## Supported formats and limitations

The crawler finds **one file per dataset** formats. The file extension must equal the format
identifier exactly. The identifiers are `parquet`, `geoparquet`, `csv`, `nc` for NetCDF, `bbf`,
`arrow`, `tiff` and `atlas`. The crawler does **not** read an alias extension. A file must use the canonical
extension. The crawler therefore skips `.tsv` (CSV), `.feather` (Arrow) and `.tif` (TIFF). The
readers open those files directly. Register such a file with a table function or with
`CREATE EXTERNAL TABLE`.

The crawler **skips** a store with a directory and a marker file. **Zarr** (`*.zarr/zarr.json`) is
such a store. The listing path does not register it as an external table. Read a Zarr store with
[`read_zarr`](/docs/2.0.0-rc5/sql/table-functions#read-zarr). A crawl ignores these stores
and continues with the other datasets. Register them with a table function or with
`CREATE EXTERNAL TABLE`.

An **Atlas** collection *is* crawled. A collection is one file, `data.atlas`, so its extension is
its format and the rule above admits it. Each collection lives in its own directory and tables
group by directory, so a crawl of several collections registers one table each. Use
`CREATE EXTERNAL TABLE ... LOCATION 'collections/*/data.atlas'` to put them in one table
instead.

The crawler matches a GeoParquet file by the `.geoparquet` extension. It creates a GeoParquet table
with GeoArrow geometry decoding. The crawler reads a plain `.parquet` file as an ordinary Parquet
table, also with `geo` metadata. To get geometry decoding for such a file, give it the
`.geoparquet` extension. You can also register a GeoParquet external table yourself.

The crawler also **skips** a **Delta Lake** table. Such a table is a directory with a `_delta_log/`
folder. Register it with
[`CREATE EXTERNAL TABLE ... STORED AS DELTA`](/docs/2.0.0-rc5/formats/delta-lake).

## See also

- [External Tables](/docs/2.0.0-rc5/data-sources/external-tables): the tables that a crawler produces
- [`CREATE EXTERNAL TABLE`](/docs/2.0.0-rc5/sql/create-external-table): the manual form, with `PARTITIONED BY`
- [Configuration](/docs/2.0.0-rc5/server/configuration): every Beacon setting
