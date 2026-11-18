# Performance Tuning

## Beacon Query Engine Settings

This chapter gives the performance settings that are safe to change in production.

Beacon uses DataFusion. Performance tuning covers three points:

- The parallelism that Beacon can use, in threads.
- The memory of the query engine, before a spill to disk.
- The I/O that Beacon can avoid, with projection pushdown and caches.

::: tip
Every setting below is an environment variable. [configuration.md](configuration.md) holds the full list.
:::

### CPU and concurrency

Beacon runs two Tokio runtimes. The API runtime serves HTTP and Flight SQL. The query runtime
plans and runs queries. A long scan holds a query thread, but the API runtime keeps its own
threads, so a login, a health check, or the admin UI answers at once.

#### `BEACON_WORKER_THREADS`

This value sizes the query runtime. The crawlers and the file statistics run there too.

- On a dedicated machine, set `BEACON_WORKER_THREADS` to the number of physical cores.
- On a shared host, set a lower value. Other services then keep enough CPU.

More threads help an I/O-heavy workload, such as a read from object storage or a NetCDF read over
HTTP. A CPU-heavy workload, such as an aggregate or a join, does not scale past the CPU count.

#### `BEACON_API_THREADS`

This value sizes the API runtime. The default of `4` is enough for most deployments. The API
runtime encodes and compresses the result stream for each client. A deployment with many
concurrent large downloads can raise the value.

### Memory and disk spilling

#### `BEACON_VM_MEMORY_SIZE`

This value sets the size of the DataFusion memory pool, in MB. A query above this limit spills to
disk.

- A larger value gives better performance, because Beacon spills less often.
- Do you see high disk activity and slow queries under load? Then increase this value first.

::: warning
A spill goes to the OS temp area, through the DataFusion disk manager. Put that directory on fast
storage. It also needs enough free space.
:::

### Avoid unnecessary reads

#### `BEACON_ENABLE_PUSHDOWN_PROJECTION`

With this setting on, Beacon projects only the columns from the `select` list of your JSON query. It
builds the scan with those columns.

- The default is `true`. A query on a wide dataset therefore decodes only the selected columns.
- Set it to `false` only for a suspected projection bug, or to force the simplest scan.

### Query language and parsing

#### `BEACON_ENABLE_SQL`

This flag controls the SQL parser and the SQL execution.

- Set it to `true` to allow SQL queries.
- Keep it `false` if you use the JSON query API only. This reduces the exposed interface.

### Object store listings

#### `BEACON_S3_DATASETS`, `BEACON_S3_BUCKET`, `BEACON_S3_ENABLE_VIRTUAL_HOSTING`

These settings decide if the datasets store lives on an S3-compatible bucket. They also set the
address form. Every listing and every read on object storage costs network latency.

- Put Beacon near the object store in the network. This gives better performance.
- Are the listings slow? Then use a [crawler](/docs/2.1.0-rc3/server/crawlers). It keeps the
  catalog current in the background. Beacon then does not scan at query time.

## File statistics

File statistics record the lowest and the highest value of each column in each file. A query with a
`WHERE` clause then prunes each file that cannot hold a match. Beacon does not open that file.

- Set `'use_statistics_pruning' 'true'` in the `OPTIONS` of an external table that you filter often.
- A table refreshes its statistics every 15 minutes. `statistics_refresh_interval` changes this.
- Run `ANALYZE FILES` to start a refresh now. It runs in the background.
- `BEACON_FILE_STATS_MAX_REFRESHES` limits the tables that refresh at the same time.

`BEACON_NETCDF_ENABLE_STATISTICS` controls the statistics of each file. Beacon uses them to prune a
query on a table with `use_statistics_pruning`. The default is `true`. Keep it on. Switch it off only
to debug the pruning.

## NetCDF and HDF5 reader

Beacon reads NetCDF and HDF5 with a pure-Rust reader. It holds no lock, so Beacon reads many
files at the same time. It also reads byte ranges through the object store, so a file in S3, GCS or
Azure needs no local copy. The reader has no setting. Writes use the netCDF-C library.

The reader also reads two layouts of a plain HDF5 file:

- **A nested group.** Beacon walks every group. A dataset outside the root group takes its path as
  its column name, such as `observations/qc/flag`.
- **A compound dataset.** Each member becomes its own column, named `dataset/member`. Beacon skips a
  member that holds a pointer into a heap, such as a variable-length string, and it logs a message
  that names the dataset and every member type.

## Zarr predicate pushdown

The Zarr reader of Beacon applies predicate pushdown **automatically**. It uses the shared
N-dimensional engine. Beacon reads the filters of your query. It then does two things:

- It prunes the Zarr chunks that cannot match the predicate. It reads only the other chunks.
- It slices the one-dimensional coordinate arrays to your ranges. Examples are `time`, `latitude`
  and `longitude`.

You configure nothing. You set no table option. You compute no statistics first. Read the store and
filter it. The engine does the rest.

### SQL

```http
POST /api/query
Content-Type: application/json

{
	"sql": "SELECT * FROM read_zarr(['**/*.zarr/zarr.json']) WHERE valid_time >= '2025-01-01' AND longitude < 30 LIMIT 100",
	"output": { "format": "csv" }
}
```

### JSON

```http
POST /api/query
Content-Type: application/json

{
	"from": {
		"zarr": {
			"paths": ["**/*.zarr/zarr.json"]
		}
	},
	"select": ["valid_time", "latitude", "longitude"],
	"filters": [
        { "column": "valid_time", "min": "2025-01-01" },
        { "column": "longitude", "min": 15, "max": 30 }
    ],
	"limit": 100,
	"output": { "format": "csv" }
}
```

::: tip
Do you query a collection often? Then convert the Zarr stores into one
[Atlas](/docs/2.1.0-rc3/formats/atlas) collection. Atlas adds dataset pruning
with statistics, next to the chunk pruning. It drops whole datasets before it reads a chunk.
:::

## Atlas Tuning

Beacon opens an [Atlas](/docs/2.1.0-rc3/formats/atlas) collection by reading the footer of its
`data.atlas` file. Each table keeps its open collections in a cache of 512 entries, so a query
does not read that footer again. Each cached collection holds its own block cache, 256 MiB of
decompressed blocks and 64 MiB of raw slabs, so the cache is a memory bound as much as a handle
count. There is no setting for it.

### Dataset pruning

Pruning is always on. A query with a predicate judges every dataset of a collection against the
statistics in its footer, in one pass, and never opens the ones that cannot match. Pruning never
changes an answer, and the filter above the scan still decides every row.

`EXPLAIN ANALYZE` reports what it did as `atlas_datasets_pruned` and `atlas_datasets_scanned`, with
the time spent as `atlas_open_time` and `atlas_prune_time`.
