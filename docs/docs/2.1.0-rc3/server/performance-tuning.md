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

## Datasets on NFS or slow disks

Without the cache, Beacon opens a dataset file again for each read. A query on a NetCDF or Zarr
collection reads many small ranges, so it opens each file many times. On NFS, each open costs
network round trips. On a hard disk (HDD), each small read costs a seek.

The datasets store cache removes most of this cost. It has two parts:

- **The handle cache** keeps files open between reads. Beacon then opens a file once for many reads.
- **The block cache** keeps aligned blocks of file data in memory. It is off by default.

[Datasets store cache](/docs/2.1.0-rc3/server/configuration#datasets-store-cache) gives the six
settings and their defaults.

### When to turn the cache on

- Turn the cache on when the datasets folder is on NFS or on an HDD.
- On a local SSD or NVMe disk, the handle cache helps a little. Keep the block cache off there.

To turn on the handle cache, set `BEACON_DATASETS_CACHE_ENABLED=true`. In tests, reads with the
handle cache were never slower than reads without the cache.

### Block size

The block cache reads more data than a query asks for. This helps when a seek or a round trip
costs more than the extra bytes. It makes reads slower when the network bandwidth is the limit.
The best block size depends on the storage. Start from this table, and measure with your own
queries:

| Storage behind the datasets folder | `BEACON_DATASETS_CACHE_BLOCK_SIZE` |
| --- | --- |
| Local SSD or NVMe | `0` (off) |
| NFS on SSD, 1 Gbit | `0`, or `64KiB` for a collection of small files |
| NFS on SSD, 10 Gbit | `256KiB` |
| HDD, local or NFS | `1MiB` |
| High latency (WAN, cloud NFS) | `512KiB` to `1MiB` |

Use binary units such as `KiB` and `MiB`. `MB` means 1000000 bytes, and this value is not a
power of two.

`BEACON_DATASETS_CACHE_BLOCK_CACHE_SIZE` sets the memory limit of the block cache. Add this value
to the memory that you give to Beacon.

Beacon keeps the cached blocks of a file only while the file handle stays in the cache. A handle
closes when the handle cache is full, or after `BEACON_DATASETS_CACHE_HANDLE_IDLE_SECS` without a
read. Beacon does not use the blocks of a closed handle again. With the block cache on, set
`BEACON_DATASETS_CACHE_MAX_OPEN_FILES` above the number of files that your queries read together.

### When Beacon sees a changed file

- A write through Beacon shows at once. Examples are a file upload and a file delete in the admin API.
- A change by another program shows after at most `BEACON_DATASETS_CACHE_REVALIDATE_MS`, plus the
  attribute cache time of the NFS client.
- A change also shows at the next listing of the file, when the listing gives a new size or a new
  modification time.

Beacon compares only the size and the modification time of a file. Some tools change a file but
keep both values. Examples are `rsync -t`, `cp -p`, and `touch -d` on a file with the same size.

::: warning A change that keeps the size and the modification time
Beacon does not see such a change. It reads the old data until the file handle leaves the cache.
A handle leaves the cache after `BEACON_DATASETS_CACHE_HANDLE_IDLE_SECS` without a read, or when
the cache is full. A file that Beacon reads without a pause keeps its handle. To make Beacon see the
change, do one of these steps:

- Restart Beacon.
- Change the size or the modification time of the file.
- Write the file through Beacon. A file upload or a file delete removes the handle from the cache.
:::

### File descriptor limit

Each cached handle holds one open file descriptor. Keep `BEACON_DATASETS_CACHE_MAX_OPEN_FILES` well
below the file limit of the process (`ulimit -n`). Beacon also needs file descriptors for network
connections, logs and the tables store.

`BEACON_DATASETS_CACHE_MAX_OPEN_FILES` is a target, not a hard limit. The cache can hold more
handles for a short time. It closes the extra handles at its next maintenance run, after 5 seconds
at most. A file that a query still reads also stays open after the cache drops its handle. Keep a
margin for these handles.

With the block cache on, a low `BEACON_DATASETS_CACHE_MAX_OPEN_FILES` also lowers the block cache
hits. See [Block size](#block-size).

### Hidden `.nfs` files

NFS renames a deleted file to `.nfs...` while a process holds the file open. The handle cache can
keep such a file open for a short time. While the cache is on, Beacon hides every file whose name
starts with `.nfs` from the listings. A query therefore does not read such a file as data.

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
Azure needs no local copy. The reader has no setting. Beacon writes NetCDF with a pure-Rust writer.

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
