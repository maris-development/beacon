# Read Files

A table function queries files directly in a `FROM` clause. You create no
[external table](/docs/2.0.0-rc3/data-sources/external-tables) first. Use a table function
for ad-hoc exploration. Also use it to put the file paths inside a
[view](/docs/2.0.0-rc3/server/view).

The first argument of every function gives the file paths. Use **one path or glob string**, or **a
list of strings**. Beacon resolves a glob against its dataset storage root.

```sql
-- Single path
SELECT * FROM read_parquet('profiles/2024.parquet')

-- Single folder glob
SELECT * FROM read_netcdf('argo/**/*.nc')

-- A list, to combine multiple paths or globs in one call
SELECT * FROM read_netcdf(['argo/**/*.nc', 'wod/**/*.nc'])
```

The `glob_paths` argument of every signature below accepts both forms: one string or a list of
strings.

## `read_netcdf`

```text
read_netcdf(glob_paths)
read_netcdf(glob_paths, dimensions)
```

Beacon reads the NetCDF files that match one or more glob patterns.

The optional `dimensions` argument selects the variables. Beacon returns a variable only if the list
holds all of its dimensions. Use the argument to drop variables with many dimensions. Also use it
when the files hold variables with different dimensions.

```sql
SELECT time, latitude, longitude, temperature
FROM read_netcdf('argo/**/*.nc')

-- With explicit dimension columns
SELECT *
FROM read_netcdf(['argo/**/*.nc'], ['time', 'pressure'])
```

### Variable attributes

Beacon shows the NetCDF variable attributes, such as `units` and `long_name`, as extra columns. It
uses the pattern `<variable>.<attribute>`. An attribute column keeps the type from the file: string,
integer, float and so on. A file attribute has no variable prefix. It uses a leading dot:
`.<attribute>`. Quote these column names, because they contain a dot.

```sql
-- Variable attribute
SELECT temperature, "temperature.units", "temperature.long_name"
FROM read_netcdf('argo/**/*.nc')
LIMIT 1

-- Global attribute
SELECT ".source", temperature
FROM read_netcdf('argo/**/*.nc')
LIMIT 1
```

## `read_zarr`

```text
read_zarr(glob_paths)
read_zarr(glob_paths, dimensions)
```

Beacon reads the Zarr stores that match one or more glob patterns. Each path must point at a
`zarr.json` entry file.

The optional `dimensions` argument selects the arrays. Beacon returns an array only if the list
holds all of its dimensions. Use the argument to drop arrays with many dimensions.

Predicate pushdown is automatic. Beacon prunes chunks and slices the coordinate dimensions such as
`time`, `latitude` and `longitude`. It uses the `WHERE` clause of your query. You declare no
statistics columns.

```sql
SELECT * FROM read_zarr('sst/*/zarr.json')

-- Range queries are pruned automatically
SELECT time, sst
FROM read_zarr('sst/*/zarr.json')
WHERE time >= '2024-01-01'
```

### Array attributes

Beacon shows the attributes of an array as extra columns. It uses the pattern
`<array>.<attribute>`. An attribute column keeps the type from the file: string, integer, float and
so on. A root attribute of the store has no array prefix. It uses a leading dot: `.<attribute>`.
Quote these column names, because they contain a dot.

```sql
-- Array attribute
SELECT sst, "sst.units", "sst.long_name"
FROM read_zarr('sst/*/zarr.json')
LIMIT 1

-- Root-level global attribute
SELECT ".Conventions", sst
FROM read_zarr('sst/*/zarr.json')
LIMIT 1
```

## `read_atlas`

```text
read_atlas(glob_paths)
read_atlas(glob_paths, dimensions)
```

Beacon reads the [Atlas](/docs/2.0.0-rc3/formats/atlas) array stores that match
one or more glob patterns. Each path must point at an `atlas.json` marker file. Give an exact path
or a glob such as `**/atlas.json`.

The optional `dimensions` argument selects the arrays with the listed dimension names. Atlas holds
statistics for each column. Beacon drops whole datasets with those statistics. A range query over a
large collection therefore reads only the datasets that can match the predicate.

```sql
SELECT * FROM read_atlas('collections/sensor/atlas.json')

-- Combine every Atlas store under a prefix, keeping a subset of dimensions
SELECT time, temperature
FROM read_atlas(['collections/**/atlas.json'], ['time', 'latitude', 'longitude'])
WHERE time >= '2024-01-01'
```

## `read_parquet`

```text
read_parquet(glob_paths)
```

```sql
SELECT * FROM read_parquet('obs/**/*.parquet') LIMIT 100
```

## `read_geoparquet`

```text
read_geoparquet(glob_paths)
```

Beacon reads [GeoParquet](https://geoparquet.org/) files. The `geo` metadata of a file describes its
geometry columns. Beacon decodes those columns to native [GeoArrow](https://geoarrow.org/). Beacon
reads a file without geometry as ordinary Parquet.

```sql
SELECT * FROM read_geoparquet('spatial/**/*.geoparquet') LIMIT 100
```

## `read_arrow`

```text
read_arrow(glob_paths)
```

Beacon reads Arrow IPC stream files (`.arrow`, `.feather`).

```sql
SELECT * FROM read_arrow('streams/*.arrow')
```

## `read_csv`

```text
read_csv(glob_paths)
read_csv(glob_paths, delimiter)
read_csv(glob_paths, delimiter, infer_records)
```

Beacon infers the schema from the file contents. The first row must be a header row.

- `delimiter`: the field separator, one character (default: `,`)
- `infer_records`: the number of rows that Beacon samples for the column types (default: `128000`)

```sql
SELECT * FROM read_csv('metadata/*.csv')

-- Tab-separated, sample 500 rows for type inference
SELECT * FROM read_csv(['data/*.tsv'], '\t', 500)
```

## `read_odv_ascii`

```text
read_odv_ascii(glob_paths)
```

```sql
SELECT * FROM read_odv_ascii('odv/**/*.txt')
```

## `read_bbf`

```text
read_bbf(glob_paths)
```

Beacon reads Beacon Binary Format files.

```sql
SELECT * FROM read_bbf('bbf/**/*.bbf')
```

## `read_tiff`

```text
read_tiff(glob_paths)
```

Beacon reads GeoTIFF and Cloud-Optimized GeoTIFF files.

```sql
SELECT * FROM read_tiff('rasters/elevation.tif')
```

### Tag attributes

Beacon shows the TIFF tags of a band as extra columns. It uses the pattern `<band>.<attribute>`. An
attribute column keeps the type from the file: string, integer, float and so on. A file tag belongs
to no band. It uses a leading dot: `.<attribute>`. Quote these column names, because they contain a
dot.

```sql
-- Band attribute
SELECT band_1, "band_1.nodata", "band_1.scale"
FROM read_tiff('rasters/elevation.tif')
LIMIT 1

-- File-level global tag
SELECT ".crs", band_1
FROM read_tiff('rasters/elevation.tif')
LIMIT 1
```

## `read_delta`

```text
read_delta(location)
read_delta(location, version_or_timestamp)
```

Beacon reads a [Delta Lake](/docs/2.0.0-rc3/formats/delta-lake) table. The
`location` argument differs from the other functions. It is **one path to the Delta table
directory**. That directory holds `_delta_log/`. It is not a glob and not a list. Beacon reads the
schema from the transaction log.

The optional second argument selects a snapshot for **time travel**:

- An integer gives a Delta **version** number, for example `12`.
- Any other string gives an RFC-3339 **timestamp**. Beacon takes the last version at or before it.

```sql
-- Latest version
SELECT * FROM read_delta('delta/ocean_profiles') LIMIT 100

-- Time travel to a specific version
SELECT count(*) FROM read_delta('delta/ocean_profiles', 12)

-- Time travel as of a timestamp
SELECT * FROM read_delta('delta/ocean_profiles', '2026-01-01T00:00:00Z')
```

Use [`CREATE EXTERNAL TABLE … STORED AS DELTA`](/docs/2.0.0-rc3/formats/delta-lake)
to register a Delta table permanently. That form also supports `INSERT INTO`.

## `read_iceberg`

```text
read_iceberg(location)
read_iceberg(location, snapshot_id)
```

Beacon reads an [Apache Iceberg](/docs/2.0.0-rc3/formats/iceberg) table. The `location`
argument differs from the other functions. It gives **one path to the Iceberg table directory**.
That directory holds `metadata/`. Give no glob and no list. Beacon reads the columns from the table
metadata. Beacon also finds the current metadata file.

The second argument gives an Iceberg **snapshot id** for time travel. The id is a 64-bit integer.
Write the id in quotes if your client holds no 64-bit integer.

```sql
-- Current snapshot
SELECT * FROM read_iceberg('iceberg/ocean_profiles') LIMIT 100

-- Time travel to a specific snapshot
SELECT count(*) FROM read_iceberg('iceberg/ocean_profiles', 3821550127947089060)
```

Use [`CREATE EXTERNAL TABLE … STORED AS ICEBERG`](/docs/2.0.0-rc3/formats/iceberg)
to keep an Iceberg table in the catalog. Beacon reads an Iceberg table. Beacon writes none.

## `read_icechunk`

```text
read_icechunk(location)
read_icechunk(location, branch)
read_icechunk(location, branch, snapshot)
read_icechunk(location, branch, snapshot, dimensions)
```

Beacon reads an [Icechunk](/docs/2.0.0-rc3/formats/icechunk) repository as a Zarr store. A
repository is a Zarr store with commits, branches, tags and snapshots.

Give one `location`. The `location` is the path to the directory of the repository. The `location`
is not a glob. The `location` is not a list.

The other arguments select the version and the arrays:

- `branch` reads the tip of that branch. The default branch is `main`.
- `snapshot` reads that snapshot. The snapshot does not move after a later commit.
- Give `NULL` for `branch` to set `snapshot` by position.
- `dimensions` selects the arrays. Beacon reads an array only if the list holds all dimensions of
  the array.

A branch selects a different version than a snapshot. Give one of the two.

```sql
-- Read the tip of `main`
SELECT * FROM read_icechunk('sst/repo') LIMIT 100

-- Read the tip of a different branch
SELECT count(*) FROM read_icechunk('sst/repo', 'dev')

-- Read one snapshot
SELECT avg(sst) FROM read_icechunk('sst/repo', NULL, 'NNNGCAX7Z99K7XTTYK8G')
```

[`CREATE EXTERNAL TABLE … STORED AS ICECHUNK`](/docs/2.0.0-rc3/formats/icechunk) adds a repository
to the catalog. Beacon reads Icechunk. Beacon does not write Icechunk.
