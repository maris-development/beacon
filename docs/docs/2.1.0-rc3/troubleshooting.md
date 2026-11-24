---
description: Common Beacon errors and what causes them. Missing columns, zero rows, unsupported calendars, slow queries, connection failures and permission errors.
---

# Troubleshooting

Each entry below names a symptom, its usual cause and the fix.

::: tip Find the log first
Many entries below tell you to read the log. Beacon writes to stdout and to a dated file in
`/beacon/logs/`. Use `docker logs -f beacon`, or mount the directory to keep the files. See
[Log files](/docs/2.1.0-rc3/server/configuration#log-files).
:::

## Reading files

### A variable is missing from `SELECT *`

Beacon dropped it because it does not fit the grid it chose. This happens when a file holds
variables on mutually incompatible dimensions, such as `sst(time, lat, lon)` next to
`lat_bnds(lat, nv)`.

Check the server log. Beacon writes one `info` line for each query. The line names what it
dropped:

```text
SELECT * auto-selected dimensions ["time", "lat", "lon"]; excluded variables
["lat_bnds"] have incompatible dimensions and were omitted.
```

Ask for the other grid by name:

```sql
SELECT * FROM read_netcdf('sst/*.nc', ['lat', 'nv']);
```

See [Excluded variables](/docs/2.1.0-rc3/arrays-to-tables#excluded-variables).

### The query returns more rows than the file has values

The row count is the product of the grid dimension sizes. It is not the count of one variable. A
`time=100, lat=180, lon=360` file gives 6,480,000 rows. That number is correct.

Is the number larger still? Then your `dimensions` list probably mixes unrelated dimensions. Make
the list smaller. See [Arrays to tables](/docs/2.1.0-rc3/arrays-to-tables).

### `SELECT *` returns hundreds of columns

Every variable attribute is its own column, as `<variable>.<attribute>`. A file with 200 variables
and two attributes each returns roughly 600 columns.

Name the columns you want. Projection pushdown then reads only those variables:

```sql
SELECT time, lat, lon, sst FROM read_netcdf('sst/*.nc');
```

### A time column is a large number, not a date

The variable has no CF `units` attribute, so Beacon has nothing to decode from. Its `units` must
contain `since`, as in `days since 1950-01-01`.

Check what the file declares:

```sql
SELECT DISTINCT "time.units" FROM read_netcdf('cruises/*.nc');
```

### A time column holds numbers, not timestamps

Beacon supports the Gregorian and Julian calendars only. Beacon does not decode a time with
`noleap`, `365_day`, `360_day`, `all_leap` or `366_day`. The column holds the stored numbers, not
a wrong instant.

Convert the time axis before you query. See
[Supported calendars](/docs/2.1.0-rc3/cf-decoding#supported-calendars).

### Values look like small integers, not physical units

The variable is CF-packed and something removed its `scale_factor` or `add_offset`. Beacon applies
both when they are present. Check them:

```sql
SELECT DISTINCT "sst.scale_factor", "sst.add_offset" FROM read_netcdf('sst/*.nc');
```

See [CF decoding](/docs/2.1.0-rc3/cf-decoding).

### Missing data shows as `-999`, not `NULL`

The file uses `missing_value` rather than `_FillValue`. The NetCDF reader nulls both. The HDF5
and Zarr readers null `_FillValue` only. With those readers, filter in SQL:

```sql
SELECT * FROM read_hdf5('old/*.h5') WHERE sst != -999.0;
```

### `read_schema(...)` is not a function

There is no generic `read_schema`. Each reader has its own counterpart, and the format lives in the
name:

```sql
SELECT * FROM read_geoparquet_schema('spatial/*.geoparquet');
```

See [`read_<format>_schema`](/docs/2.1.0-rc3/sql/table-functions-utility#read-format-schema).

### A NetCDF file on S3 fails to open, but Parquet works

NetCDF on object storage supports **anonymous access only**. The native reader opens the file by
URL and does not go through the credential chain.

Either make the bucket readable anonymously, or convert the files, or serve them from a server whose
datasets store is that bucket. See [NetCDF](/docs/2.1.0-rc3/formats/netcdf).

### Zero rows from a glob that matches files

Check the pattern reaches into subdirectories. `*` stops at one level. `**` recurses:

```sql
SELECT * FROM read_parquet('cruises/*.parquet');      -- only cruises/
SELECT * FROM read_parquet('cruises/**/*.parquet');   -- every level below cruises/
```

### A column has two types across the files

A collection can type one column as a number in one file and as a string in another. No type holds
both, so the table answers no query:

```
Incompatible types for field 'platform': Int32 in 'cruises/2019.nc' vs Utf8 in 'cruises/2020.nc'
```

The message names the column, both types and both files. Fix the files, or drop the column from the
table, or keep the type of the first file. An external table takes the rule from its own `OPTIONS`:

```sql
CREATE OR REPLACE EXTERNAL TABLE cruises
STORED AS NC
LOCATION 'cruises/**/*.nc'
OPTIONS ('type_widening_on_conflict' 'keep_first')
```

The key `type_widening` takes `default` or `numpy` in the same way. See
[Schema inference](/docs/2.1.0-rc3/sql/create-external-table#schema-inference). A `read_*()`
function takes the rule from the server:

```bash
BEACON_TYPE_WIDENING_ON_CONFLICT=keep_first
```

The table then reports the type of the first file. Every other file casts to it, and a value that
type cannot hold reads as `NULL`. The first file of an external table is the first in path order.
A numeric pair, such as `Int32` beside `Float64`, widens
either way. A CSV collection parses every column as text under this setting and casts it afterwards,
because a CSV file states no types.

A number beside a string is no conflict in numpy: the number reads as text. Take the numpy
strategy to apply that rule, and the rest of the promotion table of `numpy.result_type`:

```bash
BEACON_TYPE_WIDENING_STRATEGY=numpy
```

The column then reports `Utf8`, and the scan writes each number as text. No value is lost, and the
listing order does not change the result. The numpy strategy also joins a boolean with a number, a
`Float16` with the other floats, and a date with a timestamp. The default strategy refuses each of
those pairs. A number beside a timestamp stays a conflict under both strategies, and
`BEACON_TYPE_WIDENING_ON_CONFLICT` settles it. See
[Configuration](/docs/2.1.0-rc3/server/configuration#query-engine).

### One invalid file fails each query on a table

A collection can hold a file that Beacon cannot read. A query on the external table then fails, and
the error names the file:

```
external table 'cruises' (netcdf): cannot read file 'cruises/2019/a.nc': ...
```

Repair or delete the file. To query the other files without it, set
`'skip_invalid_files_on_scan' 'true'` in the `OPTIONS` of the table. If `CREATE EXTERNAL TABLE`
fails on the file, also set `'skip_invalid_files_on_inference' 'true'`. See
[Skip invalid files](/docs/2.1.0-rc3/sql/create-external-table#skip-invalid-files).

### New files do not appear

Beacon does not watch storage. It reads no filesystem events and no S3 events. Register a
[crawler](/docs/2.1.0-rc3/server/crawlers) with a schedule.

## Performance

### The first query is slow. Later queries are fast

Beacon lists the files on first use. It also infers the schema. It caches both results. For a large
tree, use a [crawler](/docs/2.1.0-rc3/server/crawlers). The crawler does this work in advance.

### A filter does not reduce the time

Check the plan:

```sql
EXPLAIN SELECT * FROM read_parquet('obs/*.parquet') WHERE depth < 50;
```

If the predicate does not appear near the scan, it did not push down. Common causes: a function on
the filtered column, a type mismatch that forces a cast, or a format that carries no statistics.
See [Speed up slow queries](/docs/2.1.0-rc3/guides/speed-up-queries).

### The query runs out of memory

Do not collect the whole result. Ask for a file format and stream it to disk, or read record batches
over Flight SQL. See [Export query results](/docs/2.1.0-rc3/guides/export-results).

A server-side query can spill to disk. The spill goes to the OS temp area. Put that on fast storage
with free space. See [Performance Tuning](/docs/2.1.0-rc3/server/performance-tuning).

## Connecting

### The server starts and then exits immediately

Read the first lines of the log. Configuration is validated at startup, so a bad setting fails there
rather than on the first query. The most common one:

```text
BEACON_S3_DATASETS is set but BEACON_S3_BUCKET is missing;
the bucket is never inferred from AWS_ENDPOINT
```

See [Configuration](/docs/2.1.0-rc3/server/configuration).

### Two servers cannot share a data directory

Correct. Beacon holds `beacon.db` under an exclusive lock, so one server opens one data directory.
Give each server its own `BEACON_DATA_DIR`. See
[Storage internals](/docs/2.1.0-rc3/internals/storage).

### `CREATE PERSISTENT SECRET` fails

It needs a master key. Set `BEACON_SECRETS_KEY` to base64 of 32 bytes. Beacon fails without the
key. It never writes a plaintext credential.

A plain `CREATE SECRET` needs no key. That secret lives for the session only. See
[Secrets](/docs/2.1.0-rc3/sql/secrets).

### `ATTACH` fails after a key change

Beacon decrypts a persistent secret with the same key only. Drop each persistent secret and create
it again under the new key.

### A remote table fails to authenticate

A remote table connects **anonymously** and stores no credentials. The remote server must allow
anonymous Flight SQL access (`BEACON_FLIGHT_SQL_ALLOW_ANONYMOUS=true`).

For an authenticated connection, use [`ATTACH`](/docs/2.1.0-rc3/data-sources/attach) with a secret.

### A remote table shows the old columns

Beacon reads a remote schema once, at creation time, and pins it. Drop the table and create it again
to pick up a change.

## Permissions

### Anyone can read the data even though the admin password is set

`BEACON_ADMIN_*` protects the admin UI and the write operations. It does not gate reads. Set
`BEACON_AUTH_ENFORCE=true` to apply read grants. See
[Access Control](/docs/2.1.0-rc3/security/access-control).

### A user has a grant but still cannot read

A deny always wins over a grant, at every level. Look for a deny rule on the table. Also look for a
deny rule on a path glob that covers the table.

### A user created with SQL cannot run DDL

By design. There is exactly one super-user, and it comes from `BEACON_ADMIN_USERNAME` and
`BEACON_ADMIN_PASSWORD`. Every user and role created through SQL is read-only.

### The container reports Permission denied on a mounted folder

The log shows `Failed to list table directory: ... Permission denied (os error 13)`. An `ls` of
`/beacon/data/datasets` in the container also fails. The host folders have mode `777`.

The cause is SELinux, not the file permissions. The host folders have no container label, so SELinux
blocks the container. Run `getenforce` on the host. The output `Enforcing` tells you that SELinux is
active.

Add the `:z` flag to each bind mount:

```bash
-v ./datasets:/beacon/data/datasets:z \
-v ./tables:/beacon/data/tables:z
```

Use `:Z` when only one container uses the folders. For a filesystem without labels, such as NFS,
use `--security-opt label=disable`. Do not add `:z` or `:Z` to a system folder, such as `/home`.
See the [FAQ](/docs/2.1.0-rc3/faq#the-container-reports-permission-denied-on-a-mounted-folder).

## Still stuck

- [FAQ](/docs/2.1.0-rc3/faq)
- [GitHub issues](https://github.com/maris-development/beacon/issues)
- [Community Slack](https://beacontechnic-wwa5548.slack.com/join/shared_invite/zt-2dp1vv56r-tj_KFac0sAKNuAgUKPPDRg)
