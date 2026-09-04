# Changelog

Notable changes to Beacon. Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Every artifact in this repository — the `beacon-server` image, the `beacondb` Python package,
`beacon-datalake-cli`, and `@beacon/client` — shares one version and is released from a single `v*`
tag. Releases before 2.0.0 are recorded in the
[GitHub releases](https://github.com/maris-development/beacon/releases).

## [Unreleased]

### Added

- **The server root is a home page instead of a jump to Swagger.** `http://localhost:5001/` sent
  every visitor straight to the Swagger UI, which hid the admin panel, the API reference and the
  documentation from anyone who did not know their paths. The root now answers with a small page
  that links to all of them, plus the OpenAPI document and the health endpoint, and names the
  running version. The documentation link is pinned to that version, so a server two releases old
  no longer sends its operator to the newest manual. The admin card appears only when the admin UI
  is mounted, and the MCP address only when MCP is enabled. Every link carries the configured
  `BEACON_BASE_PATH`. The page carries the colors, the type and the card layout of the
  documentation site, and it loads nothing from the network, so it also renders on a server with
  no route out. Swagger keeps its own path, so a bookmark to `/swagger` is unaffected.
- **`BEACON_TYPE_WIDENING_ON_CONFLICT` settles a column that no type holds.** A collection can
  type one column as a number in one file and as a string in another. No type holds both, so the
  schema merge refused the whole table and the table answered no query: `Incompatible types for
  field 'platform': Int32 in 'argo/2019.nc' vs Utf8 in 'argo/2020.nc'`. That stays the default,
  under the name `fail`. `BEACON_TYPE_WIDENING_ON_CONFLICT=keep_first` keeps the type of the first
  file instead. The table reports that type, every other file casts to it, and a value the type
  cannot hold reads as null. The merge marks such a column in the merged schema, so the scan reads
  the decision and no scan needs the setting. Two costs follow, and both apply to `keep_first`
  alone. The merge reads the listing order, so it drops no repeat schema and starts no thread.
  The first type is the first in that order, so a store that lists in two orders reports two types.
  `fail` keeps the order-independent merge it always ran. A numeric pair, such as `Int32` beside
  `Float64`, widens as before under either setting. An unknown value logs a warning and
  reads as `fail`, because a server that cannot start over a typo is worse than one that names the
  column. An embedded build sets the same rule with `RuntimeBuilder::with_type_conflict`. See
  [Configuration](docs/docs/2.0.0-rc5/server/configuration.md#query-engine) and
  [Troubleshooting](docs/docs/2.0.0-rc5/troubleshooting.md#a-column-has-two-types-across-the-files).
- **A gateway can sign you in to the admin UI.** A deployment that puts nginx or oauth2-proxy in
  front of Beacon holds the super-user credentials in the gateway, and the operator had to type
  them a second time in the login screen. The UI now calls `GET /admin/api/admin/check` with no
  credentials on first load. Beacon answers `401` to a request that carries none, so a `200` proves
  a gateway adds them. The UI then starts a **proxy session**: it stores no credentials in the
  browser, it sends no `Authorization` header of its own, and the user menu reads `Proxy session`.
  The check fails safe: `401`, `403` or a network error gives the login screen. **Sign out** stops
  the detection for that browser tab, and a new tab starts a proxy session again. An injected
  header makes every caller behind the gateway a super-user, so put your own authentication in
  front of it. See
  [the admin web UI](docs/docs/2.0.0-rc5/connect/web-admin-ui.md#a-gateway-can-sign-you-in).
- **`COMPACT TABLE` reclaims what a managed table's writes leave behind.** A Lance table never
  shrinks on its own: every `INSERT` commits its own fragments, a `DELETE` writes a deletion file
  and keeps the rows, an `UPDATE` rewrites fragments, and each superseded version still holds its
  files. A table filled by a long series of small inserts therefore carries far more fragments than
  its row count justifies — and a fragment is a scan partition, so that is planning cost as well as
  disk. `COMPACT TABLE measurements` merges those fragments into target-sized ones, materializes
  the deletions, drops the versions that are old enough to go, and returns one report row:
  `fragments_removed`, `fragments_added`, `files_removed`, `files_added`, `versions_removed` and
  `bytes_removed`. The table's indexes survive — Lance remaps them onto the rewritten fragments as
  part of the same commit — which is what separates this from rebuilding a table by hand with
  `CREATE TABLE … AS SELECT`. Two options tune it, as
  `WITH ('target_rows_per_fragment' '500000', 'cleanup_older_than' '2h')`: the first sets the
  fragment size to aim for (default 1Mi rows) and thereby which fragments count as too small, the
  second the age below which a superseded version is kept. That second one defaults to `7d` and is
  a safety window, not a tuning knob: a running query reads the version it opened at planning time,
  so a cleanup that deletes those files underneath it breaks the query. `'0s'` reclaims the space
  immediately and is the right setting on a quiet instance; `'never'` compacts and cleans up
  nothing. Lance-backed tables only, and super-user only like the rest of managed-table DDL.
- **`pip install beacondb` works again.** The release workflow of the `beacondb` wheel is back,
  together with the `beacondb` entry in the version scripts. A `v*` tag publishes the wheels and
  the sdist to PyPI. A `beacondb-v*` tag publishes the wheel alone, on its own version line. The
  wheels cover manylinux x86_64 and aarch64, macOS arm64, and Windows x64. Each wheel carries its
  own netCDF, HDF5, OpenSSL and PROJ, so a machine needs none of them installed. PROJ is the
  addition: `ST_Transform` links it, and no runner holds a recent enough copy, so the build
  compiles it from source. That build keeps the CRS database of PROJ inside the compiled library,
  so the wheel stays self-contained and reads no `PROJ_DATA`. Start the workflow by hand with
  `dry_run` to build and test every wheel before a tag exists. The wheel is **AGPL-3.0**, like the
  engine it holds.
- **`PARTITIONED BY` works for netCDF, HDF5 and GeoTIFF tables.** `CREATE EXTERNAL TABLE
  observations STORED AS NC LOCATION 'obs/' PARTITIONED BY (year, month)` used to fail with a
  `NotImplemented` error naming the format and the columns; it now plans and runs, and a filter on
  a partition column still prunes whole directories out of the listing. A partition value is in the
  *path* of a file rather than inside it, and DataFusion's `FileStream` appends it per plan entry —
  which it can do only because an entry is a file. These three formats read a whole collection
  behind one entry, so each file's values now travel on the queue with that file, and the reader
  appends them itself: as one value on no axis, which broadcasts over whatever grid the file's own
  columns define, so nothing is built per row. A query for nothing but the partition column
  (`SELECT year, count(*) … GROUP BY year`) states the value over the rows of the read instead,
  because there is no other column to define a grid. `ZARR` still refuses the clause: a Zarr table
  holds groups inside one store, not files in directories, so a path holds no value to read.
- **The admin UI shows the disk space of the datasets store.** An operator sees the total space,
  the used space, the free space and the used percent on the Server page and on the Datasets page,
  and no longer opens a shell to read them. The bar turns amber at 70% and red at 85%. The values
  come from `GET /api/admin/datasets/storage`, which reads the disk that holds the datasets
  directory through `sysinfo`; `client.admin.datasetStorage()` calls it from the TypeScript SDK. An
  S3 bucket has no disk limit, so it answers with the bucket name, the total size of the objects
  and the object count, and reads `n/a` for the total space, the free space and the used percent.
  That answer needs a full bucket listing, so poll the endpoint at a low rate.
- **122 spatial functions with PostGIS names.** `ST_Distance`, `ST_Intersects`, `ST_Buffer`,
  `ST_Centroid`, `ST_Simplify` and the rest now run in SQL — 117 scalar functions, 3 aggregate
  functions (`ST_Extent`, `ST_Collect`, `ST_MemUnion`) and 2 window functions
  (`ST_ClusterKMeans`, `ST_ClusterDBSCAN`). They replace `geodatafusion`, which held a much
  smaller set. A netCDF, Zarr, CSV or Parquet table holds coordinate columns, not geometry, and
  `ST_Point(longitude, latitude)` builds a geometry from those, so the whole set reaches every
  format. A GeoParquet geometry column is a native GeoArrow column, and the functions read it
  directly.
  Each predicate runs a bounding box test before the exact test, and a constant argument gets a
  cached R-tree. This set is now the whole geospatial surface of Beacon: the two Beacon geo UDFs
  that used to sit beside it are gone (see Removed). Some functions differ from PostGIS. Measurement is
  planar, so `ST_Distance` over longitude and latitude returns degrees. The coordinate reference
  system belongs to the column, not to the row. The `&&` operator is the
  `ST_BBoxIntersects` function, and the one-argument `ST_Union` is `ST_MemUnion`. `SHOW FUNCTIONS`
  lists only the functions that take numbers or text, such as `ST_Point` and `ST_GeomFromText`: it
  reads `information_schema.parameters`, and a function that accepts any argument type states no
  argument types and so gets no row there
  ([datafusion-spatial#1](https://github.com/robinskil/datafusion-spatial/issues/1)). Every
  function runs, listed or not. See
  [the function reference](docs/docs/2.0.0-rc5/sql/function-reference.md#geospatial-functions),
  which is the full list.
- **`ST_Transform` reprojects a geometry**, and a standard build ships it. That makes 123 spatial
  functions. It links [PROJ](https://proj.org), so **a build from source now needs PROJ 9.6.2 or
  later and pkg-config**, beside the netCDF and HDF5 it already needs (`apt-get install
  libproj-dev pkg-config`, or `brew install proj pkg-config`). The image installs both, and the
  runtime image carries `proj-data` for the `proj.db` that resolves an EPSG code. Two options
  cover a machine without PROJ: `--features spatial-proj-bundled` builds PROJ from source, and
  `--no-default-features` drops `ST_Transform` and the PROJ dependency with it. The other 122
  functions need no native library either way.
- **Zarr stores supply column ranges for file pruning.** A Zarr store recorded nothing in
  `beacon.system.file_stats`, so every query opened every store. It now reports a range per
  coordinate: an array of rank 0 or rank 1 is read and measured, and an array of rank 2 or higher —
  a data grid — reports unknown, so a scan costs what it always did. An array that states its own
  `actual_range` is bounded from metadata alone, with no chunk read at all. `valid_min` and
  `valid_max` are deliberately **not** used: they state which values are valid, not which values a
  store holds, and a store may hold values outside them. `BEACON_ZARR_ENABLE_STATISTICS=false`
  (or `OPTIONS (enable_statistics 'false')` on one table) turns the whole thing off.
- **Beacon reads Apache Iceberg tables.** `CREATE EXTERNAL TABLE … STORED AS ICEBERG LOCATION
  'iceberg/obs'` registers an existing table, and `read_iceberg('iceberg/obs')` queries one
  ad-hoc; both take an optional `snapshot_id` for time travel. A table is named by its directory,
  with no catalog: Beacon finds the current metadata file from `metadata/version-hint.text`, or
  from the highest-numbered `*.metadata.json`. Every byte is read through the datasets store, so a
  table on S3 reads with no local copy and needs no separate credentials, and the absolute paths
  the metadata records are rebased onto the location you gave — a table written elsewhere and
  mounted here just reads. A registered table re-reads its metadata per query, so a snapshot or a
  column another writer commits shows on the next query without a restart. A `WHERE` clause is
  pushed into the Iceberg scan, which drops data files from the manifests' statistics. Reads only:
  no `INSERT`, `MERGE` or snapshot expiry, and no REST or Glue catalog yet. See
  [Apache Iceberg](docs/docs/2.0.0-rc5/formats/iceberg.md).
- **Icechunk repositories read through the Zarr reader.** An Icechunk repository is a Zarr v3 store
  with commits, branches and snapshots. `read_icechunk('sst/repo')` and `CREATE EXTERNAL TABLE …
  STORED AS ICECHUNK` read one version of it: the tip of a branch by default, or a fixed `tag` /
  `snapshot`, so a query reproduces after a later commit. The repository only supplies the storage
  a group is opened over — schema inference, the array handling and the chunk-level predicate
  pushdown are the same code a plain Zarr store already went through. A repository reads in place
  from the datasets store, S3, GCS or Azure, with no local copy. Reads only: no commit, no branch
  creation, no `INSERT`. Virtual chunk references — chunks that stay inside a netCDF or HDF5 file
  outside the repository, as VirtualiZarr produces — are **not** followed, because that read needs
  the credentials of a different store than the one the caller was granted.
- **Every endpoint answers on a second path below `/admin`.** `POST /admin/api/query` runs the same
  handler as `POST /api/query`, and `/admin/api/admin/crawlers` the same as `/api/admin/crawlers`.
  The whole alias sits behind the admin Basic auth gate, the client endpoints included: `/api/info`
  answers any caller, `/admin/api/info` only the super-user. The
  [admin web UI](docs/docs/2.0.0-rc5/connect/web-admin-ui.md) now calls the alias, so a deployment
  that puts its own security in front of `/api/*` keeps a working admin panel. `@beacon/client`
  reaches the alias with the new `apiPrefix: ADMIN_API_PREFIX` client option. The alias stays out of
  `/openapi.json`: publishing it would list every operation twice and repeat each operation id. See
  [the REST API reference](docs/docs/2.0.0-rc5/api/index.md#admin-path-alias).

### Changed

- **A file statistics pass drains the queue, and one pass runs at a time.** A pass used to stop
  after one batch of `BEACON_FILE_STATS_BATCH_FILES` files, 10 000 by default. A fresh archive of
  a million files therefore needed 100 ticks, which is over 24 hours at the default interval of 900
  seconds, and every query before that read the schema of each file again. A pass now takes batch
  after batch until the queue is empty, so the first pass that reaches a store covers it. The batch
  still bounds the memory the pass holds. `BEACON_FILE_STATS_INTERVAL_SECS` is now the gap between
  drains, not the rate at which an archive is covered. Three things start a pass: the timer, the
  startup collection and `ANALYZE FILES`. Nothing claims a file when it leaves the queue, so two
  passes at once read the same files and pay for each read twice. A pass therefore holds a lock for
  its length. A timer tick that lands on a running pass is skipped, because the running pass drains
  exactly the same queue. The startup collection waits instead, since it runs once. `ANALYZE FILES`
  reports an error and names the query that shows the progress of the running pass, because a pass
  over a large archive runs for minutes. The timer also starts its interval again when a pass ends,
  so a pass that outruns the interval no longer fires every missed tick back to back, each
  re-listing the store for a queue the pass just emptied. See
  [File statistics](docs/docs/2.0.0-rc5/internals/file-statistics.md).
- **The GeoJSON filter of the JSON query plans `ST_Within`.** A request carries
  `longitude_column`, `latitude_column` and `geometry`. It used to build
  `st_within_point(st_geojson_as_wkt('<geojson>'), lon, lat)`. That path turned the geometry into
  WKT text, then parsed the text back. It now builds
  `ST_Within(ST_Point(lon, lat), ST_GeomFromGeoJSON('<geojson>'))`. The JSON path and the SQL path
  now state one test under one name. **The request format does not change**, and neither do the
  rows a request returns. The two functions the old expression called are removed with it; see
  Removed. Neither expression prunes GeoParquet row groups; that needs a bare geometry column.
- **File statistics are on by default.** `BEACON_FILE_STATS_ENABLE` now defaults to `true`. The
  reason for the old default is gone. netcdf-c reported no range, and the pure-Rust readers for
  netCDF and HDF5 are the default now, so a pass records a real range. The same store holds the
  schema cache. A server without that store reads the schema of each file again on each cold query,
  which was 83% of one netCDF query over 100000 files. The timer still runs its first pass one
  interval after boot. `ANALYZE FILES` fills the store at a time you choose. Set
  `BEACON_FILE_STATS_ENABLE=false` for an archive of formats that supply no range: ODV, CSV and
  TIFF record zero columns.
  `BEACON_FILE_STATS_ON_STARTUP` stays `false`. A pass at startup holds the database file while it
  reads a batch. A caller that drops a runtime and opens the same file again then gets a lock
  error. Set the flag to true after a shutdown waits for the pass.
- **An embedded database configures file statistics**, through `OpenOptions::file_stats`. It
  configures crawlers the same way. The subsystem needs a database file and a datasets store, so an
  in-memory database and a dynamic-mode database leave it off, whatever the option says.
- **The documented defaults for the pure-Rust netCDF and HDF5 readers match the code.**
  `BEACON_NETCDF_USE_RUST_READER` and `BEACON_HDF5_USE_RUST_READER` default to `true`; the pages
  and doc comments still described them as off.
- **One entry point merges every schema, and it merges in parallel.** Each format merged the
  schemas of the files behind a URL itself. The table above merged the URLs with the rule of the
  session. The applied rule therefore depended on the spelling of a `read_*`. Each format and the
  table now read the same `ArrowTypeWidening` from the session and merge with it, so
  `RuntimeBuilder::with_type_widening` sets the rule for the whole process. Because the rule is a
  lattice join, the entry point drops a schema it has seen and gives each contiguous chunk to a
  thread. A collection of 100000 files from one instrument holds few distinct schemas, so the merge
  reads few schemas. Column order still follows the listing, because `SELECT *` shows it.
- **The pure-Rust reader is the default of the library too.** `NetcdfConfig::default()` and
  `Hdf5Config::default()` select it, so an embedded caller and a `RuntimeBuilder` read a `.nc`,
  `.h5` or `.hdf5` file the way the server does. The variables keep their names:
  `BEACON_NETCDF_USE_RUST_READER=false`, `BEACON_HDF5_USE_RUST_READER=false` and
  `OPTIONS ('use_rust_reader' 'false')` still select netCDF-C, and every write still uses it.
- **An HDF5 table on netCDF-C read through the Rust reader.** The HDF5 format has no netCDF-C
  reader of its own: it hands the file to the netCDF format, which picks its own reader. That
  reader is the Rust one by default, so `BEACON_HDF5_USE_RUST_READER=false` reached it anyway
  unless netCDF was also set to `false`. The HDF5 fallback now names netCDF-C on the format it
  delegates to, so each variable decides its own format.
- **The admin UI renders a result from the Arrow columns.** The query workbench reads each record
  batch as it arrives and shows it. It no longer builds a JS object for each row first, which cost
  one object per row and one property per column — on a beacon table that carries 100K+ columns,
  the decode was the wait, not the query. The grid now reads one value as `column.get(row)`, and
  the preview and dataset pages take the same path. Duplicate column names survive, because the
  columns come from the Arrow schema and not from the keys of a decoded row. The SDK types state
  the columnar access the UI uses: `ArrowTable` and `ArrowRecordBatch` declare `schema` and
  `getChildAt`, with the new `ArrowVector`, `ArrowField` and `ArrowSchema`.
- **Minimum supported Rust is 1.94**, up from 1.91. `iceberg` and `iceberg-datafusion` 0.10 — the
  only release line built against the DataFusion 53 and Arrow 58 this workspace unifies on —
  declare `rust-version = "1.94"`, so the workspace floor follows. Beacon's own code uses no
  feature newer than 1.91. CI builds the floor leg at 1.94.
- **One product, one name.** "Beacon Data Lake" and "BeaconDB" are gone as marketed products.
  There is one thing, and it is called **Beacon**. Where the running process needs a name, the
  docs say "the Beacon server" in lowercase. The tagline is now "a query engine for scientific
  data"; "data lake" and "data lakehouse" are dropped everywhere, because Beacon reads files in
  place and does not own the storage those terms promise.
- **Crates and directories follow the name.** `beacon-datalake/` is now `beacon-server/`, with
  the crates `beacon-datalake` → `beacon-server` and `beacon-datalake-config` →
  `beacon-server-config`. The binary is `beacon-server`. Internally the `DataLake` type is now
  `Server` and its module is `crate::server`.
- **`beacon-datalake-clients/` is now `beacon-clients/`.** The `beacon-datalake-cli` package keeps
  its name, its module and its console script — only the directory above it moved.
- **`BEACON_S3_DATA_LAKE` is now `BEACON_S3_DATASETS`.** The old name still works and still turns
  the S3 datasets store on; it is deprecated and will be removed one major version after 2.0.
- **Licensing is stated in one place.** The root `LICENSE` (AGPL-3.0) covers the Rust workspace;
  the `beacon-server` crates and `beacon-db-py` restate it in their manifests because they are
  published. The clients under `beacon-clients/` remain Apache-2.0, with their own `LICENSE`.
- **Secrets are documented as an `ATTACH` mechanism only.** A server reads one datasets store,
  local or a single bucket, chosen at startup from configuration. `CREATE SECRET` covers reaching
  another Beacon server.

### Removed

- **`st_within_point` and `st_geojson_as_wkt`.** These were the two geospatial functions Beacon
  carried of its own, beside the 123 PostGIS-named ones. Both are gone, and a query that calls
  either now fails with an unknown-function error. The PostGIS set states the same tests:
  `st_within_point('<wkt>', lon, lat)` becomes
  `ST_Within(ST_Point(lon, lat), ST_GeomFromText('<wkt>'))`, and `st_geojson_as_wkt('<geojson>')`
  becomes `ST_GeomFromGeoJSON('<geojson>')`, which returns a geometry rather than text and so
  needs no second parse. One spatial vocabulary is easier to explain than two.

  This costs speed. `st_within_point` held a bounding rectangle prefilter and an LRU cache over
  the coordinate pair, and it read two ordinate columns with no geometry column in between. A
  bench measured it at 1.6 to 2.5 times the speed of the `ST_Within` expression over a whole
  query, widest on a table that repeats its coordinates — which one station reporting at many
  depths produces. The `within_point` bench that measured this is removed with the functions.
  `geo`, `geojson`, `wkt`, `ordered-float` and `anyhow` leave `beacon-functions` with them.
- **The netCDF and HDF5 reader caches, with `BEACON_NETCDF_USE_READER_CACHE`,
  `BEACON_NETCDF_READER_CACHE_SIZE`, `BEACON_HDF5_USE_READER_CACHE`, `BEACON_HDF5_READER_CACHE_SIZE`
  and the `use_reader_cache` table option.** Both formats held opened datasets in a `moka` cache
  keyed by path, modification time and reader. The schema cache
  (`BEACON_FILE_STATS_SCHEMA_CACHE`) answers the repeated inference that cost the most, so what
  remained was the second open a file takes inside one query — in exchange for two caches, four
  variables and a cache key threaded through every format, source and opener. One open now reads one
  file, and the read path is a single line from the format to the reader. Setting a removed variable
  is ignored, not an error.

  Measured on 100000 netCDF files: a query that reads a few files costs the same, an analyzed
  archive scans 11% slower, and an archive with no statistics store scans 17% slower.
  `ANALYZE FILES` is 6% faster, because a pass paid to fill a cache it never read from. The cache
  size the tuning page recommended, 16384, was slower than the default 128 on every measurement.

### Fixed

- **A query could lose the rows of a file that changed after its analysis.** File statistics let a
  `WHERE` drop whole files before the scan opens them, on the column ranges a background pass
  recorded. That pass compares the size, the modification time and the etag of every listed file
  against its record, and a file that changed stops being trusted. The query path made no such
  comparison: it resolved a file by its path alone, so between two passes a rewritten file still
  carried the ranges of the content it no longer held, and a predicate the new content matched
  pruned it away. The answer was short by those rows, with nothing to show for it. A scan already
  holds the metadata of every file it planned to read, so it now checks that metadata against the
  record itself. A file the record no longer describes reads as unanalyzed and is kept, which is
  the fail-open rule the rest of pruning follows. This closes a window of one pass interval
  (`BEACON_FILE_STATS_INTERVAL_SECS`, 900 seconds by default, and longer on a fresh server because
  `BEACON_FILE_STATS_ON_STARTUP` is off), and it covers a store no pass ever lists at all. Zarr and
  Icechunk plan entries that state a path and no metadata, so there the pass stays the only check
  and pruning is unchanged.
- **A spatial filter on a federated table stopped with `Unsupported scalar: Union`.** `SELECT *
  FROM lake.public.lidar WHERE ST_Within(ST_MakePoint(x, y), ST_GeomFromGeoJSON('…'))` failed at
  plan time against a remote-Beacon table and against a SQL-database table. DataFusion evaluates a
  constant call before it optimizes, so `ST_GeomFromGeoJSON` left the plan and a geometry value
  took its place. GeoArrow stores a mixed geometry as an Arrow union, and a point or a box as an
  Arrow struct. The SQL unparser has no syntax for either, so the federated sub-plan never became
  SQL. 61 of the 117 spatial functions return such a value — `ST_GeomFromText`, `ST_GeomFromWKB`,
  `ST_Buffer`, `ST_Union`, `ST_Centroid`, `ST_MakePoint` and `ST_Envelope` among them. The other 56
  return a number, a boolean or a string, and always worked. A bound parameter did not help,
  because DataFusion writes one into a literal before it optimizes. A federated sub-plan now
  rebuilds each geometry constant as `ST_GeomFromText('…')`, wrapped in `ST_SetSRID` where the
  constant carries an SRID, in the step directly before it becomes SQL. Text keeps every coordinate
  digit and the z ordinate, which GeoJSON drops. Nothing else changes: a local query still folds
  the constant once and never rebuilds it, and the plan schema stays as it was. A SQL-database
  table does this for PostgreSQL alone, because PostGIS reads both calls, while MySQL sets an SRID
  with `ST_SRID` and SQL Server over ODBC uses `geometry::STGeomFromText`. One case stays open:
  `ST_AsBinary`, `ST_AsEWKB` and `ST_Dump` fold to plain binary, which carries no GeoArrow mark, so
  a fully constant call to one of them still has no SQL form.
- **A long query in the admin UI failed after a minute.** `@beacon/client` put a 60-second deadline
  on every request, query execution included, and reported the abort as a `TimeoutError`. Analyze
  was the visible victim: `/api/explain-analyze-query` runs the query to completion before it
  answers, so a query slower than a minute never returned a plan. A query has no bounded duration,
  so the query endpoints (`/api/query`, `/api/explain-query`, `/api/explain-analyze-query`) now run
  without a deadline. Stop the work with the Stop button, which cancels it on the server too. The
  60-second default still guards the bounded endpoints — metadata, schemas, admin calls — and a new
  `queryTimeoutMs` client option puts a deadline back on query execution for a caller that wants
  one.
- **Run and Analyze fought over the query editor.** The workbench started an action without
  stopping the one before it, so an Analyze and a Run raced over the same result panel and the
  server ran the query twice. Whichever finished last won: an Analyze that failed after a Run had
  already drawn its rows replaced them with its own error, and the panel stayed on that error
  because only a *new* action clears it. The editor looked stuck. Run, Explain, Analyze and
  Download now cancel whatever is still running and take the panel for themselves, and a superseded
  action writes nothing when it ends. Switching tabs cancels the query filling the panel instead of
  leaving it to land under another tab's SQL. A second Explain or Analyze also clears the old plan,
  so the panel shows progress rather than the previous answer, and the panel header names the
  action that is running.
- **Seven statements lowercased a table name.** Beacon turns identifier normalization off, so the
  catalog holds a table under the exact name the statement writes. `INSERT`, `CREATE TABLE AS
  SELECT`, `ALTER TABLE`, `CREATE INDEX`, `DROP INDEX`, `SHOW INDEXES`, `REFRESH`, the table
  extension statements and the admin `table-config` route rebuilt the reference from a string with
  `TableReference::parse_str`, which lowercases every unquoted part. Each one asked for a name the
  catalog does not hold, so `CREATE TABLE MyManaged` was followed by `No table named 'mymanaged'`.
  `CREATE MATERIALIZED VIEW MyView` was worse: it registered `myview` but persisted `MyView`, so a
  restart renamed the view and broke every query that used the old spelling. A new `table_name`
  module builds the reference and keeps the case, and every path uses it. A new
  [identifiers page](docs/docs/2.0.0-rc5/sql/identifiers.md) states the rule and its limits.
- **`OPTIONS` on a NetCDF, HDF5, Zarr or BBF external table had no effect**
  ([#421](https://github.com/maris-development/beacon/issues/421)). DataFusion's SQL planner
  renames an `OPTIONS` key without a `.` to `format.<key>`. Those four factories read the bare key
  alone, so `CREATE EXTERNAL TABLE … OPTIONS ('read_dimensions' 'time,lat,lon')` was dropped
  without a word, and so were `use_rust_reader`, `enable_statistics`, `unify_phony_dimensions`,
  `convention` and `split_streams_slice`. The table read the default of the server instead. A
  factory now reads both spellings, as CSV, Delta, Iceberg, Icechunk and the SQL databases already
  did. A crawler passes the bare key, so a crawler option always worked; only SQL was affected.
- **`CREATE EXTERNAL TABLE` did not document `OPTIONS`**
  ([#421](https://github.com/maris-development/beacon/issues/421)). The syntax block omitted the
  clause, and no page listed the keys of a format, so a reader had to open the Rust source. The
  [create external table page](docs/docs/2.0.0-rc5/sql/create-external-table.md#options) now holds the clause, the
  rules that apply to every key, and an index of the keys of each `STORED AS` value. Each format
  page holds a table of its own keys, with a type, a default and a description. The page for a
  format that reads no key says so. Two examples also spelled an option `OPTIONS`, which does not parse: an `OPTIONS` list takes a key and a value, with no `=`.
- **A schema merge depended on the disk answer order**
  ([#377](https://github.com/maris-development/beacon/issues/377)). A table over many files merges
  their schemas into one schema, and a query plans against that schema. Beacon's own "super typing"
  widened a column that two files gave two types, and the result depended on the merge order.
  `Int32` beside `Float32` gave `Float32` in one order and `Float64` in the other. `Date32` beside
  `Float64` gave `Float64` or an error. Five formats read their schemas with `buffer_unordered`,
  which returns them in completion order: Parquet, GeoParquet, CSV, Arrow IPC and BBF. The same
  query over the same files could therefore return different types, or fail, between two runs.
  The rules are now the join of a lattice, so the schema order, the group boundaries and the repeat
  count change no result and no failure. A column widens inside one family only, and the five
  formats read their schemas in listing order, so column order is stable too. The families are the
  numbers, the timestamps (the finer unit, in one time zone), the strings (`Utf8`, `Utf8View`,
  `LargeUtf8`), the binaries (`Binary`, `BinaryView`, `LargeBinary`), the dates (`Date32`,
  `Date64`) and the times (`Time32`, `Time64`). `Null` takes the type of the other file.
  Three results changed. **A number and a string are now an error**, where super typing stringified
  the number. **A `Boolean` and a number are an error**, and so are a `Timestamp` and an integer.
  **An integer beside a `Float32` widens to `Float64`**, where the old table kept `Float32` for a
  narrow integer: a `Float32` holds no `Int32`, and a lattice cannot keep `Int8` and drop `Int32`.
  A time zone follows DataFusion: one file with a zone gives that zone, and two files with two
  zones give `UTC`. DataFusion keeps the zone of the left operand there, and a merge has no left
  operand. A merged column also keeps the field metadata of the first file that states it, and it
  is nullable unless every file holds it and every file requires it.
- **The GeoParquet scan applied only part of a pushed-down projection.** A `FileSource` that
  accepts a projection has to apply the whole of it. This one accepted a projection and then read
  only the column names out of it, which dropped everything else. Geometry is written last,
  so every query over it failed, and so did a plain `WHERE temperature > 0`. Two quieter faults
  came with it: `SELECT x AS y` found no column named `y` in the file and returned a column of
  NULLs, and a `PARTITIONED BY` value was null-filled instead of taken from the path. The scan now
  splits the projection with DataFusion's own `SplitProjection`: the reader selects the file
  columns, and `ProjectionOpener` applies the rest above it. The reader also stops decoding the
  columns it then threw away — it reads only the projected ones.
- **A large GeoParquet file returned every row once per partition.** DataFusion divides a file over
  the repartition threshold into byte ranges, one per partition, and the GeoParquet reader ignored
  the range it was given, so each partition read the whole file. The reader now takes the row groups
  whose first page starts inside its own range, which is the rule the plain Parquet reader uses.
- **A GeoParquet geometry column reached the spatial functions as a plain struct.** Merging the
  file schemas rebuilt every field from its name and type, which dropped the GeoArrow extension
  keys that mark a column as a geometry. `ST_Extent(geometry)` answered `Extension type name
  missing`. A field now keeps its own metadata whenever every file states the same metadata for it.
- **A GeoParquet scan read every row group, and reported no statistics.** A GeoParquet file states
  a bounding box per row group, in its `covering` metadata or in the coordinate columns of a native
  encoding, and Beacon read neither. A filter of `ST_Intersects`, `ST_Within`, `ST_Contains`,
  `ST_BBoxIntersects` or `ST_DWithin` with a constant distance now drops each row group whose box
  lies outside the query box, before it reads a byte. The box test is not the exact test, so the
  predicate still runs over every row the scan keeps. `EXPLAIN ANALYZE` reports
  `geoparquet_row_groups_considered`, `geoparquet_row_groups_pruned` and `geoparquet_files_pruned`.
  `infer_stats` also reads the file metadata now, through the same converter plain Parquet uses, so
  `beacon.system.file_stats` holds a row count and a range per plain column and file pruning drops
  a file before the row group step runs.
- **The admin web UI ignored the server URL prefix.** Behind `BEACON_BASE_PATH=/beacon` the page at
  `/beacon/admin` asked for `/admin/assets/*` and stayed blank. Vite wrote the prefix into every
  asset URL at build time, and one build cannot know a run time setting. The build now emits URLs
  relative to the document, and the page resolves its own root from the current URL before the
  first asset loads. The router basename and the API URL come from that same root, so the UI works
  under any prefix, including one that contains the word `admin`. The server also sends
  `{base_path}/admin` to `{base_path}/admin/`, which keeps the first load free of 404s.
- **File statistics pruned no netCDF or HDF5 file.** The ranges were recorded and then never used.
  Pruning rewrites the file list of a built scan, and it looked for that list on the plan's root
  node. A netCDF or HDF5 scan is not that node: its arrays reach the plan encoded, so the format
  returns a decode and a broadcast above the scan, and the file list sits two nodes down. Pruning
  now descends to it and rebuilds the plan over the shorter list. A `WHERE` clause on a recorded
  column drops the files it rules out, exactly as it already did for Parquet. Requires the Rust
  reader (`BEACON_HDF5_USE_RUST_READER`, `BEACON_NETCDF_USE_RUST_READER`), which is what records
  the ranges in the first place.
- **A pass on netcdf-c never said why it recorded nothing.** A `.nc`, `.h5` or `.hdf5` file read
  through netcdf-c analyzes cleanly and contributes no ranges, which reads exactly like a file that
  has none. Each pass now logs the reason once, and names the variable to set.
- **A netCDF time variable kept its `_FillValue` cells as dates.** Both netCDF readers dropped the
  `_FillValue` of a CF time variable, so a fill cell reached a query as a real timestamp:
  `units = "days since 1970-01-01"` with `_FillValue = -32768` gave `1880-03-15`. Such a value
  passed a filter and joined a group. The readers now decode the fill with the same CF arithmetic
  as the data, and the cell is NULL. Zarr already did this, so the same dataset gave two answers.
- **`read_schema(paths, format)` never existed.** The docs, and one integration test, called a
  generic function that is not registered. The real API is a per-reader counterpart —
  `read_parquet_schema`, `read_netcdf_schema`, and so on, one for every reader including
  GeoParquet, Atlas, Delta and ODV.
- **`SUMMARIZE read_netcdf(...)` does not parse.** `SUMMARIZE` takes a name or a query, so a bare
  table function needs wrapping: `SUMMARIZE (SELECT * FROM read_netcdf(...))`.
- **HDF5 was undocumented.** `read_hdf5` and `STORED AS HDF5` have always worked; they now have a
  format page and a row in the format tables.
- **Dead benchmark link.** `benchmarks/README.md` pointed at a write-up that has never existed in
  this repository.

## [2.0.0-rc.1] — 2026-07-31

The 2.0 line turns Beacon from a server into an engine you can also embed. The same SQL, readers
and catalog now run in three places: in-process from Python, behind the HTTP/Flight SQL server, and
over MCP.

### Added

- **An optional pure-Rust HDF5 reader.** `BEACON_HDF5_USE_RUST_READER=true`, or
  `OPTIONS ('use_rust_reader' 'true')` on one table, reads `.h5`/`.hdf5` without the netCDF-C
  library: no process-global lock, so scans run in parallel, and byte ranges through the object
  store, so a file in S3, GCS or Azure needs no local copy. It also reads two layouts the netCDF
  data model cannot express — a nested group, whose datasets take their path as their column name,
  and a compound dataset, whose members each become a column. Off by default; a server that leaves
  it off behaves exactly as before, and every write still uses the netCDF-C library.
- **BeaconDB — the engine as an embeddable Python package.** `pip install beacondb`, `import
  beacondb`, and the whole engine runs in-process; no server, no HTTP. Results cross into Python
  over the Arrow PyCapsule protocol, so pyarrow/pandas/polars are only needed by the methods that
  return their types. Ships `py.typed` stubs and a `beacondb://` SQLAlchemy dialect.
- **Single-file databases.** A new redb-backed object store lets one `beacon.db` hold everything
  Beacon owns — its catalog and managed data — while still referencing everything else in place.
  Copy the file and the managed lake travels with it.
- **N-dimensional execution.** Zarr and netCDF now flow through the engine as nd-encoded batches
  rather than being flattened on read, with projection and filter pushdown evaluated *before* the
  broadcast so element-wise work and `WHERE` predicates run on the compact representation.
- **`beacon-mcp`** — an MCP server exposing tables to LLM clients over streamable HTTP, including
  an `export_query` tool with guard rails for large results.
- **`@beacon/client`** — a TypeScript client for the data lake's HTTP API.
- **Object-store secrets.** `CREATE SECRET` for S3/GCS/Azure/HTTP credentials, scoped by URL
  prefix, session-only by default. `CREATE PERSISTENT SECRET` encrypts them into the database file
  (XChaCha20-Poly1305) so a copied file carries its own cloud access; it requires a master key,
  because Beacon refuses to write a plaintext credential to disk.
- **Remote catalogs.** `ATTACH` another Beacon over Arrow Flight SQL and query it as
  `name.schema.table`, joining remote tables against local files. The federation optimizer pushes
  the largest federatable sub-plan to the remote, so the heavy scan stays there.
- **Source distributions** for `beacondb`, so `pip install` works on platforms with no wheel.

### Changed

- **`beacon-cli` is now `beacon-datalake-cli`** on PyPI, with the module renamed to
  `beacon_datalake_cli`. The old package name is not updated; install the new one.
- **Minimum supported Rust is 1.91**, declared as `rust-version` in the workspace and enforced by
  Cargo rather than only by a toolchain file.
- **Releases verify rather than rewrite the version.** CI no longer stamps a version into the
  manifests from the tag; the committed manifests are authoritative and a mismatched tag fails the
  release. Bump with `scripts/bump-version.py`, commit, then tag.

### Known issues

- `beacondb` wheels are large — each embeds a full DataFusion/Lance/netCDF engine — and there is no
  minimal, feature-gated build yet.
- A source build links netCDF and HDF5 dynamically, unlike the wheels, so it needs those libraries
  present. Pass `MATURIN_PEP517_ARGS="--features static-netcdf"` for the self-contained variant.
- One handle per database file: the container is held under an exclusive lock, so
  `read_only=True` is a per-connection guarantee, not multi-process concurrency.

[2.0.0-rc.1]: https://github.com/maris-development/beacon/compare/v1.8.0...v2.0.0-rc.1
