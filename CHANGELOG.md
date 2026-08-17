# Changelog

Notable changes to Beacon. Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Every artifact in this repository — the `beacon-server` image, the `beacondb` Python package,
`beacon-datalake-cli`, and `@beacon/client` — shares one version and is released from a single `v*`
tag. Releases before 2.0.0 are recorded in the
[GitHub releases](https://github.com/maris-development/beacon/releases).

## [Unreleased]

### Added

- **122 spatial functions with PostGIS names.** `ST_Distance`, `ST_Intersects`, `ST_Buffer`,
  `ST_Centroid`, `ST_Simplify` and the rest now run in SQL — 117 scalar functions, 3 aggregate
  functions (`ST_Extent`, `ST_Collect`, `ST_MemUnion`) and 2 window functions
  (`ST_ClusterKMeans`, `ST_ClusterDBSCAN`). They replace `geodatafusion`, which held a much
  smaller set. A netCDF, Zarr, CSV or Parquet table holds coordinate columns, not geometry, and
  `ST_Point(longitude, latitude)` builds a geometry from those, so the whole set reaches every
  format. A GeoParquet geometry column is a native GeoArrow column, and the functions read it
  directly.
  Each predicate runs a bounding box test before the exact test, and a constant argument gets a
  cached R-tree. Beacon's own `st_within_point` and `st_geojson_as_wkt` stay beside the set: they
  need no geometry column, and `beacon-functions/benches/within_point.rs` measures the first one at
  4 to 12 times the speed of `ST_Within` on a column that repeats its coordinates, which is what
  one station reporting at many depths produces. Some functions differ from PostGIS. Measurement is
  planar, so `ST_Distance` over longitude and latitude returns degrees. The coordinate reference
  system belongs to the column, not to the row. The `&&` operator is the
  `ST_BBoxIntersects` function, and the one-argument `ST_Union` is `ST_MemUnion`. `SHOW FUNCTIONS`
  lists only the functions that take numbers or text, such as `ST_Point` and `ST_GeomFromText`: it
  reads `information_schema.parameters`, and a function that accepts any argument type states no
  argument types and so gets no row there
  ([datafusion-spatial#1](https://github.com/robinskil/datafusion-spatial/issues/1)). Every
  function runs, listed or not. See
  [the function reference](docs/docs/2.0.0-rc2/sql/function-reference.md#geospatial-functions),
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
  [Apache Iceberg](docs/docs/2.0.0-rc2/formats/iceberg.md).
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

### Changed

- **File statistics are on by default.** `BEACON_FILE_STATS_ENABLE` now defaults to `true`. The
  reason it was off is gone: netcdf-c reported no ranges at all, and the pure-Rust readers for
  netCDF and HDF5 are now the default, so a pass records real ranges. This also turns on the schema
  cache, which lives in the same store: without it a query derives every file's schema again on
  every cold plan, which was 83% of a netCDF query over a hundred thousand files. The timer's first
  pass still runs one interval after boot — `ANALYZE FILES` fills the store at a time you choose.
  Set `BEACON_FILE_STATS_ENABLE=false` for an archive of formats that supply no ranges: ODV, CSV
  and TIFF record zero columns.
  `BEACON_FILE_STATS_ON_STARTUP` stays `false`. A startup pass holds the database file while a
  batch runs, so a caller that drops a runtime and reopens the same file gets a lock error; it can
  be turned on once teardown waits for the pass.
- **An embedded database can configure file statistics**, through `OpenOptions::file_stats`,
  the way it already configures crawlers. It still needs a database file and a datasets store, so
  an in-memory or dynamic-mode database leaves the subsystem off whatever the option says.
- **The documented defaults for the pure-Rust netCDF and HDF5 readers match the code.**
  `BEACON_NETCDF_USE_RUST_READER` and `BEACON_HDF5_USE_RUST_READER` default to `true`; the pages
  and doc comments still described them as off.
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
  the `beacon-server` crates restate it in their manifests because they are publishable. The
  clients under `beacon-clients/` remain Apache-2.0. [LICENSING.md](LICENSING.md) documents it.
- **Secrets are documented as an `ATTACH` mechanism only.** A server reads one datasets store,
  local or a single bucket, chosen at startup from configuration. `CREATE SECRET` covers reaching
  another Beacon server.

### Removed

- **The `beacondb` wheel is no longer published.** Its release workflow, the manylinux build
  scripts and the `make wheel` targets are gone, and the version scripts no longer track it. The
  crate stays in the workspace and still builds locally with maturin; it is marked
  `publish = false` and `Private :: Do Not Upload`. Beacon is a server, and the embeddable wheel
  was the last artifact still selling it as something else.

### Fixed

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
