# Changelog

## (Unreleased)

## (v1.7.0 - 2026-06-10)

- **Changed**: Managed SQL tables (`CREATE TABLE`) are now backed by [Apache Iceberg](https://iceberg.apache.org/) instead of the previous Parquet-manifest format, giving them an ACID, schema-tracked, snapshot-based storage layer. Their data and metadata live in Beacon's internal area of the configured storage (local or S3), alongside the datasets.
- **Added**: Row mutations on managed tables. In addition to `INSERT`, managed tables now support `DELETE ... WHERE`, `UPDATE ... SET ... WHERE`, and `CREATE TABLE AS SELECT`. `DELETE` and `UPDATE` are copy-on-write.
- **Added**: Schema evolution with `ALTER TABLE` on managed tables — `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN`, and `ALTER COLUMN ... TYPE` (safe widening promotions). Existing rows keep reading correctly: added columns read `NULL` and renames preserve values. See [CREATE TABLE (Managed)](/docs/1.7.0/sql/managed-tables).

## (v1.6.1 - 2026-06-04)

- **Added**: Support for the Atlas file format. Atlas is a directory-based array store — a single `atlas.json` registry describing one or more datasets — that Beacon discovers and queries automatically, just like Parquet or Zarr. Query a store with the `read_atlas()` table function or register it as an external table with `STORED AS ATLAS`. Atlas keeps per-dataset column statistics, so Beacon prunes whole datasets before reading any array data and only loads the projected arrays. This makes re-encoding large NetCDF or Zarr collections into a single Atlas collection the recommended way to speed up repeated spatial/temporal range queries. See [github.com/maris-development/atlas](https://github.com/maris-development/atlas).
- **Added**: Materialized views. `CREATE MATERIALIZED VIEW` runs a query once and persists the result as Parquet, so repeated, aggregation-heavy queries read straight from the cached result instead of recomputing. Use the new `REFRESH` statement to recompute a view when its underlying data changes — a full recompute that atomically swaps in the new result, leaving the previous result intact if the refresh fails.
- **Added**: Configurable base path via the `BEACON_BASE_PATH` environment variable. The HTTP API, OpenAPI document, and Swagger UI can now be served under a path prefix, making it easier to run Beacon behind a reverse proxy or on a shared subpath.
- **Added**: Support for decompressing LZW-compressed, stripped GeoTIFFs, broadening the range of GeoTIFF/COG files Beacon can read.
- **Changed**: Upgraded the query engine to DataFusion 53 and Arrow 58. The Arrow version is now configurable at build time to ease integration with downstream tooling.
- **Fixed**: EDMO code extraction now uses the last set of parentheses in a SeaDataNet originator string, so institution names that themselves contain parentheses are mapped to the correct EDMO code.

## (v1.6.0 - 2026-05-08)

- **Added**: Support for Flight SQL. In addition to the existing HTTP query endpoint, users can now query datasets using the Flight SQL protocol. This provides a more efficient and performant interface for querying datasets. It also allows Beacon to be used with a wider range of clients (JetBrains DataGrip, DBeaver) and tools that support Flight SQL, such as Apache Arrow Flight clients and BI tools.
- **Added**: Support for SQL Tables. Possibility to create a SQL table, backed by Parquet files (local or in the cloud). This allows users to create custom tables that are not directly tied to existing datasets, and populate those tables with data from other files or bare insert statements.
- **Added**: Support for SQL views. Create SQL views on top of datasets and expose those views via the API. This allows for more flexible data modeling and querying capabilities, as data managers can define custom views that combine and transform their underlying datasets in various ways.
- **Added**: Support for Tiff files. Beacon can now read and query Tiff files, including geotiff and cloud-optimized geotiff formats. This allows users to work with raster data in addition to the existing support for tabular data formats.
- **Added**: Support for ODV ASCII files. ODV ASCII files can be read directly as datasets in Beacon and queried via the API. ODV ASCII files can also be streamed via the S3 protocol, allowing for efficient access to large ODV datasets stored in S3-compatible object storage.
- **Fixed**: Fixed a bug in the NetCDF reader, where attributes stored as a list of values were not being read correctly while containing a single value. This ensures that all attributes that are scalars can be read by Beacon.
- **Added**: NetCDF reader supports streamed reading using chunks. This allows for more efficient reading of large NetCDF files, as data can be read in smaller chunks rather than loading the entire file into memory at once. This is especially beneficial for users working with large NetCDF datasets, as it can significantly reduce memory usage and improve performance.
- **Added**: NetCDF reader supports pushing down filters to the reader for coordinate variables. This allows for more efficient querying of NetCDF datasets, as filters can be applied directly to the coordinate variables during reading, reducing the amount of data that needs to be read and processed in memory.
- **Added**: Support for reading "ragged" NetCDF & Zarr datasets. Ragged datasets are a type of file that can contain variables with different lengths, allowing for more flexible data structures. This allows users to work with a wider range of datasets, including those that do not conform to the traditional rectangular structure.

## (v1.5.4 - 2026-01-05)

- **Added**: Support for SQL querying. Users can now query datasets using SQL syntax, in addition to the existing JSON query format. This provides a more familiar and powerful interface for users who are comfortable with SQL.
- **Added**: Support for SQL querying for native datasets (eg. NetCDF, Parquet). Users can now use SQL to query native datasets directly, without needing to create a collection first. This allows for more flexible and ad-hoc querying of datasets.
- **Fixed**: Fixed a bug in NetCDF output where the FillValue was not being set correctly. This ensures that missing values are properly represented in the NetCDF output.
- **Fixed**: Updated querying documentation to include examples for SQL querying. This provides users with clear guidance on how to use the new SQL querying features.
- **Fixed**: Update querying documentation to include examples for JSON querying. This ensures that the documentation is comprehensive and up-to-date with the latest features.
- **Added**: Support for setting the "unit" field in the ODV output schema for columns. This allows users to specify the units of measurement for columns in the ODV output, improving the clarity and usability of the output data.

## (v1.2.0 - 2025-09-01)

- **Added**: Support for S3. This all is abstracted via the Object Store Crate. All file sources are abstracted via the Object Store itself and support various backends. (Eg. MinIO, AWS S3, Cloudflare R2)
- **Added**: Implemented support for ODV ASCII streaming via S3 protocol.
- **Added**: Support NetCDF cloud support using #mode=bytes. This only works with http/https specified cloud storage endpoints.
- **Fixed**: Update dependencies to work with the latest version of: Arrow, Datafusion, GeoParquet.
- **Fixed**: Rewrite of file source reading to support S3.
- **Fixed**: Added new table version such as: Preset Tables (Allows to create data collections with meta data descriptions.), Geo Spatial Tables (Allows to create geo-spatial data collections with meta data descriptions.).

## (v1.0.1 - 2025-05-05)

- **Added**: Support for SQL Querying. https://github.com/maris-development/beacon/issues/6
- **Added**: Support for SQL Querying for native datasets (eg. NetCDF, Parquet). https://github.com/maris-development/beacon/issues/33

- **Fixed**: Fixed a bug in NetCDF output where the FillValue was not being set correctly. https://github.com/maris-development/beacon/issues/45
- **Fixed**: Updated querying documentation to include examples for SQL querying.
- **Fixed**: Update querying documentation to include examples for JSON querying.

- **Added**: Support for setting the "unit" field in the ODV output schema for columns. https://github.com/maris-development/beacon/issues/52
- **Fixed**: Missing value flag in NetCDF output schema. https://github.com/maris-development/beacon/issues/45