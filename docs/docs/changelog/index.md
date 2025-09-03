# Changelog

## Latest (v1.2.0 - 2025-09-01)

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