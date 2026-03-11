# Datasets

All datasets intended for querying through Beacon must be placed in the following directory inside the Docker container: `/beacon/data/datasets/`
No other configuration is required to make the datasets available for querying. Beacon will be able to infer the schema (columns,variables,attributes) of the dataset and make it available for querying.

## Supported File Formats

Beacon supports the following file formats for querying datasets:

### Zarr

Beacon supports Zarr datasets extensively with the following features:

- Support for Zarr v3 format.
- Support for chunked datasets.
- Support for Array Slice Pushdown using Predicate Pushdown. This allows Beacon to only read the necessary chunks of data based on the query predicates, improving performance significantly (eg, spatial and temporal queries or both together). Beacon can leverage Zarr's chunking scheme to minimize data reads.
- Support for compressed datasets (e.g., zstd, gzip).
- Support for nested directories containing multiple Zarr datasets.
- Support for cloud storage backends (e.g., S3, GCS).

Limitations:

- Datasets with user defined data types are not supported.

### NetCDF

::: tip
It is recommended to convert NetCDF files to a Beacon Binary Format dataset using the [beacon-binary-format-toolbox](../beacon-binary-format/how-to-use.md#creating-a-beacon-binary-format-file-collection) for optimal performance and full feature support.
NetCDF S3 support is limited to anonymous access only. Authenticated access is not supported yet.
:::

Beacon supports NetCDF but limited to native data types. User defined types are not supported.

- NetCDF4 (recommended)
- NetCDF3
  - char* arrays with a string like dimension (eg. STRLEN) will be inferred as a fixed size string

Limitations:

- S3 Cloud Storage backends for NetCDF are only supported when allowing anonymous access. Authenticated access is not supported yet.

### Parquet

Beacon supports parquet natively through datafusion.

Limitations:

- Hive partioning is not supported.

### CSV

Beacon supports CSV files with the following limitations:
  
- The first row of the CSV file must contain the column names. (Header Row)
- The CSV file must be well-formed and properly formatted.
- The CSV file must be encoded in UTF-8.
- Beacon will infer the schema based on the entire CSV file, this can be optionally changed in the configuration.

### ODV ASCII

Fully supported. It is recommended to store the ODV ASCII files using zstd compression. This can be done using the `zstd` command line tool:

```bash
zstd -9 < input.txt > output.txt.zst
```

Beacon will automatically detect the compression and decompress the file on the fly. This also works when ODV ASCII is stored in cloud compatible storage.

### Arrow IPC

Fully supported.

### Beacon Binary Format

Fully supported. Learn more more about this format and its usage.
It fully supports S3 data lake integration and allows for efficient pruning similar to parquet.

## Exploring Datasets

Once the datasets are mounted into the container, Beacon will automatically discover them and make them available for querying.
To list the available datasets, you can use the following API endpoint:

```http
GET /api/list-datasets
```

### Listing available columns/variables/attributes of a dataset

```http
GET /api/dataset-schema?file=example.nc
```

However, it it also possible to list the merged schema of all datasets found using a glob path:

```http
GET /api/dataset-schema?file=*.nc
```
