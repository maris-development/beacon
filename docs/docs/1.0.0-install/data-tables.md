# Data Tables

> [!WARNING]
> Data Tables currently only work when combining datasets of the same file format (e.g. CSV, Parquet, NetCDF, ODV, etc.). Combining datasets of different file formats is not yet supported.

> [!INFO]
> A default table is constructed on startup that includes all netcdf datasets in the `data/datasets/*.nc` directory. This table is named `default`. The table name can be configured to point to another table as the default table using the environment variables.
> It's recommended to set a default table as queries without a `from` clause will use the default table.

Sometimes it is useful to make multiple datasets available via a single `table` name. This can be done by creating a `table` that consists of multiple file paths (or glob paths) that point to multiple datasets in the `data/datasets` directory.

This is useful when you have multiple datasets that are related to each other and you want to query them together.
For example, you may have multiple datasets that are related to the same geographic region, and you want to query them together.

## 1. Creating a Data Table

To create a data table, you need to create a JSON file in the `data/tables` directory. The JSON file should have the following structure:

```json
{
  "name": "table_name",
  "paths": [
    "/atlantic/dataset1.nc",
    "/atlantic/dataset2.nc",
    "/north-sea/*.nc"
  ],
  "file_format": "netcdf"
}
```

- `name`: The name of the table.
- `paths`: An array of file paths (or glob paths) that point to the datasets that you want to combine. These paths should be relative to the `data/datasets` directory. For example, if you have a dataset at `/data/datasets/atlantic/dataset1.nc`, you should use `/atlantic/dataset1.nc` as the path.
- `file_format`: The file format of the datasets. This is required to infer the schema of the datasets. Available file formats are:
  - `netcdf` for NetCDF files
  - `csv` for CSV files
  - `parquet` for Parquet files
  - `arrow` for Arrow IPC files

## 2. Using a Data Table

Once you have created a data table, it will be available to query in the Beacon API.
You can query the data table using the `from` clause in the query API.
