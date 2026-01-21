# Collections (Data Tables)

Collections (Data Tables) can be created on top of multiple datasets to make them available as a single data table which is then accessible via a single `table` name.
This is useful when you have multiple datasets that are related to each other and you want to query them together using a single collection name.

To list the available data tables, you can use the following API endpoint:

```http
GET /api/tables
```

To view the columns (and datatype) of a data table, you can use the following API endpoint:

```http
GET /api/table-schema?table_name=your_data_table_name
```

## Logical Data Tables

> [!WARNING]
> Logical Data Tables currently only work when combining datasets of the same file format (e.g. Zarr, Parquet, NetCDF, ODV, etc.). Combining datasets of different file formats is not yet supported.

Sometimes it is useful to make multiple datasets available as a single data collection which is then accessible via a single `table` name. This can be done by creating a `table` that consists of multiple file paths (or glob paths) that point to multiple datasets in the `/beacon/data/datasets` directory.

```http
POST /api/admin/create-table/

Content-Type: application/json
{
  "table_name": "argo",
  "table_type": {
    "logical": {
        "paths": [
          "argo/*.parquet"
        ],
        "file_format": "parquet"
    }
  }
}

```

This is useful when you have multiple datasets that are related to each other and you want to query them together using a single name.
For example, you may have multiple datasets that are related to the same geographic region, and you want to query them together.

### Creating a Logical Data Table

Creating a logical data table is done using the `create-table` endpoint of the admin API. The following example shows how to create a logical data table that consists of multiple datasets.
You can use the `paths` parameter to specify the paths to the datasets that you want to include in the logical data table.
The paths can be specified using glob patterns, so you can use wildcards to match multiple datasets.

The paths are relative to the `/beacon/data/datasets` directory inside the beacon docker container or to the `s3-bucket` if running while using s3 cloud storage solution as storage system, so you need to use relative paths to specify the datasets that you want to include in the logical data table.

Various file formats are supported:

::: details Zarr (example):

Beacon fully supports zarr datasets as logical data tables. You can create a logical data table that consists of multiple zarr datasets by specifying the paths to the top-level `zarr.json` files of each zarr dataset.
For example, if you have a zarr dataset stored in `/beacon/data/datasets/my_zarr_dataset/`, you will need to specify the path as `my_zarr_dataset/zarr.json`.
You can also use glob patterns to match multiple zarr datasets, e.g. `my_zarr_datasets/*/zarr.json`.

``` http
POST /api/admin/create-table/

Content-Type: application/json
{
  "table_name": "your_collection_name",
  "table_type": {
    "logical": {
        "paths": [
          "dataset/zarr.json",
          "another_dataset/zarr.json",
          "folder_with_zarr_datasets/*/zarr.json"
        ],
        "file_format": "zarr",
    }
  }
}

```

> [!TIP] [Performance Tip] Push Down Predicates
> When using zarr datasets in logical tables, Beacon can efficiently prune data based on query predicates, similar to how it works with parquet files. This means that only the necessary chunks of data will be read from the zarr datasets based on the query conditions, improving performance.
> To enable this feature, ensure you specify which columns can be used by the pruning predicate to push down filters. Only apply this to columns that are frequently used in query filters such as geo-spatial, time or other commonly filtered columns.

``` http{15-21}

POST /api/admin/create-table/

Content-Type: application/json
{
  "table_name": "your_collection_name",
  "table_type": {
    "logical": {
        "paths": [
          "dataset/zarr.json",
          "another_dataset/zarr.json",
          "folder_with_zarr_datasets/*/zarr.json"
        ],
        "file_format": "zarr",
        "statistics": {
            "columns": [
                "valid_time",
                "latitude",
                "longitude"
            ]
        }
    }
  }
}

```

:::

::: details NetCDF (example):

You can use the `paths` parameter to specify the paths to the datasets that you want to include in the logical data table.
The paths can be specified using glob patterns, so you can use wildcards to match multiple datasets.

```http
POST /api/admin/create-table/

Content-Type: application/json
{
  "table_name": "your_netcdf_collection_name",
  "table_type": {
    "logical": {
        "paths": [
          "dataset.nc",
          "folder/*.nc",
          "**/*.nc"
        ],
        "file_format": "netcdf"
    }
  }
}

```

:::

::: details Parquet (example):

You can use the `paths` parameter to specify the paths to the datasets that you want to include in the logical data table.
The paths can be specified using glob patterns, so you can use wildcards to match multiple datasets.

```http
POST /api/admin/create-table/

Content-Type: application/json
{
  "table_name": "your_parquet_collection_name",
  "table_type": {
    "logical": {
        "paths": [
          "dataset1.parquet",
          "/path/to/dataset2.parquet",
          "/folder/*.parquet",
          "**/*.parquet"
        ],
        "file_format": "parquet"
    }
  }
}

```

:::

::: details CSV (example):

You can use the `paths` parameter to specify the paths to the datasets that you want to include in the logical data table.
The paths can be specified using glob patterns, so you can use wildcards to match multiple datasets.

> [!WARNING]
> When using CSV files in logical tables, Beacon cannot efficiently prune data based on query predicates. This means that the entire CSV files will be read for each query, which may impact performance for large datasets. Consider using other file formats like Parquet or Zarr for better performance with logical tables.

```http
POST /api/admin/create-table/

Content-Type: application/json
{
  "table_name": "your_csv_collection_name",
  "table_type": {
    "logical": {
        "paths": [
          "dataset1.csv",
          "/path/to/dataset2.csv",
          "/folder/*.csv",
          "**/*.csv"
        ],
        "file_format": "csv"
    }
  }
}

```

:::

::: details Arrow IPC (example):

You can use the `paths` parameter to specify the paths to the datasets that you want to include in the logical data table.
The paths can be specified using glob patterns, so you can use wildcards to match multiple datasets.

> [!WARNING]
> When using Arrow files in logical tables, Beacon cannot efficiently prune data based on query predicates. This means that the requested columns for Arrow files will be read for each query, which may impact performance for large datasets. Consider using other file formats like Parquet for better performance with logical tables.

```http
POST /api/admin/create-table/

Content-Type: application/json
{
  "table_name": "your_arrow_collection_name",
  "table_type": {
    "logical": {
        "paths": [
          "dataset1.arrow",
          "/path/to/dataset2.arrow",
          "/folder/*.arrow",
          "**/*.arrow"
        ],
        "file_format": "arrow"
    }
  }
}

```

:::

::: details Beacon Binary Format (example):

You can use the `paths` parameter to specify the paths to the datasets that you want to include in the logical data table.
The paths can be specified using glob patterns, so you can use wildcards to match multiple datasets.

> [!NOTE]
> When using BBF files in logical tables, Beacon will efficiently pushdown predicates to minimize data reads based on query conditions. Also for S3 based datasets, only the required byte-ranges will be fetched from S3 to satisfy the query.
> This makes BBF a very efficient format for logical tables. Similar to how Parquet works.

```http
POST /api/admin/create-table/

Content-Type: application/json
{
  "table_name": "your_bbf_collection_name",
  "table_type": {
    "logical": {
        "paths": [
          "dataset1.bbf",
          "/path/to/dataset2.bbf",
          "/folder/*.bbf",
          "**/*.bbf"
        ],
        "file_format": "bbf"
    }
  }
}

```

:::

## Preset Data Tables

Preset Data Tables allow you to take as input other logical tabels and manipulate which columns should be exposed further to the client. You can then also add metadata info, filters ranges for columns and other essential information to the data table. This can turn the data table effectively into a live data product.

```http
POST /api/admin/create-table/

Content-Type: application/json
{
    "table_name": "easy_era5_daily_mean_2m_temperature",
    "table_type": {
        "preset": {
            "logical": {
              "paths": [
                  "/era5_daily_mean_2m_temperature/*.parquet"
              ],
              "file_format": "parquet"
            },
            "data_columns": [
                {
                    "column_name": "valid_time",
                    "alias": "time",
                    "description": "UTC time of the measurement",
                    "filter": {
                        "min": "1950-01-01T00:00:00Z",
                        "max": "2023-12-31T23:59:59Z"
                    }
                },
                {
                    "column_name": "longitude",
                    "alias": "longitude",
                    "description": "Longitude in degrees",
                    "filter": {
                        "min": -180.0,
                        "max": 180.0
                    }
                },
                {
                    "column_name": "latitude",
                    "alias": "latitude",
                    "description": "Latitude in degrees",
                    "filter": {
                        "min": -90.0,
                        "max": 90.0
                    }
                },
                {
                    "column_name": "t2m",
                    "alias": "temperature",
                    "description": "Temperature value in degrees kelvin"
                }
            ],
            "metadata_columns": []
        }
    }
}

```
