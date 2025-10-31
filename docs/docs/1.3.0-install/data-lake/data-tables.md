# Data Tables (Data Collections)

Data tables/collections can be used to organize and manage multiple datasets as a single data collection.
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
> Logical Data Tables currently only work when combining datasets of the same file format (e.g. CSV, Parquet, NetCDF, ODV, etc.). Combining datasets of different file formats is not yet supported.

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

- `netcdf`
- `parquet`
- `arrow` (Apache Arrow IPC format)
- `csv`
- `bbf` (Beacon Binary Format)

```http
POST /api/admin/create-table/

Content-Type: application/json
{
  "table_name": "your_collection_name",
  "table_type": {
    "logical": {
        "paths": [
          "*.nc"
        ],
        "file_format": "netcdf"
    }
  }
}

```

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
        ],
        "file_format": "parquet"
    }
  }
}

```

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

## Geo Spatial Data Tables

ToDo!
