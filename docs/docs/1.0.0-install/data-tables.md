# Data Tables

Data tables can be used to organize and manage datasets in a structured way.
They allow you to define the structure of your data and how it should be stored and accessed. This is useful when you have multiple datasets that are related to each other and you want to query them together using a single name.
Furthermore, data tables allow you to optimize the performance of your queries by creating a physical data table that is optimized for your specific use case.

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

Sometimes it is useful to make multiple datasets available via a single `table` name. This can be done by creating a `table` that consists of multiple file paths (or glob paths) that point to multiple datasets in the `/beacon/data/datasets` directory.

This is useful when you have multiple datasets that are related to each other and you want to query them together using a single name.
For example, you may have multiple datasets that are related to the same geographic region, and you want to query them together.

### Creating a Logical Data Table

Creating a logical data table is done using the `create-table` endpoint of the admin API. The following example shows how to create a logical data table that consists of multiple datasets.
You can use the `paths` parameter to specify the paths to the datasets that you want to include in the logical data table.
The paths can be specified using glob patterns, so you can use wildcards to match multiple datasets.

The paths are relative to the `/beacon/data/datasets` directory inside the beacon docker container, so you need to use relative paths to specify the datasets that you want to include in the logical data table.

Various file formats are supported:

- `netcdf`
- `parquet`
- `ipc` (Apache Arrow IPC format)
- `csv`

```http
POST /api/admin/create-table/

Content-Type: application/json
{
  "table_name": "your_collection_name",
  "table_type": {
    "logical": {
      "logical_table": {
        "table_name": "your_collection_name",
        "paths": [
          "*.nc"
        ],
        "file_format": "netcdf"
      }
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
      "logical_table": {
        "table_name": "your_parquet_collection_name",
        "paths": [
          "dataset1.parquet",
          "/path/to/dataset2.parquet",
        ],
        "file_format": "parquet"
      }
    }
  }
}

```

## Physical Data Tables

Physical data tables are used to create a single physical data table from a query (can be JSON or SQL). Using this method you can create a physical data table based on the results of a query.
This allows you to optimize the performance of your queries by creating a physical data table that is optimized for your specific use case. For example, you can create a physical data table that is a subset of a larger netcdf dataset collection and store that in parquet format for faster access (using row group & pages filtering).
This is useful when you have a large dataset and want ot optimize the performance of your queries by creating a physical data table that is optimized for your specific use case.

Physical Data Tables are powered by a `table_engine`. This table engine is responsible for creating the physical data table from the query results. The following table engines are supported:

- `parquet` - Creates a physical data table in parquet format.
- `beacon-binary-format` - Creates a physical data table in beacon binary format (requires the standard edition).

### Creating a Physical Data Table

```http
POST /api/admin/create-table/

Content-Type: application/json
{
  "table_name": "your-netcdf-collection-2020",
  "table_type": {
    "physical": {
      "physical_table": {
        "table_name": "your-netcdf-collection-2020",
        "table_generation_query": {
          "sql": "SELECT TEMP,PSAL,LONGITUDE,LATITUDE,PRES,TIME FROM read_netcdf('*.nc') WHERE TIME > '2020-01-01' AND TIME < '2020-12-31' AND TEMP IS NOT NULL"
        },
        "table_engine": "parquet"
      }
    }
  }
}
```

You can also optimize formats by transforming them from their original format to a more optimized format (e.g. from netcdf or odv-ascii to parquet/beacon-binary-format).

```http
POST /api/admin/create-table/

Content-Type: application/json
{
  "table_name": "your-odv-collection",
  "table_type": {
    "physical": {
      "physical_table": {
        "table_name": "your-odv-collection",
        "table_generation_query": {
          "sql": "SELECT * FROM read_odv_ascii('*.txt')"
        },
        "table_engine": "parquet"
      }
    }
  }
}
```

## Table Extensions

Table extensions are used to extend the definition of a data table. This allows you to expose your data using a common predefined schema.
This is useful when you want to expose your data using a common schema that is used by other tools or libraries.

For example, you can use the `table_extension` to expose your data table as WMS compliant by exposing a standardized longitude, latitude, depth, and time columns without affecting the physical column names. In this case it would work as a transformation layer.
This allows you to use your data table with other tools or libraries that support WMS.

Supported table extensions are:

- `wms` - Exposes the data table as WMS compliant by exposing a standardized longitude, latitude, depth and time columns without affecting the physical column names.
- `geo-spatial` - Exposes the data table as a geospatial data table by exposing a standardized longitude, latitude.
- `temporal` - Exposes the data table as a temporal data table by exposing a standardized time column.
- `vertical-axis` - Exposes the data table as a vertical axis data table by exposing a standardized depth column.
