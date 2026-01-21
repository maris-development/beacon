
# Exploring the Data Lake (API)

This chapter shows how to discover what data is available on a running Beacon instance using raw HTTP request templates.

All paths are shown as relative URLs (for example `GET /api/info`). Send them to your Beacon base URL.

## Concepts

- **Datasets**: individual assets (e.g. a single `.nc` file, a single `.parquet` file, a Zarr group).
- **Tables (collections)**: named logical tables that Beacon registers (often spanning many datasets).
- **Schemas/columns**: returned as an Arrow schema (fields + types). Use schemas to discover which columns you can `select` and `filter` on.

## System info

```http
GET /api/info
```

## Datasets

### List datasets

Preferred endpoint:

```http
GET /api/list-datasets
```

Optional query parameters:

- `pattern`: glob pattern to filter dataset paths (example: `*.nc`, `**/*.parquet`)
- `offset`: pagination offset
- `limit`: pagination limit

Examples:

```http
GET /api/list-datasets?pattern=*.nc&limit=50&offset=0
```

### Total dataset count

```http
GET /api/total-datasets
```

### Get dataset schema (columns)

To get the Arrow schema for a single dataset path:

```http
GET /api/dataset-schema?file=test-files/gridded-example.nc
```

To infer a merged schema across multiple datasets using a glob:

```http
GET /api/dataset-schema?file=**/*.nc
```

::: tip
The response is an Arrow schema JSON. Column names are under `.fields[].name`.

:::

## Tables (collections)

### List tables

```http
GET /api/tables
```

### Get default table name

If the query request does not specify `from`, Beacon uses this table.

```http
GET /api/default-table
```

### Get table schema (columns)

```http
GET /api/table-schema?table_name=default
```

### List all tables with schemas

This is convenient for UI discovery, but can be heavier on large installations.

```http
GET /api/tables-with-schema
```

### Get table configuration

Useful to see how a table was constructed (paths, file format, statistics settings, etc.).

```http
GET /api/table-config?table_name=default
```

## Functions

Beacon exposes DataFusion scalar functions and Beacon table functions.

```http
GET /api/functions
```

```http
GET /api/table-functions
```

Table functions are especially useful for SQL queries (e.g. `read_netcdf([...])`, `read_parquet([...])`).

## Admin endpoints (tables/files)

Admin endpoints are under `/api/admin/*` (create table, delete table, upload/download/delete files) and are protected by HTTP Basic Auth.

See the Data Lake docs for deeper table concepts, or browse `/swagger` for the exact request/response shapes.

### Create a table (collection)

Raw HTTP (as a template):

```http
POST /api/admin/create-table
Authorization: Basic <base64(username:password)>
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
