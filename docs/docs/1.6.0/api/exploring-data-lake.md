# Exploring the Data Lake (REST API)

Use these endpoints to discover what data is available on a running Beacon instance — without running a full query.

**Concepts:**

- **Datasets** — individual files (a single `.nc` file, a `.parquet` file, a Zarr group, etc.)
- **Tables** — named logical tables registered in Beacon, often spanning many datasets
- **Schemas** — Arrow field lists (name + type) describing the columns available for `select` and `filter`

## System info

```http
GET /api/info
```

Returns Beacon version, configuration summary, and registered table count.

## Datasets

### List datasets

```http
GET /api/list-datasets
```

Optional query parameters:

| Parameter | Description |
| --------- | ----------- |
| `pattern` | Glob to filter paths (e.g. `*.nc`, `**/*.parquet`) |
| `offset` | Pagination offset |
| `limit` | Pagination limit |

```http
GET /api/list-datasets?pattern=argo/**/*.nc&limit=50&offset=0
```

### Dataset count

```http
GET /api/total-datasets
```

### Dataset schema

Returns the Arrow schema (fields + types) for a single path:

```http
GET /api/dataset-schema?file=argo/profile_001.nc
```

To infer a merged schema across multiple files using a glob:

```http
GET /api/dataset-schema?file=argo/**/*.nc
```

The response contains an Arrow schema JSON. Column names are under `.fields[].name`.

## Tables

### List tables

```http
GET /api/tables
```

### Default table

Beacon uses this table when a query omits `from`:

```http
GET /api/default-table
```

### Table schema

```http
GET /api/table-schema?table_name=default
```

### All tables with schemas

Convenient for UI discovery, but can be slow on large installations:

```http
GET /api/tables-with-schema
```

### Table configuration

Shows how a table was constructed — paths, file format, statistics settings, etc.:

```http
GET /api/table-config?table_name=default
```

## Functions

List all registered DataFusion scalar functions:

```http
GET /api/functions
```

List all registered Beacon table functions (e.g. `read_netcdf`, `read_zarr`):

```http
GET /api/table-functions
```

See the [Function Reference](../sql/function-reference.md) for descriptions and signatures.

## Admin (file management)

File upload, download, and deletion endpoints are available under `/api/admin/*` and are protected by HTTP Basic Auth.

**Table lifecycle is SQL-only.** Create, replace, or remove tables by sending SQL DDL to the query endpoint:

```http
POST /api/query
Content-Type: application/json

{ "sql": "CREATE EXTERNAL TABLE argo STORED AS PARQUET LOCATION 'argo/'" }
```

```http
POST /api/query
Content-Type: application/json

{ "sql": "DROP TABLE argo" }
```

Browse `/swagger` for the full admin request and response shapes.
