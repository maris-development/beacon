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

::: info Deprecated alias
`GET /api/datasets` is a deprecated alias of `/api/list-datasets`. Use
`/api/list-datasets` in new code.
:::

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

### Default table schema

The Arrow schema of the default table (the one queried when a request omits
`from`):

```http
GET /api/default-table-schema
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

## Table lifecycle

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

Write operations require the admin credentials (`BEACON_ADMIN_USERNAME` /
`BEACON_ADMIN_PASSWORD`) via HTTP basic auth; anonymous requests are read-only.

## Admin

All `/api/admin/*` endpoints require the admin credentials
(`BEACON_ADMIN_USERNAME` / `BEACON_ADMIN_PASSWORD`) via HTTP basic auth;
unauthenticated requests get `401`.

Creating, replacing, and removing tables can still be done through authenticated
SQL DDL on `/api/query` (see [Table lifecycle](#table-lifecycle) above). In
addition, these dedicated, JSON-typed admin endpoints are available:

| Method | Path | Purpose |
| ------ | ---- | ------- |
| `GET` | `/api/admin/check` | Connectivity check; returns `{ "is_admin": true }` |
| `POST` | `/api/admin/crawlers` | Define (or replace) a crawler |
| `GET` | `/api/admin/crawlers` | List defined crawlers |
| `GET` | `/api/admin/crawlers/{name}` | Get one crawler (or `404`) |
| `POST` | `/api/admin/crawlers/{name}/run` | Run a crawler once; returns its crawl report |
| `DELETE` | `/api/admin/crawlers/{name}` | Drop a crawler (crawled tables are left in place) |
| `POST` | `/api/admin/external-tables` | Create an external table from structured fields |

Create a crawler (the structured equivalent of [`CREATE CRAWLER`](../data-lake/crawlers.md)):

```http
POST /api/admin/crawlers
Authorization: Basic <base64(username:password)>
Content-Type: application/json

{ "name": "argo", "target_prefix": "argo/", "format_filter": ["parquet"],
  "schedule_secs": 900, "table_naming": "crawler_prefixed" }
```

Create an external table (the structured equivalent of
[`CREATE EXTERNAL TABLE`](../sql/create-table.md)):

```http
POST /api/admin/external-tables
Authorization: Basic <base64(username:password)>
Content-Type: application/json

{ "name": "observations", "file_type": "PARQUET", "location": "obs/",
  "partition_cols": ["year", "month"] }
```

## OpenAPI

This page is a curated subset. The complete, always-current request and response
shapes are generated from the server itself:

- Swagger UI: `/swagger`
- Scalar UI: `/scalar/`
- OpenAPI document: `/openapi.json`
