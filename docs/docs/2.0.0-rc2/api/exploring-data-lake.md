# Exploring the Data Lake (REST API)

Use these endpoints to discover what data is available on a running Beacon instance, without running a full query.

**Concepts:**

- **Datasets**: individual files (a single `.nc` file, a `.parquet` file, a Zarr group, etc.)
- **Tables**: named logical tables registered in Beacon, often spanning many datasets
- **Schemas**: Arrow field lists (name + type) describing the columns available for `select` and `filter`

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

The response is the Arrow schema, serialized as Arrow serializes it:

```json
{
  "fields": [
    { "name": "TEMP", "data_type": "Float64", "nullable": true, "metadata": {} },
    { "name": "TIME", "data_type": { "Timestamp": ["Microsecond", null] }, "nullable": true, "metadata": {} }
  ],
  "metadata": {}
}
```

Column names are under `.fields[].name`. A simple `data_type` is a string; a
parameterized one is a single-key object carrying its arguments, so a client can
reconstruct the exact type rather than parse a display string. (Arrow also emits
`dict_id`/`dict_is_ordered` per field; they are dictionary-encoding internals and
can be ignored.)

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

### Catalogs

`GET /api/tables` lists only the tables in the default schema
(`beacon.public`). To browse the whole namespace — for the super-user that
includes beacon's `system` schema, `information_schema`, and any catalog added
with `ATTACH` — ask for the catalog tree:

```http
GET /api/catalogs
```

```json
{
  "default_catalog": "beacon",
  "default_schema": "public",
  "catalogs": [
    {
      "name": "beacon",
      "schemas": [
        { "name": "public", "tables": [{ "name": "default", "table_type": "BASE TABLE" }] },
        { "name": "system", "tables": [{ "name": "query_metrics", "table_type": "BASE TABLE" }] }
      ]
    }
  ]
}
```

Tables outside `default_catalog`.`default_schema` need their qualified name in
SQL (`beacon.system.query_metrics`).

Both this and `/api/tables` answer per caller: the metadata schemas
(`information_schema`, `beacon.system`) are the super-user's alone, and everyone
else is listed only the tables their roles grant `SELECT` on — so a listing shows
exactly what that caller could go on to read. See
[Access control](/docs/2.0.0-rc2/security/access-control).

### Table schema

```http
GET /api/table-schema?table_name=default
```

The table is resolved in the default catalog and schema. Add `catalog` and
`schema` for one that lives elsewhere:

```http
GET /api/table-schema?table_name=query_metrics&catalog=beacon&schema=system
```

### Default table schema

The Arrow schema of the default table (the one queried when a request omits
`from`):

```http
GET /api/default-table-schema
```

::: info Deprecated alias
`GET /api/query/available-columns` is a deprecated endpoint that returns only the
column names of the default table schema. Use `/api/default-table-schema` in new
code.
:::

### All tables with schemas

Convenient for UI discovery, but can be slow on large installations:

```http
GET /api/tables-with-schema
```

### Table configuration

::: warning Deprecated — no longer supported
`GET /api/admin/table-config` no longer returns a table's configuration. A
table's stored definition is how Beacon rebuilds it — credentials and internal
option keys included — which is engine bookkeeping rather than an API contract,
so it is not served over HTTP at all.

The endpoint stays routed (still admin-only) and answers `200` with a notice:

```json
{ "message": "Table configuration is no longer supported. ..." }
```

Use `GET /api/table-schema` for a table's columns, and
`SHOW EXTENSIONS FOR <table>` through `/api/query` for its extensions.
:::

## Functions

List the scalar, aggregate, and window functions available in queries, with their
signatures and descriptions:

```http
GET /api/functions
```

This is DataFusion's own function catalog (the same one `SHOW FUNCTIONS` reads).
Table functions (`read_netcdf`, `read_zarr`, …) are **not** in it — DataFusion
does not catalog table-valued functions.

::: info Deprecated
`GET /api/table-functions` is still routed for clients that call it, but nothing
catalogs table-valued functions, so it always returns an empty list.
:::

See the [table function reference](/docs/2.0.0-rc2/beacondb/sql/table-functions)
for every table function and its signature, and the
[Function Reference](/docs/2.0.0-rc2/beacondb/sql/function-reference) for the rest.

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
| `GET` | `/api/admin/table-config` | **Deprecated** — answers a notice; table configuration is no longer served |
| `POST` | `/api/admin/crawlers` | Define (or replace) a crawler |
| `GET` | `/api/admin/crawlers` | List defined crawlers |
| `GET` | `/api/admin/crawlers/{name}` | Get one crawler (or `404`) |
| `POST` | `/api/admin/crawlers/{name}/run` | Run a crawler once; returns its crawl report |
| `DELETE` | `/api/admin/crawlers/{name}` | Drop a crawler (crawled tables are left in place) |
| `POST` | `/api/admin/external-tables` | Create an external table from structured fields |

Every example below sends the credentials via HTTP Basic auth
(`Authorization: Basic <base64(username:password)>`); the header is omitted from
the snippets after the first for brevity.

Check that your credentials are accepted:

```http
GET /api/admin/check
Authorization: Basic <base64(username:password)>
```

Create a crawler (the structured equivalent of [`CREATE CRAWLER`](/docs/2.0.0-rc2/data-lake/crawlers)):

```http
POST /api/admin/crawlers
Authorization: Basic <base64(username:password)>
Content-Type: application/json

{ "name": "argo", "target_prefix": "argo/", "format_filter": ["parquet"],
  "schedule_secs": 900, "table_naming": "crawler_prefixed" }
```

Create an external table (the structured equivalent of
[`CREATE EXTERNAL TABLE`](/docs/2.0.0-rc2/beacondb/sql/create-table)):

```http
POST /api/admin/external-tables
Authorization: Basic <base64(username:password)>
Content-Type: application/json

{ "name": "observations", "file_type": "PARQUET", "location": "obs/",
  "partition_cols": ["year", "month"] }
```

List the defined crawlers, or fetch a single one by name:

```http
GET /api/admin/crawlers
```

```http
GET /api/admin/crawlers/argo
```

Run a crawler once on demand (returns its crawl report):

```http
POST /api/admin/crawlers/argo/run
```

Drop a crawler (its already-crawled tables are left in place):

```http
DELETE /api/admin/crawlers/argo
```

## OpenAPI

This page is a curated subset. The complete, always-current request and response
shapes are generated from the server itself:

- Swagger UI: `/swagger`
- Scalar UI: `/scalar/`
- OpenAPI document: `/openapi.json`
