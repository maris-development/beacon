# Explore the catalog (REST API)

Use these endpoints to see the available data on a Beacon server. You run no full query.

**Concepts:**

- **Datasets**: single files, such as one `.nc` file, one `.parquet` file or a Zarr group.
- **Tables**: named tables in Beacon. One table often covers many datasets.
- **Schemas**: Arrow field lists with a name and a type. They give the columns for `select` and
  `filter`.

## System info

```http
GET /api/info
```

Returns the Beacon version, a summary of the configuration and the number of registered tables.

## Datasets

### List datasets

```http
GET /api/list-datasets
```

::: info Deprecated alias
`GET /api/datasets` is a deprecated alias of `/api/list-datasets`. Use `/api/list-datasets` in new
code.
:::

The optional query parameters are:

| Parameter | Description |
| --------- | ----------- |
| `pattern` | A glob that filters the paths, for example `*.nc` or `**/*.parquet` |
| `offset` | The offset of the page |
| `limit` | The size of the page |

```http
GET /api/list-datasets?pattern=argo/**/*.nc&limit=50&offset=0
```

### Dataset count

```http
GET /api/total-datasets
```

### Dataset schema

Returns the Arrow schema of one path. The schema holds the fields and the types:

```http
GET /api/dataset-schema?file=argo/profile_001.nc
```

Use a glob to get one merged schema over several files:

```http
GET /api/dataset-schema?file=argo/**/*.nc
```

The response is the Arrow schema, in the Arrow serialization form:

```json
{
  "fields": [
    { "name": "TEMP", "data_type": "Float64", "nullable": true, "metadata": {} },
    { "name": "TIME", "data_type": { "Timestamp": ["Microsecond", null] }, "nullable": true, "metadata": {} }
  ],
  "metadata": {}
}
```

The column names are in `.fields[].name`. A simple `data_type` is a string. A type with parameters
is an object with one key. That key holds the arguments. A client can therefore build the exact
type. It parses no display string. Arrow also writes `dict_id` and `dict_is_ordered` for each field.
Those two fields are internal to the dictionary encoding. You can ignore them.

## Tables

### List tables

```http
GET /api/tables
```

### Default table

Beacon uses this table when a query has no `from` field:

```http
GET /api/default-table
```

### Catalogs

`GET /api/tables` lists only the tables of the default schema, `beacon.public`. Ask for the catalog
tree to see the whole namespace. For the super-user, that tree also holds the `system` schema of
Beacon, the `information_schema` and every catalog from an `ATTACH`:

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

A table outside `default_catalog`.`default_schema` needs its full name in SQL, for example
`beacon.system.query_metrics`.

This endpoint and `/api/tables` answer per caller. The metadata schemas belong to the super-user
only. Those schemas are `information_schema` and `beacon.system`. Every other caller sees only the
tables with a `SELECT` grant from their roles. A listing therefore shows exactly what that caller
can read. See [Access control](/docs/2.0.0-rc5/security/access-control).

### Table schema

```http
GET /api/table-schema?table_name=default
```

Beacon resolves the table in the default catalog and schema. Add `catalog` and `schema` for a table
in another place:

```http
GET /api/table-schema?table_name=query_metrics&catalog=beacon&schema=system
```

### Default table schema

The Arrow schema of the default table. Beacon queries that table when a request has no `from`
field:

```http
GET /api/default-table-schema
```

::: info Deprecated alias
`GET /api/query/available-columns` is deprecated. It returns only the column names of the default
table schema. Use `/api/default-table-schema` in new code.
:::

### All tables with schemas

This helps a UI. It can be slow on a large installation:

```http
GET /api/tables-with-schema
```

### Table configuration

::: warning Deprecated. No longer supported
`GET /api/admin/table-config` no longer returns the configuration of a table. Beacon rebuilds a
table from its stored definition. That definition holds the credentials and the internal option
keys. It is internal to the engine. It is not an API contract. Beacon therefore never serves it over
HTTP.

The endpoint still exists, for an admin only. It answers `200` with a notice:

```json
{ "message": "Table configuration is no longer supported. ..." }
```

Use `GET /api/table-schema` for the columns of a table. Use `SHOW EXTENSIONS FOR <table>` through
`/api/query` for its extensions.
:::

## Functions

List the scalar, aggregate and window functions of a query. The list holds their signatures and
descriptions:

```http
GET /api/functions
```

This is the function catalog of DataFusion. `SHOW FUNCTIONS` reads the same catalog. A table
function such as `read_netcdf` or `read_zarr` is **not** in it. DataFusion does not catalog a table
function.

::: info Deprecated
`GET /api/table-functions` still exists for older clients. No catalog holds a table function. The
endpoint therefore always returns an empty list.
:::

The [table function reference](/docs/2.0.0-rc5/sql/table-functions) holds every table
function and its signature. The
[Function Reference](/docs/2.0.0-rc5/sql/function-reference) holds the other functions.

## Table lifecycle

**The table lifecycle uses SQL only.** Send SQL DDL to the query endpoint to create, replace or
remove a table:

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

A write operation needs the admin credentials, `BEACON_ADMIN_USERNAME` and
`BEACON_ADMIN_PASSWORD`, over HTTP basic auth. An anonymous request is read-only.

## Admin

Every `/api/admin/*` endpoint needs the admin credentials, `BEACON_ADMIN_USERNAME` and
`BEACON_ADMIN_PASSWORD`, over HTTP basic auth. A request without credentials gets `401`.

You can still create, replace and remove a table with authenticated SQL DDL on `/api/query`. See
[Table lifecycle](#table-lifecycle) above. These JSON admin endpoints also exist:

| Method | Path | Purpose |
| ------ | ---- | ------- |
| `GET` | `/api/admin/check` | Check the connection. Returns `{ "is_admin": true }` |
| `GET` | `/api/admin/table-config` | **Deprecated.** It answers a notice. Beacon no longer serves a table configuration |
| `POST` | `/api/admin/crawlers` | Define or replace a crawler |
| `GET` | `/api/admin/crawlers` | List every crawler |
| `GET` | `/api/admin/crawlers/{name}` | Return one crawler. Returns `404` for an unknown name |
| `POST` | `/api/admin/crawlers/{name}/run` | Run a crawler once. Returns its crawl report |
| `DELETE` | `/api/admin/crawlers/{name}` | Drop a crawler. Beacon keeps the crawled tables |
| `POST` | `/api/admin/external-tables` | Create an external table from structured fields |

Every example below sends the credentials over HTTP Basic auth, as
`Authorization: Basic <base64(username:password)>`. Only the first example shows the header.

Check that Beacon accepts your credentials:

```http
GET /api/admin/check
Authorization: Basic <base64(username:password)>
```

Create a crawler. This endpoint matches [`CREATE CRAWLER`](/docs/2.0.0-rc5/server/crawlers):

```http
POST /api/admin/crawlers
Authorization: Basic <base64(username:password)>
Content-Type: application/json

{ "name": "argo", "target_prefix": "argo/", "format_filter": ["parquet"],
  "schedule_secs": 900, "table_naming": "crawler_prefixed" }
```

Create an external table. This endpoint matches
[`CREATE EXTERNAL TABLE`](/docs/2.0.0-rc5/sql/create-external-table):

```http
POST /api/admin/external-tables
Authorization: Basic <base64(username:password)>
Content-Type: application/json

{ "name": "observations", "file_type": "PARQUET", "location": "obs/",
  "partition_cols": ["year", "month"] }
```

List the crawlers, or fetch one crawler by name:

```http
GET /api/admin/crawlers
```

```http
GET /api/admin/crawlers/argo
```

Run a crawler once on demand. It returns its crawl report:

```http
POST /api/admin/crawlers/argo/run
```

Drop a crawler. Beacon keeps the tables that the crawler created:

```http
DELETE /api/admin/crawlers/argo
```

## OpenAPI

This page holds a selection. The server generates the full request and response shapes. Those shapes
are always current:

- Swagger UI: `/swagger`
- Scalar UI: `/scalar/`
- OpenAPI document: `/openapi.json`
