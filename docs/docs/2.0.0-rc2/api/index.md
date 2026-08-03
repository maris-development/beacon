# REST API

Beacon gives an HTTP API. Use it to query datasets, to inspect schemas and to manage the data lake.
Every endpoint uses JSON over HTTP.

## OpenAPI reference

Beacon generates an OpenAPI specification at run time. Start the server. Then open one of these
URLs:

| UI | URL |
| -- | --- |
| Swagger UI | `/swagger` |
| Scalar UI | `/scalar/` |
| Raw spec (JSON) | `/openapi.json` |

## Base URL

This documentation shows every endpoint as a relative path, for example `GET /api/health`. Send your
request to the base URL of your Beacon server. The default is `http://localhost:5001`. Behind a
reverse proxy, use the URL of that proxy.

## Health check

```http
GET /api/health
```

Returns `200 OK` when Beacon runs and is ready.

## What's in the API

| Section | Description |
| ------- | ----------- |
| [Exploring the Data Lake](/docs/2.0.0-rc2/api/exploring-data-lake) | Find datasets, tables and schemas |
| [Querying](/docs/2.0.0-rc2/api/querying/) | Run a query with the JSON DSL or with SQL, and get the results |
| [JSON Query DSL](/docs/2.0.0-rc2/api/querying/json) | A structured query format for a client program |
| [SQL](/docs/2.0.0-rc2/api/querying/sql) | Full SQL through DataFusion |
| [Examples](/docs/2.0.0-rc2/api/querying/examples) | Query patterns that you can copy |
