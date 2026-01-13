# Introduction to Data Lake Querying with Beacon

Beacon exposes your data lake through a simple HTTP API. Clients (scripts, notebooks, web apps) send a query request, Beacon plans and executes it using its columnar execution engine, reads data from your configured storage (local filesystem and/or S3), and returns the results.

Beacon supports two query styles:

- **SQL**: write a SQL query string.
- **JSON**: send a structured JSON query (selection, filters, sorting, limits), which Beacon converts into a query plan.

Both styles use the same endpoint: `POST /api/query`.

## How querying works

1. Your client sends a query to `POST /api/query`.
2. Beacon parses the query (SQL or JSON).
3. Beacon builds an execution plan (DataFusion) and scans the requested tables/files.
4. If you request an output format, Beacon writes a temporary output file and returns it as a download.
5. Otherwise, Beacon streams results back as an Arrow IPC stream.

Every query response includes an `x-beacon-query-id` header that you can use to fetch execution metrics.

## Useful endpoints

- `POST /api/query`: run a query (SQL or JSON).
- `POST /api/parse-query`: validate a query body (syntax/shape) without executing it.
- `POST /api/explain-query`: return the query plan explanation (useful for debugging).
- `GET /api/query/metrics/{query_id}`: get consolidated metrics for a query id.

Schema discovery:

- `GET /api/dataset-schema?file=some_dataset.nc`: Arrow schema of a dataset file.
- `GET /api/dataset-schema?file=*.nc`: Can be a glob pattern.

- `GET /api/default-table`: name of the default table.
- `GET /api/default-table-schema`: Arrow schema of the default table.
- `GET /api/tables`: list all tables.
- `GET /api/table-schema?table_name=...`: schema for a specific table.

## Output formats

Beacon can return results either as a **stream** or as a **single file download**.

- **Streaming mode** (no `output` field): Beacon streams results as an Arrow IPC stream (`application/vnd.apache.arrow.stream`).
- **File mode** (set `output.format`): Beacon executes the query fully, writes a temporary file, and returns it with a `Content-Disposition: attachment; filename="output.<ext>"` header.

Supported file output formats:

- `csv`
- `parquet`
- `ipc` (alias: `arrow`)
- `netcdf`
- `nd_netcdf` (NetCDF with explicit dimension columns)
- `geoparquet`
- `odv` (ODV ASCII export, returned as a zip)

## Recommended: use the Beacon Python SDK

For notebooks and data pipelines, using the Beacon Python SDK is recommended. It handles request/response details for you and can materialize results into common Python data structures.

Install:

```bash
pip install beacon-api
```

Example (run a SQL query and get a pandas DataFrame):

```python
from beacon_api import Client

client = Client("http://localhost:3000")

query = client.sql_query(
    "SELECT time, latitude, longitude, temp "
    "FROM read_netcdf(['**/*.nc']) "
    "WHERE temp BETWEEN 2 AND 10 "
    "LIMIT 10000"
)

df = query.to_pandas_dataframe()
print(df.head())
```

Python SDK docs: [maris-development.github.io/beacon-py](https://maris-development.github.io/beacon-py/)

Next:

- See [sql.md](sql.md) for SQL querying.
- See [json.md](json.md) for JSON querying.
