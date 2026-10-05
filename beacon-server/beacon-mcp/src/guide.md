# Beacon guide

Beacon is a SQL query engine for scientific data. It reads files where they are: NetCDF, Zarr,
HDF5, Parquet, CSV, GeoTIFF, Atlas and more. It does not copy the data. The SQL dialect is Apache
DataFusion SQL.

## Tables

- An external table is a SQL table over files. Beacon reads the files at query time.
- A managed table belongs to Beacon. Beacon stores its rows.
- A remote table points at a table on another Beacon server.
- A view is a stored query. Use it like a table.
- A table function reads files without a table, for example `read_netcdf('argo/*.nc')`,
  `read_zarr(...)`, `read_parquet(...)` and `read_csv(...)`. Your role must be able to read the
  files.

## Arrays become rows

- Beacon turns an n-dimensional array into rows: one row for each grid point. A CF ragged-array
  file gives one row for each observation.
- A variable attribute is a column `<variable>.<attribute>`, for example `"temperature.units"`.
- A global attribute of a file is a column `.<attribute>`, for example `".title"`.
- Names keep their case. Put double quotes around a name with upper case, a dot or a space:
  `"Temperature"`, `"temperature.units"`.

## Write a query

1. Call `list_tables`. Read the description of each table.
2. Call `describe_table` before you write SQL for a table. The column descriptions give the units
   and the meaning.
3. Select only the columns that you need. Filter on time, position and depth first. Beacon can
   then skip the files that cannot match. A filter on a computed value skips no files.
4. Use `run_sql` for a preview. It returns 1000 rows or fewer.
5. Use `export_query` for a large result. It returns a request that a script sends.

The MCP tools are read-only. Beacon rejects `CREATE`, `INSERT`, `COMMENT ON` and every other
statement that changes data.

## Get the data in a script

The address of this server is `{beacon_url}`. Send the same credential that you use for MCP. A
request without credentials is anonymous and read-only.

HTTP: send `POST {beacon_url}/api/query` with a JSON body. `output.format` is `parquet`, `csv`,
`netcdf` or `arrow`:

```json
{ "sql": "SELECT ...", "output": { "format": "parquet" } }
```

Python, with the `beacon-api` package:

```python
# pip install beacon-api
from beacon_api import Client

client = Client("{beacon_url}")  # or basic_auth=("user", "pass"), or jwt_token="..."
df = client.sql_query("SELECT ...").to_pandas_dataframe()
```
