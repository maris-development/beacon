---
description: Read the columns and types of a file before you query it. read_<format>_schema, SUMMARIZE, DESCRIBE and LIMIT 0 each answer a different question.
---

# Inspect a schema

Check the columns of a file before you write a query. Also check their types. Four statements do
this. They differ in cost and in what they tell you.

| Statement | Reads | Gives |
|---|---|---|
| `read_<format>_schema` | File metadata only | Column names, types, nullability |
| `SUMMARIZE` | Every row | Names, types, min, max, distinct count, null share |
| `DESCRIBE` | Catalog only | Column names and types of a registered table |
| `SELECT … LIMIT 0` | File metadata only | The result schema of a query, before you run it |

## `read_<format>_schema`: names and types

Every reader has a `_schema` counterpart. It opens the file metadata and returns one row per
column. It reads no data.

```sql
SELECT * FROM read_netcdf_schema('argo/**/*.nc');
```

| column_name | data_type | nullable |
|---|---|---|
| JULD | Timestamp(Nanosecond) | true |
| LATITUDE | Float64 | true |
| LONGITUDE | Float64 | true |
| PRES | Float64 | true |
| TEMP | Float64 | true |

Pass a list to get the combined schema of several locations. This shows the files that disagree
about a column:

```sql
SELECT * FROM read_parquet_schema(['obs/2023/*.parquet', 'obs/2024/*.parquet']);
```

The function name carries the format, so there is no format argument. One exists for every reader:
`read_parquet_schema`, `read_geoparquet_schema`, `read_csv_schema`, `read_arrow_schema`,
`read_netcdf_schema`, `read_hdf5_schema`, `read_zarr_schema`, `read_atlas_schema`,
`read_tiff_schema`, `read_bbf_schema`, `read_delta_schema`, `read_iceberg_schema` and
`read_odv_ascii_schema`.

Because the result is an ordinary table, you can query it:

```sql
SELECT column_name FROM read_netcdf_schema('argo/**/*.nc')
WHERE data_type LIKE 'Timestamp%';
```

## `SUMMARIZE`: profile every column

[`SUMMARIZE`](/docs/2.0.0-rc5/sql/summarize) gives more than names and types. It profiles every
column in one pass. It adds the minimum, the maximum, the distinct count and the share of nulls:

```sql
SUMMARIZE (SELECT * FROM read_netcdf('argo/**/*.nc'));
```

`SUMMARIZE` reads every row. On a large collection it costs a full scan. Use it on a subset first:

```sql
SUMMARIZE (SELECT * FROM read_netcdf('argo/2024/01/*.nc'));
```

## `DESCRIBE`: a registered table

If the files already have a table name, `DESCRIBE` answers from the catalog. It touches no file:

```sql
DESCRIBE argo;
```

## `LIMIT 0`: the schema of a result

`LIMIT 0` returns the columns of a query without any rows. Use it when the query changes the shape,
for example with a `JOIN` or a computed column:

```sql
SELECT platform, temperature * 1.8 + 32 AS temp_f
FROM read_netcdf('argo/**/*.nc')
LIMIT 0;
```

Every client returns the schema of that empty result. In Python:

```python
from beacon_api import Client

client = Client("https://beacon.example.com")
schema = client.sql_query(
    "SELECT * FROM read_netcdf('argo/**/*.nc') LIMIT 0"
).to_pandas_dataframe().dtypes
```

## Next

- [File formats](/docs/2.0.0-rc5/formats/) and the capability matrix
- [Arrays to tables](/docs/2.0.0-rc5/arrays-to-tables), for what a dimension does to the column list
- [SUMMARIZE](/docs/2.0.0-rc5/sql/summarize) for the full statement grammar
