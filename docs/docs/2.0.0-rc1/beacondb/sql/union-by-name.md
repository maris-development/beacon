# UNION ALL BY NAME

`UNION ALL BY NAME` merges rows from multiple queries by matching columns by **name** rather than position. It is the standard way to harmonize datasets that share a common set of column names but may differ in column order, optional variables, or numeric precision.

```sql
SELECT * FROM read_netcdf(['argo/**/*.nc'])
UNION ALL BY NAME
SELECT * FROM read_netcdf(['wod/**/*.nc'])
UNION ALL BY NAME
SELECT * FROM read_netcdf(['cora/**/*.nc'])
```

## How it differs from plain `UNION ALL`

| | `UNION ALL` | `UNION ALL BY NAME` |
| --- | --- | --- |
| Column matching | By position | By name |
| Column order must match | Yes | No |
| Missing columns | Error | Filled with `NULL` |
| Type mismatches | Error | Widened automatically |

## Missing columns become NULL

If a column exists in one input but not another, the missing side is filled with `NULL` and the column is marked nullable in the result:

```sql
-- argo has 'salinity', wod does not
SELECT * FROM argo_table
UNION ALL BY NAME
SELECT * FROM wod_table
-- salinity is NULL for all wod rows
```

## Automatic type widening

When the same column name has different numeric types across inputs, Beacon widens to a common supertype:

| Left | Right | Result |
| ---- | ----- | ------ |
| `Float32` | `Float64` | `Float64` |
| `Int8` | `Int32` | `Int32` |
| `Int32` | `Int64` | `Int64` |
| `Int32` | `Float64` | `Float64` |
| `Utf8` | `LargeUtf8` | `LargeUtf8` |
| `Date32` | `Date64` | `Date64` |
| any | `Null` | the non-null type |

Incompatible types (e.g. `Boolean` and `Int32`) produce a planning error.

## Narrowing to a shared schema

Select only the columns you want before the union to keep the output schema clean, regardless of what extra variables each source file contains:

```sql
SELECT time, latitude, longitude, temperature, salinity
FROM read_netcdf(['argo/**/*.nc'])

UNION ALL BY NAME

SELECT time, latitude, longitude, temperature, salinity
FROM read_netcdf(['wod/**/*.nc'])
```

## Persisting the result as a view

Wrap the union in a `CREATE VIEW` to give it a stable name:

```sql
CREATE VIEW all_profiles AS
    SELECT time, latitude, longitude, temperature, salinity
    FROM read_netcdf(['argo/**/*.nc'])
    UNION ALL BY NAME
    SELECT time, latitude, longitude, temperature, salinity
    FROM read_netcdf(['wod/**/*.nc'])
```
