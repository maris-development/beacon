# UNION ALL BY NAME

`UNION ALL BY NAME` merges the rows of several queries. It matches the columns by **name**, not by
position. Use it on datasets that share column names. The datasets can differ in column order, in
optional variables and in numeric precision.

```sql
SELECT * FROM read_netcdf(['argo/**/*.nc'])
UNION ALL BY NAME
SELECT * FROM read_netcdf(['wod/**/*.nc'])
UNION ALL BY NAME
SELECT * FROM read_netcdf(['cora/**/*.nc'])
```

## The difference from plain `UNION ALL`

| | `UNION ALL` | `UNION ALL BY NAME` |
| --- | --- | --- |
| Column matching | By position | By name |
| Column order must match | Yes | No |
| Missing columns | Error | Beacon sets them to `NULL` |
| Type mismatches | Error | Beacon widens the type |

## Missing columns become NULL

A column can exist in one input and not in the other. Beacon sets the missing side to `NULL`. The
column is then nullable in the result:

```sql
-- argo has 'salinity', wod does not
SELECT * FROM argo_table
UNION ALL BY NAME
SELECT * FROM wod_table
-- salinity is NULL for all wod rows
```

## Automatic type widening

One column name can have different numeric types in the inputs. Beacon then widens the type to a
common supertype:

| Left | Right | Result |
| ---- | ----- | ------ |
| `Float32` | `Float64` | `Float64` |
| `Int8` | `Int32` | `Int32` |
| `Int32` | `Int64` | `Int64` |
| `Int32` | `Float64` | `Float64` |
| `Utf8` | `LargeUtf8` | `LargeUtf8` |
| `Date32` | `Date64` | `Date64` |
| any | `Null` | the non-null type |

Two incompatible types give a planning error. `Boolean` and `Int32` are an example.

## Reduce to a shared schema

Select only the columns that you need before the union. The output schema then stays clean. Extra
variables in a source file do not matter:

```sql
SELECT time, latitude, longitude, temperature, salinity
FROM read_netcdf(['argo/**/*.nc'])

UNION ALL BY NAME

SELECT time, latitude, longitude, temperature, salinity
FROM read_netcdf(['wod/**/*.nc'])
```

## Store the result as a view

Wrap the union in a `CREATE VIEW` to give it a stable name:

```sql
CREATE VIEW all_profiles AS
    SELECT time, latitude, longitude, temperature, salinity
    FROM read_netcdf(['argo/**/*.nc'])
    UNION ALL BY NAME
    SELECT time, latitude, longitude, temperature, salinity
    FROM read_netcdf(['wod/**/*.nc'])
```
