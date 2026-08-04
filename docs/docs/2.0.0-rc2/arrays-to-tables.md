---
description: How Beacon turns an N-dimensional NetCDF or Zarr variable into SQL rows. The row count, the broadcast rule, attribute columns and the dimensions argument.
---

# Arrays to tables

A NetCDF or Zarr file holds N-dimensional arrays. SQL holds rows. This page specifies the mapping
between the two, because that mapping decides your row count, your column list and your query cost.

Parquet, CSV, Arrow IPC and ODV are already tabular. This page does not apply to them.

## The rule

Beacon picks the variable with the largest data volume. That variable's dimensions become the
**grid**. Every other variable broadcasts onto that grid.

1. **Row count** is the product of the grid dimension sizes.
2. **Each variable becomes one column.** A variable on the full grid maps element for element.
3. **A lower-rank variable repeats.** Its values cycle so that each row gets the value for its
   position along the shared dimensions.
4. **A scalar becomes a constant column.** Every row carries the same value.
5. **Beacon drops a variable outside the grid.** See [Excluded variables](#excluded-variables).

## A worked example

Take a small gridded file, `sst/2024-01.nc`:

```text
dimensions:
    time = 2
    lat  = 3
    lon  = 4

variables:
    double time(time)                 = [1704067200, 1704153600]
    float  lat(lat)                   = [50.0, 51.0, 52.0]
    float  lon(lon)                   = [-4.0, -3.0, -2.0, -1.0]
    float  sst(time, lat, lon)        = 24 values
    float  sst:units                  = "degree_Celsius"

global attributes:
    :title = "Demo SST"
```

`sst` has the largest volume, so `(time, lat, lon)` is the grid.

```sql
SELECT * FROM read_netcdf('sst/2024-01.nc') LIMIT 5;
```

| time | lat | lon | sst | sst.units | .title |
|---|---|---|---|---|---|
| 1704067200 | 50.0 | -4.0 | 12.1 | degree_Celsius | Demo SST |
| 1704067200 | 50.0 | -3.0 | 12.4 | degree_Celsius | Demo SST |
| 1704067200 | 50.0 | -2.0 | 12.6 | degree_Celsius | Demo SST |
| 1704067200 | 50.0 | -1.0 | 12.9 | degree_Celsius | Demo SST |
| 1704067200 | 51.0 | -4.0 | 11.8 | degree_Celsius | Demo SST |

```sql
SELECT count(*) FROM read_netcdf('sst/2024-01.nc');
```

| count(\*) |
|---|
| 24 |

24 rows, because `2 × 3 × 4 = 24`. Read the mapping off the table:

- `lon` is 1-D on `lon`. It cycles every 4 rows.
- `lat` is 1-D on `lat`. It repeats each value 4 times, once per `lon`.
- `time` is 1-D on `time`. It repeats each value 12 times, once per `lat × lon`.
- `sst.units` and `.title` are scalars. They are constant.

## Row counts in practice

The row count grows as the product, so it grows fast:

| Grid | Row count |
|---|---|
| `time=2, lat=3, lon=4` | 24 |
| `time=100, lat=180, lon=360` | 6,480,000 |
| `time=365, depth=50, lat=720, lon=1440` | 18,921,600,000 |

Beacon streams the rows and never materializes the whole grid. A filter still costs a scan of the
selected chunks, so narrow the grid before you widen the query. See
[Speed up slow queries](/docs/2.0.0-rc2/guides/speed-up-queries).

::: tip This is not a cartesian product across variables
The row count comes from **one** grid, not from every variable multiplied together. Two variables on
the same `(time, lat, lon)` grid give one set of 24 rows with two columns, not 576 rows.
:::

## Attribute columns

Beacon exposes metadata as columns:

- A variable attribute becomes `<variable>.<attribute>`, for example `sst.units`.
- A global file attribute becomes `.<attribute>`, for example `.title`.

An attribute is a scalar. It becomes a constant column. It costs one value, not one value for each
row. Beacon holds the constant. It expands the column only when a consumer needs every row.

`SELECT *` on a file with many variables therefore returns many columns. Take a file with 200
variables. Each variable has two attributes. The result has about 600 columns. Name the columns that
you want:

```sql
SELECT time, lat, lon, sst FROM read_netcdf('sst/*.nc');
```

Projection pushdown then reads only those variables from storage.

## The `dimensions` argument

Every nd reader takes an optional second argument: the list of dimensions to read.

```sql
SELECT * FROM read_netcdf(['argo/**/*.nc'], ['N_PROF', 'N_LEVELS']);
```

The list sets the grid explicitly. **Beacon returns a variable only if the list holds all of that
variable's dimensions.** Everything else is dropped.

Take an Argo profile file:

```text
dimensions:
    N_PROF = 100, N_LEVELS = 500, N_HISTORY = 4, DATE_TIME = 14

variables:
    PRES(N_PROF, N_LEVELS)
    TEMP(N_PROF, N_LEVELS)
    PSAL(N_PROF, N_LEVELS)
    LATITUDE(N_PROF)
    PLATFORM_NUMBER(N_PROF, DATE_TIME)
    HISTORY_DATE(N_HISTORY, N_PROF, DATE_TIME)
```

| `dimensions` argument | Rows | Columns kept | Columns dropped |
|---|---|---|---|
| `['N_PROF', 'N_LEVELS']` | 50,000 | `PRES`, `TEMP`, `PSAL`, `LATITUDE` | `PLATFORM_NUMBER`, `HISTORY_DATE` |
| `['N_PROF', 'DATE_TIME']` | 1,400 | `PLATFORM_NUMBER`, `LATITUDE` | `PRES`, `TEMP`, `PSAL`, `HISTORY_DATE` |
| `['N_PROF']` | 100 | `LATITUDE` | every 2-D and 3-D variable |

`LATITUDE` survives all three, because `N_PROF` is in every list. `PRES` needs both `N_PROF` and
`N_LEVELS`, so the second and third lists drop it.

## Excluded variables

Some files hold variables that cannot share one grid. A CF bounds variable is the common case:
`sst(time, lat, lon)` next to `lat_bnds(lat, nv)`. No single grid holds both.

When you write `SELECT *` and give no `dimensions` argument, Beacon picks a grid for you. It chooses
the variable dimension set that, in order:

1. is non-empty,
2. keeps the **most** variables,
3. is the native dimension set of the most variables,
4. holds the largest data volume,
5. was seen first.

Each variable that remains then broadcasts safely. Beacon writes one `info` line. The line names
the variables that it dropped:

```text
SELECT * auto-selected dimensions ["time", "lat", "lon"]; excluded variables
["lat_bnds", "lon_bnds"] have incompatible dimensions and were omitted.
```

To read a dropped variable, ask for its grid:

```sql
SELECT * FROM read_netcdf('sst/2024-01.nc', ['lat', 'nv']);
```

::: warning An explicit list is never narrowed
Beacon makes the automatic choice only when you give no `dimensions` argument. It uses an explicit
list without change. Does that list mix incompatible dimensions? Then the read fails. Beacon does
not guess.
:::

## Next

- [CF decoding](/docs/2.0.0-rc2/cf-decoding): what Beacon does with `scale_factor`, `_FillValue`
  and time units
- [Coming from xarray](/docs/2.0.0-rc2/coming-from-xarray): the same operations, side by side
- [NetCDF](/docs/2.0.0-rc2/formats/netcdf) and [Zarr](/docs/2.0.0-rc2/formats/zarr) for
  format-specific behaviour
