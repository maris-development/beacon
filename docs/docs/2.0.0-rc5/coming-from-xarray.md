---
description: Replace an xarray or pandas loop over NetCDF files with one SQL statement. Side-by-side translations for open_mfdataset, sel, groupby and to_dataframe.
---

# Coming from xarray

If you already read NetCDF with xarray, Beacon does not replace it. It replaces the loop you write
**before** xarray: the part that opens hundreds of files, harmonizes their columns and concatenates
the result, so that you can start work.

This page puts the two side by side.

## When to reach for which

| Task | Use |
|---|---|
| Subset a large collection down to what fits in memory | **Beacon** |
| Files with one schema each, or a variable that only some files hold | **Beacon** |
| A join across collections, or against a table | **Beacon** |
| Interpolation, regridding, rolling windows, `.plot()` | **xarray** |
| One well-formed gridded file you already have locally | **xarray** |

Most people use both. Beacon makes the collection smaller. It then gives a dataframe to the code you
already have.

## Open a collection

::: code-group

```python [xarray]
import xarray as xr

ds = xr.open_mfdataset(
    "argo/**/*.nc",
    combine="nested",
    concat_dim="N_PROF",
    parallel=True,
)
```

```sql [Beacon]
SELECT * FROM read_netcdf('argo/**/*.nc');
```

:::

`open_mfdataset` opens every file to align the coordinates. Beacon reads the metadata and defers
the rest until a query asks for it.

## Subset

::: code-group

```python [xarray]
subset = ds.sel(
    JULD=slice("2024-01-01", "2024-06-30"),
).where(
    (ds.TEMP > 20) & (ds.LATITUDE > 40),
    drop=True,
)
df = subset[["LATITUDE", "LONGITUDE", "JULD", "TEMP"]].to_dataframe()
```

```sql [Beacon]
SELECT latitude, longitude, juld, temp
FROM read_netcdf('argo/**/*.nc')
WHERE juld BETWEEN '2024-01-01' AND '2024-06-30'
  AND temp > 20
  AND latitude > 40;
```

:::

The `WHERE` clause pushes down. Beacon skips files and chunks that cannot match, so the amount of
data it reads follows the filter. `.where(..., drop=True)` runs after the read.

## Aggregate

::: code-group

```python [xarray]
monthly = (
    ds.TEMP
      .groupby("JULD.month")
      .mean()
      .to_dataframe()
)
```

```sql [Beacon]
SELECT date_trunc('month', juld) AS month,
       avg(temp) AS mean_temp,
       count(*)  AS n
FROM read_netcdf('argo/**/*.nc')
GROUP BY month
ORDER BY month;
```

:::

## The loop that goes away

Beacon replaces this pattern. The files disagree about their variables. `open_mfdataset` then fails.
You write a loop instead:

```python
# Before
import glob, xarray as xr, pandas as pd

frames = []
for path in glob.glob("argo/**/*.nc", recursive=True):
    ds = xr.open_dataset(path)
    if "TEMP" not in ds:            # not every file has it
        continue
    df = ds[["LATITUDE", "LONGITUDE", "JULD", "TEMP"]].to_dataframe().reset_index()
    df = df[df.TEMP > 20]
    frames.append(df)
    ds.close()

result = pd.concat(frames, ignore_index=True)
```

```python
# After
from beacon_api import Client

client = Client("https://beacon.example.com")

result = client.sql_query("""
    SELECT latitude, longitude, juld, temp
    FROM read_netcdf('argo/**/*.nc')
    WHERE temp > 20
""").to_pandas_dataframe()
```

Beacon joins the files by column name. A file without `TEMP` gives `NULL` in that column. The read
does not fail. See [UNION BY NAME](/docs/2.0.0-rc5/sql/union-by-name).

## Translation table

| xarray | Beacon SQL |
|---|---|
| `xr.open_mfdataset(paths)` | `read_netcdf(paths)` |
| `ds[["a", "b"]]` | `SELECT a, b` |
| `ds.sel(t=slice(x, y))` | `WHERE t BETWEEN x AND y` |
| `ds.where(cond, drop=True)` | `WHERE cond` |
| `ds.isel(t=slice(0, 10))` | `LIMIT 10` |
| `ds.groupby(...).mean()` | `GROUP BY ... ` with `avg(...)` |
| `ds.to_dataframe()` | `.to_pandas_dataframe()` on the client |
| `ds.dims`, `ds.data_vars` | `read_netcdf_schema(paths)` |
| `ds.attrs["title"]` | the `.title` column |
| `ds.TEMP.attrs["units"]` | the `temp.units` column |
| `xr.merge([a, b])` | `JOIN` |
| `ds.to_netcdf(path)` | `to_nd_netcdf(path, dimension_columns=[...])` |

## Things that do not carry over

- **A Beacon result is a table, not a `Dataset`.** The array shape is flattened into rows. See
  [Arrays to tables](/docs/2.0.0-rc5/arrays-to-tables) for the row count and the broadcast rule.
- **No `.to_xarray()` on the client yet.** Round-trip through `to_nd_netcdf()` and
  `xr.open_dataset()`, or build a `Dataset` from the dataframe with
  `df.set_index([...]).to_xarray()`.
- **No lazy dask arrays.** The server streams the result instead. Aggregate or filter on the
  server. Then read back what fits in memory.
- **Non-standard calendars are unsupported.** `noleap` and `360_day` files are rejected. See
  [CF decoding](/docs/2.0.0-rc5/cf-decoding#supported-calendars).

## Keep both

The two tools work together. Beacon makes the collection smaller. You then give the result to
xarray:

```python
from beacon_api import Client

client = Client("https://beacon.example.com")

query = client.sql_query("""
    SELECT juld, latitude, longitude, pres, temp
    FROM read_netcdf('argo/**/*.nc')
    WHERE latitude BETWEEN 40 AND 65
      AND juld >= '2024-01-01'
""")

query.to_nd_netcdf("subset.nc", dimension_columns=["pres"])

import xarray as xr
ds = xr.open_dataset("subset.nc")     # back in xarray, on a subset that fits
```

## Next

- [Arrays to tables](/docs/2.0.0-rc5/arrays-to-tables): what a dimension does to your row count
- [CF decoding](/docs/2.0.0-rc5/cf-decoding): units, packing and fill values
- [Query a file collection](/docs/2.0.0-rc5/guides/query-a-collection): the full walkthrough
