---
description: What Beacon does with CF attributes. Time units become timestamps, packed values are unpacked, and fill values become NULL. The rules differ per reader.
---

# CF decoding

Scientific NetCDF files follow the [CF conventions](https://cfconventions.org/). CF stores some
values in an encoded form: a time as a count since an epoch, a temperature as a packed integer, a
gap as a sentinel number. Beacon decodes these on read.

**You get physical values, not raw stored integers.** This page says exactly which attributes Beacon
acts on, and which it ignores.

Beacon has two sets of rules:

- The **NetCDF** reader (`read_netcdf`, `STORED AS NC`) decodes with the ND framework. The
  [CF decode](/docs/2.1.0-rc3/formats/netcdf#cf-decode) section of the NetCDF page has the full
  list.
- The **HDF5** reader (`read_hdf5`, `STORED AS HDF5`) and the **Zarr** reader (`read_zarr`) apply
  a smaller set of rules.

## Summary

| Attribute | NetCDF | HDF5 and Zarr |
|---|---|---|
| `units` with `since` | `Timestamp(Microsecond)` | `Timestamp(Nanosecond)` |
| `calendar` | Gregorian or Julian. Other calendars give numbers | Gregorian or Julian. Other calendars give numbers |
| `scale_factor`, `add_offset` | The float type of the attributes, for example `Float32`. Else `Float64` | `Float64` |
| `_FillValue` | Becomes SQL `NULL` | Becomes SQL `NULL` |
| `missing_value` | Each value becomes SQL `NULL` | **Beacon ignores it.** The value stays as stored |
| `valid_min`, `valid_max`, `valid_range` | A value outside the range becomes SQL `NULL` | **Beacon ignores them.** The value stays as stored |
| `standard_name`, `long_name` | Beacon keeps each as a column. It applies neither | Beacon keeps each as a column. It applies neither |

Every attribute also appears as its own column, decoded or not. See
[Attribute columns](/docs/2.1.0-rc3/arrays-to-tables#attribute-columns).

A NetCDF-4 file is also an HDF5 file. The reader that you select sets the rules. `STORED AS NC`
uses the NetCDF rules. `STORED AS HDF5` uses the HDF5 rules.

## Time

A variable whose `units` attribute has the form `<unit> since <date>` is a CF time. Beacon converts
the count to an absolute instant and returns an Arrow timestamp. The NetCDF reader returns
microseconds. The HDF5 and Zarr readers return nanoseconds.

```text
double time(time) ;
    time:units = "days since 1950-01-01" ;
```

```sql
SELECT time FROM read_netcdf('argo/*.nc') LIMIT 3;
```

| time |
|---|
| 2024-01-01 00:00:00 |
| 2024-01-01 12:00:00 |
| 2024-01-02 00:00:00 |

The column is a real timestamp. Ordinary SQL date functions work on it:

```sql
SELECT date_trunc('month', time) AS month, avg(temperature)
FROM read_netcdf('argo/**/*.nc')
GROUP BY month
ORDER BY month;
```

A NaN or infinite time value becomes `NULL` in each reader. In the NetCDF reader, a time outside
the range of a microsecond timestamp also becomes `NULL`.

### Supported units

| Reader | Units |
|---|---|
| NetCDF | `microseconds`, `milliseconds`, `seconds`, `minutes`, `hours`, `days`, `weeks`. The singular form, abbreviations such as `ms`, `s`, `min`, `h` and `d`, and capitals also decode, for example `hour`, `h` and `Days` |
| HDF5, Zarr | `nanoseconds`, `microseconds`, `milliseconds`, `seconds`, `minutes`, `hours`, `days`, `weeks`. Only this plural, lowercase form decodes |

In the NetCDF reader, the unit `nanoseconds since <date>` gives numbers in the stored type, for
example `Int64`. Beacon's NetCDF writer writes this unit for a `Timestamp(Nanosecond)` column. The
NetCDF reader reads such a file back as numbers.

Accepted reference dates:

```text
seconds since 1970-01-01
days since 1950-01-01T00:00:00
days since -4713-01-01T00:00:00Z
```

The NetCDF reader also applies a time of day or a zone in the reference date, for example
`hours since 2000-01-01 12:00`.

### Supported calendars

The rules are the same in each reader.

| `calendar` value | Result |
|---|---|
| absent | Gregorian. This is the CF default |
| `gregorian`, `standard`, `proleptic_gregorian` | Gregorian |
| `julian` | Julian |
| `noleap`, `365_day`, `360_day`, `all_leap`, `366_day` | Numbers. Beacon does not decode the time |

Names match case-insensitively.

::: warning Non-standard calendars give numbers
Climate model output often uses `noleap` or `360_day`. Beacon does not decode such a time axis. It
does not return a wrong instant. The column holds the stored numbers. Convert the time axis before
you query, or compute the date in SQL from the numbers and the `time.units` column.

This is a known gap. Track it on
[GitHub](https://github.com/maris-development/beacon/issues).
:::

## Packed values: `scale_factor` and `add_offset`

CF packs a float into a small integer to save space. Beacon unpacks it:

```text
decoded = raw * scale_factor + add_offset
```

```text
short sst(time, lat, lon) ;
    sst:scale_factor = 0.01 ;
    sst:add_offset = 273.15 ;
    sst:_FillValue = -32768s ;
```

A raw `1210` becomes `285.25`.

The type of the decoded column depends on the reader:

- **NetCDF**: the float type of `scale_factor` or `add_offset`. A `Float32` attribute gives a
  `Float32` column. An integer attribute gives `Float64`.
- **HDF5 and Zarr**: `Float64`, whatever the stored type was.

Either attribute works alone. Beacon multiplies a variable that has only `scale_factor`. It shifts a
variable that has only `add_offset`.

### Packed time

A variable can have `scale_factor` or `add_offset`, with `units = "<unit> since <date>"`.

- **NetCDF**: Beacon unpacks the values, then decodes the time. The column is a timestamp.
- **HDF5 and Zarr**: Beacon unpacks the values only. The column is `Float64` numbers.

## Missing data: `_FillValue`

A cell equal to `_FillValue` becomes SQL `NULL` in each reader.

```sql
SELECT count(sst) AS present,
       sum(CASE WHEN sst IS NULL THEN 1 ELSE 0 END) AS filled
FROM read_netcdf('sst/2024-01.nc');
```

| present | filled |
|---|---|
| 6103122 | 376878 |

`count(sst)` skips the nulls. The `CASE` expression counts them.

Filter them out where they get in the way:

```sql
SELECT avg(sst) FROM read_netcdf('sst/*.nc') WHERE sst IS NOT NULL;
```

Aggregates such as `avg` already skip nulls, so the `WHERE` clause above changes nothing. It matters
for joins and for a result that you export.

### Fill and packing together

A variable can be both packed and filled. You do not unpack the sentinel yourself. A packed
sentinel cell becomes `NULL` in each reader.

### Fill and time together

A time variable can carry a `_FillValue` too. A fill cell becomes `NULL`, not a date far outside
the record. A `_FillValue = -32768` on `days since 1970-01-01` is a null, not `1880-03-15`.

## `missing_value`

CF also allows an older `missing_value` attribute, with one or more values.

- **NetCDF**: each value of `missing_value` becomes SQL `NULL`, as `_FillValue` does.
- **HDF5 and Zarr**: Beacon does **not** treat it as a null. Those cells keep their stored number.

With the HDF5 or Zarr reader, handle it in SQL:

```sql
SELECT * FROM read_hdf5('old/*.h5') WHERE sst != -999.0;
```

The value is visible as the `sst.missing_value` column, so you can check what it is first.

## Valid range: `valid_min`, `valid_max`, `valid_range`

- **NetCDF**: a value below `valid_min` or above `valid_max` becomes SQL `NULL`. `valid_range`
  gives both limits. Each attribute works alone.
- **HDF5 and Zarr**: Beacon keeps the attributes as columns. It does not drop or null values
  outside the range.

## Names and other attributes

Beacon keeps `standard_name` and `long_name` as columns. It does not rename a column to its
`standard_name`.

To find variables by standard name, query the attribute column:

```sql
SELECT DISTINCT "temperature.standard_name"
FROM read_netcdf('argo/**/*.nc')
LIMIT 0;
```

## Atlas

Atlas decodes nothing, and needs to. Its own types include a nanosecond timestamp, and the ingest
path applies the conventions before the write: `atlas create` reads each NetCDF file with xarray,
which applies `scale_factor`, `add_offset` and the CF time units. A collection therefore holds
decoded values already, and Beacon reads its arrays exactly as they are stored.

A `units` or `scale_factor` attribute can stay on an array, from the source file. It is a column
like any other attribute, for example `"temperature.units"`. It changes no value.

## Next

- [Arrays to tables](/docs/2.1.0-rc3/arrays-to-tables): the row count and the grid rule
- [NetCDF](/docs/2.1.0-rc3/formats/netcdf) for format-specific behaviour
- [Troubleshooting](/docs/2.1.0-rc3/troubleshooting) for the errors these attributes cause
