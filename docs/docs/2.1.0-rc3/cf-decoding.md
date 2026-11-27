---
description: What Beacon does with CF attributes. Time units become timestamps, packed values are unpacked, and fill values become NULL. The rules differ per format.
---

# CF decoding

Scientific NetCDF files follow the [CF conventions](https://cfconventions.org/). CF stores some
values in an encoded form: a time as a count since an epoch, a temperature as a packed integer, a
gap as a sentinel number. Beacon decodes these on read.

**You get physical values, not raw stored integers.** This page says exactly which attributes Beacon
acts on, and which it ignores.

Beacon decodes with the ND framework. The rules are:

- The **NetCDF** and **HDF5** readers (`read_netcdf`, `STORED AS NC`, `read_hdf5`, `STORED AS HDF5`)
  use the same rules. The [CF decode](/docs/2.1.0-rc3/formats/netcdf#cf-decode) section of the NetCDF
  page has the full list.
- The **Zarr** reader (`read_zarr`) has two differences. The [Zarr](#zarr) section gives them.

## Summary

| Attribute | NetCDF and HDF5 |
|---|---|
| `units` with `since` | `Timestamp(Microsecond)` |
| `calendar` | Gregorian or Julian. Other calendars give numbers |
| `scale_factor`, `add_offset` | The float type of the attributes, for example `Float32`. Else `Float64` |
| `_FillValue` | Becomes SQL `NULL` |
| `missing_value` | Each value becomes SQL `NULL` |
| `valid_min`, `valid_max`, `valid_range` | A value outside the range becomes SQL `NULL` |
| `standard_name`, `long_name` | Beacon keeps each as a column. It applies neither |

Every attribute with one value also appears as its own column. See
[Attribute columns](/docs/2.1.0-rc3/arrays-to-tables#attribute-columns).

A NetCDF-4 file is also an HDF5 file. `STORED AS NC` and `STORED AS HDF5` decode it with the same
rules.

## Time

A variable whose `units` attribute has the form `<unit> since <date>` is a CF time. Beacon converts
the count to an absolute instant and returns an Arrow timestamp in microseconds.

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

A NaN or infinite time value becomes `NULL`. A time outside the range of a microsecond timestamp
also becomes `NULL`.

### Supported units

`microseconds`, `milliseconds`, `seconds`, `minutes`, `hours`, `days` and `weeks` decode. The
singular form, abbreviations such as `ms`, `s`, `min`, `h` and `d`, and capitals also decode, for
example `hour`, `h` and `Days`.

The unit `nanoseconds since <date>` gives numbers in the stored type, for example `Int64`. Beacon's
NetCDF writer writes this unit for a `Timestamp(Nanosecond)` column. Beacon reads such a file back
as numbers.

Accepted reference dates:

```text
seconds since 1970-01-01
days since 1950-01-01T00:00:00
days since -4713-01-01T00:00:00Z
```

Beacon also applies a time of day or a zone in the reference date, for example
`hours since 2000-01-01 12:00`.

### Supported calendars

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

The decoded column gets the float type of `scale_factor` or `add_offset`. A `Float32` attribute
gives a `Float32` column. An integer attribute gives `Float64`.

Either attribute works alone. Beacon multiplies a variable that has only `scale_factor`. It shifts a
variable that has only `add_offset`.

### Packed time

A variable can have `scale_factor` or `add_offset`, with `units = "<unit> since <date>"`. Beacon
unpacks the values, then decodes the time. The column is a timestamp.

## Missing data: `_FillValue`

A cell equal to `_FillValue` becomes SQL `NULL`.

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
sentinel cell becomes `NULL`.

### Fill and time together

A time variable can carry a `_FillValue` too. A fill cell becomes `NULL`, not a date far outside
the record. A `_FillValue = -32768` on `days since 1970-01-01` is a null, not `1880-03-15`.

## `missing_value`

CF also allows an older `missing_value` attribute, with one or more values. Each value of
`missing_value` becomes SQL `NULL`, as `_FillValue` does.

## Valid range: `valid_min`, `valid_max`, `valid_range`

A value below `valid_min` or above `valid_max` becomes SQL `NULL`. `valid_range` gives both limits.
Each attribute works alone.

## Names and other attributes

Beacon keeps `standard_name` and `long_name` as columns. It does not rename a column to its
`standard_name`.

To find variables by standard name, query the attribute column:

```sql
SELECT DISTINCT "temperature.standard_name"
FROM read_netcdf('argo/**/*.nc')
LIMIT 0;
```

## Zarr

The Zarr reader follows the NetCDF rules, with two differences. A packed column is `Float64`. An
integer variable with `nanoseconds since` units is `Timestamp(ns)`. See [CF decode](/docs/2.1.0-rc3/formats/zarr#cf-decode) on the Zarr page for the full list.

## Next

- [Arrays to tables](/docs/2.1.0-rc3/arrays-to-tables): the row count and the grid rule
- [NetCDF](/docs/2.1.0-rc3/formats/netcdf) for format-specific behaviour
- [Troubleshooting](/docs/2.1.0-rc3/troubleshooting) for the errors these attributes cause
