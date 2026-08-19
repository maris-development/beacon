---
description: What Beacon does with CF attributes. Time units become timestamps, scale_factor and add_offset are applied, and _FillValue becomes NULL.
---

# CF decoding

Scientific NetCDF files follow the [CF conventions](https://cfconventions.org/). CF stores some
values in an encoded form: a time as a count since an epoch, a temperature as a packed integer, a
gap as a sentinel number. Beacon decodes these on read.

**You get physical values, not raw stored integers.** This page says exactly which attributes Beacon
acts on, and which it ignores.

## Summary

| Attribute | Beacon does |
|---|---|
| `units` with `since` | Decodes to a nanosecond timestamp |
| `calendar` | Selects Gregorian or Julian. Other calendars are an error |
| `scale_factor` | Multiplies. Result is `f64` |
| `add_offset` | Adds. Result is `f64` |
| `_FillValue` | Becomes SQL `NULL` |
| `missing_value` | **Beacon ignores it.** The value stays as stored |
| `valid_min`, `valid_max` | **Beacon ignores them.** The value stays as stored |
| `standard_name`, `long_name` | Beacon keeps each as a column. It applies neither |

Every attribute also appears as its own column, decoded or not. See
[Attribute columns](/docs/2.0.0-rc3/arrays-to-tables#attribute-columns).

## Time

A variable whose `units` attribute contains `since` is a CF time. Beacon converts the count to an
absolute instant and returns an Arrow nanosecond timestamp.

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

### Supported units

`nanoseconds`, `microseconds`, `milliseconds`, `seconds`, `minutes`, `hours`, `days`, `weeks`. Each
one takes the form `<unit> since <date>`.

Accepted reference dates:

```text
seconds since 1970-01-01
days since 1950-01-01T00:00:00
days since -4713-01-01T00:00:00Z
```

### Supported calendars

| `calendar` value | Result |
|---|---|
| absent | Gregorian. This is the CF default |
| `gregorian`, `standard`, `proleptic_gregorian` | Gregorian |
| `julian` | Julian |
| `noleap`, `365_day`, `360_day`, `all_leap`, `366_day` | **Error** |

Names match case-insensitively.

::: warning Non-standard calendars are not supported
Climate model output often uses `noleap` or `360_day`. Beacon rejects those files. It does not
return a wrong instant. Convert the time axis before you query. You can also remove the `units`
attribute. Beacon then reads the variable as a plain number.

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

A raw `1210` becomes `285.25`. The decoded column is `f64`, whatever the stored type was.

Either attribute works alone. Beacon multiplies a variable that has only `scale_factor`. It shifts a
variable that has only `add_offset`.

## Missing data: `_FillValue`

A cell equal to `_FillValue` becomes SQL `NULL`.

```sql
SELECT count(*) AS rows, count(sst) AS present
FROM read_netcdf('sst/2024-01.nc');
```

| rows | present |
|---|---|
| 6480000 | 6103122 |

`count(*)` counts every grid cell. `count(sst)` skips the nulls, so the difference is the fill
count.

Filter them out where they get in the way:

```sql
SELECT avg(sst) FROM read_netcdf('sst/*.nc') WHERE sst IS NOT NULL;
```

Aggregates such as `avg` already skip nulls, so the `WHERE` clause above changes nothing. It matters
for `count(*)` and for joins.

### Fill and packing together

A variable can be both packed and filled. Beacon then decodes the fill value with the same
arithmetic before it compares. A packed sentinel maps to the decoded fill exactly. Beacon sets it to
`NULL`. You do not unpack the sentinel yourself.

### Fill and time together

A time variable can carry a `_FillValue` too. Beacon decodes that fill with the same time arithmetic
as the data, then compares. A fill cell becomes `NULL`, not a date far outside the record. A
`_FillValue = -32768` on `days since 1970-01-01` is a null, not `1880-03-15`.

### `missing_value` is not applied

CF also allows an older `missing_value` attribute. Beacon does **not** treat it as a null. Those
cells keep their stored number. Handle it in SQL:

```sql
SELECT * FROM read_netcdf('old/*.nc') WHERE sst != -999.0;
```

The value is visible as the `sst.missing_value` column, so you can check what it is first.

## Names and other attributes

Beacon keeps `standard_name`, `long_name`, `valid_min` and `valid_max` as columns. It changes
nothing else. It does not rename a column to its `standard_name`. It does not drop rows outside
`valid_min` and `valid_max`.

To find variables by standard name, query the attribute column:

```sql
SELECT DISTINCT "temperature.standard_name"
FROM read_netcdf('argo/**/*.nc')
LIMIT 0;
```

## Zarr and Atlas

Zarr and Atlas go through the same decoding path as NetCDF. The attribute names and the rules above
apply unchanged.

## Next

- [Arrays to tables](/docs/2.0.0-rc3/arrays-to-tables): the row count and the grid rule
- [NetCDF](/docs/2.0.0-rc3/formats/netcdf) for format-specific behaviour
- [Troubleshooting](/docs/2.0.0-rc3/troubleshooting) for the errors these attributes cause
