# ERDDAP facts confirmed by recording

Recorded 2026-10-08 from `https://coastwatch.pfeg.noaa.gov/erddap` (ERDDAP 2.31.1). Run `record.sh` to repeat.

## Datasets

- tabledap: `erdGlobecBottle`. It exists (info HTTP 200). Rows are from 2002-05-30T03:21:00Z to 2002-08-19T20:18:00Z.
- griddap: `erdHadISST`. It exists (info HTTP 200).
- tabledap.parquet columns, in request order: `cruise_id,ship,cast,longitude,latitude,time,bottle_posn,temperature0`.
- tabledap.parquet request window: `time>=2002-05-30T00:00:00Z` and `time<=2002-05-31T00:00:00Z`. It returns rows (cruise w0205, ship Wecoma, 2002-05-30).
- griddap axes, in `dimension` row order: `time`, `latitude`, `longitude`. The data variable is `sst`.

## tabledap parquet

- Row count of `tabledap.parquet`: 62.
- Arrow type of each column: `cruise_id` Utf8, `ship` Utf8, `cast` Int32, `longitude` Float32, `latitude` Float32, `time` Timestamp(Millisecond, UTC), `bottle_posn` Int32, `temperature0` Float32.
- Time is a Parquet INT64 timestamp (isAdjustedToUTC, milliseconds). It is not a float64 of epoch seconds.
- The info type of `time` is `double`. The info type of `cast` is `short`. The info type of `bottle_posn` is `byte`. The parquet writer widens `short` and `byte` to Int32.
- Parquet column names equal the ERDDAP variable names.
- The parquet file has the key-value metadata `column_names` and `column_units`. The time unit there is `milliseconds since 1970-01-01T00:00:00Z`.
- A missing numeric value is null, not NaN. A probe of `chl_a_total` for 2002-05-30 to 2002-06-02 gave 64 nulls in 225 rows and 0 NaN values. This probe is not stored.
- A missing string value is null, not `""`. A probe of the `griddap` column of `allDatasets` gave 36 nulls in 130 rows and 0 empty strings. This probe is not stored.
- The `erdGlobecBottle` dataset has no missing string value. The CSV output shows `""` for a missing string. The parquet output shows null.
- Parquet output of `distinct()` and every `orderBy*` function, including `orderByLimit`, returns HTTP 200 with an empty body (Content-Length 0). It is not a valid parquet file.
- A plain constraint such as `&cast=1` works with parquet output.

## Errors

- `no_results.txt`: HTTP 404. First line: `Error {`. The `message` line is `Not Found: Your query produced no matching results. (time<=1900-01-01T00:00:00Z is outside of the variable's actual_range: ...)`.
- `error_500.txt`: HTTP 400, not 500. The file keeps the name from the plan. The `message` line is `Bad Request: Query error: Unrecognized variable="no_such_variable".`
- Both error bodies use the form `Error {` then `code=<n>;` then `message="...";` then `}`.

## orderByLimit

- The probe `cruise_id&orderByLimit("10")` returns 2 data rows (nh0207, w0205), not 10. The answer to "exactly 10 rows" is no.
- With more than one column, or with a column that has many distinct values, `orderByLimit("10")` returns exactly 10 rows. The rows are sorted by the result columns, so they are not the first 10 rows in file order.
- `orderByLimit("5")` on three columns returns exactly 5 rows.
- `orderByLimit("cruise_id,10")` returns up to 10 rows for each `cruise_id` value.
- The number-only form is not a safe limit. In parquet output, every `orderBy*` function returns an empty body. Do not push a limit as `orderByLimit`.

## griddap

- Type given by `griddap_info.json`: `time` double (nValues=1878, not evenly spaced), `latitude` float (nValues=180, evenly spaced, spacing -1.0), `longitude` float (nValues=360, evenly spaced, spacing 1.0), `sst` float (axes `time, latitude, longitude`).
- Axis order in the `sst` row: `time, latitude, longitude`.
- Latitude is descending: 89.5 down to -89.5. Longitude is ascending: -179.5 to 179.5. Time is ascending: 1870-01-16T11:59:59Z to 2026-06-16T00:00:00Z.
- Axis `.json` responses have the form `{"table": {"columnNames": [axis], "columnTypes": [type], "columnUnits": [unit], "rows": [[v], ...]}}`.
- Axis `.json` column types: `time` is `String` (ISO 8601 UTC with `Z`, 1878 rows, unit `UTC`). `latitude` is `float` (180 rows, `degrees_north`). `longitude` is `float` (360 rows, `degrees_east`).
- Axis `.json` time values are not numbers. The time axis uses ISO strings, such as `1870-01-16T11:59:59Z`.
- `griddap_data.nc` request `sst[0:1][10:13][20:24]` has the dimensions time=2, latitude=4, longitude=5. This is 2x4x5 as expected.
- `griddap_data.nc` values: latitude 79.5, 78.5, 77.5, 76.5. Longitude -159.5 to -155.5. All 40 sst values are -1.8 (sea ice). No fill value is present.
- `griddap_sample.nc` (request `sst[0:0][0:0][0:0]`) has the dimensions time=1, latitude=1, longitude=1. The single sst value is -1000.0 (land). Its `_FillValue` is -1e30.
- Both `.nc` files are NetCDF classic (magic bytes `CDF\x01`). Coordinate variables `time` (double), `latitude` (float) and `longitude` (float) are present. `time` has units `seconds since 1970-01-01T00:00:00Z`. `sst` is float with `_FillValue` and `missing_value` of -1e30.
- Both `.nc` files carry about 35 global attributes and per-variable attributes. They are present in the file, so the reader must not expose them as columns.
