# ERDDAP facts confirmed by recording

Recorded 2026-10-08 from `https://coastwatch.pfeg.noaa.gov/erddap` (ERDDAP 2.31.1). Run `record.sh` to repeat.

## Datasets

- tabledap: `erdGlobecBottle`. It exists (info HTTP 200). Rows are from 2002-05-30T03:21:00Z to 2002-08-19T20:18:00Z.
- tabledap.parquet columns, in request order: `cruise_id,ship,cast,longitude,latitude,time,bottle_posn,temperature0`.
- tabledap.parquet request window: `time>=2002-05-30T00:00:00Z` and `time<=2002-05-31T00:00:00Z`. It returns rows (cruise w0205, ship Wecoma, 2002-05-30).

## tabledap parquet

- Row count of `tabledap.parquet`: 62.
- `tabledap_all.parquet` holds all 25 variables in info order, for the same window: 62 rows. All variables other than `cruise_id`, `ship`, `cast`, `bottle_posn` and `time` are Float32. Null counts: `chl_a_total` 16, `chl_a_10um` 57, `phaeo_total` 16, `phaeo_10um` 57, `PO4`, `N_N`, `NO3`, `Si`, `NO2`, `NH4` 12 each, `par` 62. No other column has nulls.
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
