# Function Reference

The SQL runtime of Beacon gives three families of functions:

1. **DataFusion built-ins**: Beacon takes the full scalar and aggregate function
   library of Apache DataFusion. The tables below hold a selection. For the full
   list, see the DataFusion
   [scalar](https://datafusion.apache.org/user-guide/sql/scalar_functions.html)
   and [aggregate](https://datafusion.apache.org/user-guide/sql/aggregate_functions.html)
   references.
2. **[Beacon-specific functions](#beacon-specific-functions)**: Beacon adds these
   functions for type conversion and vocabulary mapping. Those sections hold the
   full list.
3. **[Spatial functions](/docs/2.0.0-rc4/sql/spatial-functions)**: 123 functions
   with PostGIS names, over a geometry that `ST_Point` builds from coordinate
   columns. Its own chapter holds the full list.

## DataFusion built-in functions (inherited)

This is a selection of the DataFusion library. Every function in the DataFusion
reference works, also the functions that this page does not list.

### Aggregate functions

| Function | Description |
| -------- | ----------- |
| `COUNT(*)` | Number of rows |
| `COUNT(col)` | Number of non-NULL values |
| `SUM(col)` | Sum |
| `AVG(col)` | Mean |
| `MIN(col)` | Minimum |
| `MAX(col)` | Maximum |
| `STDDEV(col)` | Standard deviation |
| `VARIANCE(col)` | Variance |
| `MEDIAN(col)` | Median |
| `APPROX_PERCENTILE_CONT(col, p)` | Approximate percentile (0–1) |

### Math functions

| Function | Description |
| -------- | ----------- |
| `abs(x)` | Absolute value |
| `acos(x)` | Inverse cosine |
| `acosh(x)` | Inverse hyperbolic cosine |
| `asin(x)` | Inverse sine |
| `asinh(x)` | Inverse hyperbolic sine |
| `atan(x)` | Inverse tangent |
| `atan2(y, x)` | Arc tangent of `y / x` |
| `atanh(x)` | Inverse hyperbolic tangent |
| `cbrt(x)` | Cube root |
| `ceil(x)` | Round up to nearest integer |
| `cos(x)` | Cosine |
| `cosh(x)` | Hyperbolic cosine |
| `cot(x)` | Cotangent |
| `degrees(x)` | Convert radians to degrees |
| `exp(x)` | `e^x` |
| `factorial(x)` | Factorial (returns 1 for values less than 2) |
| `floor(x)` | Round down to nearest integer |
| `gcd(x, y)` | Greatest common divisor |
| `isnan(x)` | Returns `true` if `x` is `+NaN` or `-NaN` |
| `iszero(x)` | Returns `true` if `x` is `+0.0` or `-0.0` |
| `lcm(x, y)` | Least common multiple |
| `ln(x)` | Natural logarithm |
| `log(base, x)` | Logarithm with specified base |
| `log(x)` | Natural logarithm (single-argument form) |
| `log10(x)` | Base-10 logarithm |
| `log2(x)` | Base-2 logarithm |
| `nanvl(x, fallback)` | Returns `x` if not NaN, otherwise `fallback` |
| `pi()` | π (3.14159…) |
| `pow(base, exp)` | Alias for `power` |
| `power(base, exp)` | `base` raised to `exp` |
| `radians(x)` | Convert degrees to radians |
| `random()` | Random float in `[0, 1)` |
| `round(x[, d])` | Round to `d` decimal places (default 0) |
| `signum(x)` | Sign of `x` (−1, 0, or 1) |
| `sin(x)` | Sine |
| `sinh(x)` | Hyperbolic sine |
| `sqrt(x)` | Square root |
| `tan(x)` | Tangent |
| `tanh(x)` | Hyperbolic tangent |
| `trunc(x[, d])` | Truncate to `d` decimal places (default 0) |

### String functions

| Function | Description |
| -------- | ----------- |
| `ascii(s)` | Unicode scalar value of the first character |
| `bit_length(s)` | Length in bits |
| `btrim(s[, chars])` | Remove leading and trailing `chars` (default: whitespace) |
| `char_length(s)` | Alias for `character_length` |
| `character_length(s)` | Number of characters |
| `chr(n)` | Character with Unicode scalar value `n` |
| `concat(s1, s2, …)` | Concatenate strings |
| `concat_ws(sep, s1, s2, …)` | Concatenate with separator |
| `contains(s, substr)` | Returns `true` if `substr` is found in `s` |
| `ends_with(s, suffix)` | Returns `true` if `s` ends with `suffix` |
| `find_in_set(s, list)` | Position of `s` in comma-separated `list` (1-based) |
| `initcap(s)` | Capitalize first letter of each word |
| `instr(s, substr)` | Alias for `strpos` |
| `left(s, n)` | First `n` characters |
| `length(s)` | Alias for `character_length` |
| `levenshtein(s1, s2)` | Edit distance between two strings |
| `lower(s)` | Lowercase |
| `lpad(s, n[, pad])` | Left-pad to length `n` with `pad` (default: space) |
| `ltrim(s[, chars])` | Remove leading `chars` (default: whitespace) |
| `octet_length(s)` | Length in bytes |
| `overlay(s PLACING repl FROM pos [FOR len])` | Replace a substring at position |
| `position(substr IN s)` | Alias for `strpos` |
| `repeat(s, n)` | Repeat `s` `n` times |
| `replace(s, from, to)` | Replace all occurrences of `from` with `to` |
| `reverse(s)` | Reverse character order |
| `right(s, n)` | Last `n` characters |
| `rpad(s, n[, pad])` | Right-pad to length `n` with `pad` (default: space) |
| `rtrim(s[, chars])` | Remove trailing `chars` (default: whitespace) |
| `split_part(s, delim, n)` | `n`-th field after splitting `s` on `delim` |
| `starts_with(s, prefix)` | Returns `true` if `s` starts with `prefix` |
| `strpos(s, substr)` | 1-based position of `substr` in `s` (0 if not found) |
| `substr(s, start[, len])` | Substring starting at `start` with optional length |
| `substr_index(s, delim, n)` | Substring before `n`-th occurrence of `delim` |
| `substring(s, start[, len])` | Alias for `substr` |
| `substring_index(s, delim, n)` | Alias for `substr_index` |
| `to_hex(n)` | Integer to hexadecimal string |
| `translate(s, from, to)` | Character-wise substitution |
| `trim(s[, chars])` | Alias for `btrim` |
| `upper(s)` | Uppercase |
| `uuid()` | Random UUID v4 string (unique per row) |

### Regular expression functions

| Function | Description |
| -------- | ----------- |
| `regexp_count(s, pattern[, start, flags])` | Number of times `pattern` matches in `s` |
| `regexp_instr(s, pattern[, start[, n[, flags[, subexpr]]]])` | Position of the `n`-th match |
| `regexp_like(s, pattern[, flags])` | Returns `true` if `pattern` has at least one match |
| `regexp_match(s, pattern[, flags])` | Returns the first match as an array of capture groups |
| `regexp_replace(s, pattern, replacement[, flags])` | Replace matches with `replacement` |

The common flags are `i` for a match without case, and `g` for every occurrence.

### Binary string functions

| Function | Description |
| -------- | ----------- |
| `encode(data, format)` | Encode binary data to text (`'hex'`, `'base64'`, `'escape'`) |
| `decode(text, format)` | Decode text back to binary |

### Date and time functions

| Function | Description |
| -------- | ----------- |
| `current_date()` | Current date in the session time zone |
| `current_time()` | Current time in the session time zone |
| `current_timestamp()` | Alias for `now()` |
| `today()` | Alias for `current_date()` |
| `now()` | Current timestamp in the configured time zone |
| `date_bin(interval, ts, origin)` | Truncate `ts` to the start of a fixed-width interval |
| `date_trunc(precision, ts)` | Truncate to `'year'`, `'month'`, `'day'`, `'hour'`, `'minute'`, `'second'` |
| `datetrunc(precision, ts)` | Alias for `date_trunc` |
| `date_part(part, ts)` | Extract a numeric part from a timestamp |
| `datepart(part, ts)` | Alias for `date_part` |
| `extract(part FROM ts)` | SQL-standard equivalent of `date_part` |
| `date_format(ts, fmt)` | Alias for `to_char` |
| `to_char(ts, fmt)` | Format a timestamp as a string using [Chrono format](https://docs.rs/chrono/latest/chrono/format/strftime/index.html) |
| `to_date(expr[, fmt…])` | Parse to a date (`YYYY-MM-DD`) |
| `to_time(expr[, fmt…])` | Parse to a time (`HH:MM:SS.nnnnnnnnn`) |
| `to_timestamp(expr[, fmt…])` | Parse to a timestamp in the session time zone |
| `to_timestamp_seconds(expr[, fmt…])` | Parse, interpreting integers as seconds since epoch |
| `to_timestamp_millis(expr[, fmt…])` | Parse, interpreting integers as milliseconds since epoch |
| `to_timestamp_micros(expr[, fmt…])` | Parse, interpreting integers as microseconds since epoch |
| `to_timestamp_nanos(expr[, fmt…])` | Parse, interpreting integers as nanoseconds since epoch |
| `from_unixtime(n[, tz])` | Convert an integer (seconds since epoch) to a timestamp |
| `to_unixtime(ts)` | Convert a timestamp to seconds since epoch |
| `to_local_time(ts)` | Strip the time zone from a timestamp-with-timezone |
| `make_date(y, m, d)` | Construct a date from year, month, and day |
| `make_time(h, min, s)` | Construct a time from hour, minute, and second |

```sql
-- Monthly averages
SELECT DATE_TRUNC('month', time) AS month, AVG(temperature)
FROM ocean_profiles
GROUP BY month
ORDER BY month

-- Extract year
SELECT EXTRACT(year FROM time) AS year, COUNT(*)
FROM ocean_profiles
GROUP BY year

-- Fixed-width 6-hour bins
SELECT DATE_BIN(INTERVAL '6 hours', time, '2024-01-01') AS bin, COUNT(*)
FROM ocean_profiles
GROUP BY bin
```

### Conditional expressions

| Function | Description |
| -------- | ----------- |
| `coalesce(e1, e2, …)` | First non-NULL argument |
| `nullif(e1, e2)` | NULL if `e1 = e2`, otherwise `e1` |
| `greatest(e1, e2, …)` | Largest value in the list |
| `least(e1, e2, …)` | Smallest value in the list |
| `nvl(e1, e2)` | `e2` if `e1` is NULL, otherwise `e1` |
| `nvl2(e1, e2, e3)` | `e2` if `e1` is not NULL, otherwise `e3` |
| `ifnull(e1, e2)` | Alias for `nvl` |

```sql
-- CASE WHEN
SELECT
    time,
    temperature,
    CASE
        WHEN temperature < 5  THEN 'cold'
        WHEN temperature < 15 THEN 'cool'
        ELSE 'warm'
    END AS temp_class
FROM ocean_profiles

-- First non-NULL
SELECT COALESCE(temperature_corrected, temperature) AS temp
FROM ocean_profiles

-- Mask a sentinel value
SELECT NULLIF(quality_flag, 9) AS qc_flag
FROM ocean_profiles
```

### Casting

```sql
SELECT CAST(pressure AS DOUBLE) AS pressure_dbar
FROM ocean_profiles

-- Short form
SELECT pressure::DOUBLE AS pressure_dbar
FROM ocean_profiles
```

### `try_arrow_cast(expr, type_str)`

This function works like `TRY_CAST`. It uses Arrow type names instead of SQL type names. It returns `NULL` if the cast fails. It raises no error.

```sql
-- Cast using an Arrow type name
SELECT try_arrow_cast(raw_value, 'Float32') AS val
FROM sensor_data

-- Cast to a timestamp with timezone
SELECT try_arrow_cast(epoch_str, 'Timestamp(Microsecond, Some("UTC"))') AS ts
FROM events
```

### `cast_int8_as_char(n)`

Reads an `Int8` value as an ASCII code. Returns the matching string of one character. Use it for a NetCDF3 `char` variable that the file stores as `Int8`.

```sql
SELECT cast_int8_as_char(platform_type_code) AS platform_type
FROM argo_profiles
```

---

## Beacon-specific functions

### `beacon_version()`

Returns the version of the Beacon server as a string.

```sql
SELECT beacon_version()
```

### `coalesce_label(col1, 'label1', col2, 'label2', …)`

Takes a list of `(column, label)` pairs. Returns the label of the first non-null column. Every label must be a non-null string literal.

```sql
SELECT coalesce_label(
    temperature_corrected, 'corrected',
    temperature,           'raw'
) AS temp_source
FROM ocean_profiles
```

---

## Geospatial functions

Beacon holds 123 spatial functions with PostGIS names: 118 scalar functions, 3 aggregate functions
and 2 window functions. The [spatial functions chapter](/docs/2.0.0-rc4/sql/spatial-functions)
lists every one of them, with its arguments and its return type.

A name is case insensitive, so `ST_Distance` and `st_distance` are the same function.

| Group | Functions | Examples |
| ----- | --------- | -------- |
| [Accessors](/docs/2.0.0-rc4/sql/spatial-functions#accessors) | 16 | `ST_X`, `ST_Y`, `ST_SRID`, `ST_GeometryType` |
| [Components](/docs/2.0.0-rc4/sql/spatial-functions#components) | 6 | `ST_StartPoint`, `ST_PointN`, `ST_GeometryN` |
| [Constructors](/docs/2.0.0-rc4/sql/spatial-functions#constructors) | 7 | `ST_Point`, `ST_MakeLine`, `ST_MakeEnvelope` |
| [Input and output](/docs/2.0.0-rc4/sql/spatial-functions#input-and-output) | 10 | `ST_AsText`, `ST_AsGeoJSON`, `ST_GeomFromText` |
| [Predicates](/docs/2.0.0-rc4/sql/spatial-functions#predicates) | 15 | `ST_Intersects`, `ST_Within`, `ST_DWithin` |
| [Measurement](/docs/2.0.0-rc4/sql/spatial-functions#measurement) | 9 | `ST_Area`, `ST_Length`, `ST_Distance` |
| [Linear reference](/docs/2.0.0-rc4/sql/spatial-functions#linear-reference) | 4 | `ST_ClosestPoint`, `ST_LineLocatePoint` |
| [Overlay](/docs/2.0.0-rc4/sql/spatial-functions#overlay) | 4 | `ST_Union`, `ST_Intersection`, `ST_Difference` |
| [Processing](/docs/2.0.0-rc4/sql/spatial-functions#processing) | 18 | `ST_Buffer`, `ST_Centroid`, `ST_Simplify` |
| [Validity](/docs/2.0.0-rc4/sql/spatial-functions#validity) | 3 | `ST_IsValid`, `ST_MakeValid` |
| [Affine](/docs/2.0.0-rc4/sql/spatial-functions#affine) | 4 | `ST_Translate`, `ST_Rotate`, `ST_Affine` |
| [Bounding box](/docs/2.0.0-rc4/sql/spatial-functions#bounding-box) | 8 | `ST_Envelope`, `ST_XMin`, `ST_YMax` |
| [Tessellation](/docs/2.0.0-rc4/sql/spatial-functions#tessellation) | 4 | `ST_DelaunayTriangles`, `ST_VoronoiPolygons` |
| [Bearings](/docs/2.0.0-rc4/sql/spatial-functions#bearings) | 2 | `ST_Azimuth`, `ST_Project` |
| [Edits](/docs/2.0.0-rc4/sql/spatial-functions#edits) | 7 | `ST_Multi`, `ST_SnapToGrid`, `ST_Dump` |
| [Aggregate functions](/docs/2.0.0-rc4/sql/spatial-functions#aggregate-functions) | 3 | `ST_Extent`, `ST_Collect`, `ST_MemUnion` |
| [Window functions](/docs/2.0.0-rc4/sql/spatial-functions#window-functions) | 2 | `ST_ClusterKMeans`, `ST_ClusterDBSCAN` |
| [Reprojection](/docs/2.0.0-rc4/sql/spatial-functions#reprojection) | 1 | `ST_Transform` |

A netCDF, Zarr, CSV or Parquet table holds coordinate columns, not geometry. `ST_Point` builds a
geometry from them, so every function above reaches every format:

```sql
SELECT count(*)
FROM read_parquet(['obs/*.parquet'])
WHERE ST_Within(
    ST_Point(longitude, latitude),
    ST_GeomFromText('POLYGON ((-10 35, 40 35, 40 60, -10 60, -10 35))')
)
```

---

## Domain mapping functions

These functions map vocabulary codes between the standards of oceanographic datasets. They cover the datasets of the BlueCloud and SeaDataNet ecosystem.

**Vocabulary abbreviations used below:**

| Code | Description |
| ---- | ----------- |
| C17 | ICES vessel country codes |
| EDMO | European Directory of Marine Organisations (numeric ID) |
| L05 | SeaDataNet instrument type (broad category) |
| L06 | SeaDataNet platform type |
| L22 | SeaDataNet instrument type (specific model) |
| L33 | SeaDataNet parameter discovery vocabulary |
| P01 | SeaDataNet parameter codes |
| P35 | EMODnet Chemistry parameter codes |
| WMO | World Meteorological Organization instrument codes |

### Physical / scientific

#### `pressure_to_depth_teos_10(pressure, latitude)`

Converts a pressure in dbar to a depth in metres. It uses the TEOS-10 formula. The function needs the latitude, because the shape of the geoid changes the result.

| Argument | Type | Description |
| -------- | ---- | ----------- |
| `pressure` | `DOUBLE` | Pressure in dbar |
| `latitude` | `DOUBLE` | Latitude in decimal degrees |

Returns `DOUBLE`, the depth in metres. A positive value goes down.

```sql
SELECT pressure_to_depth_teos_10(pressure, latitude) AS depth_m
FROM argo_profiles
```

#### `map_units(unit, target_unit, value)`

Converts a numeric `value` from `unit` to `target_unit`. It uses the SeaDataNet unit registry. Each unit string must be a valid SeaDataNet unit identifier, for example `'SDN:P06::UPAA'`.

| Argument | Type | Description |
| -------- | ---- | ----------- |
| `unit` | `VARCHAR` | Source unit identifier |
| `target_unit` | `VARCHAR` | Target unit identifier |
| `value` | `DOUBLE` | Value to convert |

Returns `DOUBLE`.

```sql
SELECT map_units("temperature.units", 'SDN:P06::UPAA', temperature) AS temperature_celsius
FROM seadatanet_profiles
```

### Cross-vocabulary mapping

Every mapping function returns `NULL` if the lookup table does not hold the input code.

#### Common

| Function | Input | Returns | Description |
| -------- | ----- | ------- | ----------- |
| `map_c17(c17)` | `VARCHAR` | `VARCHAR` | C17 country code → country name |
| `map_c17_l06(c17)` | `VARCHAR` | `VARCHAR` | C17 country code → L06 platform type |
| `map_call_sign_c17(call_sign, timestamp)` | `VARCHAR`, `TIMESTAMP` | `VARCHAR` | Vessel call sign at a given time → C17 country code |
| `map_l22_l05(l22)` | `VARCHAR` | `VARCHAR` | L22 instrument → L05 broad category |
| `map_measuring_area_type_feature_type(type)` | `VARCHAR` | `VARCHAR` | Measuring area type → CDI feature type |
| `map_wmo_instrument_type_l05(wmo)` | `VARCHAR` | `VARCHAR` | WMO instrument code → L05 |
| `map_wmo_instrument_type_l22(wmo)` | `VARCHAR` | `VARCHAR` | WMO instrument code → L22 |

#### CMEMS

| Function | Input | Returns | Description |
| -------- | ----- | ------- | ----------- |
| `map_cmems_bigram_l05(bigram)` | `VARCHAR` | `VARCHAR` | CMEMS platform bigram → L05 |
| `map_cmems_bigram_l06(bigram, wmo_instrument_type)` | `VARCHAR`, `VARCHAR` | `VARCHAR` | CMEMS platform bigram + WMO instrument type → L06 |

#### CORA

| Function | Input | Returns | Description |
| -------- | ----- | ------- | ----------- |
| `map_cora_instrument_l05(instrument)` | `VARCHAR` | `VARCHAR` | CORA instrument type → L05 |
| `map_cora_instrument_l22(instrument)` | `VARCHAR` | `VARCHAR` | CORA instrument type → L22 |
| `map_cora_platform_l06(bigram, wmo_instrument_type)` | `VARCHAR`, `VARCHAR` | `VARCHAR` | CORA platform bigram + WMO instrument type → L06 |

#### EMODnet Chemistry

| Function | Input | Returns | Description |
| -------- | ----- | ------- | ----------- |
| `map_emodnet_chemistry_instrument_l05(instrument)` | `VARCHAR` | `VARCHAR` | EMODnet Chemistry instrument code → L05 |
| `map_emodnet_chemistry_instrument_l05_multi(instrument)` | `VARCHAR` | `VARCHAR` | EMODnet Chemistry instrument code → comma-separated L05 labels |
| `map_emodnet_chemistry_instrument_info_l22(line, p01)` | `VARCHAR`, `VARCHAR` | `VARCHAR` | EMODnet Chemistry instrument line + P01 code → L22 |
| `map_emodnet_chemistry_originator_edmo(originator)` | `VARCHAR` | `VARCHAR` | EMODnet Chemistry originator code → EDMO identifier |
| `map_emodnet_chemistry_p35_contributor_codes_p01(contributor_codes, p35)` | `VARCHAR`, `VARCHAR` | `VARCHAR` | Look up the P01 parameter code for `p35` within an EMODnet Chemistry P35→P01 contributor-codes string |
| `map_emodnet_chemistry_platform_l06(platform)` | `VARCHAR` | `VARCHAR` | EMODnet Chemistry platform code → L06 |

#### SeaDataNet

| Function | Input | Returns | Description |
| -------- | ----- | ------- | ----------- |
| `map_seadatanet_instrument_l05(instrument)` | `VARCHAR` | `VARCHAR` | SeaDataNet instrument code → L05 |
| `map_seadatanet_platform_l06(platform)` | `VARCHAR` | `VARCHAR` | SeaDataNet platform code → L06 |
| `map_platform_c17_l06(c17)` | `VARCHAR` | `VARCHAR` | SeaDataNet C17 country code → L06 |
| `map_seadatanet_instrument_l05_salinity(instrument)` | `VARCHAR` | `VARCHAR` | SeaDataNet instrument code → L05, disambiguated for salinity sensors |
| `map_seadatanet_instrument_l05_temperature(instrument)` | `VARCHAR` | `VARCHAR` | SeaDataNet instrument code → L05, disambiguated for temperature sensors |
| `map_originator_edmo(originator)` | `VARCHAR` | `VARCHAR` | SeaDataNet originator code → EDMO identifier |

#### Argo

| Function | Input | Returns | Description |
| -------- | ----- | ------- | ----------- |
| `map_argo_instrument_l05(sensor_model)` | `BIGINT` | `VARCHAR` | Argo sensor model code → L05 |
| `map_argo_platform_l06(platform_type)` | `BIGINT` | `VARCHAR` | Argo platform type code → L06 |
| `map_argo_platform_edmo(platform_code)` | `VARCHAR` | `VARCHAR` | Argo platform code → EDMO institution identifier |

#### World Ocean Database (WOD)

| Function | Input | Returns | Description |
| -------- | ----- | ------- | ----------- |
| `map_wod_instrument_l05(instrument)` | `VARCHAR` | `VARCHAR` | WOD instrument code → L05 |
| `map_wod_instrument_l22(instrument)` | `VARCHAR` | `VARCHAR` | WOD instrument code → L22 |
| `map_wod_instrument_l33(instrument)` | `VARCHAR` | `VARCHAR` | WOD instrument code → L33 |
| `map_wod_platform_c17(platform)` | `VARCHAR` | `VARCHAR` | WOD platform code → C17 |
| `map_wod_quality_flag(flag)` | `BIGINT` | `VARCHAR` | WOD numeric quality flag → description string |
| `map_wod_edmo(country_institute)` | `VARCHAR` | `BIGINT` | WOD country/institute code → EDMO ID |
| `map_wod_edmo_approx(country_institute)` | `VARCHAR` | `BIGINT` | WOD country/institute code → nearest EDMO ID (approximate) |
