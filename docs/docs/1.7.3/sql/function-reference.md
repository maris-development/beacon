# Function Reference

Beacon's SQL runtime exposes two families of functions:

1. **DataFusion built-ins** — Beacon inherits Apache DataFusion's full scalar and
   aggregate function library. The tables below are a curated selection; for the
   exhaustive list see DataFusion's
   [scalar](https://datafusion.apache.org/user-guide/sql/scalar_functions.html)
   and [aggregate](https://datafusion.apache.org/user-guide/sql/aggregate_functions.html)
   references.
2. **[Beacon-specific functions](#beacon-specific-functions)** — added by Beacon
   for geospatial filtering, type conversion, and domain-specific vocabulary
   mapping. That section is exhaustive.

## DataFusion built-in functions (inherited)

A curated selection of the inherited DataFusion library — anything in
DataFusion's reference works even if it is not listed here.

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

Common flags: `i` (case-insensitive), `g` (global / all occurrences).

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

Like `TRY_CAST`, but uses Arrow type names instead of SQL type names. Returns NULL if the cast fails rather than raising an error.

```sql
-- Cast using an Arrow type name
SELECT try_arrow_cast(raw_value, 'Float32') AS val
FROM sensor_data

-- Cast to a timestamp with timezone
SELECT try_arrow_cast(epoch_str, 'Timestamp(Microsecond, Some("UTC"))') AS ts
FROM events
```

### `cast_int8_as_char(n)`

Interprets an `Int8` value as an ASCII code and returns the corresponding single-character string. Useful for NetCDF3 `char` variables stored as `Int8`.

```sql
SELECT cast_int8_as_char(platform_type_code) AS platform_type
FROM argo_profiles
```

---

## Beacon-specific functions

### `beacon_version()`

Returns the current Beacon server version as a string.

```sql
SELECT beacon_version()
```

### `coalesce_label(col1, 'label1', col2, 'label2', …)`

Returns the label corresponding to the first non-NULL column from a list of `(column, label)` pairs. All labels must be non-NULL string literals.

```sql
SELECT coalesce_label(
    temperature_corrected, 'corrected',
    temperature,           'raw'
) AS temp_source
FROM ocean_profiles
```

---

## Geospatial functions

### `st_within_point(wkt, lon, lat)`

Returns `true` if the point `(lon, lat)` lies within the geometry described by the WKT string. Supports any WKT geometry type (polygon, multipolygon, etc.). When the geometry is a scalar constant, Beacon uses a bounding-rectangle pre-filter and an LRU cache to accelerate repeated evaluations.

| Argument | Type | Description |
| -------- | ---- | ----------- |
| `wkt` | `VARCHAR` | Well-Known Text geometry |
| `lon` | `DOUBLE` | Longitude |
| `lat` | `DOUBLE` | Latitude |

Returns `BOOLEAN`. Returns `false` if any argument is NULL.

```sql
SELECT *
FROM read_netcdf(['argo/**/*.nc'])
WHERE st_within_point(
    'POLYGON ((−10 35, 40 35, 40 60, −10 60, −10 35))',
    longitude,
    latitude
)
```

### `st_geojson_as_wkt(geojson)`

Converts a GeoJSON geometry string to WKT. Use this to pass GeoJSON bounding polygons to `st_within_point`.

| Argument | Type | Description |
| -------- | ---- | ----------- |
| `geojson` | `VARCHAR` | GeoJSON geometry object |

Returns `VARCHAR`.

```sql
SELECT st_geojson_as_wkt('{"type":"Polygon","coordinates":[[[0,50],[10,50],[10,60],[0,60],[0,50]]]}')
```

---

## Domain mapping functions

These functions map vocabulary codes between standards used in oceanographic datasets. They are provided for datasets ingested through the BlueCloud / SeaDataNet ecosystem.

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

Converts pressure in dbar to depth in metres using the TEOS-10 formula. Latitude is required because the geoid shape affects the conversion.

| Argument | Type | Description |
| -------- | ---- | ----------- |
| `pressure` | `DOUBLE` | Pressure in dbar |
| `latitude` | `DOUBLE` | Latitude in decimal degrees |

Returns `DOUBLE` (depth in metres, positive downward).

```sql
SELECT pressure_to_depth_teos_10(pressure, latitude) AS depth_m
FROM argo_profiles
```

#### `map_units(unit, target_unit, value)`

Converts a numeric `value` from `unit` to `target_unit` using the SeaDataNet unit registry. Unit strings must be valid SeaDataNet unit identifiers (e.g. `'SDN:P06::UPAA'`).

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

All mapping functions return `NULL` when the input code is not found in the lookup table.

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
| `map_emodnet_chemistry_p35_contributor_codes_p01(codes)` | `VARCHAR` | `VARCHAR` | EMODnet Chemistry P35 contributor codes → P01 parameter codes |
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
