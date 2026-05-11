# Function Reference

Beacon inherits the full [Apache DataFusion](https://datafusion.apache.org/user-guide/sql/scalar_functions.html) function library. The most commonly used functions for scientific data queries are listed below.

## Aggregate functions

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

## Date and time functions

| Function | Description |
| -------- | ----------- |
| `NOW()` | Current timestamp |
| `DATE_TRUNC(unit, ts)` | Truncate timestamp to `'year'`, `'month'`, `'day'`, `'hour'`, `'minute'`, `'second'` |
| `DATE_PART(unit, ts)` | Extract a part (same units as above) |
| `EXTRACT(unit FROM ts)` | SQL-standard equivalent of `DATE_PART` |
| `TO_TIMESTAMP(str)` | Parse a string to a timestamp |
| `MAKE_DATE(y, m, d)` | Construct a date from year, month, day |
| `date + INTERVAL '1 day'` | Date arithmetic |

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
```

## Math functions

| Function | Description |
| -------- | ----------- |
| `ABS(x)` | Absolute value |
| `CEIL(x)` | Round up |
| `FLOOR(x)` | Round down |
| `ROUND(x, d)` | Round to `d` decimal places |
| `SQRT(x)` | Square root |
| `POWER(x, n)` | `x` to the power of `n` |
| `LN(x)` | Natural logarithm |
| `LOG(base, x)` | Logarithm with specified base |
| `EXP(x)` | `e^x` |
| `PI()` | π |

## String functions

| Function | Description |
| -------- | ----------- |
| `UPPER(s)` | Uppercase |
| `LOWER(s)` | Lowercase |
| `TRIM(s)` | Remove leading/trailing whitespace |
| `LENGTH(s)` | String length |
| `SUBSTRING(s, start, len)` | Extract substring |
| `CONCAT(s1, s2, …)` | Concatenate strings |
| `REPLACE(s, from, to)` | Replace substring |
| `STARTS_WITH(s, prefix)` | Boolean prefix test |
| `CONTAINS(s, substr)` | Boolean substring test |

## Conditional expressions

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

-- COALESCE (first non-NULL)
SELECT COALESCE(temperature_corrected, temperature) AS temp
FROM ocean_profiles

-- NULLIF (return NULL when equal)
SELECT NULLIF(quality_flag, 9) AS qc_flag
FROM ocean_profiles
```

## Casting

```sql
SELECT CAST(pressure AS DOUBLE) AS pressure_dbar
FROM ocean_profiles

-- Short form
SELECT pressure::DOUBLE AS pressure_dbar
FROM ocean_profiles
```
