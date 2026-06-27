# GROUP BY

Aggregate rows into groups:

```sql
SELECT
    DATE_TRUNC('month', time) AS month,
    AVG(temperature)          AS avg_temp,
    MIN(temperature)          AS min_temp,
    MAX(temperature)          AS max_temp,
    COUNT(*)                  AS observations
FROM ocean_profiles
WHERE depth < 10
GROUP BY DATE_TRUNC('month', time)
ORDER BY month
```

## Grouping by multiple columns

```sql
SELECT
    DATE_TRUNC('year', time) AS year,
    FLOOR(latitude / 10) * 10 AS lat_band,
    AVG(temperature) AS avg_temp
FROM ocean_profiles
GROUP BY year, lat_band
ORDER BY year, lat_band
```

## HAVING

Filter groups after aggregation (unlike `WHERE`, which filters rows before grouping):

```sql
SELECT
    FLOOR(latitude / 5) * 5 AS lat_bin,
    AVG(temperature)        AS avg_temp,
    COUNT(*)                AS observations
FROM ocean_profiles
GROUP BY lat_bin
HAVING COUNT(*) > 100
ORDER BY lat_bin
```

## Common aggregate functions

| Function | Description |
| -------- | ----------- |
| `COUNT(*)` | Total rows in the group |
| `COUNT(col)` | Non-NULL values in the group |
| `SUM(col)` | Sum |
| `AVG(col)` | Mean |
| `MIN(col)` | Minimum |
| `MAX(col)` | Maximum |
| `STDDEV(col)` | Standard deviation |
| `MEDIAN(col)` | Median |

See [Function Reference](./function-reference.md#aggregate-functions) for the full list.
