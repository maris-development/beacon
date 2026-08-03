# GROUP BY

`GROUP BY` aggregates the rows into groups:

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

## Group by several columns

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

`HAVING` filters the groups after the aggregation. `WHERE` filters the rows before it:

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
| `COUNT(*)` | The number of rows in the group |
| `COUNT(col)` | The number of non-null values in the group |
| `SUM(col)` | The sum |
| `AVG(col)` | The mean |
| `MIN(col)` | The minimum |
| `MAX(col)` | The maximum |
| `STDDEV(col)` | The standard deviation |
| `MEDIAN(col)` | The median |

See [Function Reference](/docs/2.0.0-rc2/beacondb/sql/function-reference#aggregate-functions) for the full list.
