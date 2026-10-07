# JOIN

A join combines the rows of two tables. A condition matches the rows:

```sql
SELECT p.time, p.latitude, p.longitude, p.temperature, m.platform_name
FROM ocean_profiles p
JOIN platform_metadata m ON p.platform_code = m.platform_code
WHERE p.time >= '2024-01-01'
```

## INNER JOIN

An inner join returns only the rows that match in both tables. `JOIN` and `INNER JOIN` do the same:

```sql
SELECT p.time, p.temperature, m.platform_name
FROM ocean_profiles p
INNER JOIN platform_metadata m ON p.platform_code = m.platform_code
```

## LEFT JOIN

A left join returns every row of the left table. A row without a match gets `NULL` in the right columns:

```sql
SELECT p.time, p.temperature, m.platform_name
FROM ocean_profiles p
LEFT JOIN platform_metadata m ON p.platform_code = m.platform_code
```

## Join on several columns

```sql
SELECT *
FROM observations o
JOIN qc_flags q
  ON o.platform_code = q.platform_code
 AND o.time          = q.time
 AND o.depth         = q.depth
```

## Join a table function

You can join directly against a `read_*` table function. You create no external table first:

```sql
SELECT p.time, p.temperature, m.platform_name
FROM read_netcdf(['argo/**/*.nc']) p
JOIN platform_metadata m ON p.platform_code = m.platform_code
```

## Subquery join

```sql
SELECT *
FROM ocean_profiles
WHERE platform_code IN (
    SELECT platform_code
    FROM platform_metadata
    WHERE ocean_basin = 'North Atlantic'
)
```
