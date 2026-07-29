# JOIN

Combine rows from two tables by matching a condition:

```sql
SELECT p.time, p.latitude, p.longitude, p.temperature, m.platform_name
FROM ocean_profiles p
JOIN platform_metadata m ON p.platform_code = m.platform_code
WHERE p.time >= '2024-01-01'
```

## INNER JOIN

Returns only rows where the join condition matches in both tables. `JOIN` and `INNER JOIN` are equivalent:

```sql
SELECT p.time, p.temperature, m.platform_name
FROM ocean_profiles p
INNER JOIN platform_metadata m ON p.platform_code = m.platform_code
```

## LEFT JOIN

Returns all rows from the left table. Rows with no match in the right table get `NULL` for the right-side columns:

```sql
SELECT p.time, p.temperature, m.platform_name
FROM ocean_profiles p
LEFT JOIN platform_metadata m ON p.platform_code = m.platform_code
```

## Joining on multiple columns

```sql
SELECT *
FROM observations o
JOIN qc_flags q
  ON o.platform_code = q.platform_code
 AND o.time          = q.time
 AND o.depth         = q.depth
```

## Joining a table function

You can join directly against a `read_*` table function without creating an external table first:

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
