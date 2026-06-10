# WHERE

Filter rows by adding a `WHERE` clause after `FROM`:

```sql
SELECT *
FROM ocean_profiles
WHERE latitude  BETWEEN 50 AND 60
  AND longitude BETWEEN -10 AND 10
  AND time >= '2024-01-01'
  AND temperature IS NOT NULL
```

## Comparison operators

`=`, `!=`, `<`, `>`, `<=`, `>=` work on numbers, strings, and timestamps:

```sql
WHERE quality_flag = 1
WHERE depth <= 200
WHERE time > '2024-01-01'
```

## BETWEEN

Inclusive range check:

```sql
WHERE depth BETWEEN 0 AND 200
WHERE time BETWEEN '2024-01-01' AND '2024-12-31'
WHERE latitude BETWEEN -60 AND 60
```

## IN

Match against a list of values:

```sql
WHERE platform_code IN ('6900001', '6900002', '6900003')
WHERE quality_flag IN (1, 2)
```

## LIKE

Pattern matching — `%` matches any sequence of characters, `_` matches one character:

```sql
WHERE cruise_id LIKE '2024%'
WHERE station_id LIKE 'ARGO___'
```

## IS NULL / IS NOT NULL

```sql
WHERE temperature IS NOT NULL
WHERE doxy IS NULL
```

## AND / OR / NOT

Combine conditions with logical operators:

```sql
WHERE temperature > 0 AND salinity > 30

WHERE temperature < 0 OR depth > 500

WHERE NOT (quality_flag = 9)

WHERE depth BETWEEN 0 AND 200
  AND (temperature IS NOT NULL OR salinity IS NOT NULL)
```

## Date and time filtering

```sql
WHERE time >= '2024-01-01'

WHERE time BETWEEN '2024-01-01' AND '2024-12-31'

WHERE DATE_TRUNC('month', time) = '2024-06-01'
```
