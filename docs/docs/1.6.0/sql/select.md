# SELECT

## Basic select

Choose specific columns or use `*` for all:

```sql
SELECT time, latitude, longitude, temperature
FROM ocean_profiles

SELECT * FROM ocean_profiles LIMIT 100
```

## Expressions and aliases

```sql
SELECT
    time,
    latitude,
    longitude,
    temperature - 273.15 AS temperature_celsius,
    ROUND(salinity, 2)   AS salinity_psu
FROM ocean_profiles
```

## ORDER BY

```sql
SELECT time, latitude, longitude, temperature
FROM ocean_profiles
ORDER BY time DESC, latitude ASC
```

## LIMIT and OFFSET

```sql
SELECT * FROM ocean_profiles LIMIT 100

SELECT * FROM ocean_profiles LIMIT 100 OFFSET 200
```

## DISTINCT

```sql
SELECT DISTINCT platform_code FROM ocean_profiles
```
