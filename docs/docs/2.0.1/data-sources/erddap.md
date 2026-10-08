# ERDDAP

Beacon reads tabledap datasets from an ERDDAP server.
An ERDDAP dataset is available as an external table only.
You cannot read it with a `read_*` function or write it with `COPY`.

## Create a table

Use the dataset URL as the `LOCATION`.

```sql
CREATE EXTERNAL TABLE bottles STORED AS ERDDAP
  LOCATION 'https://coastwatch.pfeg.noaa.gov/erddap/tabledap/erdGlobecBottle';
```

- The URL must hold the path segment `tabledap` and a dataset ID.
- Beacon removes a file extension and a query string from the URL.
- Beacon reads the columns from the dataset. Do not declare columns.
- Beacon stores the schema when you create the table. If the dataset gets new variables, create the table again.

## Options

| Option | Default | Description |
|---|---|---|
| `request_timeout_secs` | `600` | The timeout of each HTTP request, in seconds. |

```sql
CREATE EXTERNAL TABLE bottles STORED AS ERDDAP
  LOCATION 'https://coastwatch.pfeg.noaa.gov/erddap/tabledap/erdGlobecBottle'
  OPTIONS ('request_timeout_secs' '900');
```

## Columns

- A table has one column for each variable of the dataset.
- A variable with the units `seconds since 1970-01-01T00:00:00Z` is a timestamp column.
- Beacon reads an ERDDAP NaN value as `NULL`.

## Pushdown

Beacon sends the column list and filters to ERDDAP.
Beacon applies each filter again to the rows that ERDDAP returns.
Beacon applies `LIMIT` itself.

| SQL | ERDDAP request |
|---|---|
| `col = 1` on numbers and times | `&col=1` |
| `col <> 1` on integers | `&col!=1` |
| `col < 1`, `col <= 1`, `col > 1`, `col >= 1` on integers | `&col<1`, `&col<=1`, `&col>1`, `&col>=1` |
| `col BETWEEN 1 AND 5` on numbers and times | `&col>=1&col<=5` |
| `col = 'x'` on strings | `&col="x"` |
| `col <> 'x'` on strings | `&col!="x"` |
| `col IN ('a', 'b')` on strings | `&col=~"a\|b"` |
| `col IN (1, 5)` on numbers | `&col>=1&col<=5` |
| `col LIKE 'ab%'` | `&col=~"(?s)ab.*"` |
| `col IS NULL`, `col IS NOT NULL` on numbers and times | `&col=NaN`, `&col!=NaN` |

- On float and time columns, Beacon sends `>` as `>=` and `<` as `<=`. Beacon does not send `<>`.
- Beacon rounds time values to whole milliseconds.
  It rounds a lower bound down and an upper bound up.
- `LIKE` becomes a regular expression that also matches line breaks.
- Beacon does not send `OR`, string ranges, functions or comparisons between columns. Beacon applies these filters itself.

## Limits

- Beacon supports tabledap datasets only. A griddap dataset URL gives an error.
- Beacon supports public ERDDAP servers only. Beacon sends no credentials.
- Beacon does not retry a failed request.
- A query is one request.
- `EXPLAIN` shows the request URL.
