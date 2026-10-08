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

## Attributes and comments

Beacon keeps the ERDDAP attributes in the [table schema](/docs/2.0.1/sql/comment-on#read-the-comments).

- The global attributes are the metadata of the table, for example `title`, `summary` and `license`.
- The attributes of a variable are the metadata of its column, for example `units`, `standard_name` and `actual_range`.
- The `title` attribute is the comment of the table.
- The `long_name` and `units` attributes give the comment of a column, for example `Sea Water Temperature (degree_C)`. A time column shows no units.
- An ERDDAP attribute with the name `comment` has the key `erddap_comment`.
- A [`COMMENT ON`](/docs/2.0.1/sql/comment-on) statement replaces the comment from ERDDAP. The attributes do not change.
- `beacon.system.comments` shows only the comments from `COMMENT ON`. It does not show the comments from ERDDAP.

## Pushdown

Beacon sends the column list and filters to ERDDAP.
Beacon applies each filter again to the rows that ERDDAP returns.
Beacon does not send `LIMIT` to ERDDAP. Beacon applies `LIMIT` itself.

| SQL | ERDDAP request |
|---|---|
| `col = 1` on numbers | `&col=1` |
| `col = TIMESTAMP '2020-01-01 00:00:00'` on times | `&col=2020-01-01T00:00:00Z` |
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
- On 64-bit integer columns, Beacon sends `>` as `>=` and `<` as `<=`. Beacon does not send `<>`.
  ERDDAP can compare these values as doubles.
- On integer columns, Beacon rounds a number that is not a whole number.
  It rounds a lower bound down and an upper bound up.
  Beacon does not send `=` or `<>` with this number.
  For example, `depth > 19.5` becomes `&depth>=19`.
- Beacon sends a time as an ISO 8601 UTC value with whole milliseconds.
  It rounds a lower bound down and an upper bound up.
  An `=` with a time that is not a whole millisecond becomes a `>=` and `<=` pair.
- Beacon does not send an `IN` list with more than 100 values or more than 2000 characters.
- `LIKE` becomes a regular expression that also matches line breaks.
- Beacon does not send `OR`, string ranges, functions or comparisons between columns. Beacon applies these filters itself.

## Response size

- Beacon downloads the full ERDDAP response before it reads the first row.
- `LIMIT` does not make the request smaller.
- A query without filters reads every row of the selected columns.
- Use filters to make large datasets smaller.

## Speed up repeated queries

Each query on an ERDDAP table sends a new request to the server.
For repeated queries, keep a local Parquet copy in a [materialized view](/docs/2.0.1/sql/create-materialized-view).

```sql
CREATE MATERIALIZED VIEW bottles_local AS
  SELECT cruise_id, ship, time, temperature0
  FROM bottles
  WHERE time >= TIMESTAMP '2002-01-01T00:00:00Z';

REFRESH bottles_local;
```

- Beacon sends the filter of the view to ERDDAP. Use a filter to copy only the rows that you need.
- A query on the view reads the local Parquet file. It sends no request to ERDDAP.
- On the view, `LIMIT` and filters skip data in the local file.
- `REFRESH` reads the dataset from ERDDAP again and replaces the copy.

## Limits

- Beacon supports tabledap datasets only. A griddap dataset URL gives an error.
- Beacon supports public ERDDAP servers only. Beacon sends no credentials.
- A `LOCATION` with a user name or a password gives an error.
- Beacon does not retry a failed request.
- A query is one request.
- `EXPLAIN` shows the request URL.
