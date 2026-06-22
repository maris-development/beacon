# beacon-cli

A terminal client for the [Beacon](../../) data lake. Run SQL against a running
`beacon-api` server, explore tables / datasets / schemas, render results as
tables in your terminal, and export to CSV, Parquet, Arrow IPC, or NetCDF — all
without leaving the shell.

It talks to the server's `/api/*` HTTP endpoints, decoding the zstd-compressed
Arrow IPC result stream with `pyarrow`, and offers both one-shot subcommands and
an interactive REPL.

## Install

```bash
pip install -e clients/beacon-cli
# or, with uv:
uv pip install -e clients/beacon-cli
```

This installs the `beacon-cli` console script.

## Connecting

Defaults match the Beacon server (`http://localhost:5001`). Override per-invocation
or via environment variables:

| Option | Env var | Default |
| --- | --- | --- |
| `--url` | `BEACON_URL` | `http://localhost:5001` |
| `--username` | `BEACON_ADMIN_USERNAME` | _(none)_ |
| `--password` | `BEACON_ADMIN_PASSWORD` | _(none)_ |

Credentials are sent as HTTP Basic auth and elevate the session to super-user,
which is required for DDL/DML (e.g. `CREATE EXTERNAL TABLE`).

## One-shot commands

```bash
# Run SQL and render a table
beacon-cli query "SELECT * FROM default LIMIT 10"

# From a file or stdin
beacon-cli query -f query.sql
echo "SELECT count(*) FROM default" | beacon-cli query

# Export results to a file (see "Exporting results" below for all formats/options)
beacon-cli export "SELECT * FROM default" -o out.parquet

# Explore
beacon-cli tables                 # list table names
beacon-cli tables --detail        # + kind / format / location / partitions
beacon-cli tables --schema        # + each table's columns
beacon-cli schema default         # one table's schema
beacon-cli datasets               # list datasets
beacon-cli dataset-schema path/to/file.parquet
beacon-cli functions              # scalar/aggregate functions
beacon-cli functions --table      # table functions
beacon-cli info                   # server info
beacon-cli metrics <query-id>     # metrics for a prior query
```

### `query` options

| Flag | Description |
| --- | --- |
| `SQL` (positional) | The statement to run. Omit to read from `--file` or stdin. |
| `-f`, `--file PATH` | Read the SQL from a file instead of the argument. |
| `--max-rows N` | Max rows to render (default 100; `-1` = all). |
| `--all`, `-a` | Fetch the entire result instead of stopping at the render limit. |
| `--expand`, `-x` | Render rows vertically (field/value) — good for wide tables. |
| `--json` | Emit rows as JSON on stdout (footer goes to stderr). |

### Wide tables

For tables with many columns the grid layout squeezes each column too narrow.
Render rows **vertically** instead with `--expand`/`-x` (like psql's `\x`): each
row becomes a `field | value` block, so the column count no longer competes for
width.

```bash
beacon-cli query 'SELECT * FROM wod' --max-rows 1 --expand
```
```
record 1
  Temperature             | 25.62
  Temperature.coordinates | time lat lon z
  Salinity                | 36.79
  ...
```

In the REPL, toggle it with `\x`. Other ways to tame wide results: select fewer
columns in the SQL, use `--json` (one object per row), or lower `--max-rows`.

### Column names with dots or mixed case

The CLI sends SQL through unchanged, so column quoting is standard
Beacon/DataFusion SQL. An unquoted dot means `relation.column`, and unquoted
identifiers are lower-cased — so to select a column whose real name contains a
dot (e.g. `Temperature.coordinates`) or uses mixed case, wrap it in **double
quotes**:

```sql
SELECT "Temperature.coordinates", "Temperature" FROM wod
```

On the command line, put the whole statement in **single** quotes so the double
quotes survive the shell (works in PowerShell and bash):

```bash
beacon-cli query 'SELECT "Temperature.coordinates" FROM wod'
```

In the REPL there's no shell in the way — type the double quotes directly. Use
`beacon-cli schema <table>` (or `\d <table>`) to see the exact column names.

### Incremental fetching

By default `query` streams the result and **stops once it has more rows than the
render limit** (`--max-rows`, default 100), so previewing a huge table is fast
and doesn't download the whole result. When it stops early the footer reads
`first N rows (more available)`. To fetch the entire result instead (e.g. for an
accurate total or full `--json` output), pass `--all` (or `--max-rows -1`):

```bash
beacon-cli query "SELECT * FROM big_table"            # fast preview, first 100 rows
beacon-cli query "SELECT * FROM big_table" --all      # fetch everything
```

With `--json`, the rows go to stdout as valid JSON and the footer goes to stderr,
so `beacon-cli query ... --json | jq` works.

### DDL, admin & crawler statements

`query` (and the REPL) send raw SQL straight through, so anything the server
accepts works — including Beacon's custom statements. Read-only statements need
no credentials; anything that mutates state requires admin basic auth
(`--username`/`--password` or the `BEACON_ADMIN_*` env vars).

```bash
# Read-only (no credentials)
beacon-cli query "SHOW TABLES"
beacon-cli query "SHOW COLUMNS FROM default"
beacon-cli query "SHOW CRAWLERS"
beacon-cli query "SELECT * FROM information_schema.tables"

# Admin (credentials required) — DDL/DML
beacon-cli --username beacon-admin --password beacon-password \
  query "CREATE EXTERNAL TABLE obs STORED AS DELTA LOCATION 'datasets://obs/'"

# Admin — AWS Glue-style crawlers
beacon-cli -u ... query \
  "CREATE CRAWLER cr ON 'crawl_src/' WITH ('format' 'parquet', 'schedule' '15m')"
beacon-cli -u ... query "RUN CRAWLER cr"
beacon-cli -u ... query "DROP CRAWLER cr"

# Admin — materialized views / refresh
beacon-cli -u ... query "CREATE MATERIALIZED VIEW mv AS SELECT * FROM obs"
beacon-cli -u ... query "REFRESH TABLE obs"
```

Side-effecting statements print `OK`; statements that return rows
(`SHOW TABLES`, `SHOW CRAWLERS`, …) render as a table. Mutating statements
deliberately never set an output format (an `output` format would wrap the plan
in `COPY ... TO`, which cannot execute DDL).

## Interactive shell

Run `beacon-cli` with no subcommand to open the REPL:

```
beacon> SELECT * FROM default LIMIT 5;
beacon> \dt                 -- list tables
beacon> \dt+                -- tables with kind / format / location
beacon> \d default          -- table schema
beacon> \datasets           -- list datasets
beacon> \crawlers           -- SHOW CRAWLERS
beacon> \run-crawler cr     -- RUN CRAWLER cr      (admin)
beacon> \refresh obs        -- REFRESH TABLE obs   (admin)
beacon> \format parquet     -- set export format
beacon> \export out.parquet -- write the last statement to a file
beacon> \limit 50           -- change render cap
beacon> \x                  -- toggle expanded (field/value) display for wide tables
beacon> \help               -- full command list
beacon> \q                  -- quit
```

SQL statements may span multiple lines; finish with `;` or a blank line.
Command history is saved under your user config directory.

## Exporting results

Instead of rendering in the terminal, `export` asks the server to materialize the
result in a file format and streams the bytes to disk. Unlike `query`, an export
always fetches the **complete** result (the server writes the whole file).

```bash
beacon-cli export "SELECT * FROM obs WHERE temperature > 20" -o out.parquet
```

### `export` options

| Flag | Description |
| --- | --- |
| `SQL` (positional) | The `SELECT` to export. Omit to read from `--file` or stdin. |
| `-o`, `--output PATH` | **Required.** Destination file. |
| `--format FMT` | Output format. If omitted, it's inferred from the `-o` extension. |
| `-f`, `--file PATH` | Read the SQL from a file instead of the argument. |
| `--dimension-columns C` | Dimension column(s) for `nd_netcdf` (repeat per column). |
| `--lon C` / `--lat C` | Longitude/latitude columns for `geoparquet`. |

### Formats

The format is inferred from the output extension, or set explicitly with
`--format`:

| `--format` | Extension | Notes |
| --- | --- | --- |
| `csv` | `.csv` | |
| `parquet` | `.parquet` | |
| `ipc` (alias `arrow`) | `.arrow`, `.ipc` | Arrow IPC file |
| `netcdf` (alias `nc`) | `.nc` | |
| `nd_netcdf` | — | N-dimensional NetCDF; requires `--dimension-columns` |
| `geoparquet` | `.geoparquet` | optional `--lon`/`--lat` |
| `odv` | `.odv` | Ocean Data View |

### Examples

```bash
# Format inferred from the extension
beacon-cli export "SELECT * FROM obs" -o obs.csv
beacon-cli export "SELECT * FROM obs" -o obs.parquet
beacon-cli export "SELECT * FROM obs" -o obs.nc              # NetCDF

# Explicit format (extension doesn't have to match)
beacon-cli export "SELECT * FROM obs" -o obs.bin --format ipc

# GeoParquet with geometry built from lon/lat columns
beacon-cli export "SELECT lon, lat, temperature FROM obs" \
  -o obs.geoparquet --lon lon --lat lat

# N-dimensional NetCDF over time/lat/lon dimensions
beacon-cli export "SELECT time, lat, lon, temperature FROM obs" \
  -o obs.nc --format nd_netcdf \
  --dimension-columns time --dimension-columns lat --dimension-columns lon

# Read the SQL from a file
beacon-cli export -f query.sql -o out.parquet
```

In the REPL, `\format <fmt>` sets a session format and `\export <file>` (or
`\o <file>`) writes the last statement out:

```
beacon> SELECT * FROM obs WHERE temperature > 20;
beacon> \export hot.parquet      -- format from extension
beacon> \format csv              -- override for subsequent exports
beacon> \export hot.csv
```

## Project layout

```
beacon_cli/
  cli.py            Typer app + global connection callback
  __main__.py       enables `python -m beacon_cli`
  config.py         ClientConfig (URL / auth / timeout, env resolution)
  errors.py         user-facing error types
  client.py         BeaconClient — HTTP over /api/*
  arrow_io.py       QueryResult + Arrow IPC stream decoding
  formats.py        output-format inference / spec building
  render/           rich rendering: results.py, metadata.py
  commands/         one module per command group: query.py, catalog.py
  repl/             interactive shell: reader, runner, meta, state, help
tests/              one test module per unit (client, config, formats, …)
```

## Development

```bash
pip install -e "clients/beacon-cli[dev]"
ruff check clients/beacon-cli
pytest clients/beacon-cli
```
