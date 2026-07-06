# beacon-datalake-cli

A terminal client for the [Beacon](../../) data lake. Run SQL against a running
`beacon-api` server, explore tables / datasets / schemas, render results as
tables in your terminal, and export to CSV, Parquet, Arrow IPC, or NetCDF — all
without leaving the shell.

It talks to the server's `/api/*` HTTP endpoints, decoding the zstd-compressed
Arrow IPC result stream with `pyarrow`. It offers both one-shot subcommands (great
for scripting and pipes) and a full-featured interactive REPL — SQL syntax
highlighting, context-aware completion, multi-line editing (emacs or vi),
on-demand formatting, and a status bar.

## Install

```bash
pip install -e clients/beacon-datalake-cli
# or, with uv:
uv pip install -e clients/beacon-datalake-cli
```

This installs the `beacon-datalake-cli` console script.

## Connecting

Connection details are passed as explicit arguments only — the CLI does **not**
read `BEACON_*` environment variables (those configure the Beacon *server*, and
inheriting them would silently connect with the server's admin credentials). The
URL defaults to a local server:

| Option | Default |
| --- | --- |
| `--url`, `-u` | `http://127.0.0.1:5001` |
| `--username` | *(none)* |
| `--password` | *(none)* |

```bash
beacon-datalake-cli --url https://beacon.example.com --username admin --password s3cret
```

Credentials are sent as HTTP Basic auth and elevate the session to super-user,
which is required for DDL/DML (e.g. `CREATE EXTERNAL TABLE`). With no credentials
the session is anonymous and read-only.

A `localhost` URL is rewritten to `127.0.0.1` automatically: on Windows `localhost`
resolves to IPv6 (`::1`) first and, when the server listens on IPv4 only, each
connection stalls ~2s before falling back — targeting `127.0.0.1` avoids that.

When it connects, the interactive shell resolves and greets you with the access
level the server reports — `super-user`, a named `user` (valid credentials but
not an admin, read-only), or `anonymous`. If the supplied credentials are
rejected, the CLI fails fast with a clear *"could not connect … as super-user"*
error rather than silently falling back to read-only.

## One-shot commands

```bash
# Run SQL and render a table
beacon-datalake-cli query "SELECT * FROM default LIMIT 10"

# From a file or stdin
beacon-datalake-cli query -f query.sql
echo "SELECT count(*) FROM default" | beacon-datalake-cli query

# Export results to a file (see "Exporting results" below for all formats/options)
beacon-datalake-cli export "SELECT * FROM default" -o out.parquet

# Explore
beacon-datalake-cli tables                 # list table names
beacon-datalake-cli tables --detail        # + kind / format / location / partitions
beacon-datalake-cli tables --schema        # + each table's columns
beacon-datalake-cli schema default         # one table's schema
beacon-datalake-cli datasets               # list datasets
beacon-datalake-cli dataset-schema path/to/file.parquet
beacon-datalake-cli functions              # scalar/aggregate functions
beacon-datalake-cli functions --table      # table functions
beacon-datalake-cli info                   # server info
beacon-datalake-cli metrics <query-id>     # metrics for a prior query
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
beacon-datalake-cli query 'SELECT * FROM wod' --max-rows 1 --expand
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
beacon-datalake-cli query 'SELECT "Temperature.coordinates" FROM wod'
```

In the REPL there's no shell in the way — type the double quotes directly. Use
`beacon-datalake-cli schema <table>` (or `\d <table>`) to see the exact column names.

### Incremental fetching

By default `query` streams the result and **stops once it has more rows than the
render limit** (`--max-rows`, default 100), so previewing a huge table is fast
and doesn't download the whole result. When it stops early the footer reads
`first N rows (more available)`. To fetch the entire result instead (e.g. for an
accurate total or full `--json` output), pass `--all` (or `--max-rows -1`):

```bash
beacon-datalake-cli query "SELECT * FROM big_table"            # fast preview, first 100 rows
beacon-datalake-cli query "SELECT * FROM big_table" --all      # fetch everything
```

With `--json`, the rows go to stdout as valid JSON and the footer goes to stderr,
so `beacon-datalake-cli query ... --json | jq` works.

### DDL, admin & crawler statements

`query` (and the REPL) send raw SQL straight through, so anything the server
accepts works — including Beacon's custom statements. Read-only statements need
no credentials; anything that mutates state requires admin basic auth via
`--username`/`--password`.

```bash
# Read-only (no credentials)
beacon-datalake-cli query "SHOW TABLES"
beacon-datalake-cli query "SHOW COLUMNS FROM default"
beacon-datalake-cli query "SHOW CRAWLERS"
beacon-datalake-cli query "SELECT * FROM information_schema.tables"

# Admin (credentials required) — DDL/DML
beacon-datalake-cli --username beacon-admin --password beacon-password \
  query "CREATE EXTERNAL TABLE obs STORED AS DELTA LOCATION 'datasets://obs/'"

# Admin — AWS Glue-style crawlers
beacon-datalake-cli -u ... query \
  "CREATE CRAWLER cr ON 'crawl_src/' WITH ('format' 'parquet', 'schedule' '15m')"
beacon-datalake-cli -u ... query "RUN CRAWLER cr"
beacon-datalake-cli -u ... query "DROP CRAWLER cr"

# Admin — materialized views / refresh
beacon-datalake-cli -u ... query "CREATE MATERIALIZED VIEW mv AS SELECT * FROM obs"
beacon-datalake-cli -u ... query "REFRESH TABLE obs"
```

Side-effecting statements print `OK`; statements that return rows
(`SHOW TABLES`, `SHOW CRAWLERS`, …) render as a table. Mutating statements
deliberately never set an output format (an `output` format would wrap the plan
in `COPY ... TO`, which cannot execute DDL).

## Interactive shell

Run `beacon-datalake-cli` with no subcommand to open the REPL. The prompt encodes the
resolved session identity — `beacon (admin - super-user)>`, `beacon (alice - user)>`,
or `beacon (anonymous)>` — so your access level stays visible on every line:

```
beacon (admin - super-user)> SELECT * FROM default LIMIT 5;
beacon> \dt                 -- list tables
beacon> \dt+                -- tables with kind / format / location
beacon> \d default          -- table schema
beacon> \df                 -- scalar/aggregate functions   (\dft for table functions)
beacon> \datasets           -- list datasets
beacon> \crawlers           -- SHOW CRAWLERS
beacon> \run-crawler cr     -- RUN CRAWLER cr      (admin)
beacon> \refresh obs        -- REFRESH TABLE obs   (admin)
beacon> \info               -- server info
beacon> \format parquet     -- set export format
beacon> \export out.parquet -- write the last statement to a file  (\o alias)
beacon> \i migrate.sql      -- run a SQL script (multiple ;-separated statements)
beacon> \limit 50           -- change render cap
beacon> \timing             -- toggle the elapsed/row footer
beacon> \x                  -- toggle expanded (field/value) display for wide tables
beacon> \vi                 -- switch to vi key bindings (\emacs to switch back)
beacon> \help               -- full command list
beacon> \q                  -- quit
```

The prompt is a **multi-line editor**: Enter adds a line, the cursor moves freely
between lines (Up/Down/Home/End), and `;` (or a blank line, or Alt+Enter) runs the
statement. Press **Ctrl+F** (or F2) to auto-format (pretty-print) the current SQL
across lines — upper-cased keywords, one clause per line. A running query shows a
spinner with an elapsed timer; **Ctrl+C** cancels it (and aborts the rest of a
`\i` script). Editing uses **emacs** key bindings by default; start with `--vi` (or
run `\vi` in the shell) for **vi** bindings. Command history is saved under your
user config directory.

A **status bar** at the bottom shows the connection host, your identity, and the
live row-limit / export format / timing / editing mode (`emacs`, or `vi:NORMAL` /
`vi:INSERT`).

In a real terminal the shell highlights SQL as you type — a dark, purple-forward
theme (keywords in purple, plus strings, numbers, and comments) — and tints the
prompt by access level using the same palette: green for `super-user`, cyan for a
named `user`, gray for `anonymous`. Colour is dropped automatically when output
isn't a terminal (pipes / CI).

Completions suggest SQL keywords, your instance's **table names** (fetched live,
so newly created tables show up), and backslash meta-commands (`\d`, `\dt`, …).
Matching is **case-insensitive** (`sel` → `SELECT`, `OC` → `ocean_profiles`). Like
an IDE, the menu pops **while you're typing a word**; at an empty position (e.g.
just after a `WHERE` or `FROM` keyword) it stays out of the way until you ask for
it with **Tab** or **Ctrl+Space**. Tab opens/inserts (Shift+Tab cycles back), arrow keys
move through the menu, Enter accepts. Ctrl+Space is terminal-dependent — some
terminals (e.g. Git Bash's mintty) send no code for it, so use **Tab** there.

Completion is **context-aware**:

- Right after `FROM`/`JOIN` it suggests **table names** (even before you type a
  prefix) and table functions (`read_netcdf`, …).
- Once a line binds a table via `FROM`/`JOIN`, that table's **columns** are
  suggested wherever you're editing — the `WHERE`/`GROUP BY` clauses *and* the
  `SELECT` list. Because a schema can have thousands of columns, only the first
  ~10 show with no prefix; typing a few letters narrows the list.

The table list and schemas are **prefetched in the background** on connect and
indexed (sorted for `bisect` lookups), so completions are served from memory in
microseconds. Cache lookups are **non-blocking** — a miss returns immediately and
refreshes in the background — so the completer runs inline (no per-keystroke
thread hand-off) and the menu appears instantly without ever waiting on the
network.

## Exporting results

Instead of rendering in the terminal, `export` asks the server to materialize the
result in a file format and streams the bytes to disk. Unlike `query`, an export
always fetches the **complete** result (the server writes the whole file).

```bash
beacon-datalake-cli export "SELECT * FROM obs WHERE temperature > 20" -o out.parquet
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
beacon-datalake-cli export "SELECT * FROM obs" -o obs.csv
beacon-datalake-cli export "SELECT * FROM obs" -o obs.parquet
beacon-datalake-cli export "SELECT * FROM obs" -o obs.nc              # NetCDF

# Explicit format (extension doesn't have to match)
beacon-datalake-cli export "SELECT * FROM obs" -o obs.bin --format ipc

# GeoParquet with geometry built from lon/lat columns
beacon-datalake-cli export "SELECT lon, lat, temperature FROM obs" \
  -o obs.geoparquet --lon lon --lat lat

# N-dimensional NetCDF over time/lat/lon dimensions
beacon-datalake-cli export "SELECT time, lat, lon, temperature FROM obs" \
  -o obs.nc --format nd_netcdf \
  --dimension-columns time --dimension-columns lat --dimension-columns lon

# Read the SQL from a file
beacon-datalake-cli export -f query.sql -o out.parquet
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
beacon_datalake_cli/
  cli.py            Typer app + global connection callback
  __main__.py       enables `python -m beacon_datalake_cli`
  config.py         ClientConfig (URL / auth / timeout from CLI arguments)
  errors.py         user-facing error types
  client.py         BeaconClient — HTTP over /api/*
  arrow_io.py       QueryResult + Arrow IPC stream decoding
  formats.py        output-format inference / spec building
  render/           rich rendering: results.py, metadata.py
  commands/         one module per command group: query.py, catalog.py
  repl/             interactive shell: reader, runner, meta, completion, toolbar, state, help
tests/              one test module per unit (client, config, formats, …)
```

## Development

```bash
pip install -e "clients/beacon-datalake-cli[dev]"
ruff check clients/beacon-datalake-cli
pytest clients/beacon-datalake-cli
```
