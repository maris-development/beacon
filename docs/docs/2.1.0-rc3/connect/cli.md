---
description: beacon-datalake-cli is a terminal client for Beacon. Run SQL, explore tables and schemas, and export to CSV, Parquet, Arrow IPC or NetCDF.
---

# Beacon Datalake CLI

`beacon-datalake-cli` is a terminal client for a Beacon server. It runs SQL. It explores tables,
datasets and schemas. It shows the results as a table in your terminal. It exports to CSV, Parquet,
Arrow IPC and NetCDF. You stay in the shell. The client uses the `/api/*` HTTP endpoints of the
server. It decodes the zstd-compressed Arrow IPC result stream. It gives one-shot subcommands and an
interactive shell.

The client lives in the Beacon repository, under
[`beacon-clients/beacon-datalake-cli`](https://github.com/maris-development/beacon/tree/main/beacon-clients/beacon-datalake-cli).

## Install

The client needs Python 3.10 or later. Install it in editable mode from a checkout of the
repository:

```bash
pip install -e beacon-clients/beacon-datalake-cli
# or, with uv:
uv pip install -e beacon-clients/beacon-datalake-cli
```

This gives you the `beacon-datalake-cli` console script.

## Connect

The defaults match the Beacon server at `http://localhost:5001`. Change them for one call, or with
an environment variable:

| Option | Env var | Default |
| --- | --- | --- |
| `--url` | `BEACON_URL` | `http://localhost:5001` |
| `--username` | `BEACON_ADMIN_USERNAME` | _(none)_ |
| `--password` | `BEACON_ADMIN_PASSWORD` | _(none)_ |

The client sends the credentials as HTTP Basic auth. The session then runs as super-user. DDL and
DML need this, for example `CREATE EXTERNAL TABLE`. A read-only query needs no credentials.

## One-shot commands

```bash
# Run SQL and render a table
beacon-datalake-cli query "SELECT * FROM default LIMIT 10"

# From a file or stdin
beacon-datalake-cli query -f query.sql
echo "SELECT count(*) FROM default" | beacon-datalake-cli query

# Export results to a file (format inferred from the extension)
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

The `query` command takes four useful flags. `--max-rows N` limits the rows on screen. The default
is 100. Use `-1` for every row. `--all` fetches the whole result. `--expand` or `-x` shows the rows
vertically, for a wide table. `--json` writes the rows as JSON to stdout.

### DDL, admin and crawler statements

`query` and the interactive shell send the raw SQL directly. Every statement of the server therefore
works, also the custom DDL of Beacon. A read-only statement needs no credentials. A statement that
changes state needs admin basic auth.

```bash
# Read-only (no credentials)
beacon-datalake-cli query "SHOW TABLES"
beacon-datalake-cli query "SHOW CRAWLERS"

# Admin: DDL, DML, crawlers, materialized views
beacon-datalake-cli --username beacon-admin --password beacon-password \
  query "CREATE EXTERNAL TABLE obs STORED AS DELTA LOCATION 'datasets://obs/'"
beacon-datalake-cli --username beacon-admin --password beacon-password \
  query "CREATE CRAWLER cr ON 'crawl_src/' WITH ('format' 'parquet', 'schedule' '15m')"
```

## Interactive shell

Run `beacon-datalake-cli` without a subcommand. It opens the interactive shell:

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
beacon> \x                  -- toggle expanded (vertical) rendering
```

The [`beacon-clients/beacon-datalake-cli` README](https://github.com/maris-development/beacon/tree/main/beacon-clients/beacon-datalake-cli)
gives the full command list and the export options.
