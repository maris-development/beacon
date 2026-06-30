---
description: beacon-cli is a terminal client for Beacon — run SQL against a running server, explore tables/datasets/schemas, render results, and export to CSV, Parquet, Arrow IPC, or NetCDF.
---

# Beacon CLI

`beacon-cli` is a terminal client for a running Beacon server. It runs SQL,
explores tables / datasets / schemas, renders results as tables in your
terminal, and exports to CSV, Parquet, Arrow IPC, or NetCDF — all without
leaving the shell. It talks to the server's `/api/*` HTTP endpoints, decoding the
zstd-compressed Arrow IPC result stream, and offers both one-shot subcommands and
an interactive REPL.

It ships in the Beacon repository under [`clients/beacon-cli`](https://github.com/maris-development/beacon/tree/main/clients/beacon-cli).

## Install

Requires Python 3.10+. Install it (editable) from a checkout of the repo:

```bash
pip install -e clients/beacon-cli
# or, with uv:
uv pip install -e clients/beacon-cli
```

This installs the `beacon-cli` console script.

## Connecting

Defaults match the Beacon server (`http://localhost:5001`). Override per
invocation or via environment variables:

| Option | Env var | Default |
| --- | --- | --- |
| `--url` | `BEACON_URL` | `http://localhost:5001` |
| `--username` | `BEACON_ADMIN_USERNAME` | _(none)_ |
| `--password` | `BEACON_ADMIN_PASSWORD` | _(none)_ |

Credentials are sent as HTTP Basic auth and elevate the session to super-user,
which is required for DDL/DML (e.g. `CREATE EXTERNAL TABLE`). Read-only queries
need no credentials.

## One-shot commands

```bash
# Run SQL and render a table
beacon-cli query "SELECT * FROM default LIMIT 10"

# From a file or stdin
beacon-cli query -f query.sql
echo "SELECT count(*) FROM default" | beacon-cli query

# Export results to a file (format inferred from the extension)
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

Useful `query` flags: `--max-rows N` (render limit, default 100; `-1` = all),
`--all` (fetch the entire result), `--expand`/`-x` (render rows vertically for
wide tables), and `--json` (emit rows as JSON on stdout).

### DDL, admin & crawler statements

`query` (and the REPL) send raw SQL straight through, so any statement the server
accepts works — including Beacon's custom DDL. Read-only statements need no
credentials; anything that mutates state requires admin basic auth.

```bash
# Read-only (no credentials)
beacon-cli query "SHOW TABLES"
beacon-cli query "SHOW CRAWLERS"

# Admin — DDL/DML, crawlers, materialized views
beacon-cli -u beacon-admin -p beacon-password \
  query "CREATE EXTERNAL TABLE obs STORED AS DELTA LOCATION 'datasets://obs/'"
beacon-cli -u beacon-admin -p beacon-password \
  query "CREATE CRAWLER cr ON 'crawl_src/' WITH ('format' 'parquet', 'schedule' '15m')"
```

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
beacon> \x                  -- toggle expanded (vertical) rendering
```

See the [`clients/beacon-cli` README](https://github.com/maris-development/beacon/tree/main/clients/beacon-cli)
for the full command list and export options.
