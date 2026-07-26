---
description: beacondb is the Beacon database — an embeddable, DuckDB-class query engine for scientific data, with a shared SQL dialect and language bindings (Python today; more coming).
---

# beacondb

**beacondb is the Beacon database**: an embeddable, DuckDB-class query engine for scientific data.
It reads NetCDF, Zarr, Parquet/GeoParquet, CSV, HDF5, ODV, GeoTIFF, Delta and more *in place* — from
local files or S3 — and answers **SQL**, all in-process. One portable `beacon.db` file holds
everything it owns and references everything else.

The server product, [beacon-datalake](/docs/2.0.0/getting-started), runs this same engine behind an
HTTP + Flight SQL service. What follows is the database itself and how to embed it.

## SQL reference

The query language is the heart of beacondb, and it is **the same everywhere** — embedded here, or
served by beacon-datalake. Start at the [SQL reference](/docs/2.0.0/beacondb/sql/):

- Query — [SELECT](/docs/2.0.0/beacondb/sql/select), [WHERE](/docs/2.0.0/beacondb/sql/where), [GROUP BY](/docs/2.0.0/beacondb/sql/group-by), [JOIN](/docs/2.0.0/beacondb/sql/join)
- Tables & views — [CREATE TABLE](/docs/2.0.0/beacondb/sql/create-table), [managed tables](/docs/2.0.0/beacondb/sql/managed-tables), [remote tables & `ATTACH`](/docs/2.0.0/beacondb/sql/remote-tables)
- Data — [table functions](/docs/2.0.0/beacondb/sql/table-functions) (`read_netcdf`, `read_parquet`, …), [`CREATE SECRET`](/docs/2.0.0/beacondb/sql/secrets), [`SUMMARIZE`](/docs/2.0.0/beacondb/sql/summarize)

See also [Concepts](/docs/2.0.0/concepts) for how the engine, formats, catalog, and the `beacon.db`
file fit together.

## Language bindings

beacondb embeds into your language of choice. Each binding is a thin front-end over the same engine —
same SQL, same formats, same `beacon.db`.

| Binding | Status | Docs |
|---|---|---|
| **Python** (`pip install beacondb`) | ✅ Available | [Python binding →](/docs/2.0.0/beacondb/python/) |
| **Rust** | 🔜 Planned | — |
| **C ABI** (for .NET, Go, Node, Java, …) | 🔜 Planned | — |

The Python binding is the reference front-end today: a PEP 249 connection, a lazy relation API,
Arrow-native results, `ATTACH`, secrets, streaming, and file sinks.

→ **[Get started with the Python binding](/docs/2.0.0/beacondb/python/getting-started)**
