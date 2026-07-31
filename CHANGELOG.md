# Changelog

Notable changes to Beacon. Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Every artifact in this repository — the `beacon-datalake` server image, the `beacondb` Python
package, `beacon-datalake-cli`, and `@beacon/client`, shares one version and is released from a
single `v*` tag. Releases before 2.0.0 are recorded in the
[GitHub releases](https://github.com/maris-development/beacon/releases).

## [2.0.0-rc.1] — 2026-07-31

The 2.0 line turns Beacon from a server into an engine you can also embed. The same SQL, readers
and catalog now run in three places: in-process from Python, behind the HTTP/Flight SQL server, and
over MCP.

### Added

- **BeaconDB — the engine as an embeddable Python package.** `pip install beacondb`, `import
  beacondb`, and the whole engine runs in-process; no server, no HTTP. Results cross into Python
  over the Arrow PyCapsule protocol, so pyarrow/pandas/polars are only needed by the methods that
  return their types. Ships `py.typed` stubs and a `beacondb://` SQLAlchemy dialect.
- **Single-file databases.** A new redb-backed object store lets one `beacon.db` hold everything
  Beacon owns — its catalog and managed data — while still referencing everything else in place.
  Copy the file and the managed lake travels with it.
- **N-dimensional execution.** Zarr and netCDF now flow through the engine as nd-encoded batches
  rather than being flattened on read, with projection and filter pushdown evaluated *before* the
  broadcast so element-wise work and `WHERE` predicates run on the compact representation.
- **`beacon-mcp`** — an MCP server exposing tables to LLM clients over streamable HTTP, including
  an `export_query` tool with guard rails for large results.
- **`@beacon/client`** — a TypeScript client for the data lake's HTTP API.
- **Object-store secrets.** `CREATE SECRET` for S3/GCS/Azure/HTTP credentials, scoped by URL
  prefix, session-only by default. `CREATE PERSISTENT SECRET` encrypts them into the database file
  (XChaCha20-Poly1305) so a copied file carries its own cloud access; it requires a master key,
  because Beacon refuses to write a plaintext credential to disk.
- **Remote catalogs.** `ATTACH` another Beacon over Arrow Flight SQL and query it as
  `name.schema.table`, joining remote tables against local files. The federation optimizer pushes
  the largest federatable sub-plan to the remote, so the heavy scan stays there.
- **Source distributions** for `beacondb`, so `pip install` works on platforms with no wheel.

### Changed

- **`beacon-cli` is now `beacon-datalake-cli`** on PyPI, with the module renamed to
  `beacon_datalake_cli`. The old package name is not updated; install the new one.
- **Minimum supported Rust is 1.91**, declared as `rust-version` in the workspace and enforced by
  Cargo rather than only by a toolchain file.
- **Releases verify rather than rewrite the version.** CI no longer stamps a version into the
  manifests from the tag; the committed manifests are authoritative and a mismatched tag fails the
  release. Bump with `scripts/bump-version.py`, commit, then tag.

### Known issues

- `beacondb` wheels are large — each embeds a full DataFusion/Lance/netCDF engine — and there is no
  minimal, feature-gated build yet.
- A source build links netCDF and HDF5 dynamically, unlike the wheels, so it needs those libraries
  present. Pass `MATURIN_PEP517_ARGS="--features static-netcdf"` for the self-contained variant.
- One handle per database file: the container is held under an exclusive lock, so
  `read_only=True` is a per-connection guarantee, not multi-process concurrency.

[2.0.0-rc.1]: https://github.com/maris-development/beacon/compare/v1.8.0...v2.0.0-rc.1
