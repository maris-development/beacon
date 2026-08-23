# Licensing

| Path | Licence | Licence file |
|---|---|---|
| Everything, unless listed below | AGPL-3.0 | [`LICENSE`](LICENSE) |
| `beacon-db/beacon-db-py/**` | AGPL-3.0 | [`beacon-db/beacon-db-py/LICENSE`](beacon-db/beacon-db-py/LICENSE) |
| `beacon-clients/**` | Apache-2.0 | [`beacon-clients/LICENSE`](beacon-clients/LICENSE) |

## The Rust workspace

The root [`LICENSE`](LICENSE) covers it. There is no `license` field in
`[workspace.package]`, and the `beacon-db` crates declare none of their own, so
the root file is the single statement for all of them.

The three `beacon-server` crates restate it as `license = "AGPL-3.0-only"` in
their manifests. They are the crates that could plausibly be published, and a
published crate needs the field.

`beacon-db/beacon-db-py` restates it as well, for the same reason: it builds the
`beacondb` wheel, which is published. It keeps `publish = false`, because the
crate itself does not go to crates.io — only the wheel it builds goes to PyPI.
See [The wheel](#the-wheel) below.

## The clients

`beacon-clients/**` is Apache-2.0, declared in each manifest:

- `beacon-clients/beacon-datalake-cli/pyproject.toml`
- `beacon-clients/beacon-ts/package.json`
- `beacon-clients/beacon-web/package.json`

These speak HTTP and Arrow Flight SQL to a server. They link no engine code, so a
permissive licence costs nothing and removes a blocker for institutional users.

## The direction rule

`beacon-server` may depend on `beacon-db`. `beacon-db` must never depend on
`beacon-server`. That is a layering rule, not a licence one now that both sides
are AGPL, but it still holds: the engine is the lower layer.

`scripts/check-layer-boundary.sh` enforces it, and CI runs it on every push.

## The wheel

The `beacondb` wheel is AGPL-3.0. It holds the whole engine, and `beacon-core`
links the BBF submodule (`beacon-db/beacon-file-formats/beacon-binary-format`),
which is AGPL-3.0. No other licence is available to it.

Three places state this, and they must agree:

- `license = "AGPL-3.0"` in `beacon-db/beacon-db-py/Cargo.toml`
- `license` and the licence classifier in `beacon-db/beacon-db-py/pyproject.toml`
- `beacon-db/beacon-db-py/LICENSE`, a copy of the root file

maturin reads the last one and ships it in the wheel, under
`beacondb-<version>.dist-info/licenses/`. A user therefore receives the text with
the package, not only the name of the licence.

This conflicted with the engine while the engine was Apache-2.0. The engine is
AGPL-3.0 now, so the graph and the wheel state the same licence.
